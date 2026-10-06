#!/usr/bin/env python3
"""Runs one Map-Filter fusion benchmark job against a vanilla and a fused Flink
distribution, measures wall-clock runtime, and writes a report to
results/<run-id>-<job>/.

This only measures speed - it does not check correctness. Every job here is trusted to
already be correct, proven separately by
org.apache.flink.streaming.runtime.tasks.MapFilterFusionDemoITCase (flink-runtime's test
sources):
    ./mvnw test -pl flink-runtime -Dtest=MapFilterFusionDemoITCase

Usage:
    ./benchmark.py <job> <vanilla-dist-dir> <fused-dist-dir> [--count N] [--iterations N]
    ./benchmark.py --list-jobs
"""

import argparse
import csv
import datetime
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

# Registered benchmark jobs: short CLI name -> fully-qualified Java class. Each is a
# plain class under job/org/apache/flink/demo/ with a main(String[] args) that takes an
# optional record-count argument (parallelism 1, object reuse enabled - the condition
# the fusion requires to engage). Add a new job by adding a class next to the existing
# ones and registering it here.
JOBS = {
    "simple": "org.apache.flink.demo.SimpleMapFilterBenchmarkJob",
    "mixed": "org.apache.flink.demo.MixedOperatorsBenchmarkJob",
}


def job_source(job_class: str) -> Path:
    package, simple_name = job_class.rsplit(".", 1)
    return SCRIPT_DIR / "job" / package.replace(".", "/") / f"{simple_name}.java"


@dataclass(frozen=True)
class Run:
    variant: str
    iteration: int
    count: int
    runtime_ms: int | None
    fusion_events: int
    log_file: str


@dataclass(frozen=True)
class VariantSummary:
    variant: str
    successful_runs: int
    avg_runtime_ms: float | None
    total_fusion_events: int


def compile_job_jar(dist_lib_dir: Path, build_dir: Path) -> Path:
    classes_dir = build_dir / "classes"
    classes_dir.mkdir(parents=True, exist_ok=True)

    classpath = ":".join(str(p) for p in sorted(dist_lib_dir.glob("*.jar")))
    sources = sorted((SCRIPT_DIR / "job/org/apache/flink/demo").glob("*.java"))
    subprocess.run(
        [
            "javac",
            "--release",
            "17",
            "-cp",
            classpath,
            "-d",
            str(classes_dir),
            *(str(s) for s in sources),
        ],
        check=True,
    )

    # Glob every class file the compile produced (each job's main class plus any
    # nested/inner classes, e.g. MixedOperatorsBenchmarkJob$DuplicateShifted.class) -
    # not just files named after a source file, or classes the compiler emits alongside
    # them silently go missing from the jar and the job fails at runtime with
    # NoClassDefFoundError.
    class_files = sorted(classes_dir.glob("org/apache/flink/demo/*.class"))
    jar_path = build_dir / "benchmark.jar"
    subprocess.run(
        [
            "jar",
            "--create",
            "--file",
            str(jar_path),
            *(str(p.relative_to(classes_dir)) for p in class_files),
        ],
        cwd=classes_dir,
        check=True,
    )
    return jar_path


def build_image(dist_dir: Path, jar_path: Path, stage_dir: Path, tag: str) -> None:
    shutil.rmtree(stage_dir, ignore_errors=True)
    stage_dir.mkdir(parents=True)
    shutil.copytree(dist_dir, stage_dir / "flink")
    shutil.copy(jar_path, stage_dir / "benchmark.jar")
    shutil.copy(SCRIPT_DIR / "entrypoint.sh", stage_dir / "entrypoint.sh")
    shutil.copy(SCRIPT_DIR / "Dockerfile", stage_dir / "Dockerfile")
    print(f"=== staging and building image: {tag} ===")
    subprocess.run(["docker", "build", "-t", tag, str(stage_dir)], check=True)


def run_container(tag: str, env: dict[str, str], results_dir: Path) -> None:
    """Runs tag with results_dir bind-mounted at /opt/flink/results, streaming its
    output live to stdout. Raises RuntimeError if the container exits non-zero."""
    cmd = ["docker", "run", "--rm"]
    for key, value in env.items():
        cmd += ["-e", f"{key}={value}"]
    cmd += ["-v", f"{results_dir}:/opt/flink/results", tag]

    with subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    ) as proc:
        assert proc.stdout is not None
        for line in proc.stdout:
            print(line, end="")
    if proc.returncode != 0:
        raise RuntimeError(f"docker run failed for {tag} (exit {proc.returncode})")


def read_runs(metrics_csv: Path) -> list[Run]:
    runs = []
    with metrics_csv.open(newline="") as f:
        for row in csv.DictReader(f):
            runs.append(
                Run(
                    variant=row["variant"],
                    iteration=int(row["iter"]),
                    count=int(row["count"]),
                    runtime_ms=int(row["runtime_ms"]) if row["runtime_ms"] else None,
                    fusion_events=int(row["fusion_events"] or 0),
                    log_file=row["log_file"],
                )
            )
    return runs


def summarize(runs: list[Run]) -> dict[str, VariantSummary]:
    summaries = {}
    for variant in sorted({r.variant for r in runs}):
        variant_runs = [r for r in runs if r.variant == variant]
        runtimes = [r.runtime_ms for r in variant_runs if r.runtime_ms is not None]
        avg = sum(runtimes) / len(runtimes) if runtimes else None
        summaries[variant] = VariantSummary(
            variant=variant,
            successful_runs=len(runtimes),
            avg_runtime_ms=avg,
            total_fusion_events=sum(r.fusion_events for r in variant_runs),
        )
    return summaries


def format_summary(summaries: dict[str, VariantSummary]) -> str:
    lines = []
    for variant in ("vanilla", "fused"):
        s = summaries.get(variant)
        if s is None:
            continue
        avg = f"{s.avg_runtime_ms:.1f}" if s.avg_runtime_ms is not None else "n/a"
        lines.append(
            f"{variant:<10} avg_runtime_ms={avg:<10} "
            f"runs={s.successful_runs:<3} fusion_events={s.total_fusion_events}"
        )

    vanilla, fused = summaries.get("vanilla"), summaries.get("fused")
    if vanilla and fused and vanilla.avg_runtime_ms and fused.avg_runtime_ms:
        pct = (
            (vanilla.avg_runtime_ms - fused.avg_runtime_ms)
            / vanilla.avg_runtime_ms
            * 100
        )
        direction = "faster" if pct >= 0 else "slower"
        lines.append("")
        lines.append(f"fused is {abs(pct):.1f}% {direction} than vanilla on average")
    return "\n".join(lines)


def extract_javadoc(source_file: Path) -> str:
    """Pulls a Java source file's first /** ... */ block out as Markdown: comment
    syntax stripped, {@code x}/{@link x} turned into `x`."""
    match = re.search(r"/\*\*(.*?)\*/", source_file.read_text(), re.DOTALL)
    if not match:
        return ""

    lines = []
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip().removeprefix("*").removeprefix(" ")
        lines.append(line)

    text = "\n".join(lines).strip("\n")
    text = re.sub(r"\{@code ([^}]*)\}", r"`\1`", text)
    text = re.sub(r"\{@link ([^}]*)\}", r"`\1`", text)
    return text


def generate_report(
    report_path: Path,
    run_id: str,
    job_name: str,
    job_class: str,
    vanilla_dist: Path,
    fused_dist: Path,
    count: int,
    iterations: int,
    runs: list[Run],
    summaries: dict[str, VariantSummary],
    results_dir: Path,
) -> None:
    lines: list[str] = []

    def p(line: str = "") -> None:
        lines.append(line)

    p("# Map-Filter Fusion Benchmark Report")
    p()
    p(f"- Run ID: `{run_id}`")
    p(f"- Job: `{job_name}` - `{job_class}`")
    p(f"- Record count per run: {count}")
    p(f"- Iterations per variant: {iterations}")
    p(f"- Vanilla distribution: `{vanilla_dist}`")
    p(f"- Fused distribution: `{fused_dist}`")
    p()
    p(
        "This only measures wall-clock runtime. It does not check whether this job's "
        "output is correct - it is trusted to already be correct, which is proven "
        "separately (once per job's transform, not per benchmark run) by "
        "`org.apache.flink.streaming.runtime.tasks.MapFilterFusionDemoITCase` in "
        "flink-runtime's test sources. Run that with:"
    )
    p("```bash")
    p("./mvnw test -pl flink-runtime -Dtest=MapFilterFusionDemoITCase")
    p("```")
    p()
    p("## Job description")
    p()
    source = job_source(job_class)
    p(f"(`benchmark/job/{source.relative_to(SCRIPT_DIR / 'job')}`)")
    p()
    p(extract_javadoc(source))
    p()
    p(
        "Runs with parallelism 1 and object reuse enabled - the condition the fusion "
        "requires to engage (see `isMapFilterFusable` in `OperatorChain`)."
    )
    p()
    p("## Per-run metrics")
    p()
    p("| Variant | Iter | Count | Runtime (ms) | Fusion events | Log file |")
    p("|---|---|---|---|---|---|")
    for run in runs:
        runtime = run.runtime_ms if run.runtime_ms is not None else "error"
        p(
            f"| {run.variant} | {run.iteration} | {run.count} | {runtime} | "
            f"{run.fusion_events} | `{run.log_file}` |"
        )
    p()
    p("## Summary")
    p()
    p("```")
    p(format_summary(summaries))
    p("```")
    p()
    p("## Logs")
    p()
    p(
        "Per-iteration logs (CLI output plus the jobmanager/taskexecutor log lines "
        "written during that iteration, including any `[CustomOptimizer]` fusion "
        "messages) are saved alongside this report, one file per row in the metrics "
        "table above:"
    )
    p()
    for log_file in sorted({r.log_file for r in runs}):
        p(f"- `{log_file}`")
    p()
    p(
        "The full raw cluster logs for each variant's whole run (all iterations, same "
        "files Flink itself writes to `log/`) are copied into their own subdirectory "
        "here:"
    )
    p()
    for d in sorted(results_dir.glob("raw-logs-*")):
        if d.is_dir():
            p(f"- `{d.name}/`")

    report_path.write_text("\n".join(lines) + "\n")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "job",
        nargs="?",
        choices=sorted(JOBS),
        help="Which registered benchmark job to run.",
    )
    parser.add_argument(
        "vanilla_dist",
        nargs="?",
        type=Path,
        help="Path to the vanilla Flink distribution.",
    )
    parser.add_argument(
        "fused_dist", nargs="?", type=Path, help="Path to the fused Flink distribution."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=200_000_000,
        help="Records processed per run (default: 200000000).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Job submissions per variant (default: 5).",
    )
    parser.add_argument(
        "--list-jobs",
        action="store_true",
        help="List registered benchmark jobs and exit.",
    )
    args = parser.parse_args(argv)

    if args.list_jobs:
        return args

    missing = [
        name
        for name, value in (
            ("job", args.job),
            ("vanilla_dist", args.vanilla_dist),
            ("fused_dist", args.fused_dist),
        )
        if value is None
    ]
    if missing:
        parser.error(f"missing required argument(s): {', '.join(missing)}")
    return args


def check_dist(dist: Path) -> None:
    if not (dist / "bin" / "flink").is_file():
        sys.exit(
            f"error: '{dist}' does not look like a Flink distribution (missing bin/flink)"
        )


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    if args.list_jobs:
        for name, job_class in sorted(JOBS.items()):
            print(f"{name}: {job_class}")
        return 0

    job_class = JOBS[args.job]
    vanilla_dist: Path = args.vanilla_dist.resolve()
    fused_dist: Path = args.fused_dist.resolve()
    check_dist(vanilla_dist)
    check_dist(fused_dist)

    run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results_dir = SCRIPT_DIR / "results" / f"{run_id}-{args.job}"
    results_dir.mkdir(parents=True)
    print(f"=== results will be written to: {results_dir} ===")

    print("=== compiling benchmark jobs ===")
    build_dir = SCRIPT_DIR / "build"
    jar_path = compile_job_jar(fused_dist / "lib", build_dir)

    for variant, dist_dir in (("vanilla", vanilla_dist), ("fused", fused_dist)):
        build_image(
            dist_dir, jar_path, build_dir / variant, f"flink-benchmark:{variant}"
        )

    for variant in ("vanilla", "fused"):
        print(
            f"=== running flink-benchmark:{variant} job={args.job} "
            f"(count={args.count}, iterations={args.iterations}) ==="
        )
        run_container(
            f"flink-benchmark:{variant}",
            {
                "VARIANT": variant,
                "JOB_CLASS": job_class,
                "JOB_LABEL": args.job,
                "COUNT": str(args.count),
                "ITERATIONS": str(args.iterations),
                "OUTPUT_DIR": "/opt/flink/results",
            },
            results_dir,
        )

    runs = read_runs(results_dir / "metrics.csv")
    summaries = summarize(runs)

    print()
    print("=== summary ===")
    print(format_summary(summaries))

    report_path = results_dir / "REPORT.md"
    generate_report(
        report_path,
        run_id,
        args.job,
        job_class,
        vanilla_dist,
        fused_dist,
        args.count,
        args.iterations,
        runs,
        summaries,
        results_dir,
    )
    print(f"=== report written to: {report_path} ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
