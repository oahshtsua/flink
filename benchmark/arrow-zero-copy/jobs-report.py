#!/usr/bin/env python3
"""Turns the whole-job sweep (run.sh jobs-sweep) into Markdown tables.

    ./benchmark/arrow-zero-copy/jobs-report.py [results-dir] > benchmark/arrow-zero-copy/results/jobs/sweep-report.md

Reads sweep.csv from results-dir (default results/jobs/ next to this script).
"""

import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results" / "jobs"

VARIANTS = {
    "stock": "original job, stock Flink",
    "fusion": "original job, fusion build",
    "arrow": "Arrow batches, functions rewritten",
    "arrow-wrapped": "Arrow batches, functions wrapped per row",
}
JOBS = {
    "simple": "simple: map → filter",
    "mixed": "mixed: flatMap → map → filter → map",
    "sensor": "sensor: map → filter → map → filter",
}


def table(header: list[str], rows: list[list[str]]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(out)


def main() -> None:
    runs = defaultdict(list)
    counts = {}
    for row in csv.DictReader((RESULTS / "sweep.csv").open()):
        if row["net_ms"]:
            key = (row["job"], row["variant"], row["k"], row["batch"])
            runs[key].append(int(row["net_ms"]))
            counts[row["job"]] = int(row["count"])
    med = {key: statistics.median(v) for key, v in runs.items()}
    spread = {key: (max(v) - min(v)) / statistics.median(v) for key, v in runs.items() if len(v) > 1}
    n_runs = max(len(v) for v in runs.values())

    def ns(job: str, ms: float) -> str:
        return f"{ms * 1e6 / counts[job]:.1f}"

    print(f"## Whole jobs (local MiniCluster)\n\nMedian of {n_runs} runs. ns per input record"
          " (the mixed job's flatMap doubles the records after it). B = records per batch.\n")
    for job in [j for j in JOBS if j in counts]:
        keys = sorted({(v, k, b) for (j, v, k, b) in med if j == job},
                      key=lambda x: (list(VARIANTS).index(x[0]), int(x[1] or 0), int(x[2] or 0)))
        ks = sorted({k for (_, k, _) in keys}, key=lambda k: int(k or 0))
        print(f"### {JOBS[job]} ({counts[job]:,} records)\n")
        if ks == [""]:
            rows = []
            stock = med[(job, "stock", "", "")]
            for v, k, b in keys:
                m = med[(job, v, k, b)]
                rows.append([VARIANTS[v], b, f"{m:.0f}", ns(job, m), f"{stock / m:.2f}×"])
            print(table(["variant", "B", "ms", "ns/record", "vs stock"], rows))
        else:
            rows = []
            for v, b in sorted({(v, b) for (v, _, b) in keys},
                               key=lambda x: (list(VARIANTS).index(x[0]), int(x[1] or 0))):
                cells = []
                for k in ks:
                    m = med.get((job, v, k, b))
                    stock = med[(job, "stock", k, "")]
                    cells.append("—" if m is None else f"{ns(job, m)} ({stock / m:.2f}×)")
                rows.append([VARIANTS[v], b] + cells)
            print("ns/record (speed-up vs stock):\n")
            print(table(["variant", "B"] + [f"k={k}" for k in ks], rows))
        print()

    if spread:
        typical = statistics.median(spread.values())
        worst = max(spread, key=spread.get)
        print(f"Run-to-run spread (max-min over median): typical {typical:.1%}, worst"
              f" {spread[worst]:.1%} ({worst[0]} {worst[1]}"
              f"{', k=' + worst[2] if worst[2] else ''}{', B=' + worst[3] if worst[3] else ''}).")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        RESULTS = Path(sys.argv[1])
    main()
