#!/usr/bin/env bash
# Builds two Docker images - a stock ("vanilla") Flink distribution and the Map-Filter
# fusion prototype ("fused") - and runs the same benchmark job in each, then prints a
# runtime comparison and writes a Markdown report plus per-iteration logs.
#
# Usage:
#   benchmark/run_benchmark.sh <vanilla-dist-dir> <fused-dist-dir> [count] [iterations]
#
# <vanilla-dist-dir> / <fused-dist-dir> are paths to assembled Flink distributions, i.e.
# the directory that contains bin/, lib/, conf/ - e.g. what
#   ./mvnw clean install -DskipTests -Dfast -Pskip-webui-build -T1C -pl flink-dist -am
# produces at flink-dist/target/flink-<version>-bin/flink-<version>.
#
# To get a "vanilla" dist to compare against, build the same distribution from the
# commit right before the fusion prototype was added, in a separate git worktree so it
# doesn't disturb your working checkout, e.g.:
#
#   git worktree add /tmp/flink-vanilla <commit-before-fusion>
#   cd /tmp/flink-vanilla
#   ./mvnw clean install -DskipTests -Dfast -Pskip-webui-build -T1C -pl flink-dist -am
#
# [count]      number of records the benchmark job processes per run (default 200000000)
# [iterations] how many times to submit the job per variant (default 5)
#
# Output: results/<timestamp>/ (gitignored, regenerated each run), containing:
#   REPORT.md            - job description, per-run metrics table, summary, log file index
#   metrics.csv           - the same per-run metrics as REPORT.md's table, as CSV
#   <variant>-iter<N>.log - per-iteration jobmanager/taskexecutor log slice + CLI output
#   <variant>'s raw jobmanager/taskexecutor logs for the whole run (all iterations)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

VANILLA_DIST="${1:?usage: run_benchmark.sh <vanilla-dist-dir> <fused-dist-dir> [count] [iterations]}"
FUSED_DIST="${2:?usage: run_benchmark.sh <vanilla-dist-dir> <fused-dist-dir> [count] [iterations]}"
COUNT="${3:-200000000}"
ITERATIONS="${4:-5}"

for d in "$VANILLA_DIST" "$FUSED_DIST"; do
    if [ ! -x "$d/bin/flink" ]; then
        echo "error: '$d' does not look like a Flink distribution (missing bin/flink)" >&2
        exit 1
    fi
done

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_DIR="$SCRIPT_DIR/results/$RUN_ID"
mkdir -p "$RESULTS_DIR"
echo "=== results will be written to: $RESULTS_DIR ==="

echo "=== compiling benchmark job ==="
BUILD_CLASSES="$SCRIPT_DIR/build-classes"
rm -rf "$BUILD_CLASSES"
mkdir -p "$BUILD_CLASSES"
CP=$(find "$FUSED_DIST/lib" -name "*.jar" | tr '\n' ':')
javac --release 17 -cp "$CP" -d "$BUILD_CLASSES" "$SCRIPT_DIR/job/org/apache/flink/demo/MapFilterFusionBenchmarkJob.java"
(cd "$BUILD_CLASSES" && jar --create --file "$SCRIPT_DIR/benchmark.jar" \
    --main-class org.apache.flink.demo.MapFilterFusionBenchmarkJob \
    org/apache/flink/demo/MapFilterFusionBenchmarkJob.class)

build_variant() {
    local variant="$1" dist_dir="$2"
    local stage="$SCRIPT_DIR/build/$variant"
    echo "=== staging and building image: flink-benchmark:$variant ==="
    rm -rf "$stage"
    mkdir -p "$stage"
    cp -r "$dist_dir" "$stage/flink"
    cp "$SCRIPT_DIR/benchmark.jar" "$stage/benchmark.jar"
    cp "$SCRIPT_DIR/entrypoint.sh" "$stage/entrypoint.sh"
    cp "$SCRIPT_DIR/Dockerfile" "$stage/Dockerfile"
    docker build -t "flink-benchmark:$variant" "$stage"
}

build_variant vanilla "$VANILLA_DIST"
build_variant fused "$FUSED_DIST"

run_variant() {
    local variant="$1" dist_dir="$2"
    echo "=== running flink-benchmark:$variant (count=$COUNT, iterations=$ITERATIONS) ==="
    docker run --rm \
        -e VARIANT="$variant" -e COUNT="$COUNT" -e ITERATIONS="$ITERATIONS" \
        -e OUTPUT_DIR=/opt/flink/results \
        -v "$RESULTS_DIR:/opt/flink/results" \
        "flink-benchmark:$variant"
}

RUN_LOG="$RESULTS_DIR/run.log"
run_variant vanilla "$VANILLA_DIST" | tee -a "$RUN_LOG"
run_variant fused "$FUSED_DIST" | tee -a "$RUN_LOG"

echo
echo "=== summary ==="
SUMMARY=$(awk '
/^RESULT / {
    variant=""; runtime=""; fusion=""
    for (i = 1; i <= NF; i++) {
        split($i, kv, "=")
        if (kv[1] == "variant") variant = kv[2]
        if (kv[1] == "runtime_ms") runtime = kv[2]
        if (kv[1] == "fusion_events") fusion = kv[2]
    }
    if (runtime != "") {
        sum[variant] += runtime
        n[variant] += 1
    }
    if (fusion != "") {
        fusion_events[variant] = fusion
    }
}
END {
    for (v in sum) {
        avg = sum[v] / n[v]
        printf "%-10s avg_runtime_ms=%-10.1f runs=%-3d fusion_events=%s\n", v, avg, n[v], (v in fusion_events ? fusion_events[v] : "n/a")
    }
    if (("vanilla" in sum) && ("fused" in sum)) {
        va = sum["vanilla"] / n["vanilla"]
        fa = sum["fused"] / n["fused"]
        if (fa > 0) {
            printf "\nfused is %.1f%% faster than vanilla on average\n", (va - fa) / va * 100
        }
    }
}
' "$RUN_LOG")
echo "$SUMMARY"

generate_report() {
    local report="$RESULTS_DIR/REPORT.md"
    local metrics_csv="$RESULTS_DIR/metrics.csv"
    local job_src="$SCRIPT_DIR/job/org/apache/flink/demo/MapFilterFusionBenchmarkJob.java"

    {
        echo "# Map-Filter Fusion Benchmark Report"
        echo
        echo "- Run ID: \`$RUN_ID\`"
        echo "- Record count per run: $COUNT"
        echo "- Iterations per variant: $ITERATIONS"
        echo "- Vanilla distribution: \`$VANILLA_DIST\`"
        echo "- Fused distribution: \`$FUSED_DIST\`"
        echo
        echo "## Job description"
        echo
        echo "Job class: \`org.apache.flink.demo.MapFilterFusionBenchmarkJob\`"
        echo "(\`benchmark/job/org/apache/flink/demo/MapFilterFusionBenchmarkJob.java\`)"
        echo
        sed -n '/^\/\*\*/,/^ \*\//p' "$job_src" \
            | sed -e 's#^ \*/$##' -e 's#^/\*\*$##' -e 's#^ \* \?##' \
            | sed -e 's#{@code \([^}]*\)}#`\1`#g' \
            | sed -e '/./,$!d' \
            | tac | sed -e '/./,$!d' | tac
        echo
        echo "Pipeline: \`env.fromSequence(0, count - 1).map(x -> x * 2).filter(x % 3 == 0).sinkTo(new DiscardingSink<>())\`"
        echo
        echo "- Parallelism: 1"
        echo "- Object reuse: enabled"
        echo
        echo "## Per-run metrics"
        echo
        echo "| Variant | Iter | Count | Runtime (ms) | Fusion events | Log file |"
        echo "|---|---|---|---|---|---|"
        tail -n +2 "$metrics_csv" | awk -F, '{printf "| %s | %s | %s | %s | %s | `%s` |\n", $1, $2, $3, ($4==""?"error":$4), $5, $6}'
        echo
        echo "## Summary"
        echo
        echo '```'
        echo "$SUMMARY"
        echo '```'
        echo
        echo "## Logs"
        echo
        echo "Per-iteration logs (CLI output plus the jobmanager/taskexecutor log lines written"
        echo "during that iteration, including any \`[CustomOptimizer]\` fusion messages) are saved"
        echo "alongside this report, one file per row in the metrics table above:"
        echo
        (cd "$RESULTS_DIR" && ls -1 ./*-iter*.log 2>/dev/null | sed 's/^\.\//- `/; s/$/`/')
        echo
        echo "Full raw cluster logs for each variant's whole run (all iterations, same files Flink"
        echo "itself writes to \`log/\`) are also copied here:"
        echo
        (cd "$RESULTS_DIR" && ls -1 ./*.log 2>/dev/null | grep -v -- '-iter' | grep -v '^\./run\.log$' | sed 's/^\.\//- `/; s/$/`/')
    } > "$report"
    echo "=== report written to: $report ==="
}

generate_report
