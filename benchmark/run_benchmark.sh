#!/usr/bin/env bash
# Builds two Docker images - a stock ("vanilla") Flink distribution and the Map-Filter
# fusion prototype ("fused") - and runs each registered benchmark job (see JOBS below) in
# both, then prints a runtime comparison per job and writes a Markdown report plus
# per-iteration logs.
#
# This only measures wall-clock runtime - it does not check any job's output. Every job
# registered here is trusted to already be correct; that's proven once per job's
# transform by org.apache.flink.streaming.runtime.tasks.MapFilterFusionDemoITCase
# (flink-runtime's test sources), not by this script. Keeping the two concerns apart is
# the point: this runner can stay simple (run a job, measure it, write the result down)
# because it isn't also trying to verify the job it's running.
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
# [count]      number of records each benchmark job processes per run (default 200000000)
# [iterations] how many times to submit each job per variant (default 5)
#
# Output: results/<timestamp>/ (gitignored, regenerated each run), containing:
#   REPORT.md                        - job descriptions, per-run metrics table, summary,
#                                       log file index
#   metrics.csv                      - the same per-run metrics as REPORT.md's table, as CSV
#   <variant>-<job>-iter<N>.log       - per-iteration jobmanager/taskexecutor log slice +
#                                       CLI output
#   raw-logs-<variant>-<job>/        - that (variant, job)'s raw, unsliced Flink logs for
#                                       its whole run (all iterations)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Benchmark jobs to run, as "<short label>:<fully-qualified class name>". Each must have a
# public static void main(String[] args) taking an optional record-count arg, same as
# these two - see benchmark/job/org/apache/flink/demo/*.java.
JOBS=(
    "simple:org.apache.flink.demo.SimpleMapFilterBenchmarkJob"
    "mixed:org.apache.flink.demo.MixedOperatorsBenchmarkJob"
)

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

echo "=== compiling benchmark jobs ==="
BUILD_CLASSES="$SCRIPT_DIR/build-classes"
rm -rf "$BUILD_CLASSES"
mkdir -p "$BUILD_CLASSES"
CP=$(find "$FUSED_DIST/lib" -name "*.jar" | tr '\n' ':')
javac --release 17 -cp "$CP" -d "$BUILD_CLASSES" "$SCRIPT_DIR"/job/org/apache/flink/demo/*.java
# Glob every class file the compile produced (each job's main class plus any
# nested/inner classes, e.g. MixedOperatorsBenchmarkJob$DuplicateShifted.class) - not
# just files named after a source file, or classes the compiler emits alongside them
# silently go missing from the jar and the job fails at runtime with NoClassDefFoundError.
(cd "$BUILD_CLASSES" && jar --create --file "$SCRIPT_DIR/benchmark.jar" \
    org/apache/flink/demo/*.class)

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
    local variant="$1" job_label="$2" job_class="$3"
    echo "=== running flink-benchmark:$variant job=$job_label (count=$COUNT, iterations=$ITERATIONS) ==="
    docker run --rm \
        -e VARIANT="$variant" -e JOB_CLASS="$job_class" -e JOB_LABEL="$job_label" \
        -e COUNT="$COUNT" -e ITERATIONS="$ITERATIONS" \
        -e OUTPUT_DIR=/opt/flink/results \
        -v "$RESULTS_DIR:/opt/flink/results" \
        "flink-benchmark:$variant"
}

RUN_LOG="$RESULTS_DIR/run.log"
: > "$RUN_LOG"
for entry in "${JOBS[@]}"; do
    job_label="${entry%%:*}"
    job_class="${entry#*:}"
    run_variant vanilla "$job_label" "$job_class" | tee -a "$RUN_LOG"
    run_variant fused "$job_label" "$job_class" | tee -a "$RUN_LOG"
done

echo
echo "=== summary ==="
SUMMARY=$(awk '
/^RESULT / {
    variant=""; job=""; runtime=""; fusion=""
    for (i = 1; i <= NF; i++) {
        split($i, kv, "=")
        if (kv[1] == "variant") variant = kv[2]
        if (kv[1] == "job") job = kv[2]
        if (kv[1] == "runtime_ms") runtime = kv[2]
        if (kv[1] == "fusion_events") fusion = kv[2]
    }
    if (job == "" || variant == "") next
    key = job SUBSEP variant
    if (runtime != "") {
        sum[key] += runtime
        n[key] += 1
    }
    if (fusion != "") {
        fusion_events[key] = fusion
    }
    seen_job[job] = 1
}
END {
    for (j in seen_job) {
        vkey = j SUBSEP "vanilla"
        fkey = j SUBSEP "fused"
        printf "job=%s\n", j
        if (vkey in n) {
            avg = sum[vkey] / n[vkey]
            printf "  %-10s avg_runtime_ms=%-10.1f runs=%-3d fusion_events=%s\n", "vanilla", avg, n[vkey], (vkey in fusion_events ? fusion_events[vkey] : "n/a")
        }
        if (fkey in n) {
            avg = sum[fkey] / n[fkey]
            printf "  %-10s avg_runtime_ms=%-10.1f runs=%-3d fusion_events=%s\n", "fused", avg, n[fkey], (fkey in fusion_events ? fusion_events[fkey] : "n/a")
        }
        if ((vkey in n) && (fkey in n)) {
            va = sum[vkey] / n[vkey]
            fa = sum[fkey] / n[fkey]
            if (fa > 0) {
                printf "  -> fused is %.1f%% faster than vanilla\n", (va - fa) / va * 100
            }
        }
        print ""
    }
}
' "$RUN_LOG")
echo "$SUMMARY"

extract_javadoc() {
    # Prints a source file's first /** ... */ block as Markdown (stripped of comment
    # syntax, {@code x}/{@link x} turned into `x`), with no leading/trailing blank line.
    local src="$1"
    sed -n '/^\/\*\*/,/^ \*\//p' "$src" \
        | sed -e 's#^ \*/$##' -e 's#^/\*\*$##' -e 's#^ \* \?##' \
        | sed -e 's#{@code \([^}]*\)}#`\1`#g' -e 's#{@link \([^}]*\)}#`\1`#g' \
        | sed -e '/./,$!d' \
        | tac | sed -e '/./,$!d' | tac
}

generate_report() {
    local report="$RESULTS_DIR/REPORT.md"
    local metrics_csv="$RESULTS_DIR/metrics.csv"

    {
        echo "# Map-Filter Fusion Benchmark Report"
        echo
        echo "- Run ID: \`$RUN_ID\`"
        echo "- Record count per run: $COUNT"
        echo "- Iterations per variant: $ITERATIONS"
        echo "- Vanilla distribution: \`$VANILLA_DIST\`"
        echo "- Fused distribution: \`$FUSED_DIST\`"
        echo
        echo "This only measures wall-clock runtime. It does not check whether the jobs'"
        echo "output is correct - every job below is trusted to already be correct, which is"
        echo "proven separately (once per job's transform, not per run) by"
        echo "\`org.apache.flink.streaming.runtime.tasks.MapFilterFusionDemoITCase\` in"
        echo "flink-runtime's test sources. Run that with:"
        echo '```bash'
        echo "./mvnw test -pl flink-runtime -Dtest=MapFilterFusionDemoITCase"
        echo '```'
        echo
        echo "## Jobs"
        echo
        for entry in "${JOBS[@]}"; do
            local job_label="${entry%%:*}" job_class="${entry#*:}"
            local simple_name="${job_class##*.}"
            local job_src="$SCRIPT_DIR/job/org/apache/flink/demo/${simple_name}.java"
            echo "### \`$job_label\` - \`$job_class\`"
            echo
            echo "(\`benchmark/job/org/apache/flink/demo/${simple_name}.java\`)"
            echo
            extract_javadoc "$job_src"
            echo
        done
        echo "All jobs above run with parallelism 1 and object reuse enabled - the condition"
        echo "the fusion requires to engage (see \`isMapFilterFusable\` in \`OperatorChain\`)."
        echo
        echo "## Per-run metrics"
        echo
        echo "| Job | Variant | Iter | Count | Runtime (ms) | Fusion events | Log file |"
        echo "|---|---|---|---|---|---|---|"
        tail -n +2 "$metrics_csv" | awk -F, '{printf "| %s | %s | %s | %s | %s | %s | `%s` |\n", $2, $1, $3, $4, ($5==""?"error":$5), $6, $7}'
        echo
        echo "## Summary"
        echo
        echo '```'
        echo "$SUMMARY"
        echo '```'
        echo
        echo "## Logs"
        echo
        echo "Per-iteration logs (CLI output plus the jobmanager/taskexecutor log lines"
        echo "written during that iteration, including any \`[CustomOptimizer]\` fusion"
        echo "messages) are saved alongside this report, one file per row in the metrics"
        echo "table above:"
        echo
        (cd "$RESULTS_DIR" && ls -1 ./*-iter*.log 2>/dev/null | sed 's/^\.\//- `/; s/$/`/')
        echo
        echo "Each (variant, job) combination's full raw cluster logs for its whole run (all"
        echo "iterations, same files Flink itself writes to \`log/\`) are copied into their"
        echo "own subdirectory here:"
        echo
        (cd "$RESULTS_DIR" && ls -d raw-logs-*/ 2>/dev/null | sed 's#^#- `#; s#$#`#')
    } > "$report"
    echo "=== report written to: $report ==="
}

generate_report
