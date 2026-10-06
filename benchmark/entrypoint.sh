#!/usr/bin/env bash
# Runs inside the benchmark container: starts a one-node standalone Flink cluster,
# submits the benchmark job ITERATIONS times, and prints one machine-readable
# "RESULT ..." line per run plus a summary line, then tears the cluster down.
#
# Per iteration, this also slices out the new lines written to the jobmanager and
# taskexecutor logs since the previous iteration (so each iteration gets its own
# self-contained log file instead of one log shared across the whole variant run),
# and appends a row to a metrics.csv - both under OUTPUT_DIR, which the caller
# should bind-mount so the files land on the host.
#
# Env vars:
#   VARIANT     - label for this run, e.g. "vanilla" or "fused" (default: unknown)
#   COUNT       - number of records the benchmark job processes (default: 200000000)
#   ITERATIONS  - how many times to submit the job (default: 5)
#   OUTPUT_DIR  - directory (inside the container) to write per-iteration logs and
#                 metrics.csv to (default: /opt/flink/results)
set -uo pipefail

VARIANT="${VARIANT:-unknown}"
COUNT="${COUNT:-200000000}"
ITERATIONS="${ITERATIONS:-5}"
OUTPUT_DIR="${OUTPUT_DIR:-/opt/flink/results}"

mkdir -p "$OUTPUT_DIR"
METRICS_FILE="$OUTPUT_DIR/metrics.csv"
if [ ! -f "$METRICS_FILE" ]; then
    echo "variant,iter,count,runtime_ms,fusion_events,log_file" > "$METRICS_FILE"
fi

echo "=== starting cluster (variant=${VARIANT}) ==="
"${FLINK_HOME}/bin/start-cluster.sh"

echo "=== waiting for cluster to come up ==="
ready=0
for _ in $(seq 1 30); do
    if "${FLINK_HOME}/bin/flink" list >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
if [ "$ready" -ne 1 ]; then
    echo "RESULT variant=${VARIANT} error=cluster_not_ready"
    cat "${FLINK_HOME}"/log/*.log 2>/dev/null
    exit 1
fi

tm_log=$(ls "${FLINK_HOME}"/log/*taskexecutor*.log 2>/dev/null | head -n1)
jm_log=$(ls "${FLINK_HOME}"/log/*standalonesession*.log 2>/dev/null | head -n1)
prev_tm_lines=0
prev_jm_lines=0

log_delta() {
    # log_delta <file> <from-line-exclusive> <to-line-inclusive>
    local file="$1" from="$2" to="$3"
    if [ -n "$file" ] && [ -f "$file" ] && [ "$to" -gt "$from" ]; then
        tail -n "+$((from + 1))" "$file" | head -n "$((to - from))"
    fi
}

for i in $(seq 1 "$ITERATIONS"); do
    output=$("${FLINK_HOME}/bin/flink" run -c org.apache.flink.demo.MapFilterFusionBenchmarkJob \
        "${FLINK_HOME}/benchmark.jar" "$COUNT" 2>&1)
    runtime_ms=$(echo "$output" | grep -oE "Job Runtime: [0-9]+ ms" | grep -oE "[0-9]+")

    cur_tm_lines=0
    [ -n "$tm_log" ] && [ -f "$tm_log" ] && cur_tm_lines=$(wc -l < "$tm_log")
    cur_jm_lines=0
    [ -n "$jm_log" ] && [ -f "$jm_log" ] && cur_jm_lines=$(wc -l < "$jm_log")

    log_name="${VARIANT}-iter${i}.log"
    iter_log="$OUTPUT_DIR/${log_name}"
    {
        echo "=== flink run CLI output (variant=${VARIANT} iter=${i} count=${COUNT}) ==="
        echo "$output"
        echo
        echo "=== jobmanager (standalonesession) log, new lines this iteration ==="
        log_delta "$jm_log" "$prev_jm_lines" "$cur_jm_lines"
        echo
        echo "=== taskexecutor log, new lines this iteration ==="
        log_delta "$tm_log" "$prev_tm_lines" "$cur_tm_lines"
    } > "$iter_log"

    # Count actual fusion decisions only - the "[CustomOptimizer] Fusing chained ..." line -
    # not every log line tagged [CustomOptimizer] (MapFilterFusedOperator's open/close logs
    # carry the same tag but aren't themselves fusion events).
    iter_fusion_events=$(grep -c "\[CustomOptimizer\] Fusing chained" "$iter_log" 2>/dev/null || true)
    iter_fusion_events="${iter_fusion_events:-0}"

    if [ -z "$runtime_ms" ]; then
        echo "RESULT variant=${VARIANT} iter=${i} error=no_runtime_parsed log=${log_name}"
        echo "$output"
        echo "${VARIANT},${i},${COUNT},,${iter_fusion_events},${log_name}" >> "$METRICS_FILE"
    else
        echo "RESULT variant=${VARIANT} iter=${i} count=${COUNT} runtime_ms=${runtime_ms} fusion_events=${iter_fusion_events} log=${log_name}"
        echo "${VARIANT},${i},${COUNT},${runtime_ms},${iter_fusion_events},${log_name}" >> "$METRICS_FILE"
    fi

    prev_tm_lines=$cur_tm_lines
    prev_jm_lines=$cur_jm_lines
done

fusion_events=$(cat "${FLINK_HOME}"/log/*taskexecutor*.log 2>/dev/null | grep -c "\[CustomOptimizer\] Fusing chained")
echo "RESULT variant=${VARIANT} fusion_events=${fusion_events}"

cp "${FLINK_HOME}"/log/*.log "$OUTPUT_DIR/" 2>/dev/null || true

echo "=== stopping cluster ==="
"${FLINK_HOME}/bin/stop-cluster.sh"
