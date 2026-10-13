#!/usr/bin/env bash
# Inside-Flink sweep: the sensor job at several work levels (k) on stock Flink, the fusion
# prototype, and the native operator at several batch sizes, in Flink's local (MiniCluster) mode.
#
#   ./benchmark/sensor-bench/minicluster-sweep.sh
#
# Env: COUNT (records per run, default 50000000), RUNS (default 3), KS (default "1 4 16 64"),
#      BATCHES (default "1 16 64 256 1024 4096"), PIN_CPUS (default 0-7, the P-cores).
# Needs benchmark/build/{vanilla,fused}/flink (the two distributions benchmark.py stages).
#
# Runs are interleaved - run 1 of every cell, then run 2 of every cell, ... - so slow drift on the
# machine spreads across all cells instead of biasing whichever ran last. Each run reports the
# job's net runtime (NET_RUNTIME_MS, excludes JVM and MiniCluster startup). Output:
# results/minicluster-sweep.csv; summarize with report.py.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH="$(cd "$HERE/.." && pwd)"
NK="$BENCH/native-kernel/build"
CLASSES="$HERE/build/job-classes"
OUT="$HERE/results/minicluster-sweep.csv"

COUNT="${COUNT:-50000000}"
RUNS="${RUNS:-3}"
KS="${KS:-1 4 16 64}"
BATCHES="${BATCHES:-1 16 64 256 1024 4096}"
PIN_CPUS="${PIN_CPUS-0-7}"

for v in vanilla fused; do
    [ -d "$BENCH/build/$v/flink/lib" ] || { echo "missing $BENCH/build/$v/flink" >&2; exit 1; }
done

echo "=== building native bridge + kernels ==="
"$BENCH/native-kernel/build.sh" >/dev/null
echo "=== compiling jobs ==="
rm -rf "$CLASSES"; mkdir -p "$CLASSES" "$(dirname "$OUT")"
javac --release 17 -proc:none -cp "$NK/flink-accel-bridge.jar:$BENCH/build/fused/flink/lib/*" \
    -d "$CLASSES" "$BENCH"/job/org/apache/flink/demo/*.java

PIN=()
[ -n "$PIN_CPUS" ] && PIN=(taskset -c "$PIN_CPUS")

timed() { # prints the job's NET_RUNTIME_MS, or nothing on failure
    "${PIN[@]}" java "$@" 2>&1 | grep -oE "NET_RUNTIME_MS=[0-9]+" | cut -d= -f2 || true
}

echo "variant,k,batch,run,count,net_ms" > "$OUT"
for run in $(seq 1 "$RUNS"); do
    for k in $KS; do
        for v in vanilla fused; do
            label=$([ "$v" = vanilla ] && echo stock || echo fusion)
            ms=$(timed -cp "$CLASSES:$BENCH/build/$v/flink/lib/*" \
                org.apache.flink.demo.SensorScoringJob "$COUNT" "$k")
            echo "$label,$k,,$run,$COUNT,$ms" | tee -a "$OUT"
        done
        for b in $BATCHES; do
            ms=$(timed -Dflink.accel.bridge.lib="$NK/libflinkaccel_bridge.so" \
                -Dsensor.kernel.lib="$NK/libsensor_kernel.so" \
                -cp "$CLASSES:$NK/flink-accel-bridge.jar:$BENCH/build/vanilla/flink/lib/*" \
                org.apache.flink.demo.SensorScoringNativeJob "$COUNT" "$k" discard "$b")
            echo "native,$k,$b,$run,$COUNT,$ms" | tee -a "$OUT"
        done
    done
done
echo "=== done: $OUT ==="
