#!/usr/bin/env bash
# Every measurement of the project, in order, on one machine: correctness first, then the three
# experiments. Takes about 70 minutes; leave the machine plugged in and idle.
#
#   ./benchmark/run-all.sh <vanilla-dist-dir> <fused-dist-dir>
#
#   1. correctness: MapFilterFusionDemoITCase, and the sensor job's checksum for every variant
#   2. experiment 1, Map/Filter fusion: benchmark.py simple and mixed, stock vs fused, in Docker
#   3. experiment 2, the cost of calling C from Java: jni-crossing/run.sh
#   4. experiment 3, micro-batching into one C function: sensor-bench stage 1 (JMH) and stage 2
#      (local MiniCluster), then report.py
#
# Each step runs even if an earlier one failed; the log says which failed. Output lands where each
# step normally writes it (benchmark/results/, jni-crossing/results/, sensor-bench/results/), plus
# results/run-all.log. Env: ITERATIONS (Docker runs per variant, default 10).
set -uo pipefail

[ $# -eq 2 ] || { echo "usage: $0 <vanilla-dist-dir> <fused-dist-dir>" >&2; exit 2; }
VANILLA="$(cd "$1" && pwd)"
FUSED="$(cd "$2" && pwd)"
BENCH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$BENCH/.." && pwd)"
SB="$BENCH/sensor-bench"
NK="$BENCH/native-kernel/build"
ITERATIONS="${ITERATIONS:-10}"
FAILED=()

# A capped frequency ceiling (left over from running on battery) slows everything ~1.8x without
# any error, so it is recorded before every step.
machine() {
    local cpu=/sys/devices/system/cpu/cpu2/cpufreq
    echo "maxfreq=$(cat $cpu/scaling_max_freq)/$(cat $cpu/cpuinfo_max_freq)" \
        "profile=$(cat /sys/firmware/acpi/platform_profile 2>/dev/null || echo ?)" \
        "ac=$(cat /sys/class/power_supply/{AC,ADP}*/online 2>/dev/null | head -1 || echo ?)" \
        "load=$(cut -d' ' -f1 /proc/loadavg)"
}
step() { # name, command...
    local name="$1"; shift
    echo; echo "##### $name  start $(date -Iseconds)  $(machine)"
    if "$@"; then echo "##### $name  ok $(date -Iseconds)"
    else echo "##### $name  FAILED $(date -Iseconds)"; FAILED+=("$name"); fi
}

# Every variant must print the reference's CHECKSUM line, at light, middle and heavy work.
sensor_checksums() {
    local classes ok=0 k ref out
    classes="$(mktemp -d)"
    "$BENCH/native-kernel/build.sh" >/dev/null || return 1
    javac --release 17 -proc:none -cp "$NK/flink-accel-bridge.jar:$BENCH/build/fused/flink/lib/*" \
        -d "$classes" "$BENCH"/job/org/apache/flink/demo/*.java || return 1
    local native=(-Dflink.accel.bridge.lib="$NK/libflinkaccel_bridge.so"
        -Dsensor.kernel.lib="$NK/libsensor_kernel.so"
        -cp "$classes:$NK/flink-accel-bridge.jar:$BENCH/build/vanilla/flink/lib/*"
        org.apache.flink.demo.SensorScoringNativeJob 2000000)
    for k in 1 16 64; do
        ref=$(java -cp "$classes" org.apache.flink.demo.SensorScoringReference 2000000 $k | grep CHECKSUM)
        echo "k=$k reference          $ref"
        for variant in "reference-tuned|java -cp $classes org.apache.flink.demo.SensorScoringReference 2000000 $k tuned" \
            "stock|java -cp $classes:$BENCH/build/vanilla/flink/lib/* org.apache.flink.demo.SensorScoringJob 2000000 $k checksum" \
            "fusion|java -cp $classes:$BENCH/build/fused/flink/lib/* org.apache.flink.demo.SensorScoringJob 2000000 $k checksum" \
            "tuned|java -cp $classes:$BENCH/build/vanilla/flink/lib/* org.apache.flink.demo.SensorScoringJob 2000000 $k checksum tuned"; do
            out=$(${variant#*|} 2>&1 | grep CHECKSUM)
            printf 'k=%s %-18s %s %s\n' $k "${variant%%|*}" "$out" "$([ "$out" = "$ref" ] && echo MATCH || echo MISMATCH)"
            [ "$out" = "$ref" ] || ok=1
        done
        for b in 1 1024; do
            out=$(java "${native[@]}" $k checksum $b 2>&1 | grep CHECKSUM)
            printf 'k=%s %-18s %s %s\n' $k "native batch=$b" "$out" "$([ "$out" = "$ref" ] && echo MATCH || echo MISMATCH)"
            [ "$out" = "$ref" ] || ok=1
        done
    done
    rm -rf "$classes"
    return $ok
}

stage1() {
    RESULT_NAME=sweep-java "$SB/run.sh" 'SensorBatch.(generate|java|javaTuned)$' -p batch=1024 &&
        RESULT_NAME=sweep-batched "$SB/run.sh" 'SensorBatch.(javaBatched|javaTunedBatched)$' &&
        RESULT_NAME=sweep-native "$SB/run.sh" 'SensorBatch.nativePath$'
}

mkdir -p "$BENCH/results"
exec > >(tee "$BENCH/results/run-all.log") 2>&1
echo "run-all $(date -Iseconds) commit=$(git -C "$REPO" rev-parse --short HEAD)" \
    "kernel=$(uname -r) cpu=\"$(lscpu | sed -n 's/^Model name: *//p')\"" \
    "java=\"$(java -version 2>&1 | head -1)\" gcc=\"$(gcc --version | head -1)\""

step "correctness: MapFilterFusionDemoITCase" \
    "$REPO/mvnw" -o -q -f "$REPO/pom.xml" test -pl flink-runtime -Dtest=MapFilterFusionDemoITCase \
    -Dfast -Dsurefire.failIfNoSpecifiedTests=false
# benchmark.py stages the two distributions under build/, which the sensor steps also use
step "experiment 1: fusion, simple job" "$BENCH/benchmark.py" simple "$VANILLA" "$FUSED" --iterations "$ITERATIONS"
step "experiment 1: fusion, mixed job" "$BENCH/benchmark.py" mixed "$VANILLA" "$FUSED" --iterations "$ITERATIONS"
step "correctness: sensor checksums" sensor_checksums
step "experiment 2: JNI crossing and marshalling" "$BENCH/jni-crossing/run.sh"
step "experiment 3: stage 1, Java vs C on their own" stage1
step "experiment 3: stage 2, inside Flink" "$SB/minicluster-sweep.sh"
step "experiment 3: tables" sh -c "'$SB/report.py' > '$SB/results/sweep-report.md'"

echo; echo "run-all finished $(date -Iseconds); failed steps: ${FAILED[*]:-none}"
[ ${#FAILED[@]} -eq 0 ]
