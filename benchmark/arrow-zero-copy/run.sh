#!/usr/bin/env bash
# Arrow zero-copy experiment: the sensor job with Arrow batches as records (SensorScoringArrowJob),
# the work done by a C kernel reading and writing the columns in place, by one Java batch function,
# or by four Java batch steps - against the copying native operator (SensorScoringNativeJob) and
# the hand-tuned Java filter (SensorScoringJob tuned).
#
#   ./benchmark/arrow-zero-copy/run.sh check    # correctness only (~1 min)
#   ./benchmark/arrow-zero-copy/run.sh sweep    # timing sweep in a local MiniCluster (~40 min)
#   ./benchmark/arrow-zero-copy/run.sh bench    # JMH: export cost per batch, fill cost per row (~3 min)
#   ./benchmark/arrow-zero-copy/run.sh          # check, then sweep, then bench
#
# check: at k = 1, 16, 64 and batch sizes 1, 7 (partial last batch) and 1024, every Arrow engine
# must print the reference's CHECKSUM line; for the C engine, its kernel must also have seen every
# record and its zero-copy check must have compared at least one buffer.
#
# sweep: like sensor-bench/minicluster-sweep.sh - interleaved runs, NET_RUNTIME_MS per run - for
# tuned Java, and native, Arrow + C, Arrow + Java and Arrow + Java steps at every batch size.
# Output: results/sweep.csv; summarize with report.py.
#
# bench: src/.../ArrowExport.java, pinned to CPU 2 (a P-core). Output: results/arrow-export.{txt,json}.
#
# Env: COUNT (records per timed run, default 50000000), RUNS (default 3), KS (default
# "1 4 16 64"), BATCHES (default "64 256 1024 4096"), PIN_CPUS (default 0-7, the P-cores).
# Needs benchmark/build/vanilla/flink (the stock distribution benchmark.py stages). Arrow Java and
# its dependencies are fetched into deps/ on first use.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH="$(cd "$HERE/.." && pwd)"
NK="$BENCH/native-kernel/build"
FLINK_LIB="$BENCH/build/vanilla/flink/lib"
DEPS="$HERE/deps"
CLASSES="$HERE/build/classes"
RESULTS="$HERE/results"

MODE="${1:-all}"
COUNT="${COUNT:-50000000}"
RUNS="${RUNS:-3}"
KS="${KS:-1 4 16 64}"
BATCHES="${BATCHES:-64 256 1024 4096}"
PIN_CPUS="${PIN_CPUS-0-7}"
CHECK_COUNT=2000000

[ -d "$FLINK_LIB" ] || { echo "missing $FLINK_LIB" >&2; exit 1; }

# Arrow Java 19.0.0 (the version flink-python uses) and what it needs at runtime that Flink's
# lib/ doesn't provide unshaded. SLF4J comes from flink-dist.
MAVEN=https://repo1.maven.org/maven2
ARTIFACTS=(
    org/apache/arrow/arrow-vector/19.0.0
    org/apache/arrow/arrow-c-data/19.0.0
    org/apache/arrow/arrow-memory-core/19.0.0
    org/apache/arrow/arrow-memory-unsafe/19.0.0
    org/apache/arrow/arrow-format/19.0.0
    com/google/flatbuffers/flatbuffers-java/25.2.10
    com/fasterxml/jackson/core/jackson-core/2.21.0
    com/fasterxml/jackson/core/jackson-databind/2.21.0
    com/fasterxml/jackson/core/jackson-annotations/2.21
)
mkdir -p "$DEPS" "$RESULTS"
for a in "${ARTIFACTS[@]}"; do
    name="$(basename "$(dirname "$a")")-$(basename "$a").jar"
    [ -f "$DEPS/$name" ] || { echo "fetching $name"; curl -fsSL --retry 3 -o "$DEPS/$name" "$MAVEN/$a/$name"; }
done

echo "=== building native bridge + kernels ==="
"$BENCH/native-kernel/build.sh" >/dev/null
echo "=== compiling jobs ==="
rm -rf "$CLASSES"; mkdir -p "$CLASSES"
javac --release 17 -proc:none -cp "$NK/flink-accel-bridge.jar:$FLINK_LIB/*:$DEPS/*" -d "$CLASSES" \
    "$BENCH"/job/org/apache/flink/demo/*.java "$HERE"/job/org/apache/flink/demo/*.java

BRIDGE=(-Dflink.accel.bridge.lib="$NK/libflinkaccel_bridge.so")
JAVA_JOB=(-cp "$CLASSES:$FLINK_LIB/*" org.apache.flink.demo.SensorScoringJob)
NATIVE_JOB=("${BRIDGE[@]}" -Dsensor.kernel.lib="$NK/libsensor_kernel.so"
    -cp "$CLASSES:$NK/flink-accel-bridge.jar:$FLINK_LIB/*" org.apache.flink.demo.SensorScoringNativeJob)
# Arrow's memory module reads direct buffers' addresses reflectively (JDK 17 needs the opens).
# Unsafe memory access drops ArrowBuf's per-access bounds checks, as any performance-minded user
# of Arrow Java runs it; correctness is still checked by the checksum.
ARROW_JOB=(--add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_unsafe_memory_access=true
    "${BRIDGE[@]}" -Dsensor.kernel.lib="$NK/libsensor_arrow_kernel.so"
    -cp "$CLASSES:$NK/flink-accel-bridge.jar:$FLINK_LIB/*:$DEPS/*" org.apache.flink.demo.SensorScoringArrowJob)

check() {
    local ok=0 k b e ref out stats verdict
    for k in 1 16 64; do
        ref=$(java -cp "$CLASSES" org.apache.flink.demo.SensorScoringReference $CHECK_COUNT $k | grep CHECKSUM)
        echo "k=$k reference                    $ref"
        for e in native java java-steps; do
            for b in 1 7 1024; do
                out=$(java "${ARROW_JOB[@]}" $CHECK_COUNT $k checksum $b $e 2>&1)
                stats=$(grep ARROW_NATIVE <<<"$out" || true)
                out=$(grep CHECKSUM <<<"$out" || true)
                verdict=MATCH
                [ "$out" = "$ref" ] || verdict=MISMATCH
                if [ "$e" = native ] && { ! grep -q " rows=$CHECK_COUNT " <<<"$stats" \
                    || grep -q "zeroCopyBuffers=0" <<<"$stats"; }; then
                    verdict="MISMATCH (kernel did not see every record, or no zero-copy check)"
                fi
                [ "$verdict" = MATCH ] || ok=1
                printf 'k=%s %-28s %s %s\n' $k "arrow $e batch=$b" "$out" "$verdict"
                [ -z "$stats" ] || printf '    %s\n' "$stats"
            done
        done
    done
    return $ok
}

machine() {
    local cpu=/sys/devices/system/cpu/cpu2/cpufreq
    echo "maxfreq=$(cat $cpu/scaling_max_freq)/$(cat $cpu/cpuinfo_max_freq)" \
        "profile=$(cat /sys/firmware/acpi/platform_profile 2>/dev/null || echo ?)" \
        "ac=$(cat /sys/class/power_supply/{AC,ADP}*/online 2>/dev/null | head -1)" \
        "load=$(cut -d' ' -f1 /proc/loadavg)"
}

sweep() {
    local out="$RESULTS/sweep.csv" run k b ms
    local pin=()
    [ -n "$PIN_CPUS" ] && pin=(taskset -c "$PIN_CPUS")
    timed() { # prints the job's NET_RUNTIME_MS, or nothing on failure
        "${pin[@]}" java "$@" 2>&1 | grep -oE "NET_RUNTIME_MS=[0-9]+" | cut -d= -f2 || true
    }
    echo "=== sweep $(date -Iseconds) $(machine) ==="
    echo "variant,k,batch,run,count,net_ms" > "$out"
    for run in $(seq 1 "$RUNS"); do
        for k in $KS; do
            ms=$(timed "${JAVA_JOB[@]}" "$COUNT" "$k" discard tuned)
            echo "tuned,$k,,$run,$COUNT,$ms" | tee -a "$out"
            for b in $BATCHES; do
                ms=$(timed "${NATIVE_JOB[@]}" "$COUNT" "$k" discard "$b")
                echo "native,$k,$b,$run,$COUNT,$ms" | tee -a "$out"
                ms=$(timed "${ARROW_JOB[@]}" "$COUNT" "$k" discard "$b" native)
                echo "arrow,$k,$b,$run,$COUNT,$ms" | tee -a "$out"
                ms=$(timed "${ARROW_JOB[@]}" "$COUNT" "$k" discard "$b" java)
                echo "arrow-java,$k,$b,$run,$COUNT,$ms" | tee -a "$out"
                ms=$(timed "${ARROW_JOB[@]}" "$COUNT" "$k" discard "$b" java-steps)
                echo "arrow-steps,$k,$b,$run,$COUNT,$ms" | tee -a "$out"
            done
        done
    done
    echo "=== done $(date -Iseconds) $(machine): $out ==="
}

bench() {
    # Shared JMH jars, as sensor-bench/run.sh uses them.
    local jmh="$BENCH/jni-crossing/deps" v=1.37 cp proc classes="$HERE/build/bench-classes"
    mkdir -p "$jmh"
    for a in org/openjdk/jmh/jmh-core/$v org/openjdk/jmh/jmh-generator-annprocess/$v \
        net/sf/jopt-simple/jopt-simple/5.0.4 org/apache/commons/commons-math3/3.6.1; do
        name="$(basename "$(dirname "$a")")-$(basename "$a").jar"
        [ -f "$jmh/$name" ] || { echo "fetching $name"; curl -fsSL --retry 3 -o "$jmh/$name" "$MAVEN/$a/$name"; }
    done
    cp="$jmh/jmh-core-$v.jar:$jmh/jopt-simple-5.0.4.jar:$jmh/commons-math3-3.6.1.jar"
    proc="$jmh/jmh-generator-annprocess-$v.jar"
    rm -rf "$classes"; mkdir -p "$classes"
    # Only the Flink-free classes; Flink's lib/ is on the run classpath for SLF4J, which Arrow needs.
    javac --release 17 -cp "$cp:$DEPS/*" -processorpath "$proc:$cp" -d "$classes" \
        "$BENCH"/job/org/apache/flink/demo/{SensorReading,SensorScoring}.java \
        "$HERE"/job/org/apache/flink/demo/{ArrowMemory,SensorArrow}.java \
        $(find "$HERE/src" -name '*.java')
    echo "=== bench $(date -Iseconds) $(machine) ==="
    taskset -c 2 java -cp "$classes:$cp:$DEPS/*:$FLINK_LIB/*" org.openjdk.jmh.Main ArrowExport \
        -rf json -rff "$RESULTS/arrow-export.json" 2>&1 | tee "$RESULTS/arrow-export.txt"
}

case "$MODE" in
    check) check ;;
    sweep) sweep ;;
    bench) bench ;;
    all) check && sweep && bench ;;
    *) echo "usage: $0 [check|sweep|bench|all]" >&2; exit 2 ;;
esac
