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
#   ./benchmark/arrow-zero-copy/run.sh jobs-check   # whole jobs as Arrow-batch functions: correctness (~2 min)
#   ./benchmark/arrow-zero-copy/run.sh jobs-sweep   # ... and their timing sweep (~17 min with RUNS=5)
#
# check: at k = 1, 16, 64 and batch sizes 1, 7 (partial last batch) and 1024, every Arrow engine
# must print the reference's CHECKSUM line; for the C engine, its kernel must also have seen every
# record and its zero-copy check must have compared at least one buffer.
#
# sweep: like sensor-bench/minicluster-sweep.sh - interleaved runs, NET_RUNTIME_MS per run - for
# tuned Java, and native, Arrow + C, Arrow + Java and Arrow + Java steps at every batch size.
# Output: results/sweep.csv; summarize with report.py.
#
# jobs-check / jobs-sweep: the fusion benchmark jobs (simple, mixed; MapFilterArrowJob) and the
# sensor job, each as the original row-at-a-time job on stock Flink and on the fusion build, and
# with every user function rewritten for Arrow batches on stock Flink (for simple and mixed also
# wrapped, unchanged, in a per-row adapter). Every variant must print the rows job's CHECKSUM line;
# the fusion build must log that it fused the chain. Output: results/jobs/sweep.csv; summarize with
# jobs-report.py. Env: JOBS_COUNT (default 200000000, as in the fusion benchmark), JOBS_BATCHES
# (default "64 256 1024 4096 16384"), SENSOR_KS (default "1 64"), SENSOR_BATCHES (default
# "64 256 1024").
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
FUSED_LIB="$BENCH/build/fused/flink/lib"
DEPS="$HERE/deps"
CLASSES="$HERE/build/classes"
RESULTS="$HERE/results"

MODE="${1:-all}"
COUNT="${COUNT:-50000000}"
RUNS="${RUNS:-3}"
KS="${KS:-1 4 16 64}"
BATCHES="${BATCHES:-64 256 1024 4096}"
PIN_CPUS="${PIN_CPUS-0-7}"
JOBS_COUNT="${JOBS_COUNT:-200000000}"
JOBS_BATCHES="${JOBS_BATCHES:-64 256 1024 4096 16384}"
SENSOR_KS="${SENSOR_KS:-1 64}"
SENSOR_BATCHES="${SENSOR_BATCHES:-64 256 1024}"
CHECK_COUNT=2000000

for lib in "$FLINK_LIB" "$FUSED_LIB"; do
    [ -d "$lib" ] || { echo "missing $lib" >&2; exit 1; }
done

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
# Whole jobs: Arrow is on every classpath (the job class names the Arrow engines' classes), and
# log4j-fusion.properties shows the fusion build's "Fusing chained run" lines.
JOBS_JVM=(--add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_unsafe_memory_access=true
    -Dlog4j.configurationFile="$HERE/log4j-fusion.properties")
STOCK=(java "${JOBS_JVM[@]}" -cp "$CLASSES:$FLINK_LIB/*:$DEPS/*")
FUSED=(java "${JOBS_JVM[@]}" -cp "$CLASSES:$FUSED_LIB/*:$DEPS/*")
MF=org.apache.flink.demo.MapFilterArrowJob
SENSOR=org.apache.flink.demo.SensorScoringJob
SENSOR_ARROW=org.apache.flink.demo.SensorScoringArrowJob

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

# Prints MATCH or MISMATCH for one run's output against a reference CHECKSUM line; with a third
# argument, also requires a "Fusing chained run of <n> operators" line.
verdict() {
    local out="$1" ref="$2" fused_ops="${3:-}"
    if [ "$(grep CHECKSUM <<<"$out")" != "$ref" ]; then
        echo MISMATCH
    elif [ -n "$fused_ops" ] && ! grep -q "Fusing chained run of $fused_ops operators" <<<"$out"; then
        echo "MISMATCH (not fused)"
    else
        echo MATCH
    fi
}

jobs_check() {
    local ok=0 job ops e b k ref out v
    for job in simple mixed; do
        ops=$([ $job = simple ] && echo 2 || echo 3)
        ref=$("${STOCK[@]}" $MF $job $CHECK_COUNT rows checksum 2>&1 | grep CHECKSUM)
        echo "$job rows, stock              $ref"
        out=$("${FUSED[@]}" $MF $job $CHECK_COUNT rows checksum 2>&1); v=$(verdict "$out" "$ref" $ops)
        [ "$v" = MATCH ] || ok=1; echo "$job rows, fusion build       $(grep CHECKSUM <<<"$out") $v"
        for e in arrow arrow-wrapped; do
            for b in 1 7 1024; do
                out=$("${STOCK[@]}" $MF $job $CHECK_COUNT $e checksum $b 2>&1); v=$(verdict "$out" "$ref")
                [ "$v" = MATCH ] || ok=1
                printf '%s %-24s %s %s\n' $job "$e batch=$b" "$(grep CHECKSUM <<<"$out")" "$v"
            done
        done
    done
    # The sensor job's Arrow engines are checked by check(); here only its fusion-build run.
    for k in 1 64; do
        ref=$(java -cp "$CLASSES" org.apache.flink.demo.SensorScoringReference $CHECK_COUNT $k | grep CHECKSUM)
        out=$("${FUSED[@]}" $SENSOR $CHECK_COUNT $k checksum chain 2>&1); v=$(verdict "$out" "$ref" 4)
        [ "$v" = MATCH ] || ok=1; echo "sensor k=$k rows, fusion build $(grep CHECKSUM <<<"$out") $v"
    done
    return $ok
}

jobs_sweep() {
    local out="$RESULTS/jobs/sweep.csv" run job b k ms
    local pin=()
    [ -n "$PIN_CPUS" ] && pin=(taskset -c "$PIN_CPUS")
    timed() { # prints the job's NET_RUNTIME_MS, or nothing on failure
        "${pin[@]}" "$@" 2>&1 | grep -oE "NET_RUNTIME_MS=[0-9]+" | cut -d= -f2 || true
    }
    mkdir -p "$RESULTS/jobs"
    echo "=== jobs sweep $(date -Iseconds) $(machine) ==="
    echo "job,variant,k,batch,run,count,net_ms" > "$out"
    for run in $(seq 1 "$RUNS"); do
        for job in simple mixed; do
            ms=$(timed "${STOCK[@]}" $MF $job "$JOBS_COUNT" rows)
            echo "$job,stock,,,$run,$JOBS_COUNT,$ms" | tee -a "$out"
            ms=$(timed "${FUSED[@]}" $MF $job "$JOBS_COUNT" rows)
            echo "$job,fusion,,,$run,$JOBS_COUNT,$ms" | tee -a "$out"
            for b in $JOBS_BATCHES; do
                ms=$(timed "${STOCK[@]}" $MF $job "$JOBS_COUNT" arrow discard "$b")
                echo "$job,arrow,,$b,$run,$JOBS_COUNT,$ms" | tee -a "$out"
                ms=$(timed "${STOCK[@]}" $MF $job "$JOBS_COUNT" arrow-wrapped discard "$b")
                echo "$job,arrow-wrapped,,$b,$run,$JOBS_COUNT,$ms" | tee -a "$out"
            done
        done
        for k in $SENSOR_KS; do
            ms=$(timed "${STOCK[@]}" $SENSOR "$COUNT" "$k" discard chain)
            echo "sensor,stock,$k,,$run,$COUNT,$ms" | tee -a "$out"
            ms=$(timed "${FUSED[@]}" $SENSOR "$COUNT" "$k" discard chain)
            echo "sensor,fusion,$k,,$run,$COUNT,$ms" | tee -a "$out"
            for b in $SENSOR_BATCHES; do
                ms=$(timed "${STOCK[@]}" $SENSOR_ARROW "$COUNT" "$k" discard "$b" java-steps)
                echo "sensor,arrow,$k,$b,$run,$COUNT,$ms" | tee -a "$out"
            done
        done
    done
    echo "=== done $(date -Iseconds) $(machine): $out ==="
}

case "$MODE" in
    check) check ;;
    sweep) sweep ;;
    bench) bench ;;
    all) check && sweep && bench ;;
    jobs-check) jobs_check ;;
    jobs-sweep) jobs_sweep ;;
    *) echo "usage: $0 [check|sweep|bench|all|jobs-check|jobs-sweep]" >&2; exit 2 ;;
esac
