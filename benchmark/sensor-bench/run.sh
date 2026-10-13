#!/usr/bin/env bash
# Build and run the sensor-scoring JMH measurements.
#
#   ./benchmark/sensor-bench/run.sh [extra JMH args...]
#   RESULT_NAME=batch ./benchmark/sensor-bench/run.sh SensorBatch   # one benchmark class
#
# SensorWork: the Java work per record. SensorBatch: where the native path's time goes.
# Builds native-kernel/ first (bridge + kernel), since SensorBatch calls the kernel.
#
# JDK: the javac/java on PATH unless JAVA_HOME is set. Use JDK 17 to match the Docker harness
# image (eclipse-temurin:17-jre); results from a different JDK are not comparable with it.
#
# Pinning: runs under `taskset -c $PIN_CPUS` (default 2, a P-core on the i5-12500H this was
# written on; CPUs 8-15 there are slower E-cores). Set PIN_CPUS= to disable.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOB_SRC="$HERE/../job/org/apache/flink/demo"
DEPS="$HERE/../jni-crossing/deps"   # shared JMH jars; jni-crossing/run.sh fetches them
BUILD="$HERE/build"
RESULTS="$HERE/results"
PIN_CPUS="${PIN_CPUS-2}"

if [ -n "${JAVA_HOME:-}" ]; then
    JAVA="$JAVA_HOME/bin/java"
    JAVAC="$JAVA_HOME/bin/javac"
else
    JAVA=java
    JAVAC=javac
fi

JMH_VERSION=1.37
MAVEN=https://repo1.maven.org/maven2
fetch() { # url -> deps/
    local url="$1" out="$DEPS/$(basename "$1")"
    [ -f "$out" ] || { echo "fetching $(basename "$1")"; curl -fsSL -o "$out" "$url"; }
}
mkdir -p "$DEPS" "$BUILD" "$RESULTS"
fetch "$MAVEN/org/openjdk/jmh/jmh-core/$JMH_VERSION/jmh-core-$JMH_VERSION.jar"
fetch "$MAVEN/org/openjdk/jmh/jmh-generator-annprocess/$JMH_VERSION/jmh-generator-annprocess-$JMH_VERSION.jar"
fetch "$MAVEN/net/sf/jopt-simple/jopt-simple/5.0.4/jopt-simple-5.0.4.jar"
fetch "$MAVEN/org/apache/commons/commons-math3/3.6.1/commons-math3-3.6.1.jar"

CP="$DEPS/jmh-core-$JMH_VERSION.jar:$DEPS/jopt-simple-5.0.4.jar:$DEPS/commons-math3-3.6.1.jar"
PROC="$DEPS/jmh-generator-annprocess-$JMH_VERSION.jar"

echo "=== compiling java ($("$JAVAC" -version 2>&1)) ==="
echo "=== building native bridge + kernels ==="
"$HERE/../native-kernel/build.sh" >/dev/null
NK="$(cd "$HERE/../native-kernel" && pwd)"

rm -rf "$BUILD/classes"; mkdir -p "$BUILD/classes"
# Only the Flink-free parts of the job (the record, its arithmetic, the kernel layouts) plus the
# bridge, so no Flink is needed on the classpath.
"$JAVAC" --release 17 -cp "$CP" -processorpath "$PROC:$CP" -d "$BUILD/classes" \
    "$JOB_SRC/SensorReading.java" "$JOB_SRC/SensorScoring.java" "$JOB_SRC/SensorLayout.java" \
    "$NK/bridge/org/apache/flink/accel/NativeBridge.java" \
    $(find "$HERE/src" -name '*.java')

PIN=()
[ -n "$PIN_CPUS" ] && PIN=(taskset -c "$PIN_CPUS")
NATIVE_PROPS="-Dflink.accel.bridge.lib=$NK/build/libflinkaccel_bridge.so -Dsensor.kernel.lib=$NK/build/libsensor_kernel.so"
RESULT_NAME="${RESULT_NAME:-sensor-bench}"

echo "=== running (pinned to CPUs: ${PIN_CPUS:-none}) ==="
"${PIN[@]}" "$JAVA" -cp "$BUILD/classes:$CP" org.openjdk.jmh.Main \
    -jvmArgsAppend "$NATIVE_PROPS" \
    -rf json -rff "$RESULTS/$RESULT_NAME.json" \
    "$@" 2>&1 | tee "$RESULTS/$RESULT_NAME.txt"
