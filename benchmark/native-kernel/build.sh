#!/usr/bin/env bash
# Builds the generic JNI bridge and the batch kernels into benchmark/native-kernel/build/:
#
#   libflinkaccel_bridge.so   JNI side of org.apache.flink.accel.NativeBridge
#   flink-accel-bridge.jar    NativeBridge.class - goes in flink/lib/ next to the .so
#   libsensor_kernel.so       sensor_score(): the whole sensor chain as one C function
#
#   ./benchmark/native-kernel/build.sh
#
# Builds for the host. Libraries meant for the Docker harness image must be built against a
# glibc no newer than the image's; that is a separate (container) build, not done here.
#
# Kernel flags: -O3 -march=native for a good-faith optimized build, but -ffp-contract=off so the
# compiler never fuses a*b+c into an FMA (Java doesn't), and no -ffast-math, so no reassociation:
# the kernel's results stay bit-identical to SensorScoring.java. -fno-math-errno only drops
# sqrt()'s errno bookkeeping; it does not change any result.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD="$HERE/build"

if [ -z "${JAVA_HOME:-}" ]; then
    javac_bin="$(command -v javac || true)"
    [ -n "$javac_bin" ] || { echo "no JDK found: set JAVA_HOME=/path/to/jdk" >&2; exit 1; }
    JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$javac_bin")")")"
fi
[ -f "$JAVA_HOME/include/jni.h" ] || { echo "no jni.h under $JAVA_HOME -- is it a JDK?" >&2; exit 1; }

mkdir -p "$BUILD"

echo "=== bridge: libflinkaccel_bridge.so ==="
gcc -O2 -fPIC -shared -Wall -Wextra -Wno-unused-parameter \
    -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux" \
    -o "$BUILD/libflinkaccel_bridge.so" "$HERE/bridge/native_bridge.c" -ldl

echo "=== bridge: flink-accel-bridge.jar ==="
rm -rf "$BUILD/bridge-classes"
"$JAVA_HOME/bin/javac" --release 17 -d "$BUILD/bridge-classes" \
    "$HERE/bridge/org/apache/flink/accel/NativeBridge.java"
"$JAVA_HOME/bin/jar" --create --file "$BUILD/flink-accel-bridge.jar" -C "$BUILD/bridge-classes" .

echo "=== kernel: libsensor_kernel.so ==="
gcc -O3 -march=native -ffp-contract=off -fno-math-errno -fPIC -shared -Wall -Wextra \
    -o "$BUILD/libsensor_kernel.so" "$HERE/kernels/sensor_kernel.c" -lm

ls -l "$BUILD"/*.so "$BUILD"/*.jar
