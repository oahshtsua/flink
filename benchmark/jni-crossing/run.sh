#!/usr/bin/env bash
# Build and run the JNI crossing-cost measurement.
#
#   ./benchmark/jni-crossing/run.sh [extra JMH args...]
#
# JDK: defaults to the JDK 21 installed via Homebrew, because that is representative of a real
# Flink TaskManager (Flink 2.x targets Java 17 bytecode) and because JMH 1.37 is well-tested
# there. Override with JAVA_HOME=... to measure on a different JVM.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPS="$HERE/deps"
BUILD="$HERE/build"
RESULTS="$HERE/results"

# Pick a JDK: an explicit JAVA_HOME wins, then a Homebrew OpenJDK 21, then whatever javac is on
# PATH. We need a JDK (not a JRE) for javac, and its include/ dir for jni.h.
if [ -z "${JAVA_HOME:-}" ]; then
    for cand in /home/linuxbrew/.linuxbrew/Cellar/openjdk@21/*/libexec /usr/lib/jvm/java-21-openjdk; do
        [ -x "$cand/bin/javac" ] && { JAVA_HOME="$cand"; break; }
    done
fi
if [ -z "${JAVA_HOME:-}" ]; then
    javac_bin="$(command -v javac || true)"
    [ -n "$javac_bin" ] || { echo "no JDK found: set JAVA_HOME=/path/to/jdk" >&2; exit 1; }
    JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$javac_bin")")")"
fi
[ -f "$JAVA_HOME/include/jni.h" ] || { echo "no jni.h under $JAVA_HOME -- is it a JDK?" >&2; exit 1; }

JAVA="$JAVA_HOME/bin/java"
JAVAC="$JAVA_HOME/bin/javac"
echo "using JDK: $JAVA_HOME"

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

echo "=== compiling native (gcc -O2) ==="
gcc -O2 -fPIC -shared \
    -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux" \
    -o "$BUILD/libjnicrossing.so" "$HERE/native/jni_crossing.c"

echo "=== compiling java ($("$JAVAC" -version 2>&1)) ==="
rm -rf "$BUILD/classes"; mkdir -p "$BUILD/classes"
"$JAVAC" -cp "$CP" -processorpath "$PROC:$CP" -d "$BUILD/classes" \
    $(find "$HERE/src" -name '*.java')

echo "=== running ==="
"$JAVA" -cp "$BUILD/classes:$CP" \
    -Djnicrossing.lib="$BUILD/libjnicrossing.so" \
    org.openjdk.jmh.Main \
    -jvmArgsAppend "-Djnicrossing.lib=$BUILD/libjnicrossing.so" \
    -rf json -rff "$RESULTS/jni-crossing.json" \
    "$@" 2>&1 | tee "$RESULTS/jni-crossing.txt"
