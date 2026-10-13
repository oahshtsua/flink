# Sensor scoring benchmark: Java vs one C function

Measures whether moving a whole `map -> filter -> map -> filter` chain into a single native C
function pays off once each record does real work. How it's built and why:
`docs/microbatch-native-implementation.md`; what was found: `docs/microbatch-native-summary.md`;
this file is how to run it.

The comparison runs in two stages:

| stage | what runs | script |
|---|---|---|
| **1 — Java vs C, on their own** | only the four steps, no Flink (JMH) | `run.sh` |
| **2 — plugged into Flink** | the whole job in Flink's local mode: stock Flink, the fusion prototype, and the native operator | `minicluster-sweep.sh` |

Both sweep the work per record (`k`, the number of reference shapes compared in step 3) and, for
the native version, the batch size (records per call into C).

## What's where

| path | what |
|---|---|
| `../job/org/apache/flink/demo/SensorScoringJob.java` | the plain Java job (stock / fusion variants) |
| `../job/org/apache/flink/demo/SensorScoringNativeJob.java` | the same job with the four steps replaced by the native operator |
| `../job/org/apache/flink/demo/SensorScoringReference.java` | Flink-free oracle: prints the checksum every variant must match |
| `../job/org/apache/flink/demo/MicroBatchingNativeOperator.java` | the generic micro-batching operator |
| `../native-kernel/` | the JNI bridge and the C kernel; `build.sh` builds them into `native-kernel/build/` |
| `src/.../SensorWork.java` | JMH: the Java work per record, by `k` |
| `src/.../SensorBatch.java` | JMH: stage 1 — Java vs the C path (copy in, call, copy out), by `k` × batch size |
| `run.sh` | builds the native libraries and runs JMH |
| `minicluster-sweep.sh` | stage 2 sweep |
| `report.py` | turns both stages' results into Markdown tables |

## Prerequisites

- **JDK 17** with `javac` and `jni.h` (the Docker harness image runs 17; results from another JDK
  aren't comparable). `JAVA_HOME` is honoured; otherwise the `javac` on `PATH` is used.
- **gcc** (the kernel is built with `-O3 -march=native -ffp-contract=off`, so it targets this
  machine's CPU).
- **curl**, the first time only: `run.sh` downloads the JMH jars into `../jni-crossing/deps/`.
- **Two Flink distributions** for stage 2 and the correctness check, at
  `../build/vanilla/flink` (stock) and `../build/fused/flink` (with the fusion prototype).
  `../benchmark.py` stages them there on any run; see `../README.md` for building them.

Commands below are run from the repository root unless noted.

## 1. Check correctness first (~1 min)

All three must print the identical `CHECKSUM survivors=... hash=...` line. From `benchmark/`:

```bash
# Build the native side into native-kernel/build/: the JNI bridge library
# (libflinkaccel_bridge.so), its Java class packaged as flink-accel-bridge.jar, and the
# C kernel with the whole four-step chain (libsensor_kernel.so).
./native-kernel/build.sh

# Compile all the benchmark job classes into /tmp/sensor. Flink's jars (from the fused
# distribution) and the bridge jar are only on the classpath so the jobs compile; they
# are not copied into the output.
javac --release 17 -proc:none -cp "native-kernel/build/flink-accel-bridge.jar:build/fused/flink/lib/*" \
    -d /tmp/sensor job/org/apache/flink/demo/*.java

# The reference: run the four steps over 2,000,000 generated records with k=16 in a plain
# Java loop, no Flink at all, and print the checksum of the survivors. This is the
# expected answer.
java -cp /tmp/sensor org.apache.flink.demo.SensorScoringReference 2000000 16

# The plain Java Flink job, run against the fused distribution's jars, so the Map/Filter
# fusion prototype merges the four steps. Prints its checksum when the input ends.
# (Use build/vanilla/flink/lib/* instead to check stock Flink.)
java -cp "/tmp/sensor:build/fused/flink/lib/*" \
    org.apache.flink.demo.SensorScoringJob 2000000 16 checksum

# The native Flink job: the four steps replaced by the micro-batching operator calling
# the C kernel. The two -D flags tell it where the bridge library and the kernel library
# are; the bridge jar goes on the classpath next to stock Flink's jars.
java -Dflink.accel.bridge.lib=$PWD/native-kernel/build/libflinkaccel_bridge.so \
     -Dsensor.kernel.lib=$PWD/native-kernel/build/libsensor_kernel.so \
     -cp "/tmp/sensor:native-kernel/build/flink-accel-bridge.jar:build/vanilla/flink/lib/*" \
     org.apache.flink.demo.SensorScoringNativeJob 2000000 16 checksum
```

Job arguments: `SensorScoringJob [records] [k] [discard|checksum]` and
`SensorScoringNativeJob [records] [k] [discard|checksum] [batchSize]`. Without `bin/flink`, both run
in an in-process local cluster. Expected for 2,000,000 records:

| k | survivors | hash |
|---|---|---|
| 1 | 602267 | `3d0044a86f2e5afb` |
| 16 | 617961 | `83dda26b980dfdf6` |
| 64 | 621176 | `a3f1c1b1dad62df3` |

## 2. Stage 1 — Java vs C, on their own (~15 min)

```bash
RESULT_NAME=sweep-java   ./benchmark/sensor-bench/run.sh 'SensorBatch.(generate|java)$' -p batch=1024
RESULT_NAME=sweep-native ./benchmark/sensor-bench/run.sh 'SensorBatch.nativePath$'
```

Any extra arguments go straight to JMH, e.g. `-p k=1,64 -p batch=1,1024` for a smaller grid, or
`-f 1 -wi 2 -i 3` for a quicker, noisier run. The Java work per record alone (by `k`):

```bash
./benchmark/sensor-bench/run.sh SensorWork
```

`run.sh` pins JMH to CPU 2 (`PIN_CPUS`, default `2`; set `PIN_CPUS=` to disable). That default
suits the i5-12500H this was written on, where CPUs 0–7 are the fast P-cores and 8–15 the slower
E-cores — check `lscpu --extended` and adjust on another machine.

## 3. Stage 2 — plugged into Flink (~25 min)

```bash
./benchmark/sensor-bench/minicluster-sweep.sh
```

Runs stock, fusion, and native at every batch size, for every `k`: 50M records per run, 3 runs per
combination, interleaved (run 1 of everything, then run 2, …) so machine drift spreads evenly.
Each run reports the job's net runtime, excluding JVM and cluster startup. Tune with environment
variables:

| variable | default | meaning |
|---|---|---|
| `COUNT` | `50000000` | records per run |
| `RUNS` | `3` | runs per combination |
| `KS` | `1 4 16 64` | work levels |
| `BATCHES` | `1 16 64 256 1024 4096` | native batch sizes |
| `PIN_CPUS` | `0-7` | CPUs to pin to (empty = no pinning) |

A quick smoke run: `COUNT=200000 RUNS=1 KS="1 64" BATCHES="1 1024" ./benchmark/sensor-bench/minicluster-sweep.sh`.

## 4. Tables

```bash
./benchmark/sensor-bench/report.py > benchmark/sensor-bench/results/sweep-report.md
```

Prints whichever results exist: stage 1 (ns per record and C's speed-up over Java) and stage 2
(ns per record, and the native speed-up over the fusion prototype, with run-to-run spread).

## Outputs

Everything lands in `results/` (gitignored):

| file | from |
|---|---|
| `sweep-java.json`, `sweep-native.json` (+ `.txt`) | stage 1 JMH runs (named by `RESULT_NAME`; default `sensor-bench`) |
| `minicluster-sweep.csv` | stage 2: `variant,k,batch,run,count,net_ms` |
| `sweep-report.md` | `report.py` |

## Not covered yet

- **The Docker harness** (`../benchmark.py sensor ...`) runs only the plain job, stock vs fusion,
  at `k=1`. The native job isn't wired into it: that needs the bridge jar and both `.so` files in
  the image's `flink/lib/`, with the kernel built against the image's glibc.
- One machine only; stage 2 uses Flink's local mode, not a real cluster.
