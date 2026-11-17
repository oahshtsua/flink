# Arrow zero-copy: the sensor job with Arrow batches as records

The sensor job from `../sensor-bench/`, with the copying between Java objects and the C kernel
removed. Instead of `SensorReading` objects, the stream carries **Arrow record batches**. The
source writes readings straight into Arrow columns. The operator hands the kernel the columns'
addresses through the Arrow C Data Interface. The kernel writes its results straight into Arrow
columns and returns a selection vector of the survivors. Nothing is copied in or out.

Why, what was found, and what's next: `docs/arrow-zero-copy-experiment.md`.

## Files

| File | What it is |
|---|---|
| `job/.../ArrowBatch.java` | The stream record: an Arrow `VectorSchemaRoot` plus an optional selection vector. Its type information's serializer throws if Flink ever copies or serializes a batch |
| `job/.../ArrowMemory.java` | The shared Arrow root allocator, and a helper to mark every row valid |
| `job/.../ArrowNativeOperator.java` | Generic operator: export the batch (no copy), call the kernel through `NativeBridge`, emit input columns + result columns + selection vector. Checks on the first batch that the export really shared the buffers |
| `job/.../SensorArrow.java` | The sensor job's Arrow schema, generation straight into columns, and reading a row back for the checksum |
| `job/.../SensorScoringArrowJob.java` | The job: batch source → `ArrowNativeOperator` → sink (discard or checksum) |
| `../native-kernel/kernels/sensor_arrow_kernel.c` | `sensor_score_arrow`: `sensor_score`'s arithmetic, reading and writing Arrow columns in place |
| `src/.../ArrowExport.java` | JMH: the export's cost per batch, and generation's cost per row |
| `run.sh` | Fetches Arrow Java, builds, then `check`, `sweep` and/or `bench` |
| `report.py` | Turns `results/sweep.csv` into Markdown tables |

The job classes are in a separate source tree from `../job/` because they need Arrow Java to
compile. `../sensor-bench/minicluster-sweep.sh` compiles `../job/` alone, without Arrow.

## Running

Needs `../build/vanilla/flink` (the stock distribution `benchmark.py` stages), a JDK 17 and gcc.
Arrow Java 19.0.0 and its dependencies are downloaded into `deps/` on the first run.

```bash
./benchmark/arrow-zero-copy/run.sh check   # correctness, ~1 min
./benchmark/arrow-zero-copy/run.sh sweep   # timing, ~20 min; leave the machine idle and plugged in
./benchmark/arrow-zero-copy/run.sh bench   # JMH, ~3 min
./benchmark/arrow-zero-copy/report.py > benchmark/arrow-zero-copy/results/sweep-report.md
```

`check` runs the Arrow job at k = 1, 16 and 64, with 1, 7 and 1024 rows per batch. 7 leaves a
partial last batch. Each run must:

- print the same `CHECKSUM` line as `SensorScoringReference`;
- report in its `ARROW_NATIVE` line that the kernel saw every record;
- report that the zero-copy check compared at least one buffer (`zeroCopyBuffers` > 0).

The expected checksums are the ones in `../sensor-bench/README.md`.

To run the job by hand:

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_unsafe_memory_access=true \
     -Dflink.accel.bridge.lib=$PWD/native-kernel/build/libflinkaccel_bridge.so \
     -Dsensor.kernel.lib=$PWD/native-kernel/build/libsensor_arrow_kernel.so \
     -cp "arrow-zero-copy/build/classes:native-kernel/build/flink-accel-bridge.jar:build/vanilla/flink/lib/*:arrow-zero-copy/deps/*" \
     org.apache.flink.demo.SensorScoringArrowJob 2000000 16 checksum 1024
```

Arguments: `[records] [k] [discard|checksum] [batchSize]`.

- `--add-opens` lets Arrow's memory module read direct buffers' addresses on JDK 17.
- `arrow.enable_unsafe_memory_access` turns off `ArrowBuf`'s bounds check on every access.

## Limits

These are deliberate, for this first step:

- **Chained, synchronous pipelines only.** The source and the operator reuse their buffers for
  the next batch, so whatever is downstream must be done with a batch when its `processElement`
  returns. The serializer guard turns any other use into an immediate failure, not wrong output.
- **No per-row event time.** A batch is one Flink record with one timestamp. The `ts` column is
  data, not Flink's record timestamp. The source attaches no timestamps or watermarks.
- **Dense input only.** `ArrowNativeOperator` rejects a batch that already carries a selection
  vector. A second filtering operator downstream would need to accept one.
- **On a cluster,** Arrow Java belongs in `flink/lib/` next to the bridge. `arrow-c-data` loads its
  own JNI library, so it has the same one-classloader limit as `NativeBridge`.
