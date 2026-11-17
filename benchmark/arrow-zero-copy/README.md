# Arrow zero-copy: the sensor job with Arrow batches as records

The sensor job from `../sensor-bench/`, with **Arrow record batches** as the stream's records
instead of `SensorReading` objects. The source writes readings straight into Arrow columns. Then
one of three engines does the four steps, each writing its results into Arrow columns and
returning a selection vector of the survivors:

- **`native`**: the operator hands a C kernel the columns' addresses through the Arrow C Data
  Interface. Nothing is copied in or out.
- **`java`**: one Java function runs all four steps over the batch. It is the Java twin of the C
  kernel.
- **`java-steps`**: the four steps stay four separate Java functions, `map -> filter -> map ->
  filter`. Each takes and returns a whole batch.

**Whole jobs** (`MapFilterArrowJob`, `run.sh jobs-check` / `jobs-sweep`): the two fusion
benchmark jobs from `../job/`, `simple` (`map -> filter`) and `mixed` (`flatMap -> map -> filter ->
map`), with every user function rewritten to take and return Arrow batches, on stock Flink.
Compared with the original jobs on stock Flink and on the fusion build, and with the original
functions wrapped, unchanged, in a generic per-row batch adapter. The sweep also reruns the sensor
job's `java-steps` engine against the original sensor job on both builds.

Why, what was found, and what's next: `docs/arrow-zero-copy-experiment.md`.

## Files

| File | What it is |
|---|---|
| `job/.../ArrowBatch.java` | The stream record: an Arrow `VectorSchemaRoot` plus an optional selection vector. Its type information's serializer throws if Flink ever copies or serializes a batch |
| `job/.../ArrowMemory.java` | The shared Arrow root allocator, and a helper to mark every row valid |
| `job/.../ResultColumns.java` | The result columns a step owns and appends to the batches it passes on |
| `job/.../ArrowNativeOperator.java` | Generic operator: export the batch (no copy), call the kernel through `NativeBridge`, emit input columns + result columns + selection vector. Checks on the first batch that the export really shared the buffers |
| `job/.../SensorArrow.java` | The sensor job's Arrow schema, generation straight into columns, and reading a row back for the checksum |
| `job/.../SensorArrowScoring.java` | The four steps in Java over Arrow columns: as one function (`scoreFused`), and as separate steps |
| `job/.../ArrowBatchFunction.java` | Base for user functions that take and return a whole `ArrowBatch` (a `RichMapFunction` to Flink) |
| `job/.../MapFilterArrowJob.java` | The `simple` and `mixed` jobs: `rows` (the original), `arrow` (functions rewritten for batches) or `arrow-wrapped` (original functions in per-row adapters) |
| `log4j-fusion.properties` | Logging for the whole-job runs: errors, plus the fusion build's "Fusing chained run" lines |
| `jobs-report.py` | Turns `results/jobs/sweep.csv` into Markdown tables |
| `job/.../SensorScoringArrowJob.java` | The job: batch source → engine → sink (discard or checksum). The Java engines are plain `RichMapFunction`s on `ArrowBatch` |
| `../native-kernel/kernels/sensor_arrow_kernel.c` | `sensor_score_arrow`: `sensor_score`'s arithmetic, reading and writing Arrow columns in place |
| `src/.../ArrowExport.java` | JMH: the export's cost per batch, and generation's cost per row |
| `run.sh` | Fetches Arrow Java, builds, then `check`, `sweep` and/or `bench` |
| `report.py` | Turns `results/sweep.csv` into Markdown tables. `results/step1/` keeps the first sweep (C only) |

The job classes are in a separate source tree from `../job/` because they need Arrow Java to
compile. `../sensor-bench/minicluster-sweep.sh` compiles `../job/` alone, without Arrow.

## Running

Needs `../build/vanilla/flink` (the stock distribution `benchmark.py` stages), a JDK 17 and gcc;
the whole-job modes also need `../build/fused/flink` (the fusion build).
Arrow Java 19.0.0 and its dependencies are downloaded into `deps/` on the first run.

```bash
./benchmark/arrow-zero-copy/run.sh check   # correctness, ~1 min
./benchmark/arrow-zero-copy/run.sh sweep   # timing, ~20 min; leave the machine idle and plugged in
./benchmark/arrow-zero-copy/run.sh bench   # JMH, ~3 min
./benchmark/arrow-zero-copy/report.py > benchmark/arrow-zero-copy/results/sweep-report.md

./benchmark/arrow-zero-copy/run.sh jobs-check          # whole jobs: correctness, ~2 min
RUNS=5 ./benchmark/arrow-zero-copy/run.sh jobs-sweep   # whole jobs: timing, ~17 min
./benchmark/arrow-zero-copy/jobs-report.py > benchmark/arrow-zero-copy/results/jobs/sweep-report.md
```

`jobs-check` runs `simple` and `mixed` with every engine (1, 7 and 1024 rows per batch for the
Arrow ones) and on the fusion build; each must print the stock `rows` run's `CHECKSUM` line (a
count and an order-sensitive hash of every value reaching the sink), and the fusion build must
log that it fused 2 (`simple`) or 3 (`mixed`) operators. It also checks the sensor job on the
fusion build (4 operators fused, reference checksum).

`check` runs every engine at k = 1, 16 and 64, with 1, 7 and 1024 rows per batch. 7 leaves a
partial last batch. Each run must print the same `CHECKSUM` line as `SensorScoringReference`. A
`native` run must also:

- report in its `ARROW_NATIVE` line that the kernel saw every record;
- report that the zero-copy check compared at least one buffer (`zeroCopyBuffers` > 0).

The expected checksums are the ones in `../sensor-bench/README.md`.

To run the job by hand:

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_unsafe_memory_access=true \
     -Dflink.accel.bridge.lib=$PWD/native-kernel/build/libflinkaccel_bridge.so \
     -Dsensor.kernel.lib=$PWD/native-kernel/build/libsensor_arrow_kernel.so \
     -cp "arrow-zero-copy/build/classes:native-kernel/build/flink-accel-bridge.jar:build/vanilla/flink/lib/*:arrow-zero-copy/deps/*" \
     org.apache.flink.demo.SensorScoringArrowJob 2000000 16 checksum 1024 native
```

Arguments: `[records] [k] [discard|checksum] [batchSize] [native|java|java-steps]`.
`MapFilterArrowJob` takes `[simple|mixed] [records] [rows|arrow|arrow-wrapped] [discard|checksum]
[batchSize]`.

- `--add-opens` lets Arrow's memory module read direct buffers' addresses on JDK 17.
- `arrow.enable_unsafe_memory_access` turns off `ArrowBuf`'s bounds check on every access.

## Limits

These are deliberate, for this first step:

- **Chained, synchronous pipelines only.** The source and the operator reuse their buffers for
  the next batch, so whatever is downstream must be done with a batch when its `processElement`
  returns. The serializer guard turns any other use into an immediate failure, not wrong output.
- **No per-row event time.** A batch is one Flink record with one timestamp. The `ts` column is
  data, not Flink's record timestamp. The source attaches no timestamps or watermarks.
- **Dense input only, for the one-function engines.** `ArrowNativeOperator` and the `java`
  engine reject a batch that already carries a selection vector. The `java-steps` functions
  accept one, since every step after the first filter needs to.
- **On a cluster,** Arrow Java belongs in `flink/lib/` next to the bridge. `arrow-c-data` loads its
  own JNI library, so it has the same one-classloader limit as `NativeBridge`.
