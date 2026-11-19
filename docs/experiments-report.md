# Flink speed-up experiments: full report

Everything behind the short report page (https://claude.ai/artifact/4H2LGn8UKXSmKhtVVbvw8o): how
each step was built, every number with its source, the raw data, how to rerun it, what we
learned, and the limits. The page has the summary, the steps in short, and the comparison
chart; this file has the rest.

Steps 3–5 are written up in more detail in `docs/microbatch-native-summary.md`, and how they are
built in `docs/microbatch-native-implementation.md`.

## Words used here

| Word | Meaning |
|---|---|
| Record | One item of data that flows through the job, for example one sensor reading |
| Operator | Flink's wrapper around one step of the job (one `map`, one `filter`) |
| Arrow batch | Many records stored together, column by column, in the Apache Arrow format. Flink passes a batch along as one record |
| B | Batch size: records per batch (or per call into C in step 4) |
| ns per record | The whole job's time divided by the number of input records. Lower is better |
| k | How much work the sensor job does per reading: the number of shapes each reading is compared with. k=1 is light, k=64 is heavy |
| Stock Flink | Unmodified Flink 2.4-SNAPSHOT (`benchmark/build/vanilla/flink`) |
| Fusion build | Our modified Flink from step 1 (`benchmark/build/fused/flink`) |
| Tuned Java | Step 5's hand-written Java: all four sensor steps in one function, one record at a time |

## The test jobs

**Sensor job** (`benchmark/job/.../SensorScoringJob.java`): a pretend sensor pipeline over 50
million readings. Each reading has 8 numbers.

```
source → map: stats → filter: not flat → map: score against k shapes → filter: good match → sink
```

- Step 1 computes the mean, spread and peak of the 8 numbers.
- Step 2 drops readings whose spread is under 0.01 (about 15%).
- Step 3 compares the reading's shape with k known shapes and keeps the best match as its score.
- Step 4 keeps readings with a score of at least 0.9 (about 30% survive).

The data is made up inside the source, the same way in every version. That costs about 40 ns per
reading one record at a time, and 33–56 ns per reading in the Arrow versions, depending on batch
size. Details: `docs/microbatch-native-summary.md` §2.

**simple** (`SimpleMapFilterBenchmarkJob`): 200 million numbers, `map(x * 2) → filter(x % 3 == 0)`.

**mixed** (`MixedOperatorsBenchmarkJob`): 200 million numbers,
`flatMap(x → x, x + n) → map(x * 2) → filter(x % 3 == 0) → map(x + 1)`. The flatMap doubles the
records.

simple and mixed were built to make Flink's per-record hand-off cost visible: their functions do
almost nothing.

All jobs run with parallelism 1 and object reuse on. Every version is checked first: it must
print exactly the same checksum as the original job. The sensor job is checked at k = 1, 16 and
64; floating point is bit-exact.

## The steps

### Step 1 — fuse map and filter steps into one operator (October)

**What:** `OperatorChain` (flink-runtime) finds runs of two or more chained map/filter operators
and builds one `MapFilterFusedOperator` that calls their functions back to back in a plain loop.
It only engages with object reuse on.

**Results** (Docker, 200M records, 10 runs each, average wall time):

| Job | Stock | Fusion build | Time saved |
|---|---|---|---|
| simple | 3,644 ms | 3,182 ms | 12.7% |
| mixed | 10,634 ms | 7,926 ms | 25.5% |

On the sensor job (MiniCluster, 21 Oct): 124 → 128 ns at k=1 (no gain), 263 → 247 ns at k=64
(1.06×).

**Correctness:** `MapFilterFusionDemoITCase`, 5/5.
**Raw data:** `benchmark/final-run-2026-10-21/fusion/{simple,mixed}/` (`REPORT.md`,
`metrics.csv`) and `sensor-bench/minicluster-sweep.csv`.
**Rerun:** `./benchmark/benchmark.py simple <vanilla-dist> <fused-dist> --iterations 10` (or
`mixed`).

### Step 2 — generate special Java code for the fused steps (designed, not built)

The fused loop calls each function through an interface the JIT cannot predict (about 2.2 ns per
step, against 0.45 ns for a predictable call). Generating one method per chain would make the
calls predictable. The measured gap puts the saving at roughly 5–6 ns per record on a 3-step
chain. Not built; step 5 tests the same question directly.
**Raw data:** `benchmark/final-run-2026-10-21/jni-crossing/jni-crossing.txt`
(`JniCrossing.javaMonomorphicCall`, `javaMegamorphicCall`).

### Step 3 — call C once per record (ruled out by measurement)

| Measurement (JMH, 21 Oct rerun) | Cost |
|---|---|
| One call into an empty C function | 6.4 ns |
| The 3-step Java hand-off it would replace | 6.6 ns |
| The same C call carrying 4,096 records | 6.9 ns (flat in batch size) |
| Copying a numeric record for C and back | 3.4 ns |
| Copying a record that contains text | 25.2 ns |

Per record, C would cost 6.4 + 3.4 = 9.8 ns to save 6.6 ns, so it loses even if the C work takes
no time. The first measurements (8 Oct) were on JDK 21; these numbers are from the 21 Oct rerun on
JDK 17.
**Raw data:** `benchmark/final-run-2026-10-21/jni-crossing/` (`jni-crossing.txt`,
`marshalling-gc.txt`). **Rerun:** `./benchmark/jni-crossing/run.sh` (~12 min).

### Step 4 — collect records into batches, call C once per batch (October)

**What:** `MicroBatchingNativeOperator` copies each reading into C-readable memory. When the
batch is full, it makes one call into a C function that runs all four steps in one loop, then
turns the survivors back into Java records.

**Results:**

- On their own (JMH, no Flink, 1,024 per call): C ties the best Java at k=1 (60 vs 61 ns) and is
  1.31× faster at k=64. The difference is in step 3's comparison loop.
- Inside Flink: no gain at k=1; 1.16–1.22× faster than the fusion build at k=64.
- Batch size barely mattered: one C call per record was at most ~2 ns per record slower than
  batches of 1,024 (62 vs 60 ns at k=1).

**Raw data:** `benchmark/final-run-2026-10-21/sensor-bench/` (`sweep-report.md`,
`sweep-native.json`, `sweep-batched.json`, `minicluster-sweep.csv`).
**Rerun:** see `benchmark/sensor-bench/README.md`; rebuild the tables with
`./benchmark/sensor-bench/report.py benchmark/final-run-2026-10-21/sensor-bench`.

### Step 5 — hand-tuned Java, all four steps in one function (October)

**What:** `SensorScoring.scoreFused`: the best Java we could write, all four steps in one
function with values in local variables, used as one filter on stock Flink.

**Results:** 120 ns at k=1, the fastest version at the time (1.07× over fusion). 232 ns at k=64
(1.07× over fusion, while C in step 4 got 1.16–1.22×).
**Raw data:** `benchmark/final-run-2026-10-21/sensor-bench/` (`sweep-java.json`,
`sensor-work.txt`, `minicluster-sweep.csv`).

### Step 6 — Arrow batches through Flink, C does the work (17 Nov)

**What:** The source writes readings straight into Arrow columns and emits a whole batch as one
Flink record (`ArrowBatch`). `ArrowNativeOperator` hands the batch's memory to C through the
Arrow C Data Interface; the C function writes four result columns in place and returns the list
of surviving rows. Nothing is copied. Flink is unchanged. `ArrowBatch`'s serializer refuses to
copy, so a batch cannot silently cross a non-chained edge.

**Results** (3 runs, tuned Java from the same run):

| | k=1 | k=64 |
|---|---|---|
| Tuned Java | 124 ns | 229 ns |
| Batches copied to C (step 4), B=4,096 | 130 ns | 212 ns |
| Arrow + C, B=4,096 | 87 ns (1.43×) | 169 ns (1.36×) |

Handing one batch to C costs about 5.4 µs (JMH): 5.4 ns per reading at B=1,024, so C needs big
batches. Arrow + C saved a nearly constant ~40 ns per reading against step 4 at every k. That
points at Flink's per-record cost, not the removed copy.
**Raw data:** `benchmark/arrow-run-2026-11-17/step1/`. **Code:** `2a510aab5b3`.
**Rerun:** `./benchmark/arrow-zero-copy/run.sh check`, `run.sh sweep`, `run.sh bench`.

### Step 7 — Arrow batches through Flink, Java does the work (17 Nov)

**What:** The same batches as step 6, with Java doing the work. Two versions on the sensor job:

- **one function**: `SensorArrowScoring.scoreFused`, all four steps in one loop over the batch;
- **four functions**: `map → filter → map → filter`, one batch function per original step. Maps
  add result columns; filters shorten the list of surviving rows.

**Results** (ns per record, each version at its usual batch size):

| Version | k=1 | k=4 | k=16 | k=64 |
|---|---|---|---|---|
| Tuned Java | 125 | 127 | 159 | 231 |
| Batches copied to C, B=64 | 127 | 132 | 154 | 210 |
| Arrow + C, B=4,096 | 84 | 90 | **109** | **164** |
| Arrow + Java, one function, B=64 | **66** | 90 | 116 | 179 |
| Arrow + Java, four functions, B=64 | 68 | **88** | 120 | 209 |

- Java on batches beat tuned Java by 1.29–1.88× with no C at all.
- With light work, one Java function was the fastest version (1.27× over Arrow + C).
- With heavy work, C was 1.06–1.09× faster than the best Java (C at B=4,096, Java at B=64).
- Java was fastest with small batches (64), C with large ones (4,096, because of the hand-off
  cost). A possible reason is that a 64-reading batch (~7 KB) stays in the fastest CPU cache.
  That has not been measured.
- Four functions matched one function at k=1 and were 17% slower at k=64.
- Four functions at B=256 measured 64 / 84 / 119 ns at k=1–16, but the k=1 cell was noisy (35%
  spread). Step 8 remeasured it at 88 ns, so B=64 is used above.

**Raw data:** `benchmark/arrow-run-2026-11-17/step2/` (204 runs). **Code:** `2790391f414`.
**Rerun:** `./benchmark/arrow-zero-copy/run.sh check` and `run.sh sweep` (~40 min).

### Step 8 — whole jobs, each function rewritten for Arrow batches (17 Nov)

**What:** simple, mixed and the sensor job on stock Flink, with each step's function rewritten to
process a whole Arrow batch (`MapFilterArrowJob`, and `SensorScoringArrowJob java-steps`). Each
job keeps its steps in the same order:

- a map writes a new column;
- a filter shortens the list of surviving rows;
- the flatMap writes a new, longer column.

As a check, the original functions also ran unchanged inside each batch, called once per record
("wrapped"). The original jobs ran on stock Flink and on the fusion build in the same session.

**Results** (ns per input record, median of 5 runs; speed-up over stock Flink in brackets; each
Arrow version at its best batch size):

| Job | Stock Flink | Fusion build | Wrapped | Rewritten |
|---|---|---|---|---|
| simple | 20.5 | 17.7 (1.16×) | 8.7 (2.36×, B=256) | 3.4 (6.00×, B=4,096) |
| mixed | 54.7 | 39.8 (1.38×) | 21.4 (2.56×, B=4,096) | 5.2 (10.47×, B=4,096) |
| sensor, k=1 | 127.3 | 126.6 (1.01×) | — | 66.9 (1.90×, B=64) |
| sensor, k=64 | 262.6 | 247.9 (1.06×) | — | 215.5 (1.22×, B=64) |

- With cheap functions, Flink's per-record cost is almost the whole job, and batches remove most
  of it.
- Rewriting matters. Wrapped functions still create an object for every value and make a call per
  record and per step, so they get less than half the gain.
- The sensor job's functions build a Java object, so wrapping them would mean building one per
  record (step 4 again). There is no wrapped version.
- simple and mixed improve up to B=1,024–4,096 and then flatten. The sensor job is fastest at
  B=64.
- The original jobs read from Flink's `fromSequence` source; the Arrow versions use a source that
  makes whole batches. Both produce the same values in the same order.

Every batch size is in the raw report.
**Raw data:** `benchmark/arrow-run-2026-11-17/jobs/` (170 runs). **Code:** `96d36d854c9`.
**Rerun:** `./benchmark/arrow-zero-copy/run.sh jobs-check`, then `RUNS=5
./benchmark/arrow-zero-copy/run.sh jobs-sweep` (~20 min), then `./benchmark/arrow-zero-copy/jobs-report.py`.

## Small costs behind the big numbers

| What | Cost | What it told us |
|---|---|---|
| One hand-off between two chained Flink steps | ~2 ns | The 3-step hand-off is 6.6 ns. Fusion removes it |
| One call into an empty C function | 6.4 ns | Too expensive per record (step 3) |
| The same call carrying 4,096 records | 6.9 ns | Flat, so batching makes the call free |
| Copying a numeric record for C and back | 3.4 ns | 25.2 ns for records with text |
| Handing one Arrow batch to C | ~5.4 µs | 5.4 ns per reading at B=1,024, ~340 ns at B=16. Why C needs big batches |
| Flink's own cost per record, sensor job | 40–60 ns | What one batch per record saves: ~40 ns with C (step 6), 47–60 ns with Java (step 8) |
| Flink's own cost per record, simple job | ~17 ns | 20.5 → 3.4 ns with batches |

## What we learned

1. **Batch what Flink sees, not only the C call.** Batching the C call saved at most ~2 ns per
   reading. Batching the records Flink passes around saved 40–60 ns on the sensor job and ~17 ns
   on simple. It needs no change to Flink.
2. **Rewrite the functions; don't wrap them.** Original functions called per record inside a batch
   got 2.4–2.6×. Rewritten to loop over a column, they got 6–10×. For small functions the
   rewrite is mechanical, so a generator could do it.
3. **The language matters only for heavy work.** On Arrow batches, C beat the best Java by
   1.06–1.09× at k=16–64 and lost by 1.27× at k=1.
4. **The best batch size depends on the record.** Small number records: 1,024–4,096. Sensor
   readings in Java: 64. C: large, because of the hand-off cost.
5. **Measure before explaining.** The removed copy in step 6 looked like the obvious reason for
   the gain, and it was not.

## Limits

- One laptop, one thread: Intel i5-12500H, pinned to its fast cores (CPUs 0–7), 4.5 GHz ceiling,
  on AC, local MiniCluster, parallelism 1. No real cluster yet.
- No state, keys, windows or shuffles: every job is a stateless map/filter chain.
- Arrow batches only cross chained edges. A shuffle would need a real batch serializer.
- The sources write data directly. A real job reads Kafka or files, and turning that into Arrow
  costs time we have not measured.
- simple and mixed are a best case for batching, because their functions do almost nothing.
- The cache explanation in step 7 is unmeasured (needs `perf stat`).
- Stock Flink and the fusion build in the sensor comparison chart are from 21 Oct; the rest is
  from 17 Nov. Tuned Java ran on both days and agreed within 5%.

## The runs

| Run | When | Code | Raw data |
|---|---|---|---|
| Final run (steps 1–5), `benchmark/run-all.sh` | 21 Oct 2026, 18:33–19:39 | `c93e49539a7` | `benchmark/final-run-2026-10-21/` (README, full console log in `run-all-output.txt`) |
| Arrow step 1 (step 6, plus the JMH hand-off cost) | 17 Nov 2026, from 09:51 | `2a510aab5b3` | `benchmark/arrow-run-2026-11-17/step1/` |
| Arrow step 2 (step 7) | 17 Nov 2026, 11:24–11:55 | `2790391f414` | `benchmark/arrow-run-2026-11-17/step2/` |
| Whole jobs (step 8) | 17 Nov 2026, 14:40–14:59 | `96d36d854c9` | `benchmark/arrow-run-2026-11-17/jobs/` |

Both raw-data folders are committed copies, and their READMEs explain how to rebuild each table
from them. The November code was committed right after each run.
