# Arrow runs, 2026-11-17

The raw results behind every number from the Arrow experiment (Idea 6, steps 1–3): Arrow record
batches as the stream's records, on stock Flink. Produced by `../arrow-zero-copy/run.sh` in three
runs on 2026-11-17. The normal output folder (`arrow-zero-copy/results/`) is gitignored; this is
a copy of it, kept in the repo as a reference.

## Setup

| | |
|---|---|
| Machine | Intel Core i5-12500H laptop; every timed run pinned to CPUs 0–7 (the P-cores) |
| Clock | 4.5 GHz ceiling, performance profile, on AC. Logged at the start and end of each sweep, in `sweep-console.txt` |
| JDK, C compiler | OpenJDK 17, GCC 16.2.1 |
| Flink | 2.4-SNAPSHOT. Stock is `../build/vanilla/flink`; the fusion build (step 3 only) is `../build/fused/flink` |
| Arrow | Arrow Java 19.0.0, run with `-Darrow.enable_unsafe_memory_access=true` |
| Method | Local MiniCluster, parallelism 1, object reuse on. Interleaved runs, each timed by Flink's net runtime (job start-up excluded) |

## Files

| Folder | Run | Code | What it is |
|---|---|---|---|
| `step1/` | from 09:51 | `2a510aab5b3` | Step 1, Arrow + C. `run.sh sweep` (sensor job, 50M readings, 3 runs per cell): tuned Java, the copying native operator and Arrow + C at k = 1, 4, 16, 64 and 64–4,096 readings per batch. Plus `run.sh bench`, JMH `ArrowExport`: the cost of handing a batch to C, and of generating a batch (`arrow-export.{txt,json}`) |
| `step2/` | 11:24–11:55 | `2790391f414` | Step 2, Arrow + Java. The same sweep plus the two Java engines (one function, four steps): 204 runs |
| `jobs/` | 14:40–14:59 | `96d36d854c9` | Step 3, whole jobs. `RUNS=5 run.sh jobs-sweep`: `simple` and `mixed` (200M records) and `sensor` (50M, k = 1 and 64) as the original job on stock Flink and on the fusion build, and as Arrow-batch functions (rewritten, and for `simple`/`mixed` also wrapped per row). 170 runs |

In each folder, `sweep.csv` has one row per run, `sweep-report.md` the tables made from it, and
`sweep-console.txt` the console output, including the machine state. Each run's code was
committed right after the run.

To rebuild the tables from this folder:

```bash
./benchmark/arrow-zero-copy/report.py benchmark/arrow-run-2026-11-17/step2
./benchmark/arrow-zero-copy/jobs-report.py benchmark/arrow-run-2026-11-17/jobs
# step 1's tables come from report.py as of 2a510aab5b3 (step 2 rewrote it):
git show 2a510aab5b3:benchmark/arrow-zero-copy/report.py | python3 - benchmark/arrow-run-2026-11-17/step1
```

Each output is identical to the folder's `sweep-report.md`.

## Headline results

ns per input record, whole job, median of the runs; speed-ups in brackets.

| Step | Result |
|---|---|
| Correctness | Every engine printed the reference `CHECKSUM` line: the sensor job at k = 1, 16 and 64 with 1, 7 and 1,024 rows per batch, and `simple`/`mixed` with every engine; the fusion build fused 2, 3 and 4 operators |
| 1. Arrow + C | 87 ns at k=1 and 169 ns at k=64 (4,096 per batch), against tuned Java's 124 and 229 (1.43×, 1.36×). Handing one batch to C costs ~5.4 µs (JMH) |
| 2. Arrow + Java | One Java function at 64 per batch: 66 ns at k=1 (1.88× over tuned Java, 1.27× over Arrow + C). At k=64, Arrow + C stays ahead (164 vs 179 ns) |
| 3. Whole jobs | Functions rewritten for batches against the original job on stock Flink: `simple` 20.5 → 3.4 ns (6.00×), `mixed` 54.7 → 5.2 ns (10.47×), sensor 127.3 → 66.9 ns at k=1 (1.90×) and 262.6 → 215.5 ns at k=64 (1.22×). The fusion build got 1.16×, 1.38×, 1.01× and 1.06× |
