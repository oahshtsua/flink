# Final run, 2026-10-21

The raw results behind every number in `docs/microbatch-native-summary.md`, the JNI README and the
presentation. Produced by one run of `../run-all.sh` on 2026-10-21, 18:33–19:39, plus two short
follow-up runs (marked below). The normal output folders (`results/`) are gitignored; this is a
copy of the files that matter, kept in the repo as a reference.

## Setup

| | |
|---|---|
| Commit | `c93e49539a7` (the run-all script was committed right after, unchanged) |
| Machine | Intel Core i5-12500H laptop; CPUs 0–7 are P-cores, 8–15 E-cores |
| Clock | 4.5 GHz ceiling, logged before every step (AC power and the performance profile were set; the script's check for them printed `?` and has been fixed since) |
| OS | Fedora, Linux 7.2.11 |
| JDK | OpenJDK 17.0.20.1 for everything, including the JNI benchmark |
| C compiler | GCC 16.2.1 |
| Flink | 2.4-SNAPSHOT: stock = upstream `953f843d578`, fused = the same plus the fusion commits |

## Files

| File | What it is |
|---|---|
| `run-all-output.txt` | The complete console output of `run-all.sh`: every step's start and end time, the CPU clock ceiling before each step, the correctness checks and the full JMH output. Renamed from `run-all.log` because Flink's `.gitignore` ignores `*.log`. |
| `fusion/simple/`, `fusion/mixed/` | Experiment 1. `REPORT.md` and `metrics.csv` from `benchmark.py`: 10 runs per build, 200M records each. The per-run Flink logs that `REPORT.md` links to are not included (2.4 MB). |
| `jni-crossing/jni-crossing.txt` | Experiment 2, the JMH output for all three benchmark classes. Copied out of `run-all-output.txt`, because a later run overwrote the original file. |
| `jni-crossing/marshalling-gc.txt` | Follow-up run: `jni-crossing/run.sh -prof gc Marshalling`, for the allocation figures. |
| `sensor-bench/sweep-{java,batched,native}.json` | Experiment 3, stage 1 (Java vs C on their own), as JMH JSON. |
| `sensor-bench/minicluster-sweep.csv` | Experiment 3, stage 2 (the whole job inside Flink): one row per run. |
| `sensor-bench/sweep-report.md` | The tables `report.py` makes from the four files above. |
| `sensor-bench/sensor-work.txt` | Follow-up run: `sensor-bench/run.sh SensorWork`, the Java work per record without data generation. |

To rebuild the tables from this folder:

```bash
./benchmark/sensor-bench/report.py benchmark/final-run-2026-10-21/sensor-bench
```

The output is identical to `sensor-bench/sweep-report.md`.

## Headline results

| Experiment | Result |
|---|---|
| Correctness | `MapFilterFusionDemoITCase` 5/5 passed; every sensor-job version matched the reference checksum at k = 1, 16 and 64 |
| 1. Fusion | simple job 3,644 → 3,182 ms (12.7% less); mixed job 10,634 → 7,926 ms (25.5% less) |
| 2. Calling C | empty call 6.4 ns vs the 3-step Java dispatch 6.6 ns (net); flat from 1 to 4,096 records per call |
| 3. Native, alone | C ties the best Java at k=1 (60 vs 61 ns); 1.31× faster at k=64 |
| 3. Native, inside Flink | tuned Java fastest at k=1 (1.07× over fusion); C 1.16–1.22× over fusion at k=64 |
