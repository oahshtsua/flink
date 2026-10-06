# Map-Filter Fusion Benchmark

Compares a stock Flink runtime against the Map-Filter fusion prototype (`OperatorChain` /
`MapFilterFusedOperator` in `flink-runtime`) by running each registered benchmark job
against two Flink distributions inside Docker and measuring wall-clock runtime.

This harness is deliberately simple because it only has one job: run a job, time it,
write the result down. It does not check whether a job's output is correct - see
**Correctness lives elsewhere**, below.

## Running it

1. Build the "fused" distribution (this repo, current checkout):
   ```bash
   ./mvnw clean install -DskipTests -Dfast -Pskip-webui-build -T1C -pl flink-dist -am
   ```
2. Build a "vanilla" distribution from the commit right before the fusion prototype, in a
   separate worktree so it doesn't disturb your checkout:
   ```bash
   git worktree add /tmp/flink-vanilla <commit-before-fusion>
   cd /tmp/flink-vanilla
   ./mvnw clean install -DskipTests -Dfast -Pskip-webui-build -T1C -pl flink-dist -am
   ```
3. Run the benchmark (requires Docker, and the running user must be able to use it):
   ```bash
   ./benchmark/run_benchmark.sh <vanilla-dist-dir> <fused-dist-dir> [count] [iterations]
   ```
   - `<vanilla-dist-dir>` / `<fused-dist-dir>`: paths to the two distributions built
     above (the directory containing `bin/`, `lib/`, `conf/`).
   - `count` (default `200000000`): records processed per run of each job.
   - `iterations` (default `5`): job submissions per variant per job.

## Correctness lives elsewhere

A faster runtime doesn't prove the fusion is *correct* - it could just as easily be fast
because it's silently dropping or corrupting records. This harness doesn't check that;
instead, every job it runs is trusted to already be correct, proven once (not per
benchmark run) by
`org.apache.flink.streaming.runtime.tasks.MapFilterFusionDemoITCase` (flink-runtime's test
sources):

```bash
./mvnw test -pl flink-runtime -Dtest=MapFilterFusionDemoITCase
```

That test covers the same transforms these benchmark jobs run at scale - a trivial
map/filter, a map/filter over a non-trivial type with a compound predicate, and a chain
with other operators mixed in around the one adjacent map/filter pair - asserting both
exact output and the exact fusion-event count for each.

## Benchmark jobs

Registered in the `JOBS` array at the top of `run_benchmark.sh`; each is a plain class
with a `main(String[] args)` under `job/org/apache/flink/demo/`, taking an optional
record-count argument:

- **`simple`** - `SimpleMapFilterBenchmarkJob`: a single, long, tight chain of nothing but
  `map -> filter`. Measures the fusion's effect in isolation.
- **`mixed`** - `MixedOperatorsBenchmarkJob`: `flatMap -> map -> filter -> map`, mirroring
  the ITCase's mixed-operator scenario at benchmark scale. Measures whether the fusion
  still pays off once it's one step of a longer chain instead of the whole job.

To benchmark a new job, add a class next to these two (parallelism 1, object reuse
enabled - the condition the fusion requires to engage) and add its `label:class` entry to
`JOBS`; `run_benchmark.sh` picks it up automatically, no other changes needed.

## What runs it

- **`run_benchmark.sh`** (host side): compiles every `*.java` under
  `job/org/apache/flink/demo/` into one `benchmark.jar`, stages it together with each
  distribution and `entrypoint.sh` into `build/vanilla/` and `build/fused/`, and
  `docker build`s `flink-benchmark:vanilla` / `flink-benchmark:fused` from those
  contexts. It then `docker run`s each image once per registered job - bind-mounting a
  results directory into the container (see below) - and finishes by printing a
  per-job runtime summary and writing `REPORT.md`.
- **`entrypoint.sh`** (inside each container): starts a one-node standalone Flink
  cluster, submits the job named by the `JOB_CLASS` env var `ITERATIONS` times via
  `bin/flink run`, and tears the cluster down afterwards. Each iteration prints a
  machine-readable `RESULT variant=... job=... iter=... runtime_ms=...
  fusion_events=... log=...` line and appends a row to `metrics.csv`.

## Where logs and metrics go

Each run writes to `benchmark/results/<run-id>/` (gitignored, regenerated every run -
`<run-id>` is a UTC timestamp, e.g. `20261006T093222Z`):

| File | Contents |
|---|---|
| `REPORT.md` | Each job's description (pulled from its own Javadoc), a per-run metrics table, the summary, and an index of every other file in this directory |
| `metrics.csv` | The same per-run metrics as `REPORT.md`'s table, as CSV: `variant,job,iter,count,runtime_ms,fusion_events,log_file` |
| `<variant>-<job>-iter<N>.log` | That iteration's `bin/flink run` CLI output, plus the jobmanager/taskexecutor log lines written during that specific iteration (including any `[CustomOptimizer]` fusion messages) |
| `raw-logs-<variant>-<job>/` | That (variant, job) combination's full, unsliced logs Flink itself writes to `log/`, covering its whole run (all iterations, not just one) |
| `run.log` | Combined stdout of every `docker run` invocation - the `RESULT ...` lines plus image-build/cluster-startup output |

`fusion_events` counts actual `[CustomOptimizer] Fusing chained ...` decisions - not
every log line tagged `[CustomOptimizer]`, since `MapFilterFusedOperator`'s own
open/close logs (see below) share that tag but aren't themselves fusion events.

## Verbose fusion logging

With this prototype in the classpath:

- `OperatorChain` logs every Map->Filter fusion decision at INFO, including the fused
  functions' class names and the task name, and every near-miss at DEBUG (why a
  candidate Map wasn't fused - object reuse disabled, not a `StreamMap`, wrong fan-out,
  or the downstream operator isn't a `StreamFilter`).
- `MapFilterFusedOperator` logs at INFO on `open()` (the fused functions' class names)
  and `close()` (processed / passed / filtered-out record counts for that operator
  instance).
