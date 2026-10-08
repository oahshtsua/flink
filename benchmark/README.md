# Map-Filter Fusion Benchmark

Compares a stock Flink runtime against the Map-Filter fusion prototype (`OperatorChain` /
`MapFilterFusedOperator` in `flink-runtime`) by running one registered benchmark job
against two Flink distributions inside Docker and measuring wall-clock runtime.

This harness is deliberately simple: give it a job name, it runs that job, times it, and
writes the result down. It does not check whether the job's output is correct - see
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
3. Run the benchmark (requires Docker, and the running user must be able to use it, plus
   Python 3.10+):
   ```bash
   ./benchmark/benchmark.py <job> <vanilla-dist-dir> <fused-dist-dir> [--count N] [--iterations N]
   ```
   - `<job>`: which registered job to run (see **Benchmark jobs** below). List them with
     `./benchmark/benchmark.py --list-jobs`.
   - `<vanilla-dist-dir>` / `<fused-dist-dir>`: paths to the two distributions built
     above (the directory containing `bin/`, `lib/`, `conf/`).
   - `--count` (default `200000000`): records processed per run.
   - `--iterations` (default `5`): job submissions per variant.

   Each invocation benchmarks exactly one job; run it again with a different job name to
   benchmark another.

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

That test (5 scenarios) covers the same transforms these benchmark jobs run at scale - a
trivial map/filter, a map/filter over a non-trivial type with a compound predicate, a
chain with a non-Map/Filter operator mixed in (proving it's left out of the fused run), a
long uninterrupted run of 5 Map/Filter operators (proving the fusion isn't limited to a
fixed length), and a non-Map/Filter operator splitting one run into two - asserting both
exact output and the exact fusion-event count/run-length for each.

## Benchmark jobs

Registered in `benchmark.py`'s `JOBS` dict; each is a plain class with a
`main(String[] args)` under `job/org/apache/flink/demo/`, taking an optional
record-count argument:

- **`simple`** - `SimpleMapFilterBenchmarkJob`: a single, long, tight chain of nothing but
  `map -> filter`. Measures the fusion's effect in isolation.
- **`mixed`** - `MixedOperatorsBenchmarkJob`: `flatMap -> map -> filter -> map`. The
  `flatMap` stays a separate operator (it's not a Map or a Filter); the trailing
  `map -> filter -> map` fuses into one 3-step operator, not just the adjacent pair.
  Measures whether the fusion still pays off once the fused run is only one part of a
  longer pipeline instead of the whole job.

To benchmark a new job, add a class next to these two (parallelism 1, object reuse
enabled - the condition the fusion requires to engage) and register it in
`benchmark.py`'s `JOBS` dict; it picks it up automatically (it shows up in
`--list-jobs` and is valid as the `<job>` argument), no other changes needed.

## What runs it

- **`benchmark.py`** (host side, plain functions in one file - compile, build, run,
  summarize, report): compiles every job source into one `benchmark.jar`, `docker
  build`s `flink-benchmark:vanilla` / `flink-benchmark:fused`, `docker run`s each with
  the requested job/count/iterations passed in as env vars (streaming output live), then
  reads the `metrics.csv` `entrypoint.sh` wrote and renders `REPORT.md` from it plus the
  job's own Javadoc.
- **`entrypoint.sh`** (inside each container, bash - a thin wrapper around Flink's own
  shell tooling, so it stays shell rather than needing a Python runtime added to the
  JRE-only image): starts a one-node standalone Flink cluster, submits the job named by
  the `JOB_CLASS` env var `ITERATIONS` times via `bin/flink run`, and tears the cluster
  down afterwards. Each iteration prints a machine-readable `RESULT variant=... job=...
  iter=... runtime_ms=... fusion_events=... log=...` line and appends a row to
  `metrics.csv`.

## Where logs and metrics go

Each run writes to `benchmark/results/<run-id>-<job>/` (gitignored, regenerated every
run - `<run-id>` is a UTC timestamp, e.g. `20261006T093222Z-simple`):

| File | Contents |
|---|---|
| `REPORT.md` | The job's description (pulled from its own Javadoc), a per-run metrics table, the summary, and an index of every other file in this directory |
| `metrics.csv` | The same per-run metrics as `REPORT.md`'s table, as CSV: `variant,job,iter,count,runtime_ms,fusion_events,log_file` |
| `<variant>-<job>-iter<N>.log` | That iteration's `bin/flink run` CLI output, plus the jobmanager/taskexecutor log lines written during that specific iteration (including any `[CustomOptimizer]` fusion messages) |
| `raw-logs-<variant>-<job>/` | That (variant, job) combination's full, unsliced logs Flink itself writes to `log/`, covering its whole run (all iterations, not just one) |

`fusion_events` counts actual `[CustomOptimizer] Fusing chained ...` decisions - not
every log line tagged `[CustomOptimizer]`, since `MapFilterFusedOperator`'s own
open/close logs (see below) share that tag but aren't themselves fusion events.

## Verbose fusion logging

With this prototype in the classpath:

- `OperatorChain` logs every fusion decision at INFO - how many operators got fused,
  each step's kind and function class name, and the task name - and every near-miss at
  DEBUG (why a run didn't extend further: object reuse disabled, not a
  `StreamMap`/`StreamFilter`, wrong fan-out, or the next operator isn't a
  `StreamMap`/`StreamFilter` either).
- `MapFilterFusedOperator` logs at INFO on `open()` (the step count and each step's kind
  and function class name) and `close()` (processed / passed / filtered-out record
  counts for that operator instance).
