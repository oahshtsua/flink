# Map-Filter Fusion Benchmark

Compares a stock Flink runtime against the Map-Filter fusion prototype (`OperatorChain` /
`MapFilterFusedOperator` in `flink-runtime`) by running the same trivial `map -> filter`
job against two Flink distributions inside Docker.

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
   - `count` (default `200000000`): records processed per run.
   - `iterations` (default `5`): job submissions per variant.

## What runs it

- **`run_benchmark.sh`** (host side): compiles
  `job/org/apache/flink/demo/MapFilterFusionBenchmarkJob.java` into `benchmark.jar`,
  stages it together with each distribution and `entrypoint.sh` into `build/vanilla/` and
  `build/fused/`, and `docker build`s `flink-benchmark:vanilla` / `flink-benchmark:fused`
  from those contexts. It then `docker run`s each image in turn - bind-mounting a results
  directory into the container (see below) - and finishes by printing a runtime summary
  and writing `REPORT.md`.
- **`entrypoint.sh`** (inside each container): starts a one-node standalone Flink
  cluster, submits the benchmark job `ITERATIONS` times via `bin/flink run`, and tears
  the cluster down afterwards. Each iteration prints a machine-readable
  `RESULT variant=... iter=... runtime_ms=... fusion_events=... checksum_count=...
  checksum_sum=... log=...` line and appends a row to `metrics.csv`.

## Correctness, not just speed

A faster runtime doesn't prove the fusion is *correct* - it could just as easily be fast
because it's silently dropping or corrupting records. So the benchmark job's sink
(`ChecksumSink`) doesn't discard its output: it accumulates the count and sum of every
record that reaches it and prints both (`CHECKSUM count=... sum=...`) via `System.out`
when the job finishes. That runs on the TaskManager, not the client, so `entrypoint.sh`
parses it out of the taskexecutor's stdout (`.out`) file rather than the `bin/flink run`
CLI output or either log4j-based `.log` file. After both variants run, `run_benchmark.sh`:

1. Computes the expected count and sum analytically from `count` (closed-form, in exact
   64-bit integer arithmetic - the sums involved exceed `2^53` at realistic record counts,
   where floating-point comparison would start silently losing precision).
2. Checks that every one of vanilla's iterations agrees with every other (the job is
   deterministic, so they must), and likewise for fused.
3. Checks vanilla's checksum against fused's, and both against the analytically-expected
   value.

The result is a `## Correctness` section in `REPORT.md` with an explicit `VERDICT:
PASS`/`FAIL` and, on failure, the reasons why - and `run_benchmark.sh` itself exits
non-zero if it fails, so a correctness regression can't be waved off as "the numbers
looked about the same."

## Where logs and metrics go

Each run writes to `benchmark/results/<run-id>/` (gitignored, regenerated every run -
`<run-id>` is a UTC timestamp, e.g. `20261006T093222Z`):

| File | Contents |
|---|---|
| `REPORT.md` | Job description (pulled from the job class's own Javadoc), a per-run metrics table, the summary, and an index of every other file in this directory |
| `metrics.csv` | The same per-run metrics as `REPORT.md`'s table, as CSV: `variant,iter,count,runtime_ms,fusion_events,checksum_count,checksum_sum,log_file` |
| `<variant>-iter<N>.log` | That iteration's `bin/flink run` CLI output, the jobmanager/taskexecutor log lines written during that specific iteration (including any `[CustomOptimizer]` fusion messages), and the taskexecutor stdout lines from that iteration (including its `CHECKSUM` line) |
| `flink-*-standalonesession-*.log`, `flink-*-taskexecutor-*.log`, `flink-*-taskexecutor-*.out`, `flink-*-client-*.log` | The full, unsliced logs/stdout Flink itself writes for that variant's whole run (covering all iterations, not just one) |
| `run.log` | Combined stdout of both `docker run` invocations - the `RESULT ...` lines plus image-build/cluster-startup output |

`fusion_events` counts actual `[CustomOptimizer] Fusing chained ...` decisions (one per
fused job submission) - not every log line tagged `[CustomOptimizer]`, since
`MapFilterFusedOperator`'s own open/close logs (see below) share that tag but aren't
themselves fusion events.

## Verbose fusion logging

With this prototype in the classpath:

- `OperatorChain` logs every Map->Filter fusion decision at INFO, including the fused
  functions' class names and the task name, and every near-miss at DEBUG (why a
  candidate Map wasn't fused - object reuse disabled, not a `StreamMap`, wrong fan-out,
  or the downstream operator isn't a `StreamFilter`).
- `MapFilterFusedOperator` logs at INFO on `open()` (the fused functions' class names)
  and `close()` (processed / passed / filtered-out record counts for that operator
  instance).
