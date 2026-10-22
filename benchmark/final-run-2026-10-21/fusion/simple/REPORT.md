# Map-Filter Fusion Benchmark Report

- Run ID: `20261021T163332Z`
- Job: `simple` - `org.apache.flink.demo.SimpleMapFilterBenchmarkJob`
- Record count per run: 200000000
- Iterations per variant: 10
- Vanilla distribution: `/home/oahshtsua/Workspace/flink-vanilla-dist-baseline`
- Fused distribution: `/home/oahshtsua/Workspace/flink/flink-dist/target/flink-2.4-SNAPSHOT-bin/flink-2.4-SNAPSHOT`

This only measures wall-clock runtime. It does not check whether this job's output is correct - it is trusted to already be correct, which is proven separately (once per job's transform, not per benchmark run) by `org.apache.flink.streaming.runtime.tasks.MapFilterFusionDemoITCase` in flink-runtime's test sources. Run that with:
```bash
./mvnw test -pl flink-runtime -Dtest=MapFilterFusionDemoITCase
```

## Job description

(`benchmark/job/org/apache/flink/demo/SimpleMapFilterBenchmarkJob.java`)

Benchmark job for comparing a stock Flink runtime against the Map-Filter fusion prototype: a
single, long, tight chain of nothing but `map -> filter`.

<p>Runs over a bounded sequence of longs, parallelism 1, with object reuse enabled so the fusion
(when present) actually engages. The work per record is intentionally trivial so that whatever
time is spent is dominated by the per-record dispatch overhead between chained operators, not by
the user functions themselves - that overhead is exactly what the fusion removes one hop of.

<p>This job's output is discarded - the benchmark runner only measures wall-clock time and
assumes whatever job it's given produces correct output; it does not verify that itself. See
`org.apache.flink.streaming.runtime.tasks.MapFilterFusionDemoITCase` (flink-runtime's test
sources) for the correctness proof this job's transform is based on.

<p>Usage: `SimpleMapFilterBenchmarkJob [recordCount]` (default 200,000,000).

Runs with parallelism 1 and object reuse enabled - the condition the fusion requires to engage (see `isMapFilterFusable` in `OperatorChain`).

## Per-run metrics

| Variant | Iter | Count | Runtime (ms) | Fusion events | Log file |
|---|---|---|---|---|---|
| vanilla | 1 | 200000000 | 4002 | 0 | `vanilla-simple-iter1.log` |
| vanilla | 2 | 200000000 | 3496 | 0 | `vanilla-simple-iter2.log` |
| vanilla | 3 | 200000000 | 3720 | 0 | `vanilla-simple-iter3.log` |
| vanilla | 4 | 200000000 | 3585 | 0 | `vanilla-simple-iter4.log` |
| vanilla | 5 | 200000000 | 3618 | 0 | `vanilla-simple-iter5.log` |
| vanilla | 6 | 200000000 | 3610 | 0 | `vanilla-simple-iter6.log` |
| vanilla | 7 | 200000000 | 3572 | 0 | `vanilla-simple-iter7.log` |
| vanilla | 8 | 200000000 | 3681 | 0 | `vanilla-simple-iter8.log` |
| vanilla | 9 | 200000000 | 3590 | 0 | `vanilla-simple-iter9.log` |
| vanilla | 10 | 200000000 | 3565 | 0 | `vanilla-simple-iter10.log` |
| fused | 1 | 200000000 | 3328 | 1 | `fused-simple-iter1.log` |
| fused | 2 | 200000000 | 3232 | 1 | `fused-simple-iter2.log` |
| fused | 3 | 200000000 | 3101 | 1 | `fused-simple-iter3.log` |
| fused | 4 | 200000000 | 3094 | 1 | `fused-simple-iter4.log` |
| fused | 5 | 200000000 | 3237 | 1 | `fused-simple-iter5.log` |
| fused | 6 | 200000000 | 3147 | 1 | `fused-simple-iter6.log` |
| fused | 7 | 200000000 | 3119 | 1 | `fused-simple-iter7.log` |
| fused | 8 | 200000000 | 3108 | 1 | `fused-simple-iter8.log` |
| fused | 9 | 200000000 | 3292 | 1 | `fused-simple-iter9.log` |
| fused | 10 | 200000000 | 3159 | 1 | `fused-simple-iter10.log` |

## Summary

```
vanilla    avg_runtime_ms=3643.9     runs=10  fusion_events=0
fused      avg_runtime_ms=3181.7     runs=10  fusion_events=10

fused is 12.7% faster than vanilla on average
```

## Logs

Per-iteration logs (CLI output plus the jobmanager/taskexecutor log lines written during that iteration, including any `[CustomOptimizer]` fusion messages) are saved alongside this report, one file per row in the metrics table above:

- `fused-simple-iter1.log`
- `fused-simple-iter10.log`
- `fused-simple-iter2.log`
- `fused-simple-iter3.log`
- `fused-simple-iter4.log`
- `fused-simple-iter5.log`
- `fused-simple-iter6.log`
- `fused-simple-iter7.log`
- `fused-simple-iter8.log`
- `fused-simple-iter9.log`
- `vanilla-simple-iter1.log`
- `vanilla-simple-iter10.log`
- `vanilla-simple-iter2.log`
- `vanilla-simple-iter3.log`
- `vanilla-simple-iter4.log`
- `vanilla-simple-iter5.log`
- `vanilla-simple-iter6.log`
- `vanilla-simple-iter7.log`
- `vanilla-simple-iter8.log`
- `vanilla-simple-iter9.log`

The full raw cluster logs for each variant's whole run (all iterations, same files Flink itself writes to `log/`) are copied into their own subdirectory here:

- `raw-logs-fused-simple/`
- `raw-logs-vanilla-simple/`
