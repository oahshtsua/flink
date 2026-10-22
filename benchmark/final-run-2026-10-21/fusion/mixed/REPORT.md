# Map-Filter Fusion Benchmark Report

- Run ID: `20261021T163558Z`
- Job: `mixed` - `org.apache.flink.demo.MixedOperatorsBenchmarkJob`
- Record count per run: 200000000
- Iterations per variant: 10
- Vanilla distribution: `/home/oahshtsua/Workspace/flink-vanilla-dist-baseline`
- Fused distribution: `/home/oahshtsua/Workspace/flink/flink-dist/target/flink-2.4-SNAPSHOT-bin/flink-2.4-SNAPSHOT`

This only measures wall-clock runtime. It does not check whether this job's output is correct - it is trusted to already be correct, which is proven separately (once per job's transform, not per benchmark run) by `org.apache.flink.streaming.runtime.tasks.MapFilterFusionDemoITCase` in flink-runtime's test sources. Run that with:
```bash
./mvnw test -pl flink-runtime -Dtest=MapFilterFusionDemoITCase
```

## Job description

(`benchmark/job/org/apache/flink/demo/MixedOperatorsBenchmarkJob.java`)

Benchmark job for the Map-Filter fusion prototype where the chain has other operators mixed in
around the one adjacent `map -> filter` pair: `flatMap -> map -> filter -> map`.

<p>The fusion only ever matches a `Map` immediately followed by a `Filter`; this job
exists to measure whether that selectivity still pays off once it's one step of a longer chain
instead of the whole job, with the `flatMap` before it and the trailing `map` after
the filter left as ordinary, separate operators either way. Parallelism 1 and object reuse
enabled, same as `SimpleMapFilterBenchmarkJob`, so the fusion engages identically.

<p>This job's output is discarded - the benchmark runner only measures wall-clock time and
assumes whatever job it's given produces correct output; it does not verify that itself. See
`org.apache.flink.streaming.runtime.tasks.MapFilterFusionDemoITCase` (flink-runtime's test
sources, `onlyTheAdjacentMapFilterPairIsFusedAmongOtherOperators`) for the correctness
proof that only this one pair fuses and the other operators are left alone.

<p>Usage: `MixedOperatorsBenchmarkJob [recordCount]` (default 200,000,000) - note the
flatMap doubles the record count flowing through the rest of the chain.

Runs with parallelism 1 and object reuse enabled - the condition the fusion requires to engage (see `isMapFilterFusable` in `OperatorChain`).

## Per-run metrics

| Variant | Iter | Count | Runtime (ms) | Fusion events | Log file |
|---|---|---|---|---|---|
| vanilla | 1 | 200000000 | 10014 | 0 | `vanilla-mixed-iter1.log` |
| vanilla | 2 | 200000000 | 10455 | 0 | `vanilla-mixed-iter2.log` |
| vanilla | 3 | 200000000 | 11166 | 0 | `vanilla-mixed-iter3.log` |
| vanilla | 4 | 200000000 | 10950 | 0 | `vanilla-mixed-iter4.log` |
| vanilla | 5 | 200000000 | 10844 | 0 | `vanilla-mixed-iter5.log` |
| vanilla | 6 | 200000000 | 10723 | 0 | `vanilla-mixed-iter6.log` |
| vanilla | 7 | 200000000 | 10363 | 0 | `vanilla-mixed-iter7.log` |
| vanilla | 8 | 200000000 | 10506 | 0 | `vanilla-mixed-iter8.log` |
| vanilla | 9 | 200000000 | 10414 | 0 | `vanilla-mixed-iter9.log` |
| vanilla | 10 | 200000000 | 10901 | 0 | `vanilla-mixed-iter10.log` |
| fused | 1 | 200000000 | 7587 | 1 | `fused-mixed-iter1.log` |
| fused | 2 | 200000000 | 7500 | 1 | `fused-mixed-iter2.log` |
| fused | 3 | 200000000 | 7860 | 1 | `fused-mixed-iter3.log` |
| fused | 4 | 200000000 | 8218 | 1 | `fused-mixed-iter4.log` |
| fused | 5 | 200000000 | 7939 | 1 | `fused-mixed-iter5.log` |
| fused | 6 | 200000000 | 8329 | 1 | `fused-mixed-iter6.log` |
| fused | 7 | 200000000 | 7924 | 1 | `fused-mixed-iter7.log` |
| fused | 8 | 200000000 | 8227 | 1 | `fused-mixed-iter8.log` |
| fused | 9 | 200000000 | 7830 | 1 | `fused-mixed-iter9.log` |
| fused | 10 | 200000000 | 7851 | 1 | `fused-mixed-iter10.log` |

## Summary

```
vanilla    avg_runtime_ms=10633.6    runs=10  fusion_events=0
fused      avg_runtime_ms=7926.5     runs=10  fusion_events=10

fused is 25.5% faster than vanilla on average
```

## Logs

Per-iteration logs (CLI output plus the jobmanager/taskexecutor log lines written during that iteration, including any `[CustomOptimizer]` fusion messages) are saved alongside this report, one file per row in the metrics table above:

- `fused-mixed-iter1.log`
- `fused-mixed-iter10.log`
- `fused-mixed-iter2.log`
- `fused-mixed-iter3.log`
- `fused-mixed-iter4.log`
- `fused-mixed-iter5.log`
- `fused-mixed-iter6.log`
- `fused-mixed-iter7.log`
- `fused-mixed-iter8.log`
- `fused-mixed-iter9.log`
- `vanilla-mixed-iter1.log`
- `vanilla-mixed-iter10.log`
- `vanilla-mixed-iter2.log`
- `vanilla-mixed-iter3.log`
- `vanilla-mixed-iter4.log`
- `vanilla-mixed-iter5.log`
- `vanilla-mixed-iter6.log`
- `vanilla-mixed-iter7.log`
- `vanilla-mixed-iter8.log`
- `vanilla-mixed-iter9.log`

The full raw cluster logs for each variant's whole run (all iterations, same files Flink itself writes to `log/`) are copied into their own subdirectory here:

- `raw-logs-fused-mixed/`
- `raw-logs-vanilla-mixed/`
