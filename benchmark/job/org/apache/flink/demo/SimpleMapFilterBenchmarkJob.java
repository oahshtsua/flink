package org.apache.flink.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

/**
 * Benchmark job for comparing a stock Flink runtime against the Map-Filter fusion prototype: a
 * single, long, tight chain of nothing but {@code map -> filter}.
 *
 * <p>Runs over a bounded sequence of longs, parallelism 1, with object reuse enabled so the fusion
 * (when present) actually engages. The work per record is intentionally trivial so that whatever
 * time is spent is dominated by the per-record dispatch overhead between chained operators, not by
 * the user functions themselves - that overhead is exactly what the fusion removes one hop of.
 *
 * <p>This job's output is discarded - the benchmark runner only measures wall-clock time and
 * assumes whatever job it's given produces correct output; it does not verify that itself. See
 * {@code org.apache.flink.streaming.runtime.tasks.MapFilterFusionDemoITCase} (flink-runtime's test
 * sources) for the correctness proof this job's transform is based on.
 *
 * <p>Usage: {@code SimpleMapFilterBenchmarkJob [recordCount]} (default 200,000,000).
 */
public class SimpleMapFilterBenchmarkJob {
    public static void main(String[] args) throws Exception {
        long count = args.length > 0 ? Long.parseLong(args[0]) : 200_000_000L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        env.fromSequence(0, count - 1)
                .map(x -> x * 2)
                .filter(x -> x % 3 == 0)
                .sinkTo(new DiscardingSink<>());

        env.execute("Simple Map-Filter Benchmark (" + count + " records)");
    }
}
