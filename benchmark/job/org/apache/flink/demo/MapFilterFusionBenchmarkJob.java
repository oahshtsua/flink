package org.apache.flink.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

/**
 * Benchmark job for comparing a stock Flink runtime against the Map-Filter fusion prototype.
 *
 * <p>Runs a single, long, tight chain of {@code map -> filter} over a bounded sequence of longs,
 * parallelism 1, with object reuse enabled so the fusion (when present) actually engages. The
 * work per record is intentionally trivial so that whatever time is spent is dominated by the
 * per-record dispatch overhead between chained operators, not by the user functions themselves -
 * that overhead is exactly what the fusion removes one hop of.
 *
 * <p>Usage: {@code MapFilterFusionBenchmarkJob [recordCount]} (default 200,000,000).
 */
public class MapFilterFusionBenchmarkJob {
    public static void main(String[] args) throws Exception {
        long count = args.length > 0 ? Long.parseLong(args[0]) : 200_000_000L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        env.fromSequence(0, count - 1)
                .map(x -> x * 2)
                .filter(x -> x % 3 == 0)
                .sinkTo(new DiscardingSink<>());

        env.execute("Map-Filter Fusion Benchmark (" + count + " records)");
    }
}
