package org.apache.flink.demo;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;

/**
 * Benchmark job for comparing a stock Flink runtime against the Map-Filter fusion prototype.
 *
 * <p>Runs a single, long, tight chain of {@code map -> filter} over a bounded sequence of longs,
 * parallelism 1, with object reuse enabled so the fusion (when present) actually engages. The
 * work per record is intentionally trivial so that whatever time is spent is dominated by the
 * per-record dispatch overhead between chained operators, not by the user functions themselves -
 * that overhead is exactly what the fusion removes one hop of.
 *
 * <p>Output is not discarded: {@link ChecksumSink} accumulates a count and sum of every record
 * that reaches it and prints both on job completion as a {@code CHECKSUM count=... sum=...} line.
 * Since the transform is deterministic, fusing Map and Filter into one operator must not change
 * which records are produced - the checksum is how the benchmark harness proves that empirically
 * (comparing the vanilla and fused runs' checksums, and both against the closed-form expected
 * value) instead of just asserting it from the runtime numbers alone.
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
                .addSink(new ChecksumSink());

        env.execute("Map-Filter Fusion Benchmark (" + count + " records)");
    }

    private static class ChecksumSink extends AbstractRichFunction implements SinkFunction<Long> {
        private static final long serialVersionUID = 1L;

        private long recordCount = 0L;
        private long sum = 0L;

        @Override
        public void invoke(Long value, Context context) {
            recordCount++;
            sum += value;
        }

        @Override
        public void close() throws Exception {
            System.out.println("CHECKSUM count=" + recordCount + " sum=" + sum);
            super.close();
        }
    }
}
