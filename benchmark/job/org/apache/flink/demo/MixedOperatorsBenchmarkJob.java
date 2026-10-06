package org.apache.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;

/**
 * Benchmark job for the Map-Filter fusion prototype where the chain has other operators mixed in
 * around the one adjacent {@code map -> filter} pair: {@code flatMap -> map -> filter -> map}.
 *
 * <p>The fusion only ever matches a {@code Map} immediately followed by a {@code Filter}; this job
 * exists to measure whether that selectivity still pays off once it's one step of a longer chain
 * instead of the whole job, with the {@code flatMap} before it and the trailing {@code map} after
 * the filter left as ordinary, separate operators either way. Parallelism 1 and object reuse
 * enabled, same as {@link SimpleMapFilterBenchmarkJob}, so the fusion engages identically.
 *
 * <p>This job's output is discarded - the benchmark runner only measures wall-clock time and
 * assumes whatever job it's given produces correct output; it does not verify that itself. See
 * {@code org.apache.flink.streaming.runtime.tasks.MapFilterFusionDemoITCase} (flink-runtime's test
 * sources, {@code onlyTheAdjacentMapFilterPairIsFusedAmongOtherOperators}) for the correctness
 * proof that only this one pair fuses and the other operators are left alone.
 *
 * <p>Usage: {@code MixedOperatorsBenchmarkJob [recordCount]} (default 200,000,000) - note the
 * flatMap doubles the record count flowing through the rest of the chain.
 */
public class MixedOperatorsBenchmarkJob {
    public static void main(String[] args) throws Exception {
        long count = args.length > 0 ? Long.parseLong(args[0]) : 200_000_000L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        env.fromSequence(0, count - 1)
                .flatMap(new DuplicateShifted(count))
                .map(x -> x * 2)
                .filter(x -> x % 3 == 0)
                .map(x -> x + 1)
                .sinkTo(new DiscardingSink<>());

        env.execute("Mixed Operators Benchmark (" + count + " records)");
    }

    private static class DuplicateShifted implements FlatMapFunction<Long, Long> {
        private static final long serialVersionUID = 1L;

        private final long shift;

        DuplicateShifted(long shift) {
            this.shift = shift;
        }

        @Override
        public void flatMap(Long value, Collector<Long> out) {
            out.collect(value);
            out.collect(value + shift);
        }
    }
}
