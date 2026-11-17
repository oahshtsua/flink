package org.apache.flink.demo;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

/**
 * The two fusion benchmark jobs, {@link SimpleMapFilterBenchmarkJob} ({@code map -> filter}) and
 * {@link MixedOperatorsBenchmarkJob} ({@code flatMap -> map -> filter -> map}), on stock Flink with
 * Arrow batches as the stream's records. Same steps, same order, same output; only the user
 * functions change. The {@code engine} argument picks how:
 *
 * <ul>
 *   <li>{@code rows}: the original job, one {@code Long} per record (the reference);
 *   <li>{@code arrow}: every user function rewritten by hand to take and return a whole batch,
 *       each with its own loop over a single int64 column;
 *   <li>{@code arrow-wrapped}: the original, unchanged user functions, each wrapped in a generic
 *       batch adapter that calls it once per row - what a mechanical translation gets without
 *       rewriting anything.
 * </ul>
 *
 * <p>A map writes its results to a column it owns, row-aligned with its input, and passes the
 * selection on; a filter only shortens the selection; the flatMap writes a new, dense column. In
 * {@code checksum} mode every engine must print the same {@code CHECKSUM} line: a count and an
 * order-sensitive hash of every value reaching the sink.
 *
 * <p>Usage: {@code MapFilterArrowJob [simple|mixed] [recordCount] [rows|arrow|arrow-wrapped]
 * [discard|checksum] [batchSize]} (defaults simple, 200,000,000, arrow, discard, 1024). Needs
 * Arrow Java on the classpath for the Arrow engines (see {@code arrow-zero-copy/run.sh}).
 */
public class MapFilterArrowJob {
    // The original jobs' user functions, shared by the rows and arrow-wrapped engines.
    static final MapFunction<Long, Long> TIMES_TWO = x -> x * 2;
    static final FilterFunction<Long> DIVISIBLE_BY_THREE = x -> x % 3 == 0;
    static final MapFunction<Long, Long> PLUS_ONE = x -> x + 1;

    static final Field VALUE = Field.notNullable("value", new ArrowType.Int(64, true));

    public static void main(String[] args) throws Exception {
        String job = args.length > 0 ? args[0] : "simple";
        long count = args.length > 1 ? Long.parseLong(args[1]) : 200_000_000L;
        String engine = args.length > 2 ? args[2] : "arrow";
        boolean checksum = args.length > 3 && args[3].equals("checksum");
        int batchSize = args.length > 4 ? Integer.parseInt(args[4]) : 1024;
        if (!job.equals("simple") && !job.equals("mixed")) {
            throw new IllegalArgumentException("job must be simple or mixed, got " + job);
        }
        boolean mixed = job.equals("mixed");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1, got " + batchSize);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // Required by the Arrow engines, not just faster: ArrowBatch's serializer refuses to copy.
        env.getConfig().enableObjectReuse();

        if (engine.equals("rows")) {
            DataStream<Long> values = env.fromSequence(0, count - 1);
            if (mixed) {
                values = values.flatMap(new MixedOperatorsBenchmarkJob.DuplicateShifted(count));
            }
            values = values.map(TIMES_TWO).filter(DIVISIBLE_BY_THREE);
            if (mixed) {
                values = values.map(PLUS_ONE);
            }
            values.sinkTo(checksum ? new RowChecksumSink() : new DiscardingSink<>());
        } else {
            boolean wrapped;
            if (engine.equals("arrow")) {
                wrapped = false;
            } else if (engine.equals("arrow-wrapped")) {
                wrapped = true;
            } else {
                throw new IllegalArgumentException(
                        "engine must be rows, arrow or arrow-wrapped, got " + engine);
            }
            long batchCount = (count + batchSize - 1) / batchSize;
            DataStream<ArrowBatch> batches =
                    env.fromSource(
                            new DataGeneratorSource<>(
                                    new GenerateValues(count, batchSize),
                                    batchCount,
                                    ArrowBatch.TYPE),
                            WatermarkStrategy.noWatermarks(),
                            "value-batches");
            if (mixed) {
                batches =
                        batches.map(
                                wrapped
                                        ? new FlatMapEachRow(
                                                new MixedOperatorsBenchmarkJob.DuplicateShifted(
                                                        count))
                                        : new DuplicateShiftedBatch(count),
                                ArrowBatch.TYPE);
            }
            batches =
                    batches.map(wrapped ? new MapEachRow(TIMES_TWO) : new TimesTwoBatch(),
                                    ArrowBatch.TYPE)
                            .map(
                                    wrapped
                                            ? new FilterEachRow(DIVISIBLE_BY_THREE)
                                            : new DivisibleByThreeBatch(),
                                    ArrowBatch.TYPE);
            if (mixed) {
                batches =
                        batches.map(
                                wrapped ? new MapEachRow(PLUS_ONE) : new PlusOneBatch(),
                                ArrowBatch.TYPE);
            }
            batches.sinkTo(checksum ? new BatchChecksumSink() : new DiscardingSink<>());
        }

        JobExecutionResult result =
                env.execute(
                        "Map-Filter Arrow Benchmark ("
                                + job
                                + ", "
                                + count
                                + " records, "
                                + engine
                                + (engine.equals("rows") ? "" : ", batch=" + batchSize)
                                + ")");
        System.out.println("NET_RUNTIME_MS=" + result.getNetRuntime());
    }

    /** The value column's data buffer: row r is the int64 at byte {@code 8r}. */
    static ArrowBuf values(VectorSchemaRoot root) {
        return root.getVector(0).getDataBuffer();
    }

    /**
     * Turns batch number {@code i} into values {@code [i * batchSize, min((i + 1) * batchSize,
     * count))} - the same sequence as {@code fromSequence(0, count - 1)} - in one reused column.
     */
    static class GenerateValues implements GeneratorFunction<Long, ArrowBatch> {
        private static final long serialVersionUID = 1L;

        private final long count;
        private final int batchSize;

        private transient BufferAllocator allocator;
        private transient ResultColumns column;
        private transient ArrowBatch batch;

        GenerateValues(long count, int batchSize) {
            this.count = count;
            this.batchSize = batchSize;
        }

        @Override
        public void open(SourceReaderContext readerContext) {
            allocator = ArrowMemory.newChild("value-source");
            column = new ResultColumns(allocator, List.of(VALUE));
            column.ensureCapacity(batchSize);
            batch = new ArrowBatch();
        }

        @Override
        public ArrowBatch map(Long index) {
            final long first = index * batchSize;
            final int n = (int) Math.min(batchSize, count - first);
            final ArrowBuf out = column.data(0);
            for (int r = 0; r < n; r++) {
                out.setLong((long) r * Long.BYTES, first + r);
            }
            return batch.set(column.alone(n));
        }

        @Override
        public void close() {
            if (column != null) {
                column.close();
            }
            if (allocator != null) {
                allocator.close();
            }
        }
    }

    /**
     * A map over the value column: writes the result for every selected row {@code r} at index
     * {@code r} of a column it owns, and passes the selection on. Subclasses supply the loop; it is
     * called once per batch, so each loop stays monomorphic.
     */
    abstract static class ValueMap extends ArrowBatchFunction {
        private static final long serialVersionUID = 1L;

        private transient ResultColumns results;

        @Override
        public void open(OpenContext openContext) {
            super.open(openContext);
            results = new ResultColumns(allocator, List.of(VALUE));
        }

        @Override
        public final ArrowBatch map(ArrowBatch batch) throws Exception {
            final int rows = batch.root().getRowCount();
            results.ensureCapacity(rows);
            apply(batch, values(batch.root()), results.data(0));
            return out.set(results.alone(rows), batch.selection(), batch.selectedCount());
        }

        protected abstract void apply(ArrowBatch batch, ArrowBuf in, ArrowBuf out)
                throws Exception;

        @Override
        public void close() {
            if (results != null) {
                results.close();
            }
            super.close();
        }
    }

    /** {@code x -> x * 2}, rewritten. */
    static class TimesTwoBatch extends ValueMap {
        private static final long serialVersionUID = 1L;

        @Override
        protected void apply(ArrowBatch batch, ArrowBuf in, ArrowBuf out) {
            for (int i = 0, n = batch.selectedCount(); i < n; i++) {
                final long at = (long) batch.selectedRow(i) * Long.BYTES;
                out.setLong(at, in.getLong(at) * 2);
            }
        }
    }

    /** {@code x -> x + 1}, rewritten. */
    static class PlusOneBatch extends ValueMap {
        private static final long serialVersionUID = 1L;

        @Override
        protected void apply(ArrowBatch batch, ArrowBuf in, ArrowBuf out) {
            for (int i = 0, n = batch.selectedCount(); i < n; i++) {
                final long at = (long) batch.selectedRow(i) * Long.BYTES;
                out.setLong(at, in.getLong(at) + 1);
            }
        }
    }

    /** {@code x -> x % 3 == 0}, rewritten: shortens the selection, touches no column. */
    static class DivisibleByThreeBatch extends ArrowBatchFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public ArrowBatch map(ArrowBatch batch) {
            final ArrowBuf in = values(batch.root());
            final int n = batch.selectedCount();
            final ArrowBuf sel = selection(n);
            int kept = 0;
            for (int i = 0; i < n; i++) {
                final int r = batch.selectedRow(i);
                if (in.getLong((long) r * Long.BYTES) % 3 == 0) {
                    sel.setInt((long) kept * Integer.BYTES, r);
                    kept++;
                }
            }
            return out.set(batch.root(), sel, kept);
        }
    }

    /**
     * {@link MixedOperatorsBenchmarkJob.DuplicateShifted}, rewritten: each selected value {@code
     * v} becomes rows {@code v, v + shift} of a new dense column, in input order.
     */
    static class DuplicateShiftedBatch extends ArrowBatchFunction {
        private static final long serialVersionUID = 1L;

        private final long shift;
        private transient ResultColumns results;

        DuplicateShiftedBatch(long shift) {
            this.shift = shift;
        }

        @Override
        public void open(OpenContext openContext) {
            super.open(openContext);
            results = new ResultColumns(allocator, List.of(VALUE));
        }

        @Override
        public ArrowBatch map(ArrowBatch batch) {
            final int n = batch.selectedCount();
            results.ensureCapacity(2 * n);
            final ArrowBuf in = values(batch.root());
            final ArrowBuf dup = results.data(0);
            for (int i = 0; i < n; i++) {
                final long v = in.getLong((long) batch.selectedRow(i) * Long.BYTES);
                dup.setLong(2L * i * Long.BYTES, v);
                dup.setLong((2L * i + 1) * Long.BYTES, v + shift);
            }
            return out.set(results.alone(2 * n));
        }

        @Override
        public void close() {
            if (results != null) {
                results.close();
            }
            super.close();
        }
    }

    /** Any {@code MapFunction<Long, Long>}, called once per selected row. */
    static class MapEachRow extends ValueMap {
        private static final long serialVersionUID = 1L;

        private final MapFunction<Long, Long> function;

        MapEachRow(MapFunction<Long, Long> function) {
            this.function = function;
        }

        @Override
        protected void apply(ArrowBatch batch, ArrowBuf in, ArrowBuf out) throws Exception {
            for (int i = 0, n = batch.selectedCount(); i < n; i++) {
                final long at = (long) batch.selectedRow(i) * Long.BYTES;
                out.setLong(at, function.map(in.getLong(at)));
            }
        }
    }

    /** Any {@code FilterFunction<Long>}, called once per selected row. */
    static class FilterEachRow extends ArrowBatchFunction {
        private static final long serialVersionUID = 1L;

        private final FilterFunction<Long> function;

        FilterEachRow(FilterFunction<Long> function) {
            this.function = function;
        }

        @Override
        public ArrowBatch map(ArrowBatch batch) throws Exception {
            final ArrowBuf in = values(batch.root());
            final int n = batch.selectedCount();
            final ArrowBuf sel = selection(n);
            int kept = 0;
            for (int i = 0; i < n; i++) {
                final int r = batch.selectedRow(i);
                if (function.filter(in.getLong((long) r * Long.BYTES))) {
                    sel.setInt((long) kept * Integer.BYTES, r);
                    kept++;
                }
            }
            return out.set(batch.root(), sel, kept);
        }
    }

    /**
     * Any {@code FlatMapFunction<Long, Long>}, called once per selected row. Its output count is
     * unknown up front, so it collects into a vector that grows as needed ({@code setSafe}).
     */
    static class FlatMapEachRow extends ArrowBatchFunction {
        private static final long serialVersionUID = 1L;

        private final FlatMapFunction<Long, Long> function;
        private transient BigIntVector column;
        private transient VectorSchemaRoot root;
        private transient Appender appender;

        FlatMapEachRow(FlatMapFunction<Long, Long> function) {
            this.function = function;
        }

        @Override
        public void open(OpenContext openContext) {
            super.open(openContext);
            column = (BigIntVector) VALUE.createVector(allocator);
            root = new VectorSchemaRoot(List.of(column));
            appender = new Appender();
        }

        @Override
        public ArrowBatch map(ArrowBatch batch) throws Exception {
            final ArrowBuf in = values(batch.root());
            appender.rows = 0;
            for (int i = 0, n = batch.selectedCount(); i < n; i++) {
                function.flatMap(in.getLong((long) batch.selectedRow(i) * Long.BYTES), appender);
            }
            root.setRowCount(appender.rows);
            return out.set(root);
        }

        private final class Appender implements Collector<Long> {
            int rows;

            @Override
            public void collect(Long value) {
                column.setSafe(rows++, value);
            }

            @Override
            public void close() {}
        }

        @Override
        public void close() {
            if (column != null) {
                column.close();
            }
            super.close();
        }
    }

    /** Count and order-sensitive hash of the values reaching the sink. */
    static final class ValueChecksum {
        private long count;
        private long hash;

        void add(long value) {
            count++;
            hash = hash * 31 + value;
        }

        String format() {
            return String.format("CHECKSUM count=%d hash=%016x", count, hash);
        }
    }

    /** Prints a {@link ValueChecksum} line when its input ends. */
    static class RowChecksumSink implements Sink<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public SinkWriter<Long> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                private final ValueChecksum checksum = new ValueChecksum();

                @Override
                public void write(Long value, Context ctx) {
                    checksum.add(value);
                }

                @Override
                public void flush(boolean endOfInput) {
                    if (endOfInput) {
                        System.out.println(checksum.format());
                    }
                }

                @Override
                public void close() {}
            };
        }
    }

    /** {@link RowChecksumSink} over the selected rows of each batch. */
    static class BatchChecksumSink implements Sink<ArrowBatch> {
        private static final long serialVersionUID = 1L;

        @Override
        public SinkWriter<ArrowBatch> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                private final ValueChecksum checksum = new ValueChecksum();

                @Override
                public void write(ArrowBatch batch, Context ctx) {
                    final ArrowBuf in = values(batch.root());
                    for (int i = 0; i < batch.selectedCount(); i++) {
                        checksum.add(in.getLong((long) batch.selectedRow(i) * Long.BYTES));
                    }
                }

                @Override
                public void flush(boolean endOfInput) {
                    if (endOfInput) {
                        System.out.println(checksum.format());
                    }
                }

                @Override
                public void close() {}
            };
        }
    }
}
