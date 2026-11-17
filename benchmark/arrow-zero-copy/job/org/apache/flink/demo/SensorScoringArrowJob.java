package org.apache.flink.demo;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * {@link SensorScoringNativeJob} with Arrow batches as the stream's records, so nothing is copied
 * between Java and the kernel in either direction: the source generates readings straight into
 * Arrow columns, {@link ArrowNativeOperator} hands those columns to {@code sensor_score_arrow} in
 * {@code libsensor_arrow_kernel.so} by address, the kernel writes its results straight into Arrow
 * columns, and the sink reads the survivors through the kernel's selection vector. Same data,
 * same arithmetic: in {@code checksum} mode it must print the same {@code CHECKSUM} line as
 * {@code SensorScoringReference}.
 *
 * <p>The batch size is set where batches are formed - the source - and every batch but possibly
 * the last is full. Flink sees one record per batch.
 *
 * <p>The {@code engine} argument picks what does the work between source and sink; all three emit
 * the same batches (input columns, then mean/stddev/max/score, plus a selection vector):
 *
 * <ul>
 *   <li>{@code native}: {@link ArrowNativeOperator} calling the C kernel (the default);
 *   <li>{@code java}: one Java batch function, {@link ScoreFusedBatch}, running all four steps -
 *       the same arrangement as the C kernel, in Java;
 *   <li>{@code java-steps}: the four steps as four Java batch functions, {@code map -> filter ->
 *       map -> filter} as in {@link SensorScoringJob}, each taking and returning a whole batch.
 *       To Flink all four are ordinary chained {@code map}s; a filter returns the batch with a
 *       smaller selection vector.
 * </ul>
 *
 * <p>Needs {@code org.apache.flink.accel.NativeBridge} and its library, as the native job does,
 * plus Arrow Java on the classpath and, on JDK 17, {@code
 * --add-opens=java.base/java.nio=ALL-UNNAMED} (see {@code arrow-zero-copy/run.sh}). The kernel
 * library path is taken from system property {@code sensor.kernel.lib}, default {@value
 * #DEFAULT_KERNEL_LIB}.
 *
 * <p>Usage: {@code SensorScoringArrowJob [recordCount] [k] [discard|checksum] [batchSize]
 * [native|java|java-steps]} (defaults 50,000,000, 1, discard, 1024, native).
 */
public class SensorScoringArrowJob {
    static final String DEFAULT_KERNEL_LIB = "/opt/flink/lib/native/libsensor_arrow_kernel.so";

    public static void main(String[] args) throws Exception {
        long count = args.length > 0 ? Long.parseLong(args[0]) : 50_000_000L;
        int k = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        boolean checksum = args.length > 2 && args[2].equals("checksum");
        int batchSize = args.length > 3 ? Integer.parseInt(args[3]) : 1024;
        String engine = args.length > 4 ? args[4] : "native";
        String kernelLib = System.getProperty("sensor.kernel.lib", DEFAULT_KERNEL_LIB);
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1, got " + batchSize);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // Required, not just faster: ArrowBatch's serializer refuses to copy (see ArrowBatch).
        env.getConfig().enableObjectReuse();

        double[] profiles = SensorScoring.profiles(k);
        long batchCount = (count + batchSize - 1) / batchSize;
        DataGeneratorSource<ArrowBatch> source =
                new DataGeneratorSource<>(
                        new GenerateBatch(profiles, k, count, batchSize), batchCount, ArrowBatch.TYPE);

        DataStream<ArrowBatch> batches =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "sensor-batches");
        DataStream<ArrowBatch> scored;
        switch (engine) {
            case "native":
                scored =
                        batches.transform(
                                "sensor-score-arrow",
                                ArrowBatch.TYPE,
                                new SensorScoreArrowOperator(kernelLib, profiles, k));
                break;
            case "java":
                scored = batches.map(new ScoreFusedBatch(profiles, k), ArrowBatch.TYPE);
                break;
            case "java-steps":
                scored =
                        batches.map(new StatsStep(), ArrowBatch.TYPE)
                                .map(
                                        new AtLeastStep("stddev", SensorScoring.FLAT_THRESHOLD),
                                        ArrowBatch.TYPE)
                                .map(new ScoreStep(profiles, k), ArrowBatch.TYPE)
                                .map(
                                        new AtLeastStep("score", SensorScoring.SCORE_THRESHOLD),
                                        ArrowBatch.TYPE);
                break;
            default:
                throw new IllegalArgumentException(
                        "engine must be native, java or java-steps, got " + engine);
        }

        if (checksum) {
            scored.sinkTo(new ChecksumSink());
        } else {
            scored.sinkTo(new DiscardingSink<>());
        }

        JobExecutionResult result =
                env.execute(
                        "Sensor Scoring Arrow Benchmark ("
                                + count
                                + " records, k="
                                + k
                                + ", batch="
                                + batchSize
                                + ", "
                                + engine
                                + ")");
        System.out.println("NET_RUNTIME_MS=" + result.getNetRuntime());
    }

    /**
     * Turns batch number {@code i} into readings {@code [i * batchSize, min((i + 1) * batchSize,
     * count))}, written into one reused batch.
     */
    static class GenerateBatch implements GeneratorFunction<Long, ArrowBatch> {
        private static final long serialVersionUID = 1L;

        private final double[] profiles;
        private final int k;
        private final long count;
        private final int batchSize;

        private transient BufferAllocator allocator;
        private transient VectorSchemaRoot root;
        private transient ArrowBatch batch;

        GenerateBatch(double[] profiles, int k, long count, int batchSize) {
            this.profiles = profiles;
            this.k = k;
            this.count = count;
            this.batchSize = batchSize;
        }

        @Override
        public void open(SourceReaderContext readerContext) {
            allocator = ArrowMemory.newChild("sensor-source");
            root = SensorArrow.allocate(allocator, batchSize);
            batch = new ArrowBatch();
        }

        @Override
        public ArrowBatch map(Long index) {
            final long first = index * batchSize;
            final int n = (int) Math.min(batchSize, count - first);
            SensorArrow.fill(root, first, n, profiles, k);
            return batch.set(root);
        }

        @Override
        public void close() {
            if (root != null) {
                root.close();
            }
            if (allocator != null) {
                allocator.close();
            }
        }
    }

    /** Wires the sensor kernel's columns and parameter block into the generic Arrow operator. */
    static class SensorScoreArrowOperator extends ArrowNativeOperator {
        private static final long serialVersionUID = 1L;

        private final double[] profiles;
        private final int k;

        SensorScoreArrowOperator(String kernelLib, double[] profiles, int k) {
            super(kernelLib, "sensor_score_arrow");
            this.profiles = profiles;
            this.k = k;
        }

        @Override
        protected Schema inputSchema() {
            return SensorArrow.INPUT_SCHEMA;
        }

        @Override
        protected List<Field> resultFields() {
            return SensorArrow.RESULT_FIELDS;
        }

        @Override
        protected int paramsBytes() {
            return SensorLayout.paramsBytes(k);
        }

        @Override
        protected void writeParams(ByteBuffer buffer) {
            SensorLayout.writeParams(buffer, profiles, k);
        }
    }

    /** The values buffer of a batch's samples column: row r's samples are doubles [8r, 8r + 8). */
    static ArrowBuf samples(VectorSchemaRoot batch) {
        return ((FixedSizeListVector) batch.getVector(SensorArrow.SAMPLES))
                .getDataVector()
                .getDataBuffer();
    }

    /** All four steps as one Java function over the batch: the Java twin of the C kernel. */
    static class ScoreFusedBatch extends ArrowBatchFunction {
        private static final long serialVersionUID = 1L;

        private final double[] profiles;
        private final int k;
        private transient ResultColumns results;

        ScoreFusedBatch(double[] profiles, int k) {
            this.profiles = profiles;
            this.k = k;
        }

        @Override
        public void open(OpenContext openContext) {
            super.open(openContext);
            results = new ResultColumns(allocator, SensorArrow.RESULT_FIELDS);
        }

        @Override
        public ArrowBatch map(ArrowBatch batch) {
            if (batch.hasSelection()) {
                throw new IllegalStateException("ScoreFusedBatch needs dense input");
            }
            final VectorSchemaRoot in = batch.root();
            final int n = in.getRowCount();
            results.ensureCapacity(n);
            final ArrowBuf sel = selection(n);
            final int kept =
                    SensorArrowScoring.scoreFused(
                            samples(in),
                            n,
                            profiles,
                            k,
                            results.data(0),
                            results.data(1),
                            results.data(2),
                            results.data(3),
                            sel);
            return out.set(results.appendTo(in), sel, kept);
        }

        @Override
        public void close() {
            if (results != null) {
                results.close();
            }
            super.close();
        }
    }

    /** Step 1 (map): appends mean, stddev and max columns. */
    static class StatsStep extends ArrowBatchFunction {
        private static final long serialVersionUID = 1L;

        private transient ResultColumns results;

        @Override
        public void open(OpenContext openContext) {
            super.open(openContext);
            results = new ResultColumns(allocator, SensorArrow.RESULT_FIELDS.subList(0, 3));
        }

        @Override
        public ArrowBatch map(ArrowBatch batch) {
            final VectorSchemaRoot in = batch.root();
            results.ensureCapacity(in.getRowCount());
            SensorArrowScoring.computeStats(
                    samples(in),
                    batch.selection(),
                    batch.selectedCount(),
                    results.data(0),
                    results.data(1),
                    results.data(2));
            return out.set(results.appendTo(in), batch.selection(), batch.selectedCount());
        }

        @Override
        public void close() {
            if (results != null) {
                results.close();
            }
            super.close();
        }
    }

    /** Step 3 (map): appends the score column, from the samples and step 1's mean and stddev. */
    static class ScoreStep extends ArrowBatchFunction {
        private static final long serialVersionUID = 1L;

        private final double[] profiles;
        private final int k;
        private transient ResultColumns results;
        private transient double[] z;

        ScoreStep(double[] profiles, int k) {
            this.profiles = profiles;
            this.k = k;
        }

        @Override
        public void open(OpenContext openContext) {
            super.open(openContext);
            results = new ResultColumns(allocator, SensorArrow.RESULT_FIELDS.subList(3, 4));
            z = new double[SensorScoring.WINDOW];
        }

        @Override
        public ArrowBatch map(ArrowBatch batch) {
            final VectorSchemaRoot in = batch.root();
            results.ensureCapacity(in.getRowCount());
            SensorArrowScoring.computeScore(
                    samples(in),
                    in.getVector("mean").getDataBuffer(),
                    in.getVector("stddev").getDataBuffer(),
                    batch.selection(),
                    batch.selectedCount(),
                    profiles,
                    k,
                    z,
                    results.data(0));
            return out.set(results.appendTo(in), batch.selection(), batch.selectedCount());
        }

        @Override
        public void close() {
            if (results != null) {
                results.close();
            }
            super.close();
        }
    }

    /**
     * Steps 2 and 4 (filter): keeps the selected rows whose {@code column} is at least {@code
     * threshold}. Touches no column: the batch goes on with a smaller selection vector.
     */
    static class AtLeastStep extends ArrowBatchFunction {
        private static final long serialVersionUID = 1L;

        private final String column;
        private final double threshold;

        AtLeastStep(String column, double threshold) {
            this.column = column;
            this.threshold = threshold;
        }

        @Override
        public ArrowBatch map(ArrowBatch batch) {
            final VectorSchemaRoot in = batch.root();
            final ArrowBuf sel = selection(batch.selectedCount());
            final int kept =
                    SensorArrowScoring.filterAtLeast(
                            in.getVector(column).getDataBuffer(),
                            threshold,
                            batch.selection(),
                            batch.selectedCount(),
                            sel);
            return out.set(in, sel, kept);
        }
    }

    /** Prints a {@link SensorChecksum} line over the selected rows when its input ends. */
    static class ChecksumSink implements Sink<ArrowBatch> {
        private static final long serialVersionUID = 1L;

        @Override
        public SinkWriter<ArrowBatch> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                private final SensorChecksum checksum = new SensorChecksum();
                private final SensorReading reading = new SensorReading();

                @Override
                public void write(ArrowBatch batch, Context ctx) {
                    for (int i = 0; i < batch.selectedCount(); i++) {
                        SensorArrow.read(batch.root(), batch.selectedRow(i), reading);
                        checksum.add(reading);
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
