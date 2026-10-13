package org.apache.flink.demo;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.nio.ByteBuffer;

/**
 * {@link SensorScoringJob} with its four {@code map -> filter -> map -> filter} stages replaced by
 * one {@link MicroBatchingNativeOperator} that hands batches of readings to {@code sensor_score}
 * in {@code libsensor_kernel.so}: the whole chain as a single sequential C function. Same source,
 * same data, same sinks; in {@code checksum} mode it must print the same {@code CHECKSUM} line as
 * {@code SensorScoringReference}.
 *
 * <p>Needs {@code org.apache.flink.accel.NativeBridge} and its library on the TaskManager (see
 * {@code benchmark/native-kernel/build.sh}). The kernel library path is taken from system property
 * {@code sensor.kernel.lib} on the client, default {@value #DEFAULT_KERNEL_LIB}; the symbol from
 * {@code sensor.kernel.symbol}, default {@code sensor_score}.
 *
 * <p>Usage: {@code SensorScoringNativeJob [recordCount] [k] [discard|checksum] [batchSize]}
 * (defaults 50,000,000, 1, discard, 1024).
 */
public class SensorScoringNativeJob {
    static final String DEFAULT_KERNEL_LIB = "/opt/flink/lib/native/libsensor_kernel.so";

    public static void main(String[] args) throws Exception {
        long count = args.length > 0 ? Long.parseLong(args[0]) : 50_000_000L;
        int k = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        boolean checksum = args.length > 2 && args[2].equals("checksum");
        int batchSize = args.length > 3 ? Integer.parseInt(args[3]) : 1024;
        String kernelLib = System.getProperty("sensor.kernel.lib", DEFAULT_KERNEL_LIB);
        // Overridable so diagnostic kernels with the same signature (e.g. one that only
        // measures the toll) can be swapped in without touching the job.
        String kernelSymbol = System.getProperty("sensor.kernel.symbol", "sensor_score");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        double[] profiles = SensorScoring.profiles(k);
        DataGeneratorSource<SensorReading> source =
                new DataGeneratorSource<>(
                        new SensorScoringJob.GenerateReading(profiles, k),
                        count,
                        TypeInformation.of(SensorReading.class));

        DataStream<SensorReading> scored =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "sensor-readings")
                        .transform(
                                "sensor-score-native",
                                TypeInformation.of(SensorReading.class),
                                new SensorScoreOperator(
                                        kernelLib, kernelSymbol, profiles, k, batchSize));

        if (checksum) {
            scored.sinkTo(new SensorScoringJob.ChecksumSink());
        } else {
            scored.sinkTo(new DiscardingSink<>());
        }

        // Net runtime (excludes JVM and cluster startup), for local MiniCluster timing runs; the
        // Docker harness parses the CLI's own "Job Runtime" line instead.
        JobExecutionResult result =
                env.execute(
                        "Sensor Scoring Native Benchmark ("
                                + count
                                + " records, k="
                                + k
                                + ", batch="
                                + batchSize
                                + ")");
        System.out.println("NET_RUNTIME_MS=" + result.getNetRuntime());
    }

    /** Wires {@link SensorLayout}'s layouts into the generic micro-batching operator. */
    static class SensorScoreOperator
            extends MicroBatchingNativeOperator<SensorReading, SensorReading> {
        private static final long serialVersionUID = 1L;

        private final double[] profiles;
        private final int k;
        private transient SensorReading reuse;

        SensorScoreOperator(
                String kernelLib, String kernelSymbol, double[] profiles, int k, int batchSize) {
            super(kernelLib, kernelSymbol, batchSize);
            this.profiles = profiles;
            this.k = k;
        }

        @Override
        public void open() throws Exception {
            reuse = new SensorReading();
            super.open();
        }

        @Override
        protected int inRecordBytes() {
            return SensorLayout.IN_BYTES;
        }

        @Override
        protected int outRecordBytes() {
            return SensorLayout.OUT_BYTES;
        }

        @Override
        protected int paramsBytes() {
            return SensorLayout.paramsBytes(k);
        }

        @Override
        protected void writeParams(ByteBuffer buffer) {
            SensorLayout.writeParams(buffer, profiles, k);
        }

        @Override
        protected void encode(SensorReading r, ByteBuffer buffer, int offset) {
            SensorLayout.encode(r, buffer, offset);
        }

        @Override
        protected SensorReading decode(ByteBuffer out, int outOffset, ByteBuffer in, int inOffset) {
            SensorLayout.decode(out, outOffset, in, inOffset, reuse);
            return reuse;
        }
    }
}
