package org.apache.flink.demo;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

/**
 * Benchmark job for the micro-batching + native-kernel experiment, plain-Java baseline: IoT-style
 * sensor window scoring, {@code map -> filter -> map -> filter} over {@link SensorReading}s of 8
 * fixed-width doubles each. Unlike the other jobs here, every stage does real floating-point work:
 *
 * <ol>
 *   <li>map: window mean, standard deviation and maximum,
 *   <li>filter: drop flat signals (standard deviation below a threshold),
 *   <li>map: z-normalize the window and score it as its best correlation with {@code k} reference
 *       profiles,
 *   <li>filter: keep readings whose score clears a threshold.
 * </ol>
 *
 * <p>Readings are generated inside a {@code DataGeneratorSource} rather than by a {@code map} after
 * {@code fromSequence}, so the operator chain is exactly these four stages: the Map-Filter fusion
 * fuses exactly four steps, the same four the native operator replaces. Parallelism 1 and object
 * reuse enabled, as in the other jobs; every stage updates the reading in place and returns it.
 *
 * <p>All arithmetic lives in {@code SensorScoring}; {@code k} scales stage 3's work linearly
 * without changing the record. In {@code checksum} mode the output goes to a sink that prints a
 * {@code CHECKSUM} line instead of being discarded; it must match {@code SensorScoringReference}'s
 * for the same record count and {@code k}.
 *
 * <p>Usage: {@code SensorScoringJob [recordCount] [k] [discard|checksum]} (defaults 50,000,000, 1,
 * discard).
 */
public class SensorScoringJob {
    public static void main(String[] args) throws Exception {
        long count = args.length > 0 ? Long.parseLong(args[0]) : 50_000_000L;
        int k = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        boolean checksum = args.length > 2 && args[2].equals("checksum");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        double[] profiles = SensorScoring.profiles(k);
        DataGeneratorSource<SensorReading> source =
                new DataGeneratorSource<>(
                        new GenerateReading(profiles, k),
                        count,
                        TypeInformation.of(SensorReading.class));

        DataStream<SensorReading> scored =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "sensor-readings")
                        .map(new ComputeStats())
                        .filter(new DropFlat())
                        .map(new ScoreAgainstProfiles(profiles, k))
                        .filter(new KeepHighScore());

        if (checksum) {
            scored.sinkTo(new ChecksumSink());
        } else {
            scored.sinkTo(new DiscardingSink<>());
        }

        // Net runtime (excludes JVM and cluster startup), for local MiniCluster timing runs; the
        // Docker harness parses the CLI's own "Job Runtime" line instead.
        JobExecutionResult result =
                env.execute("Sensor Scoring Benchmark (" + count + " records, k=" + k + ")");
        System.out.println("NET_RUNTIME_MS=" + result.getNetRuntime());
    }

    /** Fills one reused reading per call; safe because each reading is fully consumed downstream
     * (chained, object reuse) before the next one is generated. */
    static class GenerateReading implements GeneratorFunction<Long, SensorReading> {
        private static final long serialVersionUID = 1L;

        private final double[] profiles;
        private final int k;
        private transient SensorReading reuse;

        GenerateReading(double[] profiles, int k) {
            this.profiles = profiles;
            this.k = k;
        }

        @Override
        public void open(SourceReaderContext readerContext) {
            reuse = new SensorReading();
        }

        @Override
        public SensorReading map(Long seq) {
            SensorScoring.generate(seq, profiles, k, reuse);
            return reuse;
        }
    }

    static class ComputeStats implements MapFunction<SensorReading, SensorReading> {
        private static final long serialVersionUID = 1L;

        @Override
        public SensorReading map(SensorReading r) {
            SensorScoring.computeStats(r);
            return r;
        }
    }

    static class DropFlat implements FilterFunction<SensorReading> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(SensorReading r) {
            return SensorScoring.isNotFlat(r);
        }
    }

    static class ScoreAgainstProfiles extends RichMapFunction<SensorReading, SensorReading> {
        private static final long serialVersionUID = 1L;

        private final double[] profiles;
        private final int k;
        private transient double[] z;

        ScoreAgainstProfiles(double[] profiles, int k) {
            this.profiles = profiles;
            this.k = k;
        }

        @Override
        public void open(OpenContext openContext) {
            z = new double[SensorScoring.WINDOW];
        }

        @Override
        public SensorReading map(SensorReading r) {
            SensorScoring.computeScore(r, profiles, k, z);
            return r;
        }
    }

    static class KeepHighScore implements FilterFunction<SensorReading> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(SensorReading r) {
            return SensorScoring.passesScore(r);
        }
    }

    /** Prints a {@link SensorChecksum} line (to the TaskManager's stdout) when its input ends. */
    static class ChecksumSink implements Sink<SensorReading> {
        private static final long serialVersionUID = 1L;

        @Override
        public SinkWriter<SensorReading> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                private final SensorChecksum checksum = new SensorChecksum();

                @Override
                public void write(SensorReading r, Context ctx) {
                    checksum.add(r);
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
