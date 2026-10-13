package org.apache.flink.accel.bench;

import org.apache.flink.demo.SensorReading;
import org.apache.flink.demo.SensorScoring;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Per-record cost of the sensor job's actual work in plain Java ({@code W_java} in the cost
 * model), with no Flink in the path: the four {@link SensorScoring} stages called back to back, as
 * an ideally fused Java chain would.
 *
 * <p>Each invocation generates a fresh reading (the sequence number never repeats), so data is
 * cache-hot as it is in the real job, and the filters' branch outcomes are genuinely unpredictable
 * rather than a replayed pattern the branch predictor could learn. {@code generateOnly} measures
 * the generator alone; the work is {@code generateAndScore - generateOnly}. {@code statsOnly}
 * splits out stage 1, which runs on every record, from stage 3, which runs on the 85% that pass
 * stage 2 and scales with {@code k}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class SensorWork {

    @Param({"1", "4", "16", "64"})
    public int k;

    private double[] profiles;
    private double[] z;
    private SensorReading reading;
    private long seq;

    @Setup
    public void setup() {
        profiles = SensorScoring.profiles(k);
        z = new double[SensorScoring.WINDOW];
        reading = new SensorReading();
    }

    @Benchmark
    public double generateOnly() {
        SensorScoring.generate(seq++, profiles, k, reading);
        return reading.samples[SensorScoring.WINDOW - 1];
    }

    @Benchmark
    public double generateAndStats() {
        SensorScoring.generate(seq++, profiles, k, reading);
        SensorScoring.computeStats(reading);
        return reading.stddev;
    }

    @Benchmark
    public boolean generateAndScore() {
        SensorScoring.generate(seq++, profiles, k, reading);
        SensorScoring.computeStats(reading);
        if (!SensorScoring.isNotFlat(reading)) {
            return false;
        }
        SensorScoring.computeScore(reading, profiles, k, z);
        return SensorScoring.passesScore(reading);
    }
}
