package org.apache.flink.accel.bench;

import org.apache.flink.accel.NativeBridge;
import org.apache.flink.demo.SensorLayout;
import org.apache.flink.demo.SensorReading;
import org.apache.flink.demo.SensorScoring;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

/**
 * The Java and native paths of the sensor job per input record, with no Flink in the way, shaped
 * like the real operators. Every invocation processes {@value #RECORDS} fresh readings (sequence
 * numbers never repeat, so filter outcomes are unpredictable), each generated into one reused
 * object right before it is consumed - as in the chained Flink job, where the source hands each
 * reading straight to the next step.
 *
 * <ul>
 *   <li>{@code generate}: generation alone, the floor every path pays
 *   <li>{@code java}: generate, then the four stages in plain Java - an ideally fused Java chain
 *   <li>{@code encode}: generate, then pack into the kernel's input buffer
 *   <li>{@code encodeKernel}: + one JNI call into {@code sensor_score} per {@code batch} records
 *   <li>{@code nativePath}: + rebuilding every survivor - what the native operator does per record
 * </ul>
 *
 * <p>Needs the bridge and kernel libraries: {@code -Dflink.accel.bridge.lib} and {@code
 * -Dsensor.kernel.lib} (run.sh passes both).
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class SensorBatch {
    /** Records per invocation; a multiple of every {@link #batch} value. */
    static final int RECORDS = 4096;

    @Param({"1", "4", "16", "64"})
    public int k;

    /** The native operator's batch size: records per JNI call. Irrelevant to generate/java. */
    @Param({"1", "16", "64", "256", "1024", "4096"})
    public int batch;

    private double[] profiles;
    private double[] z;
    private SensorReading reading;
    private SensorReading decoded;
    private long seq;

    private ByteBuffer params;
    private ByteBuffer in;
    private ByteBuffer out;
    private ByteBuffer sel;
    private long library;
    private long kernel;
    private long paramsAddress;
    private long inAddress;
    private long outAddress;
    private long selAddress;

    @Setup(Level.Trial)
    public void setup() {
        if (RECORDS % batch != 0) {
            throw new IllegalArgumentException("batch must divide " + RECORDS + ": " + batch);
        }
        profiles = SensorScoring.profiles(k);
        z = new double[SensorScoring.WINDOW];
        reading = new SensorReading();
        decoded = new SensorReading();

        params = allocate(SensorLayout.paramsBytes(k));
        in = allocate(batch * SensorLayout.IN_BYTES);
        out = allocate(batch * SensorLayout.OUT_BYTES);
        sel = allocate(batch * Integer.BYTES);
        SensorLayout.writeParams(params, profiles, k);

        library = NativeBridge.open(System.getProperty("sensor.kernel.lib"));
        kernel = NativeBridge.lookup(library, "sensor_score");
        paramsAddress = NativeBridge.address(params);
        inAddress = NativeBridge.address(in);
        outAddress = NativeBridge.address(out);
        selAddress = NativeBridge.address(sel);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        NativeBridge.close(library);
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS)
    public double generate() {
        double acc = 0.0;
        for (int i = 0; i < RECORDS; i++) {
            SensorScoring.generate(seq++, profiles, k, reading);
            acc += reading.samples[SensorScoring.WINDOW - 1];
        }
        return acc;
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS)
    public int java() {
        int survivors = 0;
        for (int i = 0; i < RECORDS; i++) {
            final SensorReading r = reading;
            SensorScoring.generate(seq++, profiles, k, r);
            SensorScoring.computeStats(r);
            if (!SensorScoring.isNotFlat(r)) {
                continue;
            }
            SensorScoring.computeScore(r, profiles, k, z);
            if (SensorScoring.passesScore(r)) {
                survivors++;
            }
        }
        return survivors;
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS)
    public long encode() {
        for (int base = 0; base < RECORDS; base += batch) {
            fillBatch();
        }
        return in.getLong(8);
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS)
    public int encodeKernel() {
        int survivors = 0;
        for (int base = 0; base < RECORDS; base += batch) {
            fillBatch();
            survivors += invokeKernel();
        }
        return survivors;
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS)
    public double nativePath() {
        double acc = 0.0;
        for (int base = 0; base < RECORDS; base += batch) {
            fillBatch();
            final int survivors = invokeKernel();
            for (int i = 0; i < survivors; i++) {
                final int slot = sel.getInt(i * Integer.BYTES);
                SensorLayout.decode(
                        out, i * SensorLayout.OUT_BYTES, in, slot * SensorLayout.IN_BYTES, decoded);
                acc += decoded.score;
            }
        }
        return acc;
    }

    /** Generate-then-encode one batch, record by record, as the operator's processElement does. */
    private void fillBatch() {
        for (int i = 0; i < batch; i++) {
            SensorScoring.generate(seq++, profiles, k, reading);
            SensorLayout.encode(reading, in, i * SensorLayout.IN_BYTES);
        }
    }

    private int invokeKernel() {
        return NativeBridge.invoke(kernel, paramsAddress, inAddress, batch, outAddress, selAddress);
    }

    private static ByteBuffer allocate(int bytes) {
        final int rounded = (bytes + 63) / 64 * 64;
        return ByteBuffer.allocateDirect(rounded + 64).alignedSlice(64).order(ByteOrder.nativeOrder());
    }
}
