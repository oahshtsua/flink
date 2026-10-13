package org.apache.flink.accel.bench;

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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

/**
 * The batched half of the crossing measurement: one native call carrying {@code batch} records
 * instead of one call per record.
 *
 * <p>Results are reported per *call*. Per-record cost is that divided by {@code batch} -- which is
 * the {@code c/B} term in the cost model. The native side touches nothing, so a flat line in
 * {@code batch} is the expected (and required) result; if it is not flat, amortizing the crossing
 * does not work the way the model assumes.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class JniBatchCrossing {

    @Param({"1", "16", "64", "256", "1024", "4096"})
    public int batch;

    private ByteBuffer direct;
    private long[] heapArray;
    private long[] scratch;

    @Setup
    public void setup() {
        direct = ByteBuffer.allocateDirect(4096 * Long.BYTES).order(ByteOrder.nativeOrder());
        heapArray = new long[4096];
        scratch = new long[4096];
        for (int i = 0; i < 4096; i++) {
            heapArray[i] = i;
            direct.putLong(i * Long.BYTES, i);
        }
    }

    /** Crossing only: direct buffer in, immediate return, data untouched. */
    @Benchmark
    public int jniBatchNoop() {
        return JniCrossing.nativeBatchNoop(direct, batch);
    }

    /** Heap array reached via GetPrimitiveArrayCritical (pins the heap, blocks GC). */
    @Benchmark
    public long jniArrayCritical() {
        return JniCrossing.nativeArrayCritical(heapArray, batch);
    }

    /** Heap array reached via GetLongArrayRegion (copies, does not pin). */
    @Benchmark
    public long jniArrayRegion() {
        return JniCrossing.nativeArrayRegion(heapArray, batch, scratch);
    }
}
