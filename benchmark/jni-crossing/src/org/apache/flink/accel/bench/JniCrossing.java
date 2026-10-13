package org.apache.flink.accel.bench;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

/**
 * Measures the JNI crossing cost -- the "toll" in this directory's README -- against the
 * Java-side call costs it would have to beat.
 *
 * <p>Nothing here does useful work. Every native function returns immediately or performs the
 * single cheapest access its argument type allows, so the numbers are the cost of the boundary
 * itself. Java baselines are measured in the same harness so they are directly comparable rather
 * than quoted from elsewhere.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class JniCrossing {

    static {
        String lib = System.getProperty("jnicrossing.lib");
        if (lib == null) {
            throw new IllegalStateException("-Djnicrossing.lib=<path to .so> is required");
        }
        System.load(lib);
        initPojoFields(Pojo.class);
    }

    // ------------------------------------------------------------------ native declarations

    static native void initPojoFields(Class<?> pojoClass);

    static native void nativeVoid();

    static native long nativeLong(long v);

    static native long nativeLong3(long a, long b, long c);

    static native long nativePojo(Pojo p);

    static native long nativeString(String s);

    static native long nativeDirectBufAddr(ByteBuffer buf);

    static native int nativeBatchNoop(ByteBuffer buf, int n);

    static native long nativeArrayCritical(long[] arr, int n);

    static native long nativeArrayRegion(long[] arr, int n, long[] scratch);

    // ------------------------------------------------------------------ Java-side comparators

    /** The shape the fused operator actually calls through, once per step. */
    public interface Step {
        long apply(long v);
    }

    public static final class StepA implements Step {
        public long apply(long v) {
            return v + 1;
        }
    }

    public static final class StepB implements Step {
        public long apply(long v) {
            return v + 2;
        }
    }

    public static final class StepC implements Step {
        public long apply(long v) {
            return v + 3;
        }
    }

    public static final class Pojo {
        int a;
        int b;

        Pojo(int a, int b) {
            this.a = a;
            this.b = b;
        }
    }

    // ------------------------------------------------------------------ state

    private long counter;
    private Pojo pojo;
    private String str;
    private ByteBuffer direct;
    private long[] heapArray;

    /** Monomorphic: one receiver type at this call site, so the JIT inlines it. */
    private Step mono;

    /**
     * Megamorphic: three receiver types rotate through one call site, which is exactly what
     * {@code MapFilterFusedOperator}'s {@code Step[]} loop creates today, and why that call cannot
     * be inlined.
     */
    private Step[] mega;

    @Setup
    public void setup() {
        counter = 1;
        pojo = new Pojo(3, 4);
        str = "a-moderately-typical-record-key-0001";
        direct = ByteBuffer.allocateDirect(4096 * Long.BYTES).order(ByteOrder.nativeOrder());
        heapArray = new long[4096];
        for (int i = 0; i < 4096; i++) {
            heapArray[i] = i;
            direct.putLong(i * Long.BYTES, i);
        }
        mono = new StepA();
        mega = new Step[] {new StepA(), new StepB(), new StepC()};
    }

    // ------------------------------------------------------------------ baselines

    /** JMH's own floor: whatever a benchmark method costs before anything is added to it. */
    @Benchmark
    public long baselineEmpty() {
        return counter++;
    }

    @Benchmark
    public long javaMonomorphicCall() {
        return mono.apply(counter++);
    }

    /** What one fused step costs today. This is the number a native call has to beat. */
    @Benchmark
    public long javaMegamorphicCall() {
        long v = counter++;
        for (int i = 0; i < 3; i++) {
            v = mega[i].apply(v);
        }
        return v;
    }

    // ------------------------------------------------------------------ crossing, per call

    @Benchmark
    public void jniVoid() {
        nativeVoid();
    }

    @Benchmark
    public long jniLong() {
        return nativeLong(counter++);
    }

    @Benchmark
    public long jniLong3() {
        return nativeLong3(counter++, 2, 3);
    }

    @Benchmark
    public long jniPojo() {
        return nativePojo(pojo);
    }

    @Benchmark
    public long jniString() {
        return nativeString(str);
    }

    @Benchmark
    public long jniDirectBufAddr() {
        return nativeDirectBufAddr(direct);
    }

}
