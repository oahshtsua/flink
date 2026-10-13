package org.apache.flink.accel.bench;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Measures {@code m} -- the per-record cost of converting a real Java record into a flat,
 * native-readable layout and back again.
 *
 * <p>{@link JniCrossing} measured only the cost of *reaching* data that was already flat. This
 * measures the cost of *making* it flat -- the "repacking" term in this directory's README, which
 * the 51.7 ns {@code jniString} result suggested might dominate everything else.
 *
 * <p>Everything is measured over a fixed batch of {@value #BATCH} records and reported per record,
 * so the numbers are directly comparable to the per-record dispatch costs in {@link JniCrossing}. {@code
 * baselineTouch} is the control: what the existing Java fused operator already pays to walk the
 * same batch and touch the same fields. Marshalling cost is pure addition on top of that.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@OperationsPerInvocation(Marshalling.BATCH)
public class Marshalling {

    public static final int BATCH = 1024;

    /** Fixed-width record: the best realistic case for a flat layout. */
    public static final class FixedPojo {
        int a;
        int b;
        int c;
        long d;

        FixedPojo(int a, int b, int c, long d) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
        }
    }

    /** Variable-width record: one String field, which is where flat layouts get expensive. */
    public static final class StringPojo {
        int id;
        String name;
        long ts;

        StringPojo(int id, String name, long ts) {
            this.id = id;
            this.name = name;
            this.ts = ts;
        }
    }

    private static final int FIXED_STRIDE = 20; // 3 ints + 1 long
    private static final int LONGS_PER_FIXED = 3; // (a<<32|b), c, d

    private FixedPojo[] fixed;
    private StringPojo[] stringPojos;
    private String[] asciiStrings;
    private String[] unicodeStrings;
    private long[] longs;

    private ByteBuffer direct;
    private ByteBuffer stringIn;
    private ByteBuffer directOut;
    private long[] longScratch;
    private byte[] byteScratch;

    private CharsetEncoder encoder;
    private FixedPojo reusable;
    /** Decoded objects are parked here so escape analysis cannot scalar-replace the allocation --
     * downstream really does receive these objects, so measuring a scalar-replaced version would
     * flatter the allocating path by ~8x. */
    private FixedPojo[] decoded;
    private long sink;

    @Setup
    public void setup() {
        fixed = new FixedPojo[BATCH];
        stringPojos = new StringPojo[BATCH];
        asciiStrings = new String[BATCH];
        unicodeStrings = new String[BATCH];
        longs = new long[BATCH];
        for (int i = 0; i < BATCH; i++) {
            fixed[i] = new FixedPojo(i, i * 2, i * 3, i * 7L);
            // 36 chars: about the length of a session id or a typical event key
            asciiStrings[i] = "session-" + String.format("%028d", i);
            unicodeStrings[i] = "sitzung-über-" + String.format("%022d", i);
            stringPojos[i] = new StringPojo(i, asciiStrings[i], i * 7L);
            longs[i] = i * 31L;
        }
        direct = ByteBuffer.allocateDirect(BATCH * 128).order(ByteOrder.nativeOrder());
        directOut = ByteBuffer.allocateDirect(BATCH * 128).order(ByteOrder.nativeOrder());
        longScratch = new long[BATCH * LONGS_PER_FIXED];
        byteScratch = new byte[256];
        encoder =
                StandardCharsets.UTF_8
                        .newEncoder()
                        .onMalformedInput(CodingErrorAction.REPLACE)
                        .onUnmappableCharacter(CodingErrorAction.REPLACE);
        reusable = new FixedPojo(0, 0, 0, 0L);
        decoded = new FixedPojo[BATCH];

        // Pre-fill both input buffers so the decode benchmarks measure decoding only, with no
        // encoding smuggled into the same measurement.
        encodeFixedIntoDirect();
        stringIn = ByteBuffer.allocateDirect(BATCH * 128).order(ByteOrder.nativeOrder());
        encodeAsciiInto(stringIn);
    }

    // ------------------------------------------------------------------ controls

    /**
     * What the existing Java fused operator already pays to walk this batch and touch these
     * fields: a trivial map+filter over each record, no marshalling anywhere. Everything below is
     * cost added on top of this.
     */
    @Benchmark
    public long baselineTouch() {
        long acc = 0;
        FixedPojo[] in = fixed;
        for (int i = 0; i < BATCH; i++) {
            FixedPojo p = in[i];
            long mapped = p.a + p.b + p.c + p.d;
            if (mapped > 0) {
                acc += mapped;
            }
        }
        return acc;
    }

    /** The same control for String-bearing records. */
    @Benchmark
    public long baselineTouchString() {
        long acc = 0;
        StringPojo[] in = stringPojos;
        for (int i = 0; i < BATCH; i++) {
            StringPojo p = in[i];
            acc += p.id + p.ts + p.name.length();
        }
        return acc;
    }

    // ------------------------------------------------------------------ encode: Java -> flat

    @Benchmark
    public ByteBuffer encodeLongDirect() {
        ByteBuffer b = direct;
        long[] in = longs;
        for (int i = 0; i < BATCH; i++) {
            b.putLong(i * Long.BYTES, in[i]);
        }
        return b;
    }

    @Benchmark
    public long[] encodeLongArray() {
        long[] out = longScratch;
        long[] in = longs;
        System.arraycopy(in, 0, out, 0, BATCH);
        return out;
    }

    @Benchmark
    public ByteBuffer encodeFixedDirect() {
        return encodeFixedIntoDirect();
    }

    private ByteBuffer encodeFixedIntoDirect() {
        ByteBuffer b = direct;
        FixedPojo[] in = fixed;
        for (int i = 0; i < BATCH; i++) {
            FixedPojo p = in[i];
            int off = i * FIXED_STRIDE;
            b.putInt(off, p.a);
            b.putInt(off + 4, p.b);
            b.putInt(off + 8, p.c);
            b.putLong(off + 12, p.d);
        }
        return b;
    }

    @Benchmark
    public long[] encodeFixedLongArray() {
        long[] out = longScratch;
        FixedPojo[] in = fixed;
        for (int i = 0; i < BATCH; i++) {
            FixedPojo p = in[i];
            int off = i * LONGS_PER_FIXED;
            out[off] = ((long) p.a << 32) | (p.b & 0xffffffffL);
            out[off + 1] = p.c;
            out[off + 2] = p.d;
        }
        return out;
    }

    /** String -> UTF-8 via getBytes: allocates one byte[] per record. */
    @Benchmark
    public ByteBuffer encodeStringGetBytes() {
        ByteBuffer b = direct;
        String[] in = asciiStrings;
        int off = 0;
        for (int i = 0; i < BATCH; i++) {
            byte[] bytes = in[i].getBytes(StandardCharsets.UTF_8);
            b.putInt(off, bytes.length);
            b.put(off + 4, bytes, 0, bytes.length);
            off += 4 + bytes.length;
        }
        return b;
    }

    /** Same, but non-ASCII: defeats the compact-string LATIN1 fast path. */
    @Benchmark
    public ByteBuffer encodeStringGetBytesUnicode() {
        ByteBuffer b = direct;
        String[] in = unicodeStrings;
        int off = 0;
        for (int i = 0; i < BATCH; i++) {
            byte[] bytes = in[i].getBytes(StandardCharsets.UTF_8);
            b.putInt(off, bytes.length);
            b.put(off + 4, bytes, 0, bytes.length);
            off += 4 + bytes.length;
        }
        return b;
    }

    /** String -> UTF-8 via a reused CharsetEncoder straight into the buffer: no byte[] per record. */
    @Benchmark
    public ByteBuffer encodeStringEncoder() {
        ByteBuffer b = direct;
        b.clear();
        String[] in = asciiStrings;
        CharsetEncoder enc = encoder;
        for (int i = 0; i < BATCH; i++) {
            int lenPos = b.position();
            b.putInt(0);
            enc.reset();
            enc.encode(CharBuffer.wrap(in[i]), b, true);
            b.putInt(lenPos, b.position() - lenPos - 4);
        }
        return b;
    }

    /** The realistic mixed record: two primitives plus a variable-width String. */
    @Benchmark
    public ByteBuffer encodeStringPojo() {
        ByteBuffer b = direct;
        StringPojo[] in = stringPojos;
        int off = 0;
        for (int i = 0; i < BATCH; i++) {
            StringPojo p = in[i];
            b.putInt(off, p.id);
            b.putLong(off + 4, p.ts);
            byte[] bytes = p.name.getBytes(StandardCharsets.UTF_8);
            b.putInt(off + 12, bytes.length);
            b.put(off + 16, bytes, 0, bytes.length);
            off += 16 + bytes.length;
        }
        return b;
    }

    // ------------------------------------------------------------------ decode: flat -> Java

    @Benchmark
    public long decodeFixedConsume() {
        ByteBuffer b = direct;
        long acc = 0;
        for (int i = 0; i < BATCH; i++) {
            int off = i * FIXED_STRIDE;
            acc += b.getInt(off) + b.getInt(off + 4) + b.getInt(off + 8) + b.getLong(off + 12);
        }
        return acc;
    }

    /** Decode into a fresh object per record: one allocation per record, parked in an array so it
     * genuinely escapes. */
    @Benchmark
    public FixedPojo[] decodeFixedAlloc() {
        ByteBuffer b = direct;
        FixedPojo[] out = decoded;
        for (int i = 0; i < BATCH; i++) {
            int off = i * FIXED_STRIDE;
            out[i] =
                    new FixedPojo(
                            b.getInt(off), b.getInt(off + 4), b.getInt(off + 8), b.getLong(off + 12));
        }
        return out;
    }

    /** Decode into one reused mutable object -- legitimate here, since fusion already requires
     * object reuse to be enabled. */
    @Benchmark
    public long decodeFixedReuse() {
        ByteBuffer b = direct;
        FixedPojo p = reusable;
        long acc = 0;
        for (int i = 0; i < BATCH; i++) {
            int off = i * FIXED_STRIDE;
            p.a = b.getInt(off);
            p.b = b.getInt(off + 4);
            p.c = b.getInt(off + 8);
            p.d = b.getLong(off + 12);
            acc += p.a + p.d;
        }
        return acc;
    }

    /** Decode UTF-8 back into Java Strings: allocates a byte[] and a String per record. */
    @Benchmark
    public long decodeStringAlloc() {
        ByteBuffer b = stringIn;
        long acc = 0;
        int off = 0;
        for (int i = 0; i < BATCH; i++) {
            int len = b.getInt(off);
            byte[] tmp = new byte[len];
            b.get(off + 4, tmp, 0, len);
            acc += new String(tmp, StandardCharsets.UTF_8).length();
            off += 4 + len;
        }
        return acc;
    }

    /** Decode the mixed (id, ts, len, utf8) layout that {@link #encodeStringPojo} writes. */
    private long decodeStringPojoFrom(ByteBuffer b) {
        long acc = 0;
        int off = 0;
        for (int i = 0; i < BATCH; i++) {
            int id = b.getInt(off);
            long ts = b.getLong(off + 4);
            int len = b.getInt(off + 12);
            byte[] tmp = new byte[len];
            b.get(off + 16, tmp, 0, len);
            acc += id + ts + new String(tmp, StandardCharsets.UTF_8).length();
            off += 16 + len;
        }
        return acc;
    }

    private int encodeAsciiInto(ByteBuffer b) {
        String[] in = asciiStrings;
        int off = 0;
        for (int i = 0; i < BATCH; i++) {
            byte[] bytes = in[i].getBytes(StandardCharsets.UTF_8);
            b.putInt(off, bytes.length);
            b.put(off + 4, bytes, 0, bytes.length);
            off += 4 + bytes.length;
        }
        return off;
    }

    // ------------------------------------------------------------------ full round trip

    /**
     * The honest end-to-end boundary cost for a fixed-width record: encode the batch, make the one
     * native call, decode the batch back into a reused object. This is the number that has to be
     * compared against the ~7 ns/record the Java fused operator spends on dispatch today.
     */
    @Benchmark
    public long roundTripFixed() {
        encodeFixedIntoDirect();
        JniCrossing.nativeBatchNoop(direct, BATCH);
        return decodeFixedReuse();
    }

    /** The same round trip for a record carrying a String. */
    @Benchmark
    public long roundTripStringPojo() {
        encodeStringPojo();
        JniCrossing.nativeBatchNoop(direct, BATCH);
        return decodeStringPojoFrom(direct);
    }
}
