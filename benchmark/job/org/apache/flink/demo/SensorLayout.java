package org.apache.flink.demo;

import java.nio.ByteBuffer;

/**
 * The sensor kernel's memory layouts and the conversions to and from them, mirroring the structs
 * in {@code native-kernel/kernels/sensor_kernel.c}. Flink-free, so the native operator and the
 * JMH breakdown benchmark share one copy.
 *
 * <pre>
 * in      80 B: int32 deviceId | int32 pad | int64 ts | double samples[8]
 * out     32 B: double mean | stddev | max | score              (survivors only)
 * params      : int32 k | int32 pad | double flatThreshold | double scoreThreshold
 *               | double profiles[k * 8]
 * </pre>
 *
 * <p>Buffers must be in native byte order.
 */
public final class SensorLayout {
    public static final int IN_BYTES = 80;
    public static final int OUT_BYTES = 32;

    private static final int WINDOW = SensorScoring.WINDOW;
    private static final int IN_SAMPLES = 16;
    private static final int PARAMS_PROFILES = 24;

    private SensorLayout() {}

    public static int paramsBytes(int k) {
        return PARAMS_PROFILES + k * WINDOW * Double.BYTES;
    }

    public static void writeParams(ByteBuffer buffer, double[] profiles, int k) {
        buffer.putInt(0, k);
        buffer.putDouble(8, SensorScoring.FLAT_THRESHOLD);
        buffer.putDouble(16, SensorScoring.SCORE_THRESHOLD);
        for (int i = 0; i < k * WINDOW; i++) {
            buffer.putDouble(PARAMS_PROFILES + i * Double.BYTES, profiles[i]);
        }
    }

    public static void encode(SensorReading r, ByteBuffer buffer, int offset) {
        buffer.putInt(offset, r.deviceId);
        buffer.putLong(offset + 8, r.ts);
        final double[] s = r.samples;
        for (int i = 0; i < WINDOW; i++) {
            buffer.putDouble(offset + IN_SAMPLES + i * Double.BYTES, s[i]);
        }
    }

    /** Fills {@code r} from kernel output at {@code outOffset} and its input slot at {@code inOffset}. */
    public static void decode(
            ByteBuffer out, int outOffset, ByteBuffer in, int inOffset, SensorReading r) {
        r.deviceId = in.getInt(inOffset);
        r.ts = in.getLong(inOffset + 8);
        final double[] s = r.samples;
        for (int i = 0; i < WINDOW; i++) {
            s[i] = in.getDouble(inOffset + IN_SAMPLES + i * Double.BYTES);
        }
        r.mean = out.getDouble(outOffset);
        r.stddev = out.getDouble(outOffset + 8);
        r.max = out.getDouble(outOffset + 16);
        r.score = out.getDouble(outOffset + 24);
    }
}
