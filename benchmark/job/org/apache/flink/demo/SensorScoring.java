package org.apache.flink.demo;

/**
 * The arithmetic of {@link SensorScoringJob}, as plain static functions with no Flink dependency,
 * so the Flink job, the plain-Java reference ({@link SensorScoringReference}) and the JMH work
 * measurement all run exactly the same code.
 *
 * <p>This class is the single definition of the order of floating-point operations. The bit-exact
 * (v1, scalar) C kernel must reproduce it operation for operation: sums accumulated left to right
 * starting from {@code 0.0}, divisions where this code divides, no fused multiply-add (compile
 * with {@code -ffp-contract=off}), and maxima taken with a strict {@code >} comparison rather than
 * {@code Math.max}, whose {@code -0.0}/NaN handling differs from C's {@code fmax}. Only {@code +
 * - * /} and {@code Math.sqrt} are used, because those are correctly rounded in both languages;
 * {@code Math.exp}/{@code pow}/{@code sin} are not guaranteed bit-identical to C's libm.
 *
 * <p>The generator ({@link #generate}) and the reference profiles ({@link #profiles}) do use
 * {@code Math.sin}, but they only ever run in Java: generation happens before any kernel, and the
 * profiles are computed once in Java and handed to the kernel as data.
 */
public final class SensorScoring {
    public static final int WINDOW = SensorReading.WINDOW;

    /** Windows whose population standard deviation is below this are flat signals and dropped. */
    public static final double FLAT_THRESHOLD = 0.01;

    /**
     * Readings whose best correlation with a reference profile is below this are dropped. High
     * enough that pure-noise windows rarely pass even when scored against 64 profiles.
     */
    public static final double SCORE_THRESHOLD = 0.9;

    public static final int NUM_DEVICES = 1024;

    private static final long GOLDEN_GAMMA = 0x9E3779B97F4A7C15L;

    private SensorScoring() {}

    // ------------------------------------------------------------------------------------------
    // The four stages
    // ------------------------------------------------------------------------------------------

    /** Stage 1 (map): mean, population standard deviation and maximum of the window. */
    public static void computeStats(SensorReading r) {
        final double[] s = r.samples;
        double sum = 0.0;
        double max = s[0];
        for (int i = 0; i < WINDOW; i++) {
            sum += s[i];
            if (s[i] > max) {
                max = s[i];
            }
        }
        final double mean = sum / WINDOW;
        double sq = 0.0;
        for (int i = 0; i < WINDOW; i++) {
            final double d = s[i] - mean;
            sq += d * d;
        }
        r.mean = mean;
        r.stddev = Math.sqrt(sq / WINDOW);
        r.max = max;
    }

    /** Stage 2 (filter): keep windows that are not flat. Also guarantees stage 3's divisor > 0. */
    public static boolean isNotFlat(SensorReading r) {
        return r.stddev >= FLAT_THRESHOLD;
    }

    /**
     * Stage 3 (map): z-normalize the window, then score it as its best correlation with any of the
     * {@code k} reference profiles. With zero-mean, unit-variance profiles, {@code dot / WINDOW}
     * is the Pearson correlation, so the score is in [-1, 1].
     *
     * @param profiles {@code k * WINDOW} doubles, profile {@code p} at {@code [p * WINDOW, (p + 1) *
     *     WINDOW)}, as produced by {@link #profiles}
     * @param z scratch space of at least {@code WINDOW} doubles, owned by the caller so this stays
     *     allocation-free
     */
    public static void computeScore(SensorReading r, double[] profiles, int k, double[] z) {
        final double[] s = r.samples;
        final double mean = r.mean;
        final double inv = 1.0 / r.stddev;
        for (int i = 0; i < WINDOW; i++) {
            z[i] = (s[i] - mean) * inv;
        }
        double best = Double.NEGATIVE_INFINITY;
        for (int p = 0; p < k; p++) {
            final int base = p * WINDOW;
            double dot = 0.0;
            for (int i = 0; i < WINDOW; i++) {
                dot += z[i] * profiles[base + i];
            }
            if (dot > best) {
                best = dot;
            }
        }
        r.score = best / WINDOW;
    }

    /** Stage 4 (filter): keep readings that resemble a reference profile closely enough. */
    public static boolean passesScore(SensorReading r) {
        return r.score >= SCORE_THRESHOLD;
    }

    // ------------------------------------------------------------------------------------------
    // All four stages as one hand-tuned function
    // ------------------------------------------------------------------------------------------

    /**
     * All four stages in one method, restructured the way the C kernel's compiler restructures
     * them - the Java a person or an agent could write instead of C. Returns whether the reading
     * survives; fills in the same fields as the separate stages.
     *
     * <p>Two changes, neither of which changes a single bit of the result: the 8 samples, their
     * differences from the mean and the normalized values are locals (registers) instead of
     * array reads and a scratch array, and normalization reuses the differences already computed
     * for the standard deviation ({@code s[i] - mean} is the same operation on the same inputs).
     * Every sum is still accumulated left to right from {@code 0.0}, so the output is bit-identical
     * to {@link #computeStats} + {@link #isNotFlat} + {@link #computeScore} + {@link #passesScore}.
     */
    public static boolean scoreFused(SensorReading r, double[] profiles, int k) {
        final double[] s = r.samples;
        final double s0 = s[0], s1 = s[1], s2 = s[2], s3 = s[3];
        final double s4 = s[4], s5 = s[5], s6 = s[6], s7 = s[7];

        double max = s0;
        if (s1 > max) {
            max = s1;
        }
        if (s2 > max) {
            max = s2;
        }
        if (s3 > max) {
            max = s3;
        }
        if (s4 > max) {
            max = s4;
        }
        if (s5 > max) {
            max = s5;
        }
        if (s6 > max) {
            max = s6;
        }
        if (s7 > max) {
            max = s7;
        }
        final double mean = (0.0 + s0 + s1 + s2 + s3 + s4 + s5 + s6 + s7) / WINDOW;
        final double d0 = s0 - mean, d1 = s1 - mean, d2 = s2 - mean, d3 = s3 - mean;
        final double d4 = s4 - mean, d5 = s5 - mean, d6 = s6 - mean, d7 = s7 - mean;
        final double sq =
                0.0 + d0 * d0 + d1 * d1 + d2 * d2 + d3 * d3 + d4 * d4 + d5 * d5 + d6 * d6
                        + d7 * d7;
        final double stddev = Math.sqrt(sq / WINDOW);
        r.mean = mean;
        r.stddev = stddev;
        r.max = max;
        if (!(stddev >= FLAT_THRESHOLD)) {
            return false;
        }

        final double inv = 1.0 / stddev;
        final double z0 = d0 * inv, z1 = d1 * inv, z2 = d2 * inv, z3 = d3 * inv;
        final double z4 = d4 * inv, z5 = d5 * inv, z6 = d6 * inv, z7 = d7 * inv;
        double best = Double.NEGATIVE_INFINITY;
        for (int b = 0, end = k * WINDOW; b < end; b += WINDOW) {
            final double dot =
                    0.0 + z0 * profiles[b] + z1 * profiles[b + 1] + z2 * profiles[b + 2]
                            + z3 * profiles[b + 3] + z4 * profiles[b + 4] + z5 * profiles[b + 5]
                            + z6 * profiles[b + 6] + z7 * profiles[b + 7];
            if (dot > best) {
                best = dot;
            }
        }
        r.score = best / WINDOW;
        return r.score >= SCORE_THRESHOLD;
    }

    // ------------------------------------------------------------------------------------------
    // Inputs: reference profiles and the synthetic data generator
    // ------------------------------------------------------------------------------------------

    /**
     * {@code k} reference profiles, flattened: profile {@code p} is a sampled sine of 1, 2 or 3
     * cycles per window with phase {@code 0.7 * p}, normalized to zero mean and unit population
     * variance.
     */
    public static double[] profiles(int k) {
        if (k < 1) {
            throw new IllegalArgumentException("k must be >= 1, got " + k);
        }
        final double[] out = new double[k * WINDOW];
        final double[] raw = new double[WINDOW];
        for (int p = 0; p < k; p++) {
            final int cycles = 1 + (p % 3);
            final double phase = 0.7 * p;
            double sum = 0.0;
            for (int i = 0; i < WINDOW; i++) {
                raw[i] = Math.sin(2.0 * Math.PI * cycles * i / WINDOW + phase);
                sum += raw[i];
            }
            final double mean = sum / WINDOW;
            double sq = 0.0;
            for (int i = 0; i < WINDOW; i++) {
                sq += (raw[i] - mean) * (raw[i] - mean);
            }
            final double std = Math.sqrt(sq / WINDOW);
            for (int i = 0; i < WINDOW; i++) {
                out[p * WINDOW + i] = (raw[i] - mean) / std;
            }
        }
        return out;
    }

    /**
     * Fills {@code r} with the reading for sequence number {@code seq}. Deterministic in {@code
     * (seq, k)}, so every variant and the reference see identical data. Three kinds of window:
     *
     * <ul>
     *   <li>~15% flat: a constant level plus noise of amplitude 1e-4 (dropped by stage 2),
     *   <li>~30% a scaled, offset copy of one reference profile plus 30% noise (pass stage 4),
     *   <li>~55% uniform noise (pass stage 2, almost all dropped by stage 4).
     * </ul>
     */
    public static void generate(long seq, double[] profiles, int k, SensorReading r) {
        final long seed = mix64(seq);
        r.deviceId = (int) (seq % NUM_DEVICES);
        r.ts = seq;

        final double kind = unit(rand(seed, 0));
        final double level = 20.0 + 10.0 * signedUnit(rand(seed, 1));
        final double amplitude = 1.0 + 4.0 * unit(rand(seed, 2));
        final double[] s = r.samples;
        if (kind < 0.15) {
            for (int i = 0; i < WINDOW; i++) {
                s[i] = level + 1e-4 * signedUnit(rand(seed, 8 + i));
            }
        } else if (kind < 0.45) {
            final int base = (int) (unit(rand(seed, 3)) * k) * WINDOW;
            for (int i = 0; i < WINDOW; i++) {
                s[i] = level + amplitude * (profiles[base + i] + 0.3 * signedUnit(rand(seed, 8 + i)));
            }
        } else {
            for (int i = 0; i < WINDOW; i++) {
                s[i] = level + amplitude * signedUnit(rand(seed, 8 + i));
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Hashing (splitmix64 finalizer), shared by the generator and SensorChecksum
    // ------------------------------------------------------------------------------------------

    static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    private static long rand(long seed, int stream) {
        return mix64(seed + stream * GOLDEN_GAMMA);
    }

    /** Uniform in [0, 1). */
    private static double unit(long bits) {
        return (bits >>> 11) * 0x1.0p-53;
    }

    /** Uniform in [-1, 1). */
    private static double signedUnit(long bits) {
        return 2.0 * unit(bits) - 1.0;
    }
}
