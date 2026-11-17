package org.apache.flink.demo;

import org.apache.arrow.memory.ArrowBuf;

/**
 * The sensor job's four steps in Java, over Arrow columns instead of {@link SensorReading}s: the
 * work the Arrow + Java variants do in place of the C kernel. Flink-free.
 *
 * <p>Two shapes, both bit-identical to {@link SensorScoring} (every expression is copied from it
 * unchanged, in the same order; the checksum check confirms it):
 *
 * <ul>
 *   <li>{@link #scoreFused}: all four steps in one loop, the Java twin of {@code
 *       sensor_score_arrow} - arranged like {@link SensorScoring#scoreFused}, the best Java of
 *       Idea 4;
 *   <li>{@link #computeStats}, {@link #computeScore} and {@link #filterAtLeast}: the steps one at a
 *       time, each a pass over the batch that writes a result column or shrinks the selection,
 *       arranged like {@link SensorScoring#computeStats} and friends.
 * </ul>
 *
 * <p>Row {@code r}'s samples are doubles {@code [r * WINDOW, (r + 1) * WINDOW)} of {@code samples}
 * (the values buffer of the fixed-size-list column). A selection is {@code count} int32 row
 * indices, or {@code null} for rows {@code [0, count)}. Results are written at the row's own index.
 */
public final class SensorArrowScoring {
    private static final int WINDOW = SensorScoring.WINDOW;
    private static final long ROW_BYTES = (long) WINDOW * Double.BYTES;

    private SensorArrowScoring() {}

    /**
     * All four steps over rows {@code [0, n)}: writes mean/stddev/max/score for every survivor and
     * the survivors' row indices to {@code sel}. Returns the number of survivors.
     */
    public static int scoreFused(
            ArrowBuf samples,
            int n,
            double[] profiles,
            int k,
            ArrowBuf meanOut,
            ArrowBuf stddevOut,
            ArrowBuf maxOut,
            ArrowBuf scoreOut,
            ArrowBuf sel) {
        int survivors = 0;
        for (int r = 0; r < n; r++) {
            final long at = r * ROW_BYTES;
            final double s0 = samples.getDouble(at), s1 = samples.getDouble(at + 8);
            final double s2 = samples.getDouble(at + 16), s3 = samples.getDouble(at + 24);
            final double s4 = samples.getDouble(at + 32), s5 = samples.getDouble(at + 40);
            final double s6 = samples.getDouble(at + 48), s7 = samples.getDouble(at + 56);

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
            if (!(stddev >= SensorScoring.FLAT_THRESHOLD)) {
                continue;
            }

            final double inv = 1.0 / stddev;
            final double z0 = d0 * inv, z1 = d1 * inv, z2 = d2 * inv, z3 = d3 * inv;
            final double z4 = d4 * inv, z5 = d5 * inv, z6 = d6 * inv, z7 = d7 * inv;
            double best = Double.NEGATIVE_INFINITY;
            for (int b = 0, end = k * WINDOW; b < end; b += WINDOW) {
                final double dot =
                        0.0 + z0 * profiles[b] + z1 * profiles[b + 1] + z2 * profiles[b + 2]
                                + z3 * profiles[b + 3] + z4 * profiles[b + 4]
                                + z5 * profiles[b + 5] + z6 * profiles[b + 6]
                                + z7 * profiles[b + 7];
                if (dot > best) {
                    best = dot;
                }
            }
            final double score = best / WINDOW;
            if (!(score >= SensorScoring.SCORE_THRESHOLD)) {
                continue;
            }

            final long out = (long) r * Double.BYTES;
            meanOut.setDouble(out, mean);
            stddevOut.setDouble(out, stddev);
            maxOut.setDouble(out, max);
            scoreOut.setDouble(out, score);
            sel.setInt((long) survivors * Integer.BYTES, r);
            survivors++;
        }
        return survivors;
    }

    /** Step 1 (map): mean, population standard deviation and maximum of each selected row. */
    public static void computeStats(
            ArrowBuf samples,
            ArrowBuf selIn,
            int count,
            ArrowBuf meanOut,
            ArrowBuf stddevOut,
            ArrowBuf maxOut) {
        for (int i = 0; i < count; i++) {
            final int r = row(selIn, i);
            final long at = r * ROW_BYTES;
            double sum = 0.0;
            double max = samples.getDouble(at);
            for (int j = 0; j < WINDOW; j++) {
                final double s = samples.getDouble(at + (long) j * Double.BYTES);
                sum += s;
                if (s > max) {
                    max = s;
                }
            }
            final double mean = sum / WINDOW;
            double sq = 0.0;
            for (int j = 0; j < WINDOW; j++) {
                final double d = samples.getDouble(at + (long) j * Double.BYTES) - mean;
                sq += d * d;
            }
            final long out = (long) r * Double.BYTES;
            meanOut.setDouble(out, mean);
            stddevOut.setDouble(out, Math.sqrt(sq / WINDOW));
            maxOut.setDouble(out, max);
        }
    }

    /**
     * Step 3 (map): z-normalize each selected row and score it as its best correlation with the
     * {@code k} profiles.
     *
     * @param z scratch space of at least {@code WINDOW} doubles
     */
    public static void computeScore(
            ArrowBuf samples,
            ArrowBuf mean,
            ArrowBuf stddev,
            ArrowBuf selIn,
            int count,
            double[] profiles,
            int k,
            double[] z,
            ArrowBuf scoreOut) {
        for (int i = 0; i < count; i++) {
            final int r = row(selIn, i);
            final long at = r * ROW_BYTES;
            final long col = (long) r * Double.BYTES;
            final double m = mean.getDouble(col);
            final double inv = 1.0 / stddev.getDouble(col);
            for (int j = 0; j < WINDOW; j++) {
                z[j] = (samples.getDouble(at + (long) j * Double.BYTES) - m) * inv;
            }
            double best = Double.NEGATIVE_INFINITY;
            for (int p = 0; p < k; p++) {
                final int base = p * WINDOW;
                double dot = 0.0;
                for (int j = 0; j < WINDOW; j++) {
                    dot += z[j] * profiles[base + j];
                }
                if (dot > best) {
                    best = dot;
                }
            }
            scoreOut.setDouble(col, best / WINDOW);
        }
    }

    /**
     * Steps 2 and 4 (filter): keeps the selected rows whose {@code column} value is at least
     * {@code threshold}, writing their indices to {@code selOut}. Returns how many were kept.
     */
    public static int filterAtLeast(
            ArrowBuf column, double threshold, ArrowBuf selIn, int count, ArrowBuf selOut) {
        int kept = 0;
        for (int i = 0; i < count; i++) {
            final int r = row(selIn, i);
            if (column.getDouble((long) r * Double.BYTES) >= threshold) {
                selOut.setInt((long) kept * Integer.BYTES, r);
                kept++;
            }
        }
        return kept;
    }

    private static int row(ArrowBuf sel, int i) {
        return sel == null ? i : sel.getInt((long) i * Integer.BYTES);
    }
}
