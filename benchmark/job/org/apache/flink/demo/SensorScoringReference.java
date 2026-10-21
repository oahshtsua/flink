package org.apache.flink.demo;

/**
 * Plain-Java oracle for {@link SensorScoringJob}: runs the same {@link SensorScoring} functions
 * over the same generated readings in a simple loop, with no Flink involved, and prints the
 * per-stage selectivity and the {@link SensorChecksum} line. A Flink run of the job in {@code
 * checksum} mode, on any distribution or variant, must print the identical {@code CHECKSUM} line.
 *
 * <p>Usage: {@code SensorScoringReference [recordCount] [k] [chain|tuned]} (defaults 1,000,000, 1,
 * chain). {@code tuned} computes the same thing with {@link SensorScoring#scoreFused}, and must print
 * the identical {@code CHECKSUM} line.
 */
public class SensorScoringReference {
    public static void main(String[] args) {
        long count = args.length > 0 ? Long.parseLong(args[0]) : 1_000_000L;
        int k = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        boolean tuned = args.length > 2 && args[2].equals("tuned");

        double[] profiles = SensorScoring.profiles(k);
        double[] z = new double[SensorScoring.WINDOW];
        SensorReading r = new SensorReading();
        SensorChecksum checksum = new SensorChecksum();
        long notFlat = 0;

        for (long seq = 0; seq < count; seq++) {
            SensorScoring.generate(seq, profiles, k, r);
            if (tuned) {
                if (SensorScoring.scoreFused(r, profiles, k)) {
                    checksum.add(r);
                }
                // scoreFused has filled in stddev either way
                if (SensorScoring.isNotFlat(r)) {
                    notFlat++;
                }
                continue;
            }
            SensorScoring.computeStats(r);
            if (!SensorScoring.isNotFlat(r)) {
                continue;
            }
            notFlat++;
            SensorScoring.computeScore(r, profiles, k, z);
            if (SensorScoring.passesScore(r)) {
                checksum.add(r);
            }
        }

        System.out.printf(
                "records=%d k=%d mode=%s passedFlatFilter=%d (%.1f%%) passedScoreFilter=%d (%.1f%% of"
                        + " those, %.1f%% overall)%n",
                count,
                k,
                tuned ? "tuned" : "chain",
                notFlat,
                100.0 * notFlat / count,
                checksum.survivors(),
                100.0 * checksum.survivors() / notFlat,
                100.0 * checksum.survivors() / count);
        System.out.println(checksum.format());
    }
}
