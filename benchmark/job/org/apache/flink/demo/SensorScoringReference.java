package org.apache.flink.demo;

/**
 * Plain-Java oracle for {@link SensorScoringJob}: runs the same {@link SensorScoring} functions
 * over the same generated readings in a simple loop, with no Flink involved, and prints the
 * per-stage selectivity and the {@link SensorChecksum} line. A Flink run of the job in {@code
 * checksum} mode, on any distribution or variant, must print the identical {@code CHECKSUM} line.
 *
 * <p>Usage: {@code SensorScoringReference [recordCount] [k]} (defaults 1,000,000 and 1).
 */
public class SensorScoringReference {
    public static void main(String[] args) {
        long count = args.length > 0 ? Long.parseLong(args[0]) : 1_000_000L;
        int k = args.length > 1 ? Integer.parseInt(args[1]) : 1;

        double[] profiles = SensorScoring.profiles(k);
        double[] z = new double[SensorScoring.WINDOW];
        SensorReading r = new SensorReading();
        SensorChecksum checksum = new SensorChecksum();
        long notFlat = 0;

        for (long seq = 0; seq < count; seq++) {
            SensorScoring.generate(seq, profiles, k, r);
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
                "records=%d k=%d passedFlatFilter=%d (%.1f%%) passedScoreFilter=%d (%.1f%% of"
                        + " those, %.1f%% overall)%n",
                count,
                k,
                notFlat,
                100.0 * notFlat / count,
                checksum.survivors(),
                100.0 * checksum.survivors() / notFlat,
                100.0 * checksum.survivors() / count);
        System.out.println(checksum.format());
    }
}
