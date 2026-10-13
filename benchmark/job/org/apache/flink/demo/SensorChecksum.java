package org.apache.flink.demo;

/**
 * Order-independent fingerprint of the readings that survive {@link SensorScoringJob}: a count plus
 * a wrapping sum of per-record hashes over every output field, with doubles hashed by their raw
 * bits. Two runs print the same line iff they emitted the same multiset of records, bit for bit.
 *
 * <p>Order-independent (a sum, not a chained hash) so it stays valid if the job ever runs with
 * parallelism above 1.
 */
public final class SensorChecksum {
    private long survivors;
    private long hash;

    public void add(SensorReading r) {
        long h = SensorScoring.mix64(r.deviceId);
        h = SensorScoring.mix64(h ^ r.ts);
        for (double sample : r.samples) {
            h = SensorScoring.mix64(h ^ Double.doubleToRawLongBits(sample));
        }
        h = SensorScoring.mix64(h ^ Double.doubleToRawLongBits(r.mean));
        h = SensorScoring.mix64(h ^ Double.doubleToRawLongBits(r.stddev));
        h = SensorScoring.mix64(h ^ Double.doubleToRawLongBits(r.max));
        h = SensorScoring.mix64(h ^ Double.doubleToRawLongBits(r.score));
        hash += h;
        survivors++;
    }

    public long survivors() {
        return survivors;
    }

    /** The line both the job and the reference print; compare these verbatim. */
    public String format() {
        return String.format("CHECKSUM survivors=%d hash=%016x", survivors, hash);
    }
}
