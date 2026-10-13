package org.apache.flink.demo;

/**
 * One sensor reading flowing through {@link SensorScoringJob}: a device id, a payload timestamp,
 * and a window of {@link #WINDOW} samples (8 doubles = 64 bytes = one cache line), plus the fields
 * the job's stages derive from it.
 *
 * <p>Mutable on purpose. Every stage writes its result into the same object and returns it, so no
 * variant of the job (vanilla, fused, native) allocates per record per stage, and the comparison
 * between them measures arithmetic plus dispatch rather than garbage collection.
 *
 * <p>A Flink POJO (public class, public no-arg constructor, public fields), so Flink serializes it
 * with {@code PojoSerializer} wherever it crosses a non-chained edge.
 */
public class SensorReading {
    public static final int WINDOW = 8;

    public int deviceId;
    public long ts;
    public double[] samples = new double[WINDOW];

    /** Written by the statistics stage. */
    public double mean;

    public double stddev;
    public double max;

    /** Written by the scoring stage. */
    public double score;

    public SensorReading() {}
}
