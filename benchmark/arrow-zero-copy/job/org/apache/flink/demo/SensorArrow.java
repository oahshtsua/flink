package org.apache.flink.demo;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The sensor job's data as Arrow columns, mirroring the structs read by {@code
 * native-kernel/kernels/sensor_arrow_kernel.c}. Flink-free.
 *
 * <pre>
 * input   0 device_id  int32
 *         1 ts         int64
 *         2 samples    fixed_size_list&lt;float64&gt;[8]   (one block of rows * 8 doubles)
 * results   mean, stddev, max, score                    float64 each, row-aligned with the input
 * </pre>
 *
 * <p>Vectors are filled by writing their buffers directly, not through the vectors' setters, so
 * validity bits are never touched per row: {@link #allocate} sets every validity bit once ({@link
 * ArrowMemory#markAllValid}), and the columns never hold nulls.
 */
public final class SensorArrow {
    public static final int DEVICE_ID = 0;
    public static final int TS = 1;
    public static final int SAMPLES = 2;

    private static final int WINDOW = SensorScoring.WINDOW;

    private static final FieldType FLOAT64 =
            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));

    public static final Schema INPUT_SCHEMA =
            new Schema(
                    Arrays.asList(
                            new Field(
                                    "device_id",
                                    FieldType.notNullable(new ArrowType.Int(32, true)),
                                    null),
                            new Field(
                                    "ts", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                            new Field(
                                    "samples",
                                    FieldType.notNullable(new ArrowType.FixedSizeList(WINDOW)),
                                    Collections.singletonList(
                                            new Field("sample", FLOAT64, null)))));

    /** In the order the kernel's {@code out} table expects them. */
    public static final List<Field> RESULT_FIELDS =
            Arrays.asList(
                    new Field("mean", FLOAT64, null),
                    new Field("stddev", FLOAT64, null),
                    new Field("max", FLOAT64, null),
                    new Field("score", FLOAT64, null));

    private SensorArrow() {}

    /** An empty input batch with room for {@code capacity} rows, every validity bit set. */
    public static VectorSchemaRoot allocate(BufferAllocator allocator, int capacity) {
        final VectorSchemaRoot root = VectorSchemaRoot.create(INPUT_SCHEMA, allocator);
        for (FieldVector vector : root.getFieldVectors()) {
            vector.setInitialCapacity(capacity);
            vector.allocateNew();
            ArrowMemory.markAllValid(vector);
        }
        return root;
    }

    /**
     * Fills rows {@code [0, n)} of {@code root} with readings {@code firstSeq ...} - the same data
     * as {@link SensorScoring#generate}, written straight into the column buffers (as a source
     * decoding into Arrow would) instead of into a {@link SensorReading}. Every expression is
     * copied from {@code generate} unchanged, so the values are bit-identical; the checksum check
     * confirms it.
     */
    public static void fill(
            VectorSchemaRoot root, long firstSeq, int n, double[] profiles, int k) {
        final ArrowBuf deviceIds = root.getVector(DEVICE_ID).getDataBuffer();
        final ArrowBuf tss = root.getVector(TS).getDataBuffer();
        final ArrowBuf samples =
                ((FixedSizeListVector) root.getVector(SAMPLES)).getDataVector().getDataBuffer();
        for (int row = 0; row < n; row++) {
            final long seq = firstSeq + row;
            final long seed = SensorScoring.mix64(seq);
            deviceIds.setInt((long) row * Integer.BYTES, (int) (seq % SensorScoring.NUM_DEVICES));
            tss.setLong((long) row * Long.BYTES, seq);

            final double kind = SensorScoring.unit(SensorScoring.rand(seed, 0));
            final double level = 20.0 + 10.0 * SensorScoring.signedUnit(SensorScoring.rand(seed, 1));
            final double amplitude = 1.0 + 4.0 * SensorScoring.unit(SensorScoring.rand(seed, 2));
            final long s = (long) row * WINDOW * Double.BYTES;
            if (kind < 0.15) {
                for (int i = 0; i < WINDOW; i++) {
                    samples.setDouble(
                            s + i * Double.BYTES,
                            level + 1e-4 * SensorScoring.signedUnit(SensorScoring.rand(seed, 8 + i)));
                }
            } else if (kind < 0.45) {
                final int base = (int) (SensorScoring.unit(SensorScoring.rand(seed, 3)) * k) * WINDOW;
                for (int i = 0; i < WINDOW; i++) {
                    samples.setDouble(
                            s + i * Double.BYTES,
                            level
                                    + amplitude
                                            * (profiles[base + i]
                                                    + 0.3
                                                            * SensorScoring.signedUnit(
                                                                    SensorScoring.rand(
                                                                            seed, 8 + i))));
                }
            } else {
                for (int i = 0; i < WINDOW; i++) {
                    samples.setDouble(
                            s + i * Double.BYTES,
                            level
                                    + amplitude
                                            * SensorScoring.signedUnit(
                                                    SensorScoring.rand(seed, 8 + i)));
                }
            }
        }
        root.setRowCount(n);
    }

    /**
     * Copies row {@code row} of an output batch (input columns followed by {@link #RESULT_FIELDS})
     * into {@code r}. Only for the checksum sink; the timed path never builds a reading.
     */
    public static void read(VectorSchemaRoot batch, int row, SensorReading r) {
        r.deviceId = batch.getVector(DEVICE_ID).getDataBuffer().getInt((long) row * Integer.BYTES);
        r.ts = batch.getVector(TS).getDataBuffer().getLong((long) row * Long.BYTES);
        final ArrowBuf samples =
                ((FixedSizeListVector) batch.getVector(SAMPLES)).getDataVector().getDataBuffer();
        for (int i = 0; i < WINDOW; i++) {
            r.samples[i] = samples.getDouble(((long) row * WINDOW + i) * Double.BYTES);
        }
        final int results = INPUT_SCHEMA.getFields().size();
        final long at = (long) row * Double.BYTES;
        r.mean = batch.getVector(results).getDataBuffer().getDouble(at);
        r.stddev = batch.getVector(results + 1).getDataBuffer().getDouble(at);
        r.max = batch.getVector(results + 2).getDataBuffer().getDouble(at);
        r.score = batch.getVector(results + 3).getDataBuffer().getDouble(at);
    }
}
