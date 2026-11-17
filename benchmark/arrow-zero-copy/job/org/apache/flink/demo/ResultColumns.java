package org.apache.flink.demo;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * Fixed-width columns that a batch step computes and appends to the batches it passes on: the
 * step owns them, writes row {@code r}'s result at index {@code r} (row-aligned with the input),
 * and emits the input's columns followed by these. The input's columns are never copied or
 * touched.
 *
 * <p>Reused for every batch (see {@link ArrowBatch}). Every validity bit is set at allocation, so
 * writers only write the data buffers.
 */
public final class ResultColumns implements AutoCloseable {
    private final List<FieldVector> vectors = new ArrayList<>();
    private int capacity;

    private VectorSchemaRoot lastInput;
    private VectorSchemaRoot output;
    private VectorSchemaRoot alone;

    public ResultColumns(BufferAllocator allocator, List<Field> fields) {
        for (Field field : fields) {
            vectors.add(field.createVector(allocator));
        }
    }

    public int size() {
        return vectors.size();
    }

    /**
     * Grows every column to hold {@code n} rows. Returns whether the data buffers moved, so a
     * caller that handed their addresses to native code knows to hand them over again.
     */
    public boolean ensureCapacity(int n) {
        if (n <= capacity) {
            return false;
        }
        for (FieldVector vector : vectors) {
            vector.clear();
            vector.setInitialCapacity(n);
            vector.allocateNew();
            ArrowMemory.markAllValid(vector);
        }
        capacity = n;
        return true;
    }

    /** Data buffer of column {@code c}; valid until the next {@link #ensureCapacity} that grows. */
    public ArrowBuf data(int c) {
        return vectors.get(c).getDataBuffer();
    }

    /**
     * {@code in}'s columns followed by these, with {@code in}'s row count. Built once per input
     * root, so a producer that reuses its root costs nothing here per batch.
     */
    public VectorSchemaRoot appendTo(VectorSchemaRoot in) {
        if (in != lastInput) {
            final List<FieldVector> all = new ArrayList<>(in.getFieldVectors());
            all.addAll(vectors);
            output = new VectorSchemaRoot(all);
            lastInput = in;
        }
        output.setRowCount(in.getRowCount());
        return output;
    }

    /**
     * Just these columns, with {@code rowCount} rows: for a step whose output replaces its input
     * rather than extending it. Built once.
     */
    public VectorSchemaRoot alone(int rowCount) {
        if (alone == null) {
            alone = new VectorSchemaRoot(vectors);
        }
        alone.setRowCount(rowCount);
        return alone;
    }

    /**
     * Closes only these columns. Not {@code appendTo}'s root: that would also close the input's
     * vectors, which belong to the upstream producer.
     */
    @Override
    public void close() {
        vectors.forEach(FieldVector::close);
    }
}
