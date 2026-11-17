package org.apache.flink.demo;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Base for user functions that take and return whole {@link ArrowBatch}es: to Flink, an ordinary
 * {@code map}. Gives each function an allocator, the batch it returns, and a selection buffer if it
 * produces one. Everything it allocates is reused for every batch.
 */
public abstract class ArrowBatchFunction extends RichMapFunction<ArrowBatch, ArrowBatch> {
    private static final long serialVersionUID = 1L;

    protected transient BufferAllocator allocator;
    protected transient ArrowBatch out;
    private transient ArrowBuf selection;

    @Override
    public void open(OpenContext openContext) {
        allocator = ArrowMemory.newChild(getClass().getSimpleName());
        out = new ArrowBatch();
    }

    /** This function's own selection buffer, grown to hold {@code n} row indices. */
    protected ArrowBuf selection(int n) {
        selection = ArrowMemory.grow(allocator, selection, (long) n * Integer.BYTES);
        return selection;
    }

    @Override
    public void close() {
        if (selection != null) {
            selection.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }
}
