package org.apache.flink.demo;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;

/**
 * Arrow memory helpers. Holds the one Arrow root allocator per JVM (per class loader); every Arrow
 * component in the pipeline takes a child of it. Arrow only lets buffers be shared between
 * allocators with the same root, and the native operator's export re-associates the source's
 * buffers with its own allocator. Closing a child allocator fails if it still holds buffers, which
 * doubles as a leak check.
 */
public final class ArrowMemory {
    private static final RootAllocator ROOT = new RootAllocator();

    private ArrowMemory() {}

    public static BufferAllocator newChild(String name) {
        return ROOT.newChildAllocator(name, 0, Long.MAX_VALUE);
    }

    /**
     * Sets every validity bit of {@code vector} and its children, up to their capacity, for
     * vectors filled by writing their data buffers directly: their rows are then never null
     * without touching validity per row.
     */
    public static void markAllValid(FieldVector vector) {
        final ArrowBuf validity = vector.getValidityBuffer();
        validity.setOne(0L, validity.capacity());
        for (FieldVector child : vector.getChildrenFromFields()) {
            markAllValid(child);
        }
    }
}
