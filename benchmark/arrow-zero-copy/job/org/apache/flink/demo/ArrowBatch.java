package org.apache.flink.demo;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * The record that flows between the Arrow operators: one Arrow record batch, plus an optional
 * selection vector naming the rows that are actually in the stream. A batch without one holds all
 * its rows; a filtering operator adds one instead of compacting, so no column is copied.
 *
 * <p>Batches are reused: a producer refills the same vectors for its next batch, so a consumer
 * must be done with a batch when its {@code processElement} returns - true for a chained,
 * synchronous pipeline with object reuse, and nothing else. {@link #TYPE}'s serializer enforces
 * this: it throws if Flink ever tries to copy or serialize a batch, which is what would happen on
 * a non-chained edge or without object reuse.
 */
public final class ArrowBatch {
    /** Use this, not {@code TypeInformation.of(ArrowBatch.class)}, for every Arrow stream. */
    public static final TypeInformation<ArrowBatch> TYPE = new ChainedOnlyTypeInfo();

    private VectorSchemaRoot root;
    private ArrowBuf selection;
    private int selected;

    /** Sets this to all {@code root.getRowCount()} rows of {@code root}. */
    public ArrowBatch set(VectorSchemaRoot root) {
        this.root = root;
        this.selection = null;
        this.selected = root.getRowCount();
        return this;
    }

    /** Sets this to the {@code selected} rows of {@code root} listed (int32) in {@code selection}. */
    public ArrowBatch set(VectorSchemaRoot root, ArrowBuf selection, int selected) {
        this.root = root;
        this.selection = selection;
        this.selected = selected;
        return this;
    }

    public VectorSchemaRoot root() {
        return root;
    }

    public boolean hasSelection() {
        return selection != null;
    }

    /** The int32 row indices of the selected rows, or {@code null} if all rows are selected. */
    public ArrowBuf selection() {
        return selection;
    }

    /** Number of rows in the stream. */
    public int selectedCount() {
        return selected;
    }

    /** Row index in {@link #root} of the {@code i}-th selected row. */
    public int selectedRow(int i) {
        return selection == null ? i : selection.getInt((long) i * Integer.BYTES);
    }

    private static final class ChainedOnlyTypeInfo extends TypeInformation<ArrowBatch> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        public int getTotalFields() {
            return 1;
        }

        @Override
        public Class<ArrowBatch> getTypeClass() {
            return ArrowBatch.class;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<ArrowBatch> createSerializer(SerializerConfig config) {
            return new ChainedOnlySerializer();
        }

        @Override
        public String toString() {
            return "ArrowBatch";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ChainedOnlyTypeInfo;
        }

        @Override
        public int hashCode() {
            return ChainedOnlyTypeInfo.class.hashCode();
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof ChainedOnlyTypeInfo;
        }
    }

    private static final class ChainedOnlySerializer extends TypeSerializer<ArrowBatch> {
        private static final long serialVersionUID = 1L;

        private static UnsupportedOperationException crossed(String what) {
            return new UnsupportedOperationException(
                    "Flink tried to "
                            + what
                            + " an ArrowBatch: batches may only cross chained edges with object"
                            + " reuse enabled");
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<ArrowBatch> duplicate() {
            return this;
        }

        @Override
        public ArrowBatch createInstance() {
            return new ArrowBatch();
        }

        @Override
        public ArrowBatch copy(ArrowBatch from) {
            throw crossed("copy");
        }

        @Override
        public ArrowBatch copy(ArrowBatch from, ArrowBatch reuse) {
            throw crossed("copy");
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(ArrowBatch record, DataOutputView target) {
            throw crossed("serialize");
        }

        @Override
        public ArrowBatch deserialize(DataInputView source) {
            throw crossed("deserialize");
        }

        @Override
        public ArrowBatch deserialize(ArrowBatch reuse, DataInputView source) {
            throw crossed("deserialize");
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) {
            throw crossed("copy");
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ChainedOnlySerializer;
        }

        @Override
        public int hashCode() {
            return ChainedOnlySerializer.class.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<ArrowBatch> snapshotConfiguration() {
            throw crossed("snapshot the serializer of");
        }
    }
}
