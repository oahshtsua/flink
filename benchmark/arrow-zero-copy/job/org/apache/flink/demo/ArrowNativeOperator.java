package org.apache.flink.demo;

import org.apache.flink.accel.NativeBridge;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/**
 * Generic operator that runs one native batch kernel over each incoming {@link ArrowBatch} with
 * no copying in either direction. Unlike {@code MicroBatchingNativeOperator} it buffers nothing:
 * batches are formed upstream (here, by the source), so there is nothing to flush on watermarks,
 * barriers or end of input.
 *
 * <p>Per batch:
 *
 * <ol>
 *   <li>the batch is exported through the Arrow C Data Interface - a small struct listing the
 *       existing buffers' addresses, no data copied - and the kernel gets that struct's address;
 *   <li>the kernel writes its result columns straight into this operator's own fixed-width
 *       vectors (one data-buffer address per column, in a table written once) and the surviving
 *       row indices into a selection vector;
 *   <li>the output batch is the input's vectors plus the result vectors, with that selection
 *       vector. Pass-through columns are never touched.
 * </ol>
 *
 * <p>The kernel only borrows the exported struct for the duration of the call; this operator
 * releases it right after. The result vectors and selection vector are reused for the next
 * batch (see {@link ArrowBatch} on why that is safe here).
 *
 * <p>The first batch is checked for actually being zero-copy: every data buffer in the exported
 * struct must be the very buffer the Java vector holds. (Validity buffers are exempt: Arrow
 * Java's export may rebuild them when a column has no nulls.) The number of buffers compared is
 * printed with the operator's counters at close, so scripts can check that the kernel ran and that
 * the check did.
 */
public abstract class ArrowNativeOperator extends AbstractStreamOperator<ArrowBatch>
        implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

    private static final long serialVersionUID = 1L;

    private final String kernelLibrary;
    private final String kernelSymbol;

    private transient BufferAllocator allocator;
    private transient long library;
    private transient long kernel;

    private transient ByteBuffer params;
    private transient long paramsAddress;
    private transient ArrowArray exported;
    private transient ResultColumns results;
    private transient ArrowBuf resultTable;
    private transient ArrowBuf selection;

    private transient VectorSchemaRoot lastInput;
    private transient ArrowBatch outputBatch;
    /** Data buffers the zero-copy check compared on the first batch; 0 until it has run. */
    private transient int zeroCopyBuffers;

    private transient long batches;
    private transient long rows;
    private transient long survivors;

    protected ArrowNativeOperator(String kernelLibrary, String kernelSymbol) {
        this.kernelLibrary = kernelLibrary;
        this.kernelSymbol = kernelSymbol;
    }

    /** The input schema the kernel was written for; any other batch is rejected. */
    protected abstract Schema inputSchema();

    /** Fixed-width result columns, in the order of the kernel's {@code out} table. */
    protected abstract List<Field> resultFields();

    /** Bytes of the kernel's parameter block, written once at {@link #open}. */
    protected abstract int paramsBytes();

    /** Writes the parameter block at offset 0 of {@code buffer} (native byte order). */
    protected abstract void writeParams(ByteBuffer buffer);

    @Override
    public void open() throws Exception {
        super.open();
        allocator = ArrowMemory.newChild("arrow-native-operator");
        params =
                ByteBuffer.allocateDirect(Math.max(paramsBytes(), 1))
                        .order(ByteOrder.nativeOrder());
        writeParams(params);

        library = NativeBridge.open(kernelLibrary);
        kernel = NativeBridge.lookup(library, kernelSymbol);
        paramsAddress = NativeBridge.address(params);

        exported = ArrowArray.allocateNew(allocator);
        results = new ResultColumns(allocator, resultFields());
        resultTable = allocator.buffer((long) results.size() * Long.BYTES);
        outputBatch = new ArrowBatch();
        LOG.info(
                "ArrowNativeOperator opened: kernel={}#{} results={}",
                kernelLibrary,
                kernelSymbol,
                resultFields());
    }

    @Override
    public void processElement(StreamRecord<ArrowBatch> element) throws Exception {
        final ArrowBatch batch = element.getValue();
        if (batch.hasSelection()) {
            throw new IllegalStateException(
                    "ArrowNativeOperator needs dense input; it cannot take a selection vector yet");
        }
        final VectorSchemaRoot in = batch.root();
        final int n = in.getRowCount();
        if (in != lastInput) {
            checkSchema(in);
            lastInput = in;
        }
        if (results.ensureCapacity(n)) {
            // The buffers moved, so the kernel's address table is rewritten.
            for (int c = 0; c < results.size(); c++) {
                resultTable.setLong((long) c * Long.BYTES, results.data(c).memoryAddress());
            }
        }
        selection = ArrowMemory.grow(allocator, selection, (long) n * Integer.BYTES);

        Data.exportVectorSchemaRoot(allocator, in, null, exported);
        final int kept;
        try {
            if (zeroCopyBuffers == 0) {
                zeroCopyBuffers = verifyZeroCopy(in);
            }
            kept =
                    NativeBridge.invoke(
                            kernel,
                            paramsAddress,
                            exported.memoryAddress(),
                            n,
                            resultTable.memoryAddress(),
                            selection.memoryAddress());
        } finally {
            exported.release();
        }
        if (kept < 0) {
            throw new IllegalStateException(
                    kernelSymbol + " rejected the batch with error code " + kept);
        }
        batches++;
        rows += n;
        survivors += kept;

        output.collect(
                element.replace(outputBatch.set(results.appendTo(in), selection, kept)));
    }

    private void checkSchema(VectorSchemaRoot in) {
        if (!in.getSchema().equals(inputSchema())) {
            throw new IllegalArgumentException(
                    "Kernel "
                            + kernelSymbol
                            + " expects schema "
                            + inputSchema()
                            + ", got "
                            + in.getSchema());
        }
    }

    private int verifyZeroCopy(VectorSchemaRoot in) {
        final ArrowArray.Snapshot batch = exported.snapshot();
        final List<FieldVector> columns = in.getFieldVectors();
        int compared = 0;
        for (int c = 0; c < columns.size(); c++) {
            compared +=
                    verifyZeroCopy(MemoryUtil.getLong(batch.children + (long) c * Long.BYTES),
                            columns.get(c));
        }
        if (compared == 0) {
            throw new IllegalStateException("zero-copy check compared no buffers");
        }
        LOG.info("ArrowNativeOperator: zero-copy export verified ({} data buffers)", compared);
        return compared;
    }

    private static int verifyZeroCopy(long arrayAddress, FieldVector vector) {
        final ArrowArray.Snapshot array = ArrowArray.wrap(arrayAddress).snapshot();
        final List<ArrowBuf> buffers = vector.getFieldBuffers();
        int compared = 0;
        // Buffer 0 is the validity bitmap for every vector type used here.
        for (int b = 1; b < array.n_buffers; b++) {
            final long exportedAddress = MemoryUtil.getLong(array.buffers + (long) b * Long.BYTES);
            final long ownAddress = buffers.get(b).memoryAddress();
            if (exportedAddress != ownAddress) {
                throw new IllegalStateException(
                        "Export copied buffer "
                                + b
                                + " of column "
                                + vector.getName()
                                + ": exported at 0x"
                                + Long.toHexString(exportedAddress)
                                + ", vector holds 0x"
                                + Long.toHexString(ownAddress));
            }
            compared++;
        }
        final List<FieldVector> children = vector.getChildrenFromFields();
        for (int c = 0; c < children.size(); c++) {
            compared +=
                    verifyZeroCopy(
                            MemoryUtil.getLong(array.children + (long) c * Long.BYTES),
                            children.get(c));
        }
        return compared;
    }

    @Override
    public void close() throws Exception {
        try {
            // On stdout as well as in the log, so scripts can check the native path really ran.
            System.out.printf(
                    "ARROW_NATIVE kernel=%s batches=%d rows=%d survivors=%d"
                            + " zeroCopyBuffers=%d%n",
                    kernelSymbol,
                    batches,
                    rows,
                    survivors,
                    zeroCopyBuffers);
            if (results != null) {
                results.close();
            }
            if (selection != null) {
                selection.close();
            }
            if (resultTable != null) {
                resultTable.close();
            }
            if (exported != null) {
                exported.close();
            }
            if (library != 0) {
                NativeBridge.close(library);
                library = 0;
                kernel = 0;
            }
            if (allocator != null) {
                allocator.close();
            }
        } finally {
            super.close();
        }
    }
}
