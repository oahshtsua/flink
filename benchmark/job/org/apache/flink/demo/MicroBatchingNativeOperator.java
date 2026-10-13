package org.apache.flink.demo;

import org.apache.flink.accel.NativeBridge;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Generic operator that replaces a chain of per-record steps with one native batch kernel: it
 * packs incoming records into an off-heap buffer, and every {@code batchSize} records makes one
 * {@link NativeBridge#invoke} call that runs the whole chain over the batch, then emits the
 * survivors.
 *
 * <p>Subclasses supply only the job-specific parts: the fixed-width memory layouts ({@link
 * #inRecordBytes}, {@link #outRecordBytes}, {@link #paramsBytes}) and the conversions between
 * records and those layouts ({@link #encode}, {@link #decode}, {@link #writeParams}). The kernel
 * is named by library path and symbol and must match {@link NativeBridge}'s signature.
 *
 * <p>The kernel returns a selection vector - {@code sel[i]} is the input slot that output {@code
 * i} came from - so Flink metadata never crosses into native code: each survivor is re-emitted
 * with its own input record's timestamp (or none, if it had none), and {@link #decode} can read
 * pass-through fields back from that input slot instead of the kernel copying them.
 *
 * <p>Buffered records must never be overtaken by anything emitted after them, so the batch is
 * flushed before every watermark (both kinds), record-attributes change, checkpoint barrier
 * ({@link #prepareSnapshotPreBarrier}, which also means the buffer is always empty at snapshot
 * time and needs no state), and at end of input.
 *
 * <p>TODO: a processing-time flush timeout. Without one, a low-traffic stream holds up to {@code
 * batchSize - 1} records indefinitely. Not needed for the firehose benchmark; required in
 * production. Latency markers are not flushed for and can overtake buffered records - harmless for
 * correctness, but it under-reports latency.
 */
public abstract class MicroBatchingNativeOperator<IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    /** Buffer alignment: one cache line, so record {@code i} never straddles lines needlessly. */
    private static final int ALIGNMENT = 64;

    private final String kernelLibrary;
    private final String kernelSymbol;
    private final int batchSize;

    private transient long library;
    private transient long kernel;

    private transient ByteBuffer params;
    private transient ByteBuffer in;
    private transient ByteBuffer out;
    private transient ByteBuffer sel;
    private transient long paramsAddress;
    private transient long inAddress;
    private transient long outAddress;
    private transient long selAddress;

    private transient int inStride;
    private transient int outStride;
    private transient long[] timestamps;
    private transient boolean[] hasTimestamp;
    private transient int count;
    private transient StreamRecord<OUT> reuse;

    protected MicroBatchingNativeOperator(String kernelLibrary, String kernelSymbol, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1, got " + batchSize);
        }
        this.kernelLibrary = kernelLibrary;
        this.kernelSymbol = kernelSymbol;
        this.batchSize = batchSize;
    }

    /** Bytes per input record in the kernel's input layout. */
    protected abstract int inRecordBytes();

    /** Bytes per output record in the kernel's output layout. */
    protected abstract int outRecordBytes();

    /** Bytes of the kernel's parameter block, written once at {@link #open}. */
    protected abstract int paramsBytes();

    /** Writes the parameter block at offset 0 of {@code buffer}. */
    protected abstract void writeParams(ByteBuffer buffer);

    /** Packs {@code value} into {@code buffer} at {@code offset}. Must copy, not reference. */
    protected abstract void encode(IN value, ByteBuffer buffer, int offset);

    /**
     * Rebuilds an output record from the kernel's output at {@code outOffset} and the input slot
     * it came from at {@code inOffset}. May return the same reused object every call.
     */
    protected abstract OUT decode(ByteBuffer out, int outOffset, ByteBuffer in, int inOffset);

    @Override
    public void open() throws Exception {
        super.open();
        inStride = inRecordBytes();
        outStride = outRecordBytes();

        params = allocate(paramsBytes());
        in = allocate(batchSize * inStride);
        out = allocate(batchSize * outStride);
        sel = allocate(batchSize * Integer.BYTES);
        writeParams(params);

        library = NativeBridge.open(kernelLibrary);
        kernel = NativeBridge.lookup(library, kernelSymbol);
        // Resolved once: GetDirectBufferAddress costs ~17 ns, more than a whole batch's toll.
        paramsAddress = NativeBridge.address(params);
        inAddress = NativeBridge.address(in);
        outAddress = NativeBridge.address(out);
        selAddress = NativeBridge.address(sel);

        timestamps = new long[batchSize];
        hasTimestamp = new boolean[batchSize];
        count = 0;
        reuse = new StreamRecord<>(null);
        LOG.info(
                "MicroBatchingNativeOperator opened: kernel={}#{} batchSize={} inBytes={}"
                        + " outBytes={}",
                kernelLibrary,
                kernelSymbol,
                batchSize,
                inStride,
                outStride);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        encode(element.getValue(), in, count * inStride);
        timestamps[count] = element.getTimestamp();
        hasTimestamp[count] = element.hasTimestamp();
        if (++count == batchSize) {
            flush();
        }
    }

    private void flush() {
        final int n = count;
        if (n == 0) {
            return;
        }
        count = 0;
        final int survivors =
                NativeBridge.invoke(kernel, paramsAddress, inAddress, n, outAddress, selAddress);
        for (int i = 0; i < survivors; i++) {
            final int slot = sel.getInt(i * Integer.BYTES);
            final OUT value = decode(out, i * outStride, in, slot * inStride);
            if (hasTimestamp[slot]) {
                reuse.replace(value, timestamps[slot]);
            } else {
                reuse.replace(value);
                reuse.eraseTimestamp();
            }
            output.collect(reuse);
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        flush();
        super.processWatermark(mark);
    }

    @Override
    public void processWatermark(WatermarkEvent watermark) throws Exception {
        flush();
        super.processWatermark(watermark);
    }

    @Override
    public void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {
        flush();
        super.processRecordAttributes(recordAttributes);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        flush();
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void endInput() throws Exception {
        flush();
    }

    @Override
    public void finish() throws Exception {
        flush();
        super.finish();
    }

    @Override
    public void close() throws Exception {
        try {
            if (library != 0) {
                NativeBridge.close(library);
                library = 0;
                kernel = 0;
            }
        } finally {
            super.close();
        }
    }

    private static ByteBuffer allocate(int bytes) {
        // alignedSlice() rounds the start up AND the end down to the alignment, so the size is
        // rounded up first: with only "+ ALIGNMENT" slack the slice can come out up to 63 bytes
        // short. It also resets the byte order to big-endian, so the order is set after it; C
        // reads native (little-endian on x86) order, and getting this wrong yields plausible
        // garbage, not an error.
        final int rounded = (bytes + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
        return ByteBuffer.allocateDirect(rounded + ALIGNMENT)
                .alignedSlice(ALIGNMENT)
                .order(ByteOrder.nativeOrder());
    }
}
