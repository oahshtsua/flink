package org.apache.flink.accel.bench;

import org.apache.flink.demo.ArrowMemory;
import org.apache.flink.demo.SensorArrow;
import org.apache.flink.demo.SensorScoring;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * The per-batch and per-row costs of the Arrow path, outside Flink.
 *
 * <ul>
 *   <li>{@code exportRelease}: one batch through Arrow Java's C Data Interface export, then its
 *       release - what {@code ArrowNativeOperator} pays per batch on top of the kernel itself
 *       (ns per batch);
 *   <li>{@code fill}: generating readings straight into Arrow columns (ns per row), comparable to
 *       sensor-bench's {@code SensorBatch.generate} (generating into a {@code SensorReading}).
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(
        value = 1,
        jvmArgsAppend = {
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "-Darrow.enable_unsafe_memory_access=true"
        })
@State(Scope.Thread)
public class ArrowExport {
    private static final int K = 1;

    @Param({"16", "1024", "4096"})
    public int rows;

    private BufferAllocator allocator;
    private VectorSchemaRoot root;
    private ArrowArray exported;
    private double[] profiles;
    private long seq;

    @Setup(Level.Trial)
    public void setup() {
        allocator = ArrowMemory.newChild("arrow-export-bench");
        root = SensorArrow.allocate(allocator, rows);
        profiles = SensorScoring.profiles(K);
        SensorArrow.fill(root, 0, rows, profiles, K);
        exported = ArrowArray.allocateNew(allocator);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        exported.close();
        root.close();
        allocator.close();
    }

    @Benchmark
    public long exportRelease() {
        Data.exportVectorSchemaRoot(allocator, root, null, exported);
        final long address = exported.memoryAddress();
        exported.release();
        return address;
    }

    @Benchmark
    @OperationsPerInvocation(4096)
    public int fill() {
        // 4096 rows per invocation at every batch size, so the score is ns per row.
        for (int done = 0; done < 4096; done += rows) {
            SensorArrow.fill(root, seq, rows, profiles, K);
            seq += rows;
        }
        return root.getRowCount();
    }
}
