package org.apache.flink.accel;

import java.nio.ByteBuffer;

/**
 * The one JNI doorway between Flink operators and native batch kernels. Kernels are plain C
 * functions with no JNI in them, all sharing one signature:
 *
 * <pre>{@code
 * int32_t kernel(const void *params, const void *in, int32_t n, void *out, int32_t *sel);
 * }</pre>
 *
 * <p>An operator {@link #open opens} the kernel's shared library and {@link #lookup looks up} the
 * symbol once, resolves its buffer {@link #address addresses} once, then makes one {@link #invoke}
 * call per batch. Because the JNI symbol names here are fixed, adding a kernel needs no Java or
 * JNI code at all - only a new {@code .so}.
 *
 * <p>Must be deployed in Flink's {@code lib/} (the parent classloader), not inside a job jar: the
 * JVM lets only one classloader load a given native library, and every job submission gets a
 * fresh user classloader, so a bridge shipped in the job jar would fail with {@code
 * UnsatisfiedLinkError} on the second submission to the same TaskManager. ({@code
 * org.apache.flink.*} is parent-first by default, so this class resolves from {@code lib/} even
 * when a job jar also contains it.)
 *
 * <p>The bridge's own library is loaded from the path in system property {@value #LIB_PROPERTY}
 * if set, otherwise as {@code libflinkaccel_bridge.so} from {@code java.library.path}.
 */
public final class NativeBridge {
    public static final String LIB_PROPERTY = "flink.accel.bridge.lib";

    static {
        String path = System.getProperty(LIB_PROPERTY);
        if (path != null) {
            System.load(path);
        } else {
            System.loadLibrary("flinkaccel_bridge");
        }
    }

    private NativeBridge() {}

    /** {@code dlopen}s a kernel library; throws {@link UnsatisfiedLinkError} on failure. */
    public static native long open(String libraryPath);

    /** {@code dlsym}s a kernel function; throws {@link UnsatisfiedLinkError} if it's missing. */
    public static native long lookup(long library, String symbol);

    /** {@code dlclose}s a library returned by {@link #open}. */
    public static native void close(long library);

    /**
     * Address of a direct buffer's memory. Costs ~17 ns per call (measured), so resolve it once,
     * not per batch.
     */
    public static native long address(ByteBuffer directBuffer);

    /** Runs one batch through {@code kernel}; returns the number of output records. */
    public static native int invoke(long kernel, long params, long in, int n, long out, long sel);
}
