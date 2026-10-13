/*
 * JNI side of org.apache.flink.accel.NativeBridge: dlopen/dlsym/dlclose, direct-buffer address
 * lookup, and one generic call-through for every batch kernel. Kernels themselves contain no JNI;
 * they only have to match flink_batch_kernel's signature.
 */
#include <dlfcn.h>
#include <jni.h>
#include <stdint.h>

typedef int32_t (*flink_batch_kernel)(const void *params, const void *in, int32_t n, void *out,
                                      int32_t *sel);

static void throw_link_error(JNIEnv *env, const char *message) {
    jclass cls = (*env)->FindClass(env, "java/lang/UnsatisfiedLinkError");
    if (cls != NULL) {
        (*env)->ThrowNew(env, cls, message != NULL ? message : "unknown dlopen/dlsym error");
    }
}

JNIEXPORT jlong JNICALL Java_org_apache_flink_accel_NativeBridge_open(JNIEnv *env, jclass cls,
                                                                     jstring path) {
    const char *p = (*env)->GetStringUTFChars(env, path, NULL);
    if (p == NULL) {
        return 0; /* OutOfMemoryError already pending */
    }
    void *handle = dlopen(p, RTLD_NOW | RTLD_LOCAL);
    (*env)->ReleaseStringUTFChars(env, path, p);
    if (handle == NULL) {
        throw_link_error(env, dlerror());
        return 0;
    }
    return (jlong)(intptr_t)handle;
}

JNIEXPORT jlong JNICALL Java_org_apache_flink_accel_NativeBridge_lookup(JNIEnv *env, jclass cls,
                                                                       jlong library,
                                                                       jstring symbol) {
    const char *s = (*env)->GetStringUTFChars(env, symbol, NULL);
    if (s == NULL) {
        return 0;
    }
    dlerror();
    void *fn = dlsym((void *)(intptr_t)library, s);
    const char *err = dlerror();
    (*env)->ReleaseStringUTFChars(env, symbol, s);
    if (err != NULL || fn == NULL) {
        throw_link_error(env, err);
        return 0;
    }
    return (jlong)(intptr_t)fn;
}

JNIEXPORT void JNICALL Java_org_apache_flink_accel_NativeBridge_close(JNIEnv *env, jclass cls,
                                                                     jlong library) {
    if (library != 0) {
        dlclose((void *)(intptr_t)library);
    }
}

JNIEXPORT jlong JNICALL Java_org_apache_flink_accel_NativeBridge_address(JNIEnv *env, jclass cls,
                                                                        jobject buffer) {
    return (jlong)(intptr_t)(*env)->GetDirectBufferAddress(env, buffer);
}

JNIEXPORT jint JNICALL Java_org_apache_flink_accel_NativeBridge_invoke(JNIEnv *env, jclass cls,
                                                                      jlong kernel, jlong params,
                                                                      jlong in, jint n, jlong out,
                                                                      jlong sel) {
    flink_batch_kernel fn = (flink_batch_kernel)(intptr_t)kernel;
    return fn((const void *)(intptr_t)params, (const void *)(intptr_t)in, n, (void *)(intptr_t)out,
              (int32_t *)(intptr_t)sel);
}
