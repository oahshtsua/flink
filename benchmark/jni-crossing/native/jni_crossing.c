/*
 * Native side of the JNI crossing-cost measurement.
 *
 * Every function here is deliberately as close to empty as its signature allows: the point is to
 * measure the cost of *getting to* native code and back (the "toll" in this directory's
 * README), not the cost of doing anything once there. Anything that
 * does real work would confound the number we are after.
 */
#include <jni.h>
#include <string.h>
#include <stdint.h>

/* Cached field IDs for the POJO case. Looked up once from Java's static initializer so the
 * per-call path measures only GetIntField, not GetFieldID (which is what real JNI code does). */
static jfieldID pojo_a_id;
static jfieldID pojo_b_id;

JNIEXPORT void JNICALL
Java_org_apache_flink_accel_bench_JniCrossing_initPojoFields(JNIEnv *env, jclass cls, jclass pojo) {
    pojo_a_id = (*env)->GetFieldID(env, pojo, "a", "I");
    pojo_b_id = (*env)->GetFieldID(env, pojo, "b", "I");
}

/* --- pure crossing: no arguments, no return, no body --------------------------------------- */
JNIEXPORT void JNICALL
Java_org_apache_flink_accel_bench_JniCrossing_nativeVoid(JNIEnv *env, jclass cls) {
}

/* --- crossing with primitive arguments ------------------------------------------------------ */
JNIEXPORT jlong JNICALL
Java_org_apache_flink_accel_bench_JniCrossing_nativeLong(JNIEnv *env, jclass cls, jlong v) {
    return v;
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_accel_bench_JniCrossing_nativeLong3(JNIEnv *env, jclass cls,
                                                          jlong a, jlong b, jlong c) {
    return a + b + c;
}

/* --- crossing that touches a Java object: the realistic per-record POJO case ----------------- */
JNIEXPORT jlong JNICALL
Java_org_apache_flink_accel_bench_JniCrossing_nativePojo(JNIEnv *env, jclass cls, jobject p) {
    jint a = (*env)->GetIntField(env, p, pojo_a_id);
    jint b = (*env)->GetIntField(env, p, pojo_b_id);
    return (jlong) (a + b);
}

/* --- crossing that touches a Java String: UTF-16 -> UTF-8 conversion is the real cost here --- */
JNIEXPORT jlong JNICALL
Java_org_apache_flink_accel_bench_JniCrossing_nativeString(JNIEnv *env, jclass cls, jstring s) {
    const char *chars = (*env)->GetStringUTFChars(env, s, NULL);
    if (chars == NULL) {
        return -1;
    }
    jlong len = (jlong) strlen(chars);
    (*env)->ReleaseStringUTFChars(env, s, chars);
    return len;
}

/* --- crossing with a direct ByteBuffer: the batched design's argument shape ------------------ */
JNIEXPORT jlong JNICALL
Java_org_apache_flink_accel_bench_JniCrossing_nativeDirectBufAddr(JNIEnv *env, jclass cls, jobject buf) {
    void *addr = (*env)->GetDirectBufferAddress(env, buf);
    return (jlong) (intptr_t) addr;
}

/* Batched no-op: takes a direct buffer and a record count and returns immediately WITHOUT
 * touching the data. Cost is expected to be flat in n -- that flatness is the whole claim behind
 * amortizing the crossing as c/B, so it is measured rather than assumed. */
JNIEXPORT jint JNICALL
Java_org_apache_flink_accel_bench_JniCrossing_nativeBatchNoop(JNIEnv *env, jclass cls,
                                                              jobject buf, jint n) {
    return n;
}

/* --- heap primitive array access, both of the usual ways ------------------------------------ */
JNIEXPORT jlong JNICALL
Java_org_apache_flink_accel_bench_JniCrossing_nativeArrayCritical(JNIEnv *env, jclass cls,
                                                                   jlongArray arr, jint n) {
    jlong *p = (jlong *) (*env)->GetPrimitiveArrayCritical(env, arr, NULL);
    if (p == NULL) {
        return -1;
    }
    jlong first = n > 0 ? p[0] : 0;
    (*env)->ReleasePrimitiveArrayCritical(env, arr, p, JNI_ABORT);
    return first;
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_accel_bench_JniCrossing_nativeArrayRegion(JNIEnv *env, jclass cls,
                                                                 jlongArray arr, jint n,
                                                                 jlongArray scratchOut) {
    jlong buf[4096];
    jint take = n > 4096 ? 4096 : n;
    (*env)->GetLongArrayRegion(env, arr, 0, take, buf);
    return take > 0 ? buf[0] : 0;
}
