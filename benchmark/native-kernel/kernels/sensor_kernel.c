/*
 * The whole SensorScoringJob chain - map(stats) -> filter(not flat) -> map(score) ->
 * filter(score >= threshold) - as ONE sequential C function over a batch. No hops between stages,
 * no Flink or JNI concepts: plain structs in, survivors plus a selection vector out.
 *
 * Hand-written stand-in for what an agent would later generate from the Java job. It keeps
 * SensorScoring.java's floating-point operation order exactly (left-to-right sums from 0.0, the
 * same divisions, strict '>' for maxima, no fused multiply-add - build with -ffp-contract=off),
 * so its output can be checked bit for bit against the Java job's checksum.
 *
 * Memory layouts are shared with SensorScoringNativeJob's encode/decode; keep them in sync.
 */
#include <math.h>
#include <stdint.h>

#define WINDOW 8

/* 80 bytes. device_id and ts are not used here; they ride along so the Java side can rebuild the
 * full output record from the input slot that sel[] points at. */
typedef struct {
    int32_t device_id;
    int32_t pad;
    int64_t ts;
    double samples[WINDOW];
} sensor_in;

/* 32 bytes, one per survivor, compacted. */
typedef struct {
    double mean;
    double stddev;
    double max;
    double score;
} sensor_out;

/* Written once at operator open(); profiles are computed in Java and passed as data. */
typedef struct {
    int32_t k;
    int32_t pad;
    double flat_threshold;
    double score_threshold;
    double profiles[]; /* k * WINDOW */
} sensor_params;

int32_t sensor_score(const void *params_, const void *in_, int32_t n, void *out_, int32_t *sel) {
    const sensor_params *params = params_;
    const sensor_in *in = in_;
    sensor_out *out = out_;
    const int32_t k = params->k;
    const double *profiles = params->profiles;
    const double flat_threshold = params->flat_threshold;
    const double score_threshold = params->score_threshold;

    int32_t survivors = 0;
    for (int32_t r = 0; r < n; r++) {
        const double *s = in[r].samples;

        /* stage 1: stats */
        double sum = 0.0;
        double max = s[0];
        for (int i = 0; i < WINDOW; i++) {
            sum += s[i];
            if (s[i] > max) {
                max = s[i];
            }
        }
        const double mean = sum / WINDOW;
        double sq = 0.0;
        for (int i = 0; i < WINDOW; i++) {
            const double d = s[i] - mean;
            sq += d * d;
        }
        const double stddev = sqrt(sq / WINDOW);

        /* stage 2: drop flat windows (also keeps stage 3's divisor > 0) */
        if (!(stddev >= flat_threshold)) {
            continue;
        }

        /* stage 3: best correlation with any reference profile */
        const double inv = 1.0 / stddev;
        double z[WINDOW];
        for (int i = 0; i < WINDOW; i++) {
            z[i] = (s[i] - mean) * inv;
        }
        double best = -INFINITY;
        for (int32_t p = 0; p < k; p++) {
            const double *profile = profiles + (int64_t)p * WINDOW;
            double dot = 0.0;
            for (int i = 0; i < WINDOW; i++) {
                dot += z[i] * profile[i];
            }
            if (dot > best) {
                best = dot;
            }
        }
        const double score = best / WINDOW;

        /* stage 4: keep close matches */
        if (!(score >= score_threshold)) {
            continue;
        }

        out[survivors].mean = mean;
        out[survivors].stddev = stddev;
        out[survivors].max = max;
        out[survivors].score = score;
        sel[survivors] = r;
        survivors++;
    }
    return survivors;
}
