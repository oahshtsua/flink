/*
 * sensor_kernel.c's sensor_score, reading its input straight from an Arrow record batch and
 * writing its results straight into Arrow columns: nothing is copied into or out of a kernel
 * layout. The Java side passes the address of the batch's ArrowArray (Arrow C Data Interface,
 * exported by Arrow Java without copying a buffer) and of a table of result-column data buffers;
 * the kernel reads the samples column in place and writes mean/stddev/max/score in place.
 *
 * Input batch (a struct array, one child per column, no nulls):
 *
 *   0  device_id  int32                         (not read: passes through untouched)
 *   1  ts         int64                         (not read: passes through untouched)
 *   2  samples    fixed_size_list<float64>[8]
 *
 * Output: for every surviving row r, out[c][r] for the 4 result columns, at the row's own index
 * (results are row-aligned with the input, not compacted), and sel[i] = r for the i-th survivor.
 * Rows not in sel[] are left unwritten. Returns the number of survivors, or a negative error code
 * if the batch is not shaped as expected.
 *
 * The per-reading arithmetic is sensor_score's, operation for operation (left-to-right sums from
 * 0.0, the same divisions, strict '>' for maxima, no fused multiply-add - build with
 * -ffp-contract=off), so the output can be checked bit for bit against the Java job's checksum.
 * sensor_kernel.c is deliberately left unchanged rather than sharing this code with it: it is the
 * baseline every earlier measurement was taken with, and sharing an inline function changes its
 * machine code.
 */
#include <math.h>
#include <stdint.h>

#define WINDOW 8

/* Arrow C Data Interface, from the Arrow specification (cdata/abi.h). */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE
struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void **buffers;
    struct ArrowArray **children;
    struct ArrowArray *dictionary;
    void (*release)(struct ArrowArray *);
    void *private_data;
};
#endif

/* Same parameter block as sensor_kernel.c; written by SensorLayout.writeParams. */
typedef struct {
    int32_t k;
    int32_t pad;
    double flat_threshold;
    double score_threshold;
    double profiles[]; /* k * WINDOW */
} sensor_params;

enum { OUT_MEAN, OUT_STDDEV, OUT_MAX, OUT_SCORE, OUT_COLUMNS };

#define ERR_SHAPE (-1)
#define ERR_NULLS (-2)

int32_t sensor_score_arrow(const void *params_, const void *in_, int32_t n, void *out_,
                           int32_t *sel) {
    const sensor_params *params = params_;
    const struct ArrowArray *batch = in_;
    double *const *out = out_;

    if (batch->length != n || batch->n_children != 3) {
        return ERR_SHAPE;
    }
    const struct ArrowArray *list = batch->children[2];
    if (list->n_children != 1 || list->children[0]->n_buffers != 2) {
        return ERR_SHAPE;
    }
    const struct ArrowArray *values = list->children[0];
    if (batch->null_count != 0 || list->null_count != 0 || values->null_count != 0) {
        return ERR_NULLS;
    }
    /* Row r of the batch is row (batch->offset + r) of the list column, whose samples start at
     * value index (list->offset + that row) * WINDOW of the values child. */
    const double *samples = (const double *)values->buffers[1] + values->offset +
                            (list->offset + batch->offset) * WINDOW;

    double *restrict out_mean = out[OUT_MEAN];
    double *restrict out_stddev = out[OUT_STDDEV];
    double *restrict out_max = out[OUT_MAX];
    double *restrict out_score = out[OUT_SCORE];

    const int32_t k = params->k;
    const double *profiles = params->profiles;
    const double flat_threshold = params->flat_threshold;
    const double score_threshold = params->score_threshold;

    int32_t survivors = 0;
    for (int32_t r = 0; r < n; r++) {
        const double *s = samples + (int64_t)r * WINDOW;

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

        out_mean[r] = mean;
        out_stddev[r] = stddev;
        out_max[r] = max;
        out_score[r] = score;
        sel[survivors++] = r;
    }
    return survivors;
}
