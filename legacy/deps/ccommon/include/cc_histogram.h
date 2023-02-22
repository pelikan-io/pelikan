#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

typedef enum histo_rstatus {
    HISTO_OK         = 0,
    HISTO_EOVERFLOW  = -1,
    HISTO_EUNDERFLOW = -2,
    HISTO_EEMPTY     = -3,
    HISTO_EORDER     = -4,
} histo_rstatus_e;

struct percentile_profile
{
    uint8_t cap;       /* number of percentiles that can be lookedup at once */
    uint8_t count;     /* number of percentiles to look up */
    double *percentile;/* sorted percentiles to be queried, allocated at init */
    uint64_t *result;  /* results of lookup, allocated at init */
    uint64_t min;      /* min value */
    uint64_t max;      /* max value */
};

struct histo_u32
{
    /* the following variables are configurable */
    uint32_t m;
    uint32_t r;
    uint32_t n;

    /* the following variables are computed from those above */
    uint64_t M; /* Minimum Resolution: 2^m */
    uint64_t R; /* Minimum Resolution Range: 2^r - 1 */
    uint64_t N; /* Maximum Value: 2^n - 1 */
    uint64_t G; /* Grouping Factor: 2^(r-m-1) */
    uint64_t nbucket;  /* total number of buckets: (n-r+2)*G */

    /* we are treating integer operations as atomic (under relaxed constraint
     * and when there's only one writer), this is true on x86
     */
    uint64_t nrecord;  /* total number of records */
    uint32_t *buckets; /* buckets where counts are kept as uint32_t */

    /* these are only used for producer/consumer style access */
    // pthread_spinlock_t lock;
};

/* APIs */
struct histo_u32 *histo_u32_create(uint32_t m, uint32_t r, uint32_t n);
void histo_u32_destroy(struct histo_u32 **h);

struct percentile_profile *percentile_profile_create(uint8_t cap);
void percentile_profile_destroy(struct percentile_profile **pp);
histo_rstatus_e percentile_profile_set(struct percentile_profile *pp, const double *percentile, uint8_t count);

static inline uint64_t
bucket_low(const struct histo_u32 *h, uint64_t bucket)
{
    uint64_t g = bucket >> (h->r - h->m - 1); /* bucket offset in terms of G */
    uint64_t b = bucket - g * h->G;

    /* first group has a different formula */
    return (g == 0) * ((1 << h->m) * b) +
        (g > 0) * ((1 << (h->r + g - 2)) + (1 << (h->m + g - 1)) * b);
}

static inline uint64_t
bucket_high(const struct histo_u32 *h, uint64_t bucket)
{
    uint64_t g = bucket >> (h->r - h->m - 1); /* offset as multiplers of G */
    uint64_t b = bucket - g * h->G + 1; /* the next bucket */

    /* first group has a different formula */
    return (g == 0) * ((1 << h->m) * b - 1) +
        (g > 0) * ((1 << (h->r + g - 2)) + (1 << (h->m + g - 1)) * b - 1);
}

/************************
 * Non-thread-safe APIs *
 ************************/
void histo_u32_reset(struct histo_u32 *h);
histo_rstatus_e histo_u32_record(struct histo_u32 *h, uint64_t value, uint32_t count);
/* the following functions return the bucket for the percentile(s) requested.
 * If the histogram is too sparse for the percentile specified, the next
 * (higher) non-empty bucket is returned.
 *
 * It is upon the caller to translate bucket to value(s), for example by using
 * bucket_low/high to get the value range.
 */
histo_rstatus_e histo_u32_report(uint64_t *bucket, const struct histo_u32 *h, double p);
/* when using percentile_profile, min/max buckets are always updated/returned */
histo_rstatus_e histo_u32_report_multi(struct percentile_profile *pp, const struct histo_u32 *h);

#ifdef __cplusplus
}
#endif
