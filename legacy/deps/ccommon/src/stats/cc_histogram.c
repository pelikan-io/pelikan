#include <cc_histogram.h>

#include <cc_debug.h>
#include <cc_mm.h>

#include <float.h>
#include <math.h>

#ifdef __LZCNT__
#  include <x86intrin.h>
#endif /* __LZCNT__ */


struct histo_u32 *
histo_u32_create(uint32_t m, uint32_t r, uint32_t n)
{
    struct histo_u32 *histo;

    if (r <= m || r > n || n > 64) { /* validate constraints on input */
        log_error("Invalid input value among m=%"PRIu32", r=%"PRIu32", n=%"
            PRIu32, m, r, n);

        return NULL;
    }

    histo = cc_alloc(sizeof(struct histo_u32));
    if (histo == NULL) {
        log_error("Failed to allocate struct histo_u32");

        return NULL;
    }

    histo->m = m;
    histo->r = r;
    histo->n = n;

    histo->M = 1 << m;
    histo->R = (1 << r) - 1;
    histo->N = (1 << n) - 1;
    histo->G = 1 << (r - m - 1);
    histo->nbucket = (n - r + 2) * histo->G;

    histo->buckets = cc_alloc(histo->nbucket * sizeof(*histo->buckets));
    if (histo->buckets == NULL) {
        log_error("Failed to allocate buckets");
        cc_free(histo);

        return NULL;
    }

    histo_u32_reset(histo);
    log_verb("Created histogram %p with parametersm=%"PRIu32", r=%"PRIu32", n=%"
            PRIu32"; nbucket=%"PRIu64, histo, m, r, n, histo->nbucket);

    return histo;
}

void
histo_u32_destroy(struct histo_u32 **h)
{
    ASSERT(h != NULL);

    struct histo_u32 *histo = *h;

    if (histo == NULL) {
        return;
    }

    cc_free(histo->buckets);
    cc_free(histo);
    *h = NULL;

    log_verb("Destroyed histogram at %p", histo);
}

void
histo_u32_reset(struct histo_u32 *h)
{
    ASSERT(h != NULL);

    h->nrecord = 0;
    memset(h->buckets, 0, sizeof(uint32_t) * h->nbucket);
}

static inline uint64_t
_bucket_offset(uint64_t value, uint32_t m, uint32_t r, uint64_t G)
{
    uint64_t v = (value == 0) + value; /* lzcnt is undefined for 0 */
#ifdef __LZCNT__
    uint32_t h = 63 - __lzcnt64(v);
#else
    uint32_t h = 63 - __builtin_clzll(v);
#endif

    if (h < r) {
        return value >> m;
    } else {
        uint32_t d = h - r + 1;
        return (d + 1) * G + ((value - (1 << h)) >> (m + d));
    }
}

histo_rstatus_e
histo_u32_record(struct histo_u32 *h, uint64_t value, uint32_t count)
{
    uint64_t offset = 0;

    if (value > h->N) {
        log_error("Value not recorded due to overflow: %"PRIu64" is greater"
                "than max value allowed, which is %"PRIu64, value, h->N);

        return HISTO_EOVERFLOW;
    }

    offset = _bucket_offset(value, h->m, h->r, h->G);
    *(h->buckets + offset) += count;
    h->nrecord += count;

    return HISTO_OK;
}

static inline bool
_greater_dbl(double a, double b) {
    return (a - b) >= DBL_EPSILON;
}

static inline bool
_lesser_dbl(double a, double b) {
    return (b - a) >= DBL_EPSILON;
}

static inline bool
_equal_dbl(double a, double b) {
    return fabs(b - a) < DBL_EPSILON;
}

static inline uint64_t
_threshold(uint64_t nrecord, double percentile)
{
    return (uint64_t)ceil(percentile * nrecord / 100);
}

histo_rstatus_e
histo_u32_report(uint64_t *value, const struct histo_u32 *h, double p)
{
    ASSERT(h != NULL);

    uint64_t rthreshold, rcount = 0;
    uint64_t offset = 0;
    uint32_t *bucket = h->buckets;

    if (_greater_dbl(p, 100.0f)) {
        log_error("Percentile must be between [0.0, 100.0], %f provided", p);

        return HISTO_EOVERFLOW;
    }
    if (_lesser_dbl(p, 0.0f)) {
        log_error("Percentile must be between [0.0, 100.0], %f provided", p);

        return HISTO_EUNDERFLOW;
    }
    if (h->nrecord == 0) {
        log_info("No value to report due to histogram being empty");

        return HISTO_EEMPTY;
    }

    rthreshold = _threshold(h->nrecord, p);
    /* find the lowest non-empty bucket, this is done separately to make sure
     * that if the threshold is 0 (e.g. p=0.0), we still return a bucket within
     * the range of recorded values.
     */
    while (offset < h->nbucket && *bucket == 0) {
        bucket++;
        offset++;
    }
    *value = offset; /* value must be no smaller than the lowest non-empty bucket */
    /* find the first bucket where the record count threshold is met */
    for (; offset < h->nbucket && rcount < rthreshold; ++offset, ++bucket) {
        rcount += *bucket;
        *value = offset;
    }

    return HISTO_OK;
}

histo_rstatus_e
histo_u32_report_multi(struct percentile_profile *pp, const struct histo_u32 *h)
{
    ASSERT(pp != NULL);
    ASSERT(h != NULL);

    uint64_t rthreshold, rcount = 0;
    uint64_t curr = 0, offset = 0;
    uint32_t *bucket = h->buckets;
    double *p = pp->percentile;
    uint64_t *v = pp->result;
    uint8_t count = pp->count;

    if (h->nrecord == 0) {
        log_info("No value to report due to histogram being empty");

        return HISTO_EEMPTY;
    }

    /* find the lowest non-empty bucket */
    while (curr < h->nbucket && *bucket == 0) {
        bucket++;
        curr++;
    }
    pp->min = offset = curr;
    rthreshold = _threshold(h->nrecord, *p);

    /* Assume the percentiles are set according to percentile_profile_set */
    while (curr < h->nbucket && count > 0) {
        while (curr < h->nbucket && rcount < rthreshold) {
            if (*bucket > 0) {
                rcount += *bucket;
                offset = curr; /* offset always points to a non-empty bucket */
            }
            curr++;
            bucket++;
        }
        do { /* the same bucket may satisfy multiple percentile */
            *v = offset;
            count--;
            if (count == 0) {
                break;
            }
            p++;
            v++;
            rthreshold = _threshold(h->nrecord, *p);
        } while (rthreshold <= rcount);
    }

    /* scan the rest of the buckets to find max */
    pp->max = offset;
    for (;curr < h->nbucket; ++curr, ++bucket) {
        bool empty = (*bucket == 0);
        pp->max = pp->max * empty + offset * !empty;
    }

    return HISTO_OK;
}

struct percentile_profile *
percentile_profile_create(uint8_t cap)
{
    struct percentile_profile *pp;

    pp = cc_alloc(sizeof(struct percentile_profile));
    if (pp == NULL) {
        log_error("Failed to allocate struct percentile_profile");

        return NULL;
    }
    pp->percentile = cc_alloc(cap * sizeof(double));
    if (pp->percentile == NULL) {
        log_error("Failed to allocate percentile in struct percentile_profile");
        cc_free(pp);

        return NULL;
    }
    pp->result = cc_alloc(cap * sizeof(uint64_t));
    if (pp->result == NULL) {
        log_error("Failed to allocate result in struct percentile_profile");
        cc_free(pp->percentile);
        cc_free(pp);

        return NULL;
    }

    pp->cap = cap;
    pp->count = 0;

    log_verb("Created percentile_profile %p with "PRIu8" configurable "
            "percentiles", cap);

    return pp;

}

void
percentile_profile_destroy(struct percentile_profile **pp)
{
    ASSERT(pp != NULL);

    struct percentile_profile *p = *pp;

    if (p == NULL) {
        return;
    }

    cc_free(p->percentile);
    cc_free(p->result);
    cc_free(p);
    *pp = NULL;

    log_verb("Destroyed percentile_profile at %p", p);
}

histo_rstatus_e
percentile_profile_set(struct percentile_profile *pp, const double *percentile, uint8_t count)
{
    const double *src = percentile;
    double *dst = pp->percentile;
    double last = -1.0;

    pp->count = count;
    for (; count > 0; count--, src++, dst++) {
        if (_greater_dbl(*src, 100.0f)) {
            log_error("Percentile must be between [0.0, 100.0], %f provided", *src);

            return HISTO_EOVERFLOW;
        }
        if (_lesser_dbl(*src, 0.0f)) {
            log_error("Percentile must be between [0.0, 100.0], %f provided", *src);

            return HISTO_EUNDERFLOW;
        }
        if (_lesser_dbl(*src, last) || _equal_dbl(*src, last)) {
            log_error("Percentile being queried must be increasing");

            return HISTO_EORDER;
        }

        last = *src;
        *dst = *src;
    }

    log_verb("Set percentile_profile with %"PRIu8" predefined percentiles", count);

    return HISTO_OK;
}
