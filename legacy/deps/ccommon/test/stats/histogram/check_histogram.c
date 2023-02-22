#include <cc_histogram.h>

#include <check.h>

#include <float.h>
#include <stdlib.h>
#include <stdio.h>

#define SUITE_NAME "histogram"
#define DEBUG_LOG  SUITE_NAME ".log"

#define PARRAY_SIZE 7
const double parray[PARRAY_SIZE] = {25, 50, 75, 90, 95, 99, 99.9};
const double pbad[PARRAY_SIZE] = {-5, 0, 50, 50, 25, 100, 200};

/*
 * utilities
 */
static void
test_setup(void)
{
}

static void
test_teardown(void)
{
}

/*
 * tests
 */
START_TEST(test_histo_basic)
{
#define m 1
#define r 10
#define n 20
    struct histo_u32 *histo = histo_u32_create(m, r, n);

    ck_assert(histo != NULL);
    ck_assert_int_eq(histo->M, 1 << m);
    ck_assert_int_eq(histo->R, (1 << r) - 1);
    ck_assert_int_eq(histo->N, (1 << n) - 1);
    ck_assert_int_eq(histo->G, (1 << (r -m - 1)));
    ck_assert_int_eq(histo->nbucket, (n - r + 2) * histo->G);
    histo_u32_destroy(&histo);
    ck_assert(histo == NULL);
#undef n
#undef r
#undef m
}
END_TEST

START_TEST(test_percentile_basic)
{
    struct percentile_profile *pp = percentile_profile_create(PARRAY_SIZE * 2);

    ck_assert(pp != NULL);
    ck_assert_int_eq(pp->cap, PARRAY_SIZE * 2);
    ck_assert_int_eq(pp->count, 0);

    percentile_profile_set(pp, parray, PARRAY_SIZE);
    ck_assert_int_eq(pp->count, PARRAY_SIZE);
    for (int count = 0; count < PARRAY_SIZE; count++) {
        ck_assert(fabs(*(pp->percentile + count) - parray[count]) < DBL_EPSILON);
    }

    /* percentile checks */
    ck_assert(percentile_profile_set(pp, pbad, 2) == HISTO_EUNDERFLOW);
    ck_assert(percentile_profile_set(pp, pbad + 1, 3) == HISTO_EORDER);
    ck_assert(percentile_profile_set(pp, pbad + 3, 3) == HISTO_EORDER);
    ck_assert(percentile_profile_set(pp, pbad + 4, 3) == HISTO_EOVERFLOW);

    percentile_profile_destroy(&pp);
    ck_assert(pp == NULL);
}
END_TEST

START_TEST(test_record)
{
#define m 0
#define r 10
#define n 20
    struct histo_u32 *histo = histo_u32_create(m, r, n);

    ck_assert_int_eq(histo->nrecord, 0);
    histo_u32_record(histo, 0, 1);
    ck_assert_int_eq(*histo->buckets, 1);
    ck_assert_int_eq(histo->nrecord, 1);
    histo_u32_record(histo, 1, 1);
    ck_assert_int_eq(*(histo->buckets + 1), 1);
    ck_assert_int_eq(histo->nrecord, 2);
    histo_u32_record(histo, 1023, 1);
    ck_assert_int_eq(*(histo->buckets + 1023), 1);
    histo_u32_record(histo, 1024, 1);
    ck_assert_int_eq(*(histo->buckets + 1024), 1);
    histo_u32_record(histo, 1025, 1);
    ck_assert_int_eq(*(histo->buckets + 1024), 2);
    histo_u32_record(histo, 1026, 1);
    ck_assert_int_eq(*(histo->buckets + 1025), 1);
    histo_u32_record(histo, 2048, 1);
    ck_assert_int_eq(*(histo->buckets + 1536), 1);
    histo_u32_record(histo, 2051, 1);
    ck_assert_int_eq(*(histo->buckets + 1536), 2);
    histo_u32_record(histo, 2052, 1);
    ck_assert_int_eq(*(histo->buckets + 1537), 1);
    histo_u32_record(histo, (1 << 20) - 1, 1);
    ck_assert_int_eq(*(histo->buckets + histo->nbucket - 1), 1);
    ck_assert(histo_u32_record(histo, 1 << 20, 1) == HISTO_EOVERFLOW);

    histo_u32_destroy(&histo);
#undef n
#undef r
#undef m
}
END_TEST

START_TEST(test_report_sparse)
{
#define m 1
#define r 3
#define n 5
    const double percentiles[5] = {0, 10, 50, 75, 100};
    const double results[5] = {1, 1, 3, 6, 6};
    uint64_t value;
    struct histo_u32 *histo = histo_u32_create(m, r, n);
    struct percentile_profile *pp = percentile_profile_create(5);

    ck_assert(histo_u32_report(&value, histo, 0.1) == HISTO_EEMPTY);

    histo_u32_record(histo, 2, 1); /* bucket 1 */
    histo_u32_record(histo, 6, 1); /* bucket 3 */
    ck_assert_int_eq(*(histo->buckets + 3), 1);
    histo_u32_record(histo, 23, 1);/* bucket 6 */


    ck_assert(histo_u32_report(&value, histo, percentiles[0]) == HISTO_OK);
    ck_assert_int_eq(value, 1);
    ck_assert(histo_u32_report(&value, histo, percentiles[1]) == HISTO_OK);
    ck_assert_int_eq(value, 1);
    ck_assert(histo_u32_report(&value, histo, percentiles[2]) == HISTO_OK);
    ck_assert_int_eq(value, 3);
    ck_assert(histo_u32_report(&value, histo, percentiles[3]) == HISTO_OK);
    ck_assert_int_eq(value, 6);
    ck_assert(histo_u32_report(&value, histo, percentiles[4]) == HISTO_OK);
    ck_assert_int_eq(value, 6);

    ck_assert_int_eq(percentile_profile_set(pp, percentiles, 5), HISTO_OK);
    ck_assert(histo_u32_report_multi(pp, histo) == HISTO_OK);
    ck_assert_int_eq(pp->min, 1);
    ck_assert_int_eq(pp->max, 6);
    for (int i = 0; i < 5; ++i) {
        ck_assert_int_eq(*(pp->result + i), results[i]);
    }

    histo_u32_record(histo, 31, 1); /* bucket 7 */
    ck_assert(histo_u32_report_multi(pp, histo) == HISTO_OK);
    ck_assert_int_eq(pp->min, 1);
    ck_assert_int_eq(pp->max, 7);
    ck_assert_int_eq(*(pp->result + 4), 7);

    percentile_profile_destroy(&pp);
    histo_u32_destroy(&histo);
#undef n
#undef r
#undef m
}
END_TEST

START_TEST(test_report_exact)
{
#define m 0
#define r 4
#define n 4
    uint64_t value;
    struct histo_u32 *histo = histo_u32_create(m, r, n);

    for (int i = 1; i <= 10; ++i) {
        histo_u32_record(histo, i, 1);
        ck_assert_int_eq(*(histo->buckets + i), 1);
    }

    for (int i = 1; i <= 10; ++i) {
        ck_assert(histo_u32_report(&value, histo, i * 10) == HISTO_OK);
        ck_assert_int_eq(value, i);
    }

    histo_u32_destroy(&histo);
#undef n
#undef r
#undef m
}
END_TEST

START_TEST(test_bucket)
{
#define m 0
#define r 10
#define n 20
    struct histo_u32 *histo = histo_u32_create(m, r, n);

    ck_assert_int_eq(histo->nrecord, 0);

    histo_u32_destroy(&histo);
#undef n
#undef r
#undef m
}
END_TEST

/*
 * test suite
 */
static Suite *
metric_suite(void)
{
    Suite *s = suite_create(SUITE_NAME);

    /* basic requests */
    TCase *tc_histogram = tcase_create("cc_histogram test");
    suite_add_tcase(s, tc_histogram);

    tcase_add_test(tc_histogram, test_histo_basic);
    tcase_add_test(tc_histogram, test_percentile_basic);
    tcase_add_test(tc_histogram, test_record);
    tcase_add_test(tc_histogram, test_report_sparse);
    tcase_add_test(tc_histogram, test_report_exact);
    tcase_add_test(tc_histogram, test_bucket);
    return s;
}
/**************
 * test cases *
 **************/

int
main(void)
{
    int nfail;

    /* setup */
    test_setup();

    Suite *suite = metric_suite();
    SRunner *srunner = srunner_create(suite);
    srunner_set_log(srunner, DEBUG_LOG);
    srunner_run_all(srunner, CK_ENV); /* set CK_VEBOSITY in ENV to customize */
    nfail = srunner_ntests_failed(srunner);
    srunner_free(srunner);

    /* teardown */
    test_teardown();

    return (nfail == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
