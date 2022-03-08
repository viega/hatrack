/* Copyright © 2021-2022 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           functional.c
 *
 *  Description:    Basic functional tests.
 *
 *  Author:         John Viega, john@zork.org
 */

#include "testhat.h"

#include <stdio.h>
#include <string.h>

typedef struct {
    uint32_t   tid;
    char      *type;
    testhat_t *dict;
    uint32_t   range; // Specifies range of keys and values, 0 - range-1
    uint32_t   iters; // Number of times to run the test;
} func_test_info_t;

typedef bool (*test_func_t)(func_test_info_t *);

// clang-format off
uint32_t            one_thread[]       = {1, 0};
uint32_t            multiple_threads[] = {2, 4, 8, 20, 100, 0};
uint32_t            basic_sizes[]      = {10, 100, 1000, 10000, 0};
uint32_t            shrug_sizes[]      = {1, 0};
_Atomic test_func_t test_func;
//  clang-format on

static void *
start_one_functest_thread(void *info)
{
    test_func_t func;
    bool        ret;

    mmm_register_thread();

    while (!(func = atomic_load(&test_func)))
        ;

    ret = (*test_func)(info);

    mmm_clean_up_before_exit();

    return (void *)ret;
}


static bool
functionality_test(test_func_t func,
                   uint32_t    iters,
                   uint32_t    num_threads,
                   uint32_t    range,
                   char       *type)
{
    pthread_t        threads[num_threads];
    func_test_info_t info[num_threads];
    uint32_t         i;
    uint64_t         res;
    testhat_t       *dict;

    atomic_store(&test_func, NULL);
    atomic_store(&mmm_nexttid, 0); // Reset thread ids.

    dict = testhat_new(type);

    /* Make sure there are enough precomputed hash values.  Most of
     * these functional tests insert i + 1, and some insert 1 + 2.  We
     * do range * 2, just to give us some headroom for other potential
     * functional tests.
     */
    precompute_hashes(range*2);
    
    for (i = 0; i < num_threads; i++) {
        info[i].tid   = i;
        info[i].range = range;
        info[i].type  = type;
        info[i].dict  = dict;
        info[i].iters = iters / num_threads;

        pthread_create(&threads[i], NULL, start_one_functest_thread, &info[i]);
    }

    atomic_store(&test_func, func);

    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], (void *)&res);

        if (!res) {
            abort();
        }
    }

    testhat_delete(dict);

    return true;
}


static void
run_one_func_test(test_func_t func,
                  uint32_t    iters,
                  char       *type,
                  uint32_t    range,
                  uint32_t    thread_count)
{
    bool ret;

    fprintf(stderr, "%10s:\t", type);
    fflush(stderr);

    ret = functionality_test(func, iters, thread_count, range, type);

    if (ret) {
        fprintf(stderr, "pass\n");
    }
    else {
        fprintf(stderr, "FAIL\n");
    }

    return;
}

/* For the moment, skip any functional tests on items w/o 128-bit
 * hash values.
 *
 * Similarly, don't rurn non-threadsafe algorithms, if there's more
 * than one active thread.
 */
static void
run_func_test(char       *name,
              test_func_t func,
              uint32_t    iters,
              char       *types[],
              uint32_t   *ranges,
              uint32_t   *tcounts)
{
    uint32_t    dict_ix;
    uint32_t    range_ix;
    uint32_t    tcount_ix;
    alg_info_t *info;
    
    fprintf(stderr, "[[ Test: %s ]]\n", name);
    tcount_ix = 0;
    while (tcounts[tcount_ix]) {
	range_ix = 0;
	while (ranges[range_ix]) {
	    fprintf(stderr,
		    "[%10s] -- Parameters: threads=%4d, "
		    "iters=%7d, range=%6d\n",
		    name,
		    tcounts[tcount_ix],
		    iters,
		    ranges[range_ix]);
	    dict_ix = 0;
	    while (types[dict_ix]) {
		info = algorithm_info(types[dict_ix]);
		if (info->hashbytes != 16) {
		    dict_ix++;
		    continue;
		}
		if (tcounts[tcount_ix] != 1 && !info->threadsafe) {
		    dict_ix++;
		    continue;
		}
		run_one_func_test(func,
				  iters,
				  types[dict_ix],
				  ranges[range_ix],
				  tcounts[tcount_ix]);
		dict_ix++;
	    }
	    range_ix++;
	}
	tcount_ix++;
    }

    return;
}

/* [ basic ]
 * 1) Have one thread add all the key / value pairs (where key = value).
 * 2) delete the top half.
 * 3) Run get on all items, to make sure only the expected items
 *    are there.
 * Ignores the # of iterations, only the range.
 */
static bool
test_basic(func_test_info_t *info)
{
    uint32_t i;

    for (i = 0; i < info->range; i++) {
        test_put(info->dict, i + 1, i + 1);
        if (test_get(info->dict, i + 1) != i + 1) {
            fprintf(stderr,
                    "%u != %llu\n",
                    test_get(info->dict, i + 1),
                    (unsigned long long)(i + 1));
            return false;
        }
    }

    for (i = 0; i < (info->range / 2); i++) {
        test_remove(info->dict, i + 1);
    }

    for (i = 0; i < (info->range / 2); i++) {
        if (test_get(info->dict, i + 1)) {
            fprintf(stderr, "didn't delete.\n");
            return false;
        }
    }

    for (; i < info->range; i++) {
        if (test_get(info->dict, i + 1) != i + 1) {
            fprintf(stderr,
                    "%u != %llu\n",
                    test_get(info->dict, i + 1),
                    (unsigned long long)(i + 1));
            return false;
        }
    }

    return true;
}

/* [ ordering ]
 *
 * Add n items in numerical order, delete the first half, reinsert all
 * the items, and then make sure the ordering is right in our
 * iterator.
 */
static bool
test_ordering(func_test_info_t *info)
{
    uint32_t        i;
    uint64_t        n;
    uint32_t        k;
    uint32_t        v;
    hatrack_view_t *view;

    for (i = 0; i < info->range; i++) {
        test_put(info->dict, i + 1, i + 1);
    }

    for (i = 0; i < (info->range / 2); i++) {
        test_remove(info->dict, i + 1);
    }

    for (i = 0; i < info->range; i++) {
        test_put(info->dict, i + 1, i + 1);
    }

    view = test_view(info->dict, &n, true);

    if (n != info->range) {
        free(view);
        return false;
    }

    for (i = 0; i < n; i++) {
        k = (uint32_t)(((uint64_t)view[i].item) >> 32);
        v = (uint32_t)(((uint64_t)view[i].item) & 0xffffffff);
        if (k != v) {
            free(view);
            return false;
        }

        if (((i + (info->range / 2) + 1) % info->range) != (k % info->range)) {
            free(view);
            return false;
        }
    }

    free(view);

    return true;
}

/* [ condput ]
 *
 * Add n items in numerical order, check them, try to condput on all
 * of them, delete them, re-add them, and check one more time.
 */

static bool
test_condput(func_test_info_t *info)
{
    uint32_t i;

    for (i = 0; i < info->range; i++) {
        test_add(info->dict, i + 1, i + 1);
    }

    for (i = 0; i < info->range; i++) {
        if (test_get(info->dict, i + 1) != i + 1) {
            fprintf(stderr,
                    "Get != put (%d != %llu)\n",
                    test_get(info->dict, i + 1),
                    (unsigned long long)(i + 1));
            return false;
        }
    }

    for (i = 0; i < info->range; i++) {
        if (test_add(info->dict, i + 1, i + 2)) {
            fprintf(stderr, "Didn't return false when it should have.\n");
            return false;
        }
        test_remove(info->dict, i + 1);
    }

    for (i = 0; i < info->range; i++) {
        if (!test_add(info->dict, i + 1, i + 2)) {
            fprintf(stderr, "Can't reput over a deleted item\n");
            return false;
        }
    }

    for (i = 0; i < info->range; i++) {
        if (test_get(info->dict, i + 1) != i + 2) {
            fprintf(stderr,
                    "No consistency in final check (expected: "
                    "%llu, got: %u)\n",
                    (unsigned long long)(i + 2),
                    test_get(info->dict, i + 1));
            return false;
        }
    }

    return true;
}

static bool
test_replace_op(func_test_info_t *info)
{
    uint32_t i;

    for (i = 0; i < 50; i++) {
        test_put(info->dict, i + 1, i + 1);
    }
    for (i = 0; i < 100; i++) {
        test_replace(info->dict, i + 1, i + 2);
    }
    for (i = 0; i < 50; i++) {
        if (test_get(info->dict, i + 1) != i + 2) {
            return false;
        }
    }

    for (; i < 100; i++) {
        if (test_get(info->dict, i + 1)) {
            return false;
        }
    }

    return true;
}

// Validate this by looking at counters.
static bool
test_shrinking(func_test_info_t *info)
{
    uint32_t i;

    for (i = 0; i < 380; i++) {
        test_put(info->dict, i + 1, i + 1);
    }

    for (i = 0; i < 380; i++) {
        test_remove(info->dict, i + 1);
    }

    for (i = 381; i < 500; i++) {
        test_put(info->dict, i + 1, i + 1);
    }

    return true;
}

/* [ parallel ]
 *
 * Have each thread attempt to set every value in the range,
 * then check to make sure the items are all correct.
 */
static bool
test_parallel(func_test_info_t *info)
{
    uint32_t i, n;

    for (i = 0; i < info->range; i++) {
        test_put(info->dict, i, i);
    }

    for (i = 0; i < info->range; i++) {
        n = test_get(info->dict, i);

        if (n != i) {
            printf("%llu != %llu\n",
                   (unsigned long long)n,
                   (unsigned long long)i);
            printf("Is HATRACK_TEST_MAX_KEYS high enough?\n");

            return false;
        }
    }

    return true;
}

void
run_functional_tests(config_info_t *config)
{
    char **hat_list;

    hat_list = config->hat_list;
    
    run_func_test("basic",
                  test_basic,
                  1,
		  hat_list,
		  basic_sizes,
                  one_thread);
    counters_output_delta();        
    run_func_test("ordering",
                  test_ordering,
                  1,
		  hat_list,
		  basic_sizes,
                  one_thread);
    counters_output_delta();        
    run_func_test("shrinking",
		  test_shrinking,
		  1,
		  hat_list,
		  shrug_sizes,
		  one_thread);
    counters_output_delta();
    run_func_test("replace",
		  test_replace_op,
		  1,
		  hat_list,
		  shrug_sizes,
		  one_thread);
    counters_output_delta();
    run_func_test("condput",
		  test_condput,
		  1,
		  hat_list,
		  shrug_sizes,
		  one_thread);
    counters_output_delta();
    run_func_test("parallel",
                  test_parallel,
                  10,
		  hat_list,
		  basic_sizes,
                  multiple_threads);
    counters_output_delta();
    
    return;
}

