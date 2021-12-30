/*
 * Copyright © 2021 John Viega
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *  Name:           test.c
 *  Description:    Lohat test cases, and code to support tests.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "hash.h"
#include "testhat.h"
#include <fcntl.h>  // For open
#include <unistd.h> // For close and read
#include <time.h>
#include <stdio.h>

#ifndef HATRACK_TEST_MAX_KEYS
#define HATRACK_TEST_MAX_KEYS 1000000
#endif

typedef union {
    struct {
        uint32_t key;
        uint32_t value;
    } s;
    uint64_t i;
} test_item;

hatrack_hash_t precomputed_hashes[HATRACK_TEST_MAX_KEYS];

static inline void
test_put(testhat_t *self, uint32_t key, uint32_t value)
{
    test_item item;

    item.s.key   = key;
    item.s.value = value;

    testhat_put(self, &precomputed_hashes[key], (void *)item.i, NULL);
}

/*static*/ inline void
test_replace(testhat_t *self, uint32_t key, uint32_t value)
{
    test_item item;

    item.s.key   = key;
    item.s.value = value;

    testhat_replace(self, &precomputed_hashes[key], (void *)item.i, NULL);
}

static inline bool
test_add(testhat_t *self, uint32_t key, uint32_t value)
{
    test_item item;

    item.s.key   = key;
    item.s.value = value;

    return testhat_add(self, &precomputed_hashes[key], (void *)item.i);
}

static inline uint32_t
test_get(testhat_t *self, uint32_t key)
{
    test_item item;

    item.i = (uint64_t)testhat_get(self, &precomputed_hashes[key], NULL);

    return item.s.value;
}

static inline void
test_remove(testhat_t *self, uint32_t key)
{
    testhat_remove(self, &precomputed_hashes[key], NULL);
}

static inline hatrack_view_t *
test_view(testhat_t *self, uint64_t *n, bool sort)
{
    return testhat_view(self, n, sort);
}

static void
test_precompute_hashes()
{
    uint64_t i;

    for (i = 0; i < HATRACK_TEST_MAX_KEYS; i++) {
        precomputed_hashes[i] = hash_int(i);
    }
}

/* RNG support. We use a lot of random numbers in our testing, and
 * would like to avoid several things:
 *
 * 1) Calling into the kernel more than we need to.
 *
 * 2) Any locks around RNG APIs.  For instance, I'm pretty sure
 *    arc4random() has such a lock on my machine.
 *
 * 3) Holding on to too much memory.
 *
 * Our basic approach is to implement ARC4 ourselves, and keep the
 * state on a per-thread basis, with the seed xor'd with the bottom
 * byte of the thread's pthread id (just to get some variance in the
 * number streams).  We read the seed once from /dev/urandom.
 */
#ifndef HATRACK_RAND_SEED_SIZE
#define HATRACK_RAND_SEED_SIZE 32
#endif

uint8_t seed_buf[HATRACK_RAND_SEED_SIZE];

typedef struct {
    uint32_t S[256];
    uint32_t x, y;
} arc4_ctx;

__thread arc4_ctx rng_ctx;
__thread bool     rand_inited = false;

static void
test_init_rand()
{
    int rand_fd = open("/dev/urandom", O_RDONLY);

    read(rand_fd, seed_buf, HATRACK_RAND_SEED_SIZE);
    close(rand_fd);
}

static void
test_thread_init_rand()
{
    uint64_t tid = (uint64_t)pthread_self();

    uint32_t a, i, j = 0, k = 0;

    rng_ctx.x = 1;
    rng_ctx.y = 0;

    for (i = 0; i < 256; i++) {
        rng_ctx.S[i] = i;
    }

    for (i = 0; i < 256; i++) {
        a            = rng_ctx.S[i];
        j            = (j + (seed_buf[k] ^ tid) + a) & 0xff;
        rng_ctx.S[i] = rng_ctx.S[j];
        rng_ctx.S[j] = a;
        ++k;
        if (k == 32)
            k = 0;
    }
}

static uint32_t
test_rand()
{
    uint32_t out;
    uint8_t *p = (uint8_t *)&out;
    uint32_t a, b, ta, tb, ty, i;

    if (!rand_inited) {
        test_thread_init_rand();
        rand_inited = true;
    }

    a         = rng_ctx.S[rng_ctx.x];
    rng_ctx.y = (rng_ctx.y + a) & 0xff;
    b         = rng_ctx.S[rng_ctx.y];

    for (i = 0; i < 4; i++) {
        rng_ctx.S[rng_ctx.y] = a;
        a                    = (a + b) & 0xff;
        rng_ctx.S[rng_ctx.x] = b;
        rng_ctx.x            = (rng_ctx.x + 1) & 0xff;
        ta                   = rng_ctx.S[rng_ctx.x];
        ty                   = (rng_ctx.y + ta) & 0xff;
        tb                   = rng_ctx.S[ty];
        *p++                 = rng_ctx.S[a];
        rng_ctx.y            = ty;
        a                    = ta;
        b                    = tb;
    }

    return out;
}

static void
test_init()
{
    test_precompute_hashes();
    test_init_rand();
}

typedef struct {
    uint32_t   tid;
    char      *type;
    testhat_t *dict;
    uint32_t   range; // Specifies range of keys and values, 0 - range-1
    uint32_t   iters; // Number of times to run the test;
    uint32_t   extra;
} test_info_t;

typedef bool (*test_func_t)(test_info_t *);

static _Atomic test_func_t test_func;
extern _Atomic uint64_t    mmm_nexttid;

static void *
start_one_thread(void *info)
{
    test_func_t func;
    bool        ret;

    while (!(func = atomic_load(&test_func)))
        ;

    ret = (*test_func)(info);

    mmm_clean_up_before_exit();

    return (void *)ret;
}

static uint32_t
time_test(test_func_t      func,
          uint32_t         iters,
          char            *type,
          uint32_t         num_threads,
          uint32_t         range,
          uint32_t         extra,
          struct timespec *sspec,
          struct timespec *espec)
{
    clock_t     start;
    pthread_t   threads[num_threads];
    test_info_t info[num_threads];
    uint32_t    i;
    testhat_t  *dict;

    atomic_store(&test_func, NULL);
    atomic_store(&mmm_nexttid, 0); // Reset thread ids.

    dict = testhat_new(type);
    for (i = 0; i < num_threads; i++) {
        info[i].tid   = i;
        info[i].range = range;
        info[i].dict  = dict;
        info[i].type  = type;
        info[i].iters = iters / num_threads;
        info[i].extra = extra;
        pthread_create(&threads[i], NULL, start_one_thread, &info[i]);
    }

    start = clock();
    clock_gettime(CLOCK_MONOTONIC, sspec);
    atomic_store(&test_func, func); // Start the threads.
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    testhat_delete(dict);

    atomic_store(&test_func, NULL);
    clock_gettime(CLOCK_MONOTONIC, espec);
    return clock() - start;
}

static bool
functionality_test(test_func_t func,
                   uint32_t    iters,
                   uint32_t    num_threads,
                   uint32_t    range,
                   char       *type,
                   uint32_t    extra)
{
    pthread_t   threads[num_threads];
    test_info_t info[num_threads];
    uint32_t    i;
    uint64_t    res;
    testhat_t  *dict;

    atomic_store(&test_func, NULL);
    atomic_store(&mmm_nexttid, 0); // Reset thread ids.

    dict = testhat_new(type);
    for (i = 0; i < num_threads; i++) {
        info[i].tid   = i;
        info[i].range = range;
        info[i].type  = type;
        info[i].dict  = dict;
        info[i].iters = iters / num_threads;
        info[i].extra = extra;
        pthread_create(&threads[i], NULL, start_one_thread, &info[i]);
    }

    atomic_store(&test_func, func);
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], (void *)&res);
        if (!res) {
#ifdef HATRACK_DEBUG
            abort();
#endif
            return false;
        }
    }
    testhat_delete(dict);

    return true;
}

static void
run_one_time_test(char       *name,
                  test_func_t func,
                  uint32_t    iters,
                  char       *type,
                  uint32_t    range,
                  uint32_t    thread_count,
                  uint32_t    extra)
{
    uint32_t        ticks;
    double          walltime;
    struct timespec sspec;
    struct timespec espec;

    fprintf(stderr, "%10s:\t", type);
    fflush(stderr);
    ticks    = time_test(func,
                      iters,
                      type,
                      thread_count,
                      range,
                      extra,
                      &sspec,
                      &espec);
    walltime = (espec.tv_sec - sspec.tv_sec)
             + ((espec.tv_nsec - sspec.tv_nsec) / 1000000000.0);
    fprintf(stderr,
            "%.4f sec, %d clocks, \t%0.4f c/i\n",
            walltime,
            ticks,
            (double)(((double)ticks) / (double)iters));
}

static void
run_one_func_test(char       *name,
                  test_func_t func,
                  uint32_t    iters,
                  char       *type,
                  uint32_t    range,
                  uint32_t    thread_count,
                  uint32_t    extra)
{
    bool ret;

    fprintf(stderr, "%10s:\t", type);
    fflush(stderr);
    ret = functionality_test(func, iters, thread_count, range, type, extra);
    if (ret) {
        fprintf(stderr, "pass\n");
    }
    else {
        fprintf(stderr, "FAIL\n");
    }
}

static void
run_time_test(char       *name,
              test_func_t func,
              uint32_t    iters,
              char       *types[],
              uint32_t   *ranges,
              uint32_t   *tcounts,
              uint32_t   *extra)
{
    uint32_t dict_ix;
    uint32_t range_ix;
    uint32_t tcount_ix;
    uint32_t extra_ix;

    fprintf(stderr, "[[ Test: %s ]]\n", name);

    extra_ix = 0;
    do {
        tcount_ix = 0;
        while (tcounts[tcount_ix]) {
            range_ix = 0;
            while (ranges[range_ix]) {
                if (extra) {
                    fprintf(stderr,
                            "[%10s] -- Parameters: threads=%4d, "
                            "iters=%7d, range=%6d, other=%4x\n",
                            name,
                            tcounts[tcount_ix],
                            iters,
                            ranges[range_ix],
                            extra[extra_ix]);
                    dict_ix = 0;
                    while (types[dict_ix]) {
                        run_one_time_test(name,
                                          func,
                                          iters,
                                          types[dict_ix],
                                          ranges[range_ix],
                                          tcounts[tcount_ix],
                                          extra[extra_ix]);

                        dict_ix++;
                    }
                }
                else {
                    fprintf(stderr,
                            "[%10s] -- Parameters: threads=%4d, "
                            "iters=%7d, range=%6d\n",
                            name,
                            tcounts[tcount_ix],
                            iters,
                            ranges[range_ix]);
                    dict_ix = 0;

                    while (types[dict_ix]) {
                        run_one_time_test(name,
                                          func,
                                          iters,
                                          types[dict_ix],
                                          ranges[range_ix],
                                          tcounts[tcount_ix],
                                          0);
                        dict_ix++;
                    }
                }
                range_ix++;
            }
            tcount_ix++;
        }
    } while (extra && extra[++extra_ix]);
}

static void
run_func_test(char       *name,
              test_func_t func,
              uint32_t    iters,
              char       *types[],
              uint32_t   *ranges,
              uint32_t   *tcounts,
              uint32_t   *extra)
{
    uint32_t dict_ix;
    uint32_t range_ix;
    uint32_t tcount_ix;
    uint32_t extra_ix;

    fprintf(stderr, "[[ Test: %s ]]\n", name);
    extra_ix = 0;
    do {
        tcount_ix = 0;
        while (tcounts[tcount_ix]) {
            range_ix = 0;
            while (ranges[range_ix]) {
                if (extra) {
                    fprintf(stderr,
                            "[%10s] -- Parameters: threads=%4d, "
                            "iters=%7d, range=%6d, other=%4x\n",
                            name,
                            tcounts[tcount_ix],
                            iters,
                            ranges[range_ix],
                            extra[extra_ix]);
                    dict_ix = 0;
                    while (types[dict_ix]) {
                        run_one_func_test(name,
                                          func,
                                          iters,
                                          types[dict_ix],
                                          ranges[range_ix],
                                          tcounts[tcount_ix],
                                          extra[extra_ix]);

                        dict_ix++;
                    }
                }
                else {
                    fprintf(stderr,
                            "[%10s] -- Parameters: threads=%4d, "
                            "iters=%7d, range=%6d\n",
                            name,
                            tcounts[tcount_ix],
                            iters,
                            ranges[range_ix]);
                    dict_ix = 0;
                    while (types[dict_ix]) {
                        run_one_func_test(name,
                                          func,
                                          iters,
                                          types[dict_ix],
                                          ranges[range_ix],
                                          tcounts[tcount_ix],
                                          0);
                        dict_ix++;
                    }
                }
                range_ix++;
            }
            tcount_ix++;
        }
    } while (extra && extra[++extra_ix]);
}

/* Actual tests below here.
 *
 * [ basic ]
 * 1) Have one thread add all the key / value pairs (where key = value).
 * 2) delete the top half.
 * 3) Run get on all items, to make sure only the expected items
 *    are there.
 * Ignores the # of iterations, only the range.
 */
bool
test_basic(test_info_t *info)
{
    uint64_t i;

    for (i = 0; i < info->range; i++) {
        test_put(info->dict, i + 1, i + 1);
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
            fprintf(stderr, "%u != %llu\n", test_get(info->dict, i + 1), i + 1);
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
bool
test_ordering(test_info_t *info)
{
    uint64_t        i;
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
        if (((k + (info->range / 2)) % info->range) != i) {
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

bool
test_condput(test_info_t *info)
{
    uint64_t i;

    for (i = 0; i < info->range; i++) {
        test_add(info->dict, i + 1, i + 1);
    }
    for (i = 0; i < info->range; i++) {
        if (test_get(info->dict, i + 1) != i + 1) {
            fprintf(stderr, "Get != put\n");
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
            fprintf(stderr, "No consistency in final check\n");
            return false;
        }
    }

    return true;
}

// Validate this by looking at counters.
bool
test_shrinking(test_info_t *info)
{
    uint64_t i;

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
 * [ ||-large ]
 *
 * Have each thread attempt to set every value in the range,
 * then check to make sure the items are all correct.
 */
bool
test_parallel(test_info_t *info)
{
    uint64_t i, n;

    for (i = 0; i < info->range; i++) {
        test_put(info->dict, i, i);
    }

    for (i = 0; i < info->range; i++) {
        n = test_get(info->dict, i);
        if (n != i) {
            printf("%llu != %llu\n", n, i);
            printf("Is HATRACK_TEST_MAX_KEYS high enough?\n");
            return false;
        }
    }

    return true;
}
// [ rand() ]
//
// Calculate a baseline for calls to rand().
bool
test_rand_speed(test_info_t *info)
{
    uint32_t i;

    for (i = 0; i < info->iters; i++) {
        test_rand();
    }

    return true;
}

bool
test_insert_speed(test_info_t *info)
{
    uint32_t i;

    for (i = 0; i < info->iters; i++) {
        test_put(info->dict, i % info->range, test_rand() % info->range);
    }

    return true;
}

bool
test_write_speed(test_info_t *info)
{
    uint32_t i;
    uint32_t key;

    for (i = 0; i < info->iters; i++) {
        key = test_rand() % info->range;

        if (!(test_rand() % info->extra)) {
            test_remove(info->dict, key);
        }
        else {
            test_put(info->dict, key, key);
        }
    }

    return true;
}

bool
test_rw_speed(test_info_t *info)
{
    uint32_t i;
    uint32_t key;
    uint32_t action;
    uint32_t delete_odds  = info->extra & 0xff;
    uint32_t write_odds   = info->extra >> 4;
    uint32_t nonread_odds = delete_odds + write_odds;

    for (i = 0; i < info->iters; i++) {
        key    = test_rand() % info->range;
        action = test_rand() % 100;
        if (action <= nonread_odds) {
            action = test_rand() % 100;
            if (action <= delete_odds) {
                test_remove(info->dict, key);
            }
            else {
                test_put(info->dict, key, key);
            }
        }
        else {
            key = test_get(info->dict, key);
        }
    }

    return true;
}

bool
test_sort_speed(test_info_t *info)
{
    uint32_t        i;
    uint32_t        key;
    uint32_t        action;
    uint32_t        delete_odds  = info->extra & 0xff;
    uint32_t        write_odds   = info->extra >> 4;
    uint32_t        nonread_odds = delete_odds + write_odds;
    uint64_t        n;
    hatrack_view_t *v;

    for (i = 0; i < info->iters; i++) {
        key    = test_rand() % info->range;
        action = test_rand() % 100;
        if (action <= nonread_odds) {
            action = test_rand() % 100;
            if (action <= delete_odds) {
                test_remove(info->dict, key);
            }
            else {
                test_put(info->dict, key, key);
            }
        }
        else {
            key = test_get(info->dict, key);
        }
    }
    for (i = 0; i < info->iters / 100; i++) {
        v = testhat_view(info->dict, &n, true);
        free(v);
    }

    return true;
}

bool
test_sort_contention(test_info_t *info)
{
    uint32_t        i;
    uint32_t        key;
    uint32_t        action;
    uint32_t        delete_odds  = info->extra & 0xff;
    uint32_t        write_odds   = info->extra >> 4;
    uint32_t        nonread_odds = delete_odds + write_odds;
    uint64_t        n;
    hatrack_view_t *v;

    for (i = 0; i < info->iters; i++) {
        key    = test_rand() % info->range;
        action = test_rand() % 100;
        if (action <= nonread_odds) {
            action = test_rand() % 100;
            if (action <= delete_odds) {
                test_remove(info->dict, key);
            }
            else {
                test_put(info->dict, key, key);
            }
        }
        else {
            key = test_get(info->dict, key);
        }

        if (!(i % 100)) {
            v = testhat_view(info->dict, &n, false);
            free(v);
        }
    }

    return true;
}

// clang-format off
uint32_t            basic_sizes[]   = {10, 100, 1000, 10000, 0};
uint32_t            sort_sizes[]    = {10, 128, 256, 512, 1024, 2048, 4096,
                                       8192, 100000, 0};
uint32_t            large_sizes[]   = {100000, 1000000, 0};
uint32_t            shrug_sizes[]   = {1, 0};
uint32_t            small_size[]    = {10, 0};
uint32_t            one_thread[]    = {1, 0};
uint32_t            basic_threads[] = {2, 4, 8, 20, 100, 0};
uint32_t            del_rate[]      = {100, 10, 3, 0};
uint32_t            write_rates[]   = {0x010a, 0x050a, 0x0a0a, 0};
//  clang-format on

#if 0
char *threadsafe_dicts[] = {
    "swimcap", "swimcap2",
    "newshat",
    "witchhat", 
    "hihat1", "hihat1a", "hihat64",
    "ballcap",
    "woolhat",
    "lohat0", "lohat1",
    /*"lohat2",*/
    NULL
};
char *all_dicts[]     = {
    "refhat",
    "swimcap", "swimcap2",
    "newshat",
    "witchhat", 
    "hihat1", "hihat1a", "hihat64",
    "ballcap",
    "woolhat",    
    "lohat0", "lohat1",
    /*"lohat2",*/
    NULL
};
char *st_dicts[]      = {
    "refhat",
    NULL
};
#else
char *threadsafe_dicts[] = {
    NULL,
    "swimcap", "swimcap2",
    "newshat",
    "witchhat", 
    "hihat1", "hihat1a", "hihat64",
    "ballcap",
    "woolhat",
    "lohat0", "lohat1",
    /*"lohat2",*/
    NULL
};
char *all_dicts[]     = {
    "refhat",
    "swimcap", "swimcap2",
    "newshat",
    "witchhat", 
    "hihat1", "hihat1a", "hihat64",
    "ballcap",
    "woolhat",    
    "lohat0", "lohat1",
    /*"lohat2",*/
    NULL
};
char *st_dicts[]      = {
    "refhat",
    NULL
};

#endif

#ifndef DEFAULT_ITERS
#define DEFAULT_ITERS 1000000
#endif
int
main(int argc, char *argv[], char *envp[])
{
    test_init();
    run_func_test("basic",
                  test_basic,
                  1,
                  all_dicts,
                  basic_sizes,
                  one_thread,
                  0);
    counters_output_delta();        
    run_func_test("ordering",
                  test_basic,
                  1,
                  all_dicts,
                  basic_sizes,
                  one_thread,
                  0);
    counters_output_delta();        
    run_func_test("shrinking",
		  test_shrinking,
		  1,
		  all_dicts,
		  shrug_sizes,
		  one_thread,
		  0);
    counters_output_delta();
    run_func_test("condput",
		  test_condput,
		  1,
		  all_dicts,
		  shrug_sizes,
		  one_thread,
		  0);
    counters_output_delta();
    run_func_test("parallel",
                  test_parallel,
                  10,
                  threadsafe_dicts,
                  basic_sizes,
                  basic_threads,
                  0);
    counters_output_delta();        
    run_time_test("rand()-1t",
                  test_rand_speed,
                  DEFAULT_ITERS,
                  all_dicts,
                  shrug_sizes,
                  one_thread,
                  0);
    counters_output_delta();        
    run_time_test("rand()-mt",
                  test_rand_speed,
                  DEFAULT_ITERS,
                  threadsafe_dicts,
                  shrug_sizes,
                  basic_threads,
                  0);
    counters_output_delta();        
    run_time_test("insert-1t",
                  test_insert_speed,
                  DEFAULT_ITERS,
                  all_dicts,
                  basic_sizes,
                  one_thread,
                  0);
    counters_output_delta();        
    run_time_test("insert-mt",
                  test_insert_speed,
                  DEFAULT_ITERS,
                  threadsafe_dicts,
                  basic_sizes,
                  basic_threads,
                  0);
    counters_output_delta();        
    run_time_test("writes-1t",
                  test_write_speed,
                  DEFAULT_ITERS,
                  all_dicts,
                  basic_sizes,
                  one_thread,
                  del_rate);
    counters_output_delta();        
    run_time_test("writes-mt",
                  test_write_speed,
                  DEFAULT_ITERS,
                  threadsafe_dicts,
                  basic_sizes,
                  basic_threads,
                  del_rate);
    counters_output_delta();        
    run_time_test("rw speed-1t",
                  test_rw_speed,
                  DEFAULT_ITERS,
                  all_dicts,
                  basic_sizes,
                  one_thread,
                  write_rates);
    counters_output_delta();        
    run_time_test("rw speed-mt",
                  test_rw_speed,
                  DEFAULT_ITERS,
		  threadsafe_dicts,
                  basic_sizes,
                  basic_threads,
                  write_rates);
    counters_output_delta();
    run_time_test("sorts-1t",
                  test_sort_speed,
                  DEFAULT_ITERS/10,
                  all_dicts,
                  sort_sizes,
                  one_thread,
                  write_rates);
    counters_output_delta();
    run_time_test("sorts-mt",
                  test_sort_speed,
                  DEFAULT_ITERS/10,
                  threadsafe_dicts,
                  sort_sizes,
                  basic_threads,
                  write_rates);
    run_time_test("contend",
                  test_sort_contention,
                  DEFAULT_ITERS/10,
                  threadsafe_dicts,
                  sort_sizes,
                  basic_threads,
                  write_rates);
    counters_output_delta();    
#ifdef HATRACK_RUN_LARGE_TESTS    
    run_func_test("||-large",
                  test_parallel,
                  1,
                  threadsafe_dicts,
                  large_sizes,
                  basic_threads,
                  0);
#endif    

    counters_output_alltime();
    
    printf("Press <enter> to exit.\n");
    getc(stdin);
}
