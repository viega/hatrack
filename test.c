/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           test.c
 *  Description:    Lowhat tests and code to support tests.
 *  Author:         John Viega, john@zork.org
 *
 */

#include "hash.h"
#include "lowhat.h"
#include <fcntl.h>  // For open
#include <unistd.h> // For close and read
#include <stdio.h>

#ifndef LOWHAT_TEST_MAX_KEYS
#define LOWHAT_TEST_MAX_KEYS 1000000
#endif

static lowhat_hash_t precomputed_hashes[LOWHAT_TEST_MAX_KEYS];

typedef union {
    struct {
        uint32_t key;
        uint32_t value;
    } s;
    uint64_t i;
} test_item;

static inline void
test_put(lowhat_t *self, uint32_t key, uint32_t value)
{
    test_item item;

    item.s.key   = key;
    item.s.value = value;

    lowhat_put(self, &precomputed_hashes[key], (void *)item.i, false, NULL);
}

static inline uint32_t
test_get(lowhat_t *self, uint32_t key)
{
    test_item item;

    item.i = (uint64_t)lowhat_get(self, &precomputed_hashes[key], NULL);

    return item.s.value;
}

static inline void
test_remove(lowhat_t *self, uint32_t key)
{
    lowhat_remove(self, &precomputed_hashes[key], NULL);
}

static inline lowhat_view_t *
test_view(lowhat_t *self, uint64_t *n)
{
    return lowhat_view(self, n);
}

static void
test_precompute_hashes()
{
    uint64_t i;

    for (i = 0; i < LOWHAT_TEST_MAX_KEYS; i++) {
        precomputed_hashes[i] = hash_int(i);
    }
}

// RNG support. We use a lot of random numbers in our testing, and
// would like to avoid several things:
//
// 1) Calling into the kernel more than we need to.
//
// 2) Any locks around RNG APIs.  For instance, I'm pretty sure
//    arc4random() has such a lock on my machine.
//
// 3) Holding on to too much memory.
//
// Our basic approach is to implement ARC4 ourselves, and keep the
// state on a per-thread basis, with the seed xor'd with the bottom
// byte of the thread's pthread id (just to get some variance in the
// number streams).  We read the seed once from /dev/urandom.

#ifndef LOWHAT_RAND_SEED_SIZE
#define LOWHAT_RAND_SEED_SIZE 32
#endif

uint8_t seed_buf[LOWHAT_RAND_SEED_SIZE];

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

    read(rand_fd, seed_buf, LOWHAT_RAND_SEED_SIZE);
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
    uint32_t  tid;
    lowhat_t *dict;
    uint32_t  range; // Specifies range of keys and values, 0 - range-1
    uint32_t  iters; // Number of times to run the test;
    uint32_t  extra;
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
time_test(test_func_t func,
          uint32_t    iters,
          uint32_t    num_threads,
          lowhat_t   *dict,
          uint32_t    range,
          uint32_t    extra)
{
    clock_t     start;
    pthread_t   threads[num_threads];
    test_info_t info[num_threads];
    uint32_t    i;

    atomic_store(&test_func, NULL);
    atomic_store(&mmm_nexttid, 0); // Reset thread ids.

    for (i = 0; i < num_threads; i++) {
        info[i].tid   = i;
        info[i].dict  = dict;
        info[i].range = range;
        info[i].iters = iters / num_threads;
        info[i].extra = extra;
        pthread_create(&threads[i], NULL, start_one_thread, &info[i]);
    }

    start = clock();
    atomic_store(&test_func, func); // Start the threads.

    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    return clock() - start;
}

static bool
functionality_test(test_func_t func,
                   uint32_t    iters,
                   uint32_t    num_threads,
                   lowhat_t   *dict,
                   uint32_t    range,
                   uint32_t    extra)
{
    pthread_t   threads[num_threads];
    test_info_t info[num_threads];
    uint32_t    i;
    uint64_t    res;

    atomic_store(&test_func, NULL);
    atomic_store(&mmm_nexttid, 0); // Reset thread ids.

    for (i = 0; i < num_threads; i++) {
        info[i].tid   = i;
        info[i].dict  = dict;
        info[i].range = range;
        info[i].iters = iters / num_threads;
        info[i].extra = extra;
        pthread_create(&threads[i], NULL, start_one_thread, &info[i]);
    }

    atomic_store(&test_func, func);
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], (void *)&res);
        if (!res) {
            return false;
        }
    }

    return true;
}

char *dict_names[] = {"none", "lowhat1", "lowhat2", "lowhat0", NULL};

static void
run_one_time_test(char               *name,
                  test_func_t         func,
                  uint32_t            iters,
                  lowhat_table_type_t type,
                  uint32_t            range,
                  uint32_t            thread_count,
                  uint32_t            extra)
{
    lowhat_t *dict = NULL;
    uint32_t  ticks;
#ifdef LOWHAT_MMMALLOC_CTRS
    uint64_t start_allocs = atomic_load(&mmm_alloc_ctr);
    uint64_t start_frees  = atomic_load(&mmm_free_ctr);
    uint64_t diff_allocs;
    uint64_t diff_frees;
#endif

    if (strcmp(name, "rand()")) {
        dict = lowhat_new(type);
    }

    if (extra) {
        fprintf(stderr,
                "[%10s:%10s: t=%4d, r=%5d x=%4d]\t",
                name,
                dict_names[type],
                thread_count,
                range,
                extra);
    }
    else {
        fprintf(stderr,
                "[%10s:%10s: t=%4d, r=%5d]\t",
                name,
                dict_names[type],
                thread_count,
                range);
    }
    fflush(stderr);
    ticks = time_test(func, iters, thread_count, dict, range, extra);
    fprintf(stderr, "%d clocks, %d c/i\n", ticks, ticks / iters);

    if (strcmp(name, "rand()")) {
        lowhat_delete(dict);
    }

#ifdef LOWHAT_MMMALLOC_CTRS
    diff_allocs = atomic_load(&mmm_alloc_ctr) - start_allocs;
    diff_frees  = atomic_load(&mmm_free_ctr) - start_frees;

    if (diff_allocs != diff_frees) {
        printf("%llu leaks, %02llu%% of %llu allocs\n",
               diff_allocs - diff_frees,
               ((diff_allocs - diff_frees) * 100) / diff_allocs,
               diff_allocs);
    }

#endif
}

static void
run_one_func_test(char               *name,
                  test_func_t         func,
                  uint32_t            iters,
                  lowhat_table_type_t type,
                  uint32_t            range,
                  uint32_t            thread_count,
                  uint32_t            extra)
{
    lowhat_t *dict = lowhat_new(type);
    bool      ret;

    if (extra) {
        fprintf(stderr,
                "[%10s:%10s: t=%4d, r=%5d x=%4d]\t",
                name,
                dict_names[type],
                thread_count,
                range,
                extra);
    }
    else {
        fprintf(stderr,
                "[%10s:%10s: t=%4d, r=%5d]\t",
                name,
                dict_names[type],
                thread_count,
                range);
    }
    fflush(stderr);
    ret = functionality_test(func, iters, thread_count, dict, range, extra);
    if (ret) {
        fprintf(stderr, "pass\n");
    }
    else {
        fprintf(stderr, "FAIL\n");
    }

    lowhat_delete(dict);
}

static void
run_time_test(char                *name,
              test_func_t          func,
              uint32_t             iters,
              lowhat_table_type_t *types,
              uint32_t            *ranges,
              uint32_t            *tcounts,
              uint32_t            *extra)
{
    uint32_t dict_ix = 0;
    uint32_t range_ix;
    uint32_t tcount_ix;
    uint32_t extra_ix;

    while (types[dict_ix] != LOWHAT_NONE) {
        range_ix = 0;
        while (ranges[range_ix]) {
            tcount_ix = 0;
            while (tcounts[tcount_ix]) {
                if (extra) {
                    extra_ix = 0;
                    while (extra[extra_ix]) {
                        run_one_time_test(name,
                                          func,
                                          iters,
                                          types[dict_ix],
                                          ranges[range_ix],
                                          tcounts[tcount_ix],
                                          extra[extra_ix]);

                        extra_ix++;
                    }
                }
                else {
                    run_one_time_test(name,
                                      func,
                                      iters,
                                      types[dict_ix],
                                      ranges[range_ix],
                                      tcounts[tcount_ix],
                                      0);
                }
                tcount_ix++;
            }
            range_ix++;
        }
        dict_ix++;
    }
}

static void
run_func_test(char                *name,
              test_func_t          func,
              uint32_t             iters,
              lowhat_table_type_t *types,
              uint32_t            *ranges,
              uint32_t            *tcounts,
              uint32_t            *extra)
{
    uint32_t dict_ix = 0;
    uint32_t range_ix;
    uint32_t tcount_ix;
    uint32_t extra_ix;

    while (types[dict_ix] != LOWHAT_NONE) {
        range_ix = 0;
        while (ranges[range_ix]) {
            tcount_ix = 0;
            while (tcounts[tcount_ix]) {
                if (extra) {
                    extra_ix = 0;
                    while (extra[extra_ix]) {
                        run_one_func_test(name,
                                          func,
                                          iters,
                                          types[dict_ix],
                                          ranges[range_ix],
                                          tcounts[tcount_ix],
                                          extra[extra_ix]);

                        extra_ix++;
                    }
                }
                else {
                    run_one_func_test(name,
                                      func,
                                      iters,
                                      types[dict_ix],
                                      ranges[range_ix],
                                      tcounts[tcount_ix],
                                      0);
                }
                tcount_ix++;
            }
            range_ix++;
        }
        dict_ix++;
    }
}

// Actual tests below here.

// [ basic ]
// 1) Have one thread add all the key / value pairs (where key = value).
// 2) delete the top half.
// 3) Run get on all items, to make sure only the expected items
//    are there.
// Ignores the # of iterations, only the range.

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
            return false;
        }
    }
    for (; i < info->range; i++) {
        if (test_get(info->dict, i + 1) != i + 1) {
            return false;
        }
    }

    return true;
}

// [ ordering ]
//
// Add n items in numerical order, delete the first half, reinsert all
// the items, and then make sure the ordering is right in our
// iterator.

bool
test_ordering(test_info_t *info)
{
    uint64_t       i;
    uint64_t       n;
    uint32_t       k;
    uint32_t       v;
    lowhat_view_t *view;

    for (i = 0; i < info->range; i++) {
        test_put(info->dict, i + 1, i + 1);
    }
    for (i = 0; i < (info->range / 2); i++) {
        test_remove(info->dict, i + 1);
    }

    for (i = 0; i < info->range; i++) {
        test_put(info->dict, i + 1, i + 1);
    }

    view = test_view(info->dict, &n);

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

// [ parallel ]
// [ ||-large ]
//
// Have each thread attempt to set every value in the range,
// then check to make sure the items are all correct.
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
            printf("Is LOWHAT_TEST_MAX_KEYS high enough?\n");
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

uint32_t            basic_sizes[]   = {10, 100, 1000, 10000, 0};
uint32_t            large_sizes[]   = {100000, 1000000, 0};
uint32_t            shrug_sizes[]   = {1, 0};
uint32_t            one_thread[]    = {1, 0};
uint32_t            basic_threads[] = {1, 4, 10, 20, 0};
uint32_t            del_rate[]      = {100, 10, 3, 0};
lowhat_table_type_t all_dicts[]     = {LOWHAT_1, LOWHAT_NONE};

int
main(int argc, char *argv[])
{
    test_init();

    run_func_test("basic",
                  test_basic,
                  1,
                  all_dicts,
                  basic_sizes,
                  one_thread,
                  0);
    run_func_test("ordering",
                  test_basic,
                  1,
                  all_dicts,
                  basic_sizes,
                  one_thread,
                  0);
    run_func_test("parallel",
                  test_parallel,
                  10,
                  all_dicts,
                  basic_sizes,
                  basic_threads,
                  0);
    run_time_test("rand()",
                  test_rand_speed,
                  100000,
                  all_dicts,
                  shrug_sizes,
                  basic_threads,
                  0);
    run_time_test("insert",
                  test_insert_speed,
                  1000000,
                  all_dicts,
                  basic_sizes,
                  basic_threads,
                  0);
    run_time_test("writes",
                  test_write_speed,
                  1000000,
                  all_dicts,
                  basic_sizes,
                  basic_threads,
                  del_rate);
    run_func_test("||-large",
                  test_parallel,
                  1,
                  all_dicts,
                  large_sizes,
                  basic_threads,
                  0);

#ifdef LOWHAT_MMMALLOC_CTRS
    printf("allocs: %llu\n", atomic_load(&mmm_alloc_ctr));
    printf("frees:  %llu\n", atomic_load(&mmm_free_ctr));
#endif
}
