/* Copyright © 2021-2022 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           testhat.c
 *
 *  Description:    A wrapper to provide a single interface to all
 *                  the implementations, for ease of testing.
 *
 *                  Note that this interface isn't particularly high
 *                  level:
 *
 *                  1) You need to do the hashing yourself, and pass in
 *                     the value.
 *
 *                  2) You just pass in a pointer to an "item" that's
 *                     expected to represent the key/item pair.
 *
 *                  3) You need to do your own memory management for
 *                     the key / item pairs, if appropriate.
 *
 *                  Most of the implementation is inlined in the header
 *                  file, since it merely dispatches to individual
 *                  implementations.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __TESTHAT_H__
#define __TESTHAT_H__

// Pull in the various implementations.
#include <hatrack/hatvtable.h>
#include <hatrack/refhat.h>
#include <hatrack/duncecap.h>
#include <hatrack/swimcap.h>
#include <hatrack/newshat.h>
#include <hatrack/ballcap.h>
#include <hatrack/hihat.h>
#include <hatrack/oldhat.h>
#include <hatrack/tiara.h>
#include <hatrack/lohat.h>
#include <hatrack/lohat-a.h>
#include <hatrack/witchhat.h>
#include <hatrack/woolhat.h>
#include <hatrack/tophat.h>
#include <hatrack/crown.h>

typedef struct {
    hatrack_vtable_t vtable;
    void            *htable;
} testhat_t;

static inline void *
testhat_get(testhat_t *self, hatrack_hash_t hv, bool *found)
{
    return (*self->vtable.get)(self->htable, hv, found);
}

static inline void *
testhat_put(testhat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    return (*self->vtable.put)(self->htable, hv, item, found);
}

static inline void *
testhat_replace(testhat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    return (*self->vtable.replace)(self->htable, hv, item, found);
}

static inline bool
testhat_add(testhat_t *self, hatrack_hash_t hv, void *item)
{
    return (*self->vtable.add)(self->htable, hv, item);
}

static inline void *
testhat_remove(testhat_t *self, hatrack_hash_t hv, bool *found)
{
    return (*self->vtable.remove)(self->htable, hv, found);
}

static inline void
testhat_delete(testhat_t *self)
{
    (*self->vtable.delete)(self->htable);
    free(self);

    return;
}

static inline uint64_t
testhat_len(testhat_t *self)
{
    return (*self->vtable.len)(self->htable);
}

static inline hatrack_view_t *
testhat_view(testhat_t *self, uint64_t *num_items, bool sort)
{
    return (*self->vtable.view)(self->htable, num_items, sort);
}

// Convince the type system we're not crazy.
typedef void *(*get64f)(void *, uint64_t);
typedef void *(*put64f)(void *, uint64_t, void *);
typedef void *(*rep64f)(void *, uint64_t, void *);
typedef bool (*add64f)(void *, uint64_t, void *);
typedef void *(*rm64f)(void *, uint64_t);

static inline void *
testhat_get64(testhat_t *self, hatrack_hash_t *hv)
{
    return (*(get64f)self->vtable.get)(self->htable, *(uint64_t *)hv);
}

static inline void *
testhat_put64(testhat_t *self, hatrack_hash_t *hv, void *item)
{
    return (*(put64f)self->vtable.put)(self->htable, *(uint64_t *)hv, item);
}

static inline void *
testhat_replace64(testhat_t *self, hatrack_hash_t *hv, void *item)
{
    return (*(rep64f)self->vtable.replace)(self->htable, *(uint64_t *)hv, item);
}

static inline bool
testhat_add64(testhat_t *self, hatrack_hash_t *hv, void *item)
{
    return (*(add64f)self->vtable.add)(self->htable, *(uint64_t *)hv, item);
}

static inline void *
testhat_remove64(testhat_t *self, hatrack_hash_t *hv)
{
    return (*(rm64f)self->vtable.remove)(self->htable, *(uint64_t *)hv);
}

static inline void
testhat_delete64(testhat_t *self)
{
    (*self->vtable.delete)(self->htable);
    free(self);

    return;
}

static inline uint64_t
testhat_len64(testhat_t *self)
{
    return (*self->vtable.len)(self->htable);
}

static inline hatrack_view_t *
testhat_view64(testhat_t *self, uint64_t *num_items, bool sort)
{
    return (*self->vtable.view)(self->htable, num_items, sort);
}

typedef struct {
    char        *name;
    unsigned int read_pct;
    unsigned int put_pct;
    unsigned int add_pct;
    unsigned int replace_pct;
    unsigned int remove_pct;
    unsigned int view_pct;
    unsigned int sort_pct;
    unsigned int start_sz;
    unsigned int prefill_pct;
    unsigned int key_range;
    unsigned int num_threads;
    unsigned int total_ops;
    bool         shuffle;
    __int128_t   seed;
    char       **hat_list;
} benchmark_t;

typedef struct {
    bool        run_default_tests;
    bool        run_func_tests;
    bool        run_custom_test;
    benchmark_t custom;
    char       *hat_list[];
} config_info_t;

typedef struct {
    char             *name;
    hatrack_vtable_t *vtable;
    size_t            size;
    int               hashbytes;
    bool              threadsafe;
} alg_info_t;

extern _Atomic uint64_t mmm_nexttid;
extern hatrack_hash_t  *precomputed_hashes;

// clang-format off
// from rand.c
void           test_init_rand        (__int128_t);
uint32_t       test_rand             (void);
void           test_shuffle_array    (void *, uint32_t, uint32_t);

// from testhat.c: basic algorithm registering / info / instantiation.
uint32_t       algorithm_register    (char *, hatrack_vtable_t *, size_t, int,
				      bool);
uint32_t       get_num_algorithms    (void);
alg_info_t    *get_all_algorithm_info(void);
int32_t        algorithm_id_by_name  (char *);
alg_info_t    *algorithm_info        (char *);
testhat_t     *testhat_new           (char *);
testhat_t     *testhat_new_size      (char *, char);

// functional.c -- functional tests, off by default.
void           run_functional_tests  (config_info_t *);

// default.c -- default tests.
void           run_default_tests     (config_info_t *);

// performance.c -- run one performance test. Custom tests are
// kicked off from the main() function in test.c
void           run_performance_test  (benchmark_t *);

// config.c -- Command-line argument parsing.
config_info_t *parse_args            (int, char *[]);

// Items kept in test.c.
void           precompute_hashes     (uint64_t);

typedef union {
    struct {
        uint32_t key;
        uint32_t value;
    } s;
    uint64_t i;
} test_item;

static inline uint32_t
test_get(testhat_t *self, uint32_t key)
{
    test_item item;

    item.i = (uint64_t)testhat_get(self, precomputed_hashes[key], NULL);

    return item.s.value;
}

static inline void
test_put(testhat_t *self, uint32_t key, uint32_t value)
{
    test_item item;
    
    item.s.key   = key;
    item.s.value = value;
    
    testhat_put(self, precomputed_hashes[key], (void *)item.i, NULL);
    
    return;
}

static inline void
test_replace(testhat_t *self, uint32_t key, uint32_t value)
{
    test_item item;
    
    item.s.key   = key;
    item.s.value = value;
    
    testhat_replace(self, precomputed_hashes[key], (void *)item.i, NULL);
    
    return;
}

static inline bool
test_add(testhat_t *self, uint32_t key, uint32_t value)
{
    test_item item;
    
    item.s.key   = key;
    item.s.value = value;
    
    return testhat_add(self, precomputed_hashes[key], (void *)item.i);
}

static inline void
test_remove(testhat_t *self, uint32_t key)
{
    testhat_remove(self, precomputed_hashes[key], NULL);

    return;
}

static inline hatrack_view_t *
test_view(testhat_t *self, uint64_t *n, bool sort)
{
    return testhat_view(self, n, sort);
}

static inline uint32_t
test_get64(testhat_t *self, uint32_t key)
{
    uint64_t n;

    n = (uint64_t)testhat_get64(self, &precomputed_hashes[key]);

    return n >> 3;
}

static inline void
test_put64(testhat_t *self, uint32_t key, uint32_t value)
{
    uint64_t n;

    n = value << 3;
    testhat_put64(self, &precomputed_hashes[key], (void *)n);
    
    return;
}

static inline void
test_replace64(testhat_t *self, uint32_t key, uint32_t value)
{
    uint64_t n;

    n = value << 3;
    
    testhat_replace64(self, &precomputed_hashes[key], (void *)n);
    
    return;
}

static inline bool
test_add64(testhat_t *self, uint32_t key, uint32_t value)
{
    uint64_t n;

    n = value << 3;
    
    return testhat_add64(self, &precomputed_hashes[key], (void *)n);
}

static inline void
test_remove64(testhat_t *self, uint32_t key)
{
    testhat_remove64(self, &precomputed_hashes[key]);

    return;
}

static inline hatrack_view_t *
test_view64(testhat_t *self, uint64_t *n, bool sort)
{
    return testhat_view64(self, n, sort);
}

#endif
