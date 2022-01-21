/* Copyright © 2021-2022 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           testhat.c
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

testhat_t *testhat_new(char *);

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

typedef struct {
    bool    run_func_tests;
    bool    run_stress_tests;
    bool    run_throughput_tests;
    int64_t seed;
    char   *hat_list[];
} config_info_t;

typedef struct {
    uint32_t   tid;
    char      *type;
    testhat_t *dict;
    uint32_t   range; // Specifies range of keys and values, 0 - range-1
    uint32_t   iters; // Number of times to run the test;
    uint32_t   extra;
} test_info_t;

typedef union {
    struct {
        uint32_t key;
        uint32_t value;
    } s;
    uint64_t i;
} test_item;

typedef bool (*test_func_t)(test_info_t *);

extern _Atomic test_func_t test_func;
extern _Atomic uint64_t    mmm_nexttid;
extern hatrack_hash_t      precomputed_hashes[HATRACK_TEST_MAX_KEYS];
extern uint32_t            basic_sizes[];
extern uint32_t            sort_sizes[];
extern uint32_t            large_sizes[];
extern uint32_t            shrug_sizes[];
extern uint32_t            small_size[];
extern uint32_t            one_thread[];
extern uint32_t            mt_only_threads[];
extern uint32_t            basic_threads[];
extern uint32_t            del_rate[];
extern uint32_t            write_rates[];

// clang-format off
void           test_init_rand       (void);
void           test_thread_init_rand(void);
uint32_t       test_rand            (void);
config_info_t *parse_args           (int, char *[]);
void           run_functional_tests (config_info_t *);
void           run_stress_tests     (config_info_t *);
void          *start_one_thread     (void *);

#ifdef HATRACK_DEBUG
void           print_config         (config_info_t *);
#endif

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


#endif
