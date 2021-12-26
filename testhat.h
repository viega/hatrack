/* Copyright © 2021 John Viega
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

#include "refhat0.h"
#include "swimcap.h"
#include "swimcap2.h"
#include "newshat.h"
#include "hihat1.h"
#include "hihat64.h"
#include "lohat0.h"
#include "lohat1.h"
#include "lohat2.h"
#include "witchhat.h"

/* For testing, we keep vtables of the operations to make it easier to
 * switch between different algorithms for testing. These types are
 * aliases for the methods that we expect to see.
 *
 * We use void * in the first parameter to all of these methods to
 * stand in for an arbitrary pointer to a hash table.
 */

// clang-format off
typedef void            (*hatrack_init_func)   (void *);
typedef void *          (*hatrack_get_func)    (void *, hatrack_hash_t *,
						bool *);
typedef void *          (*hatrack_put_func)    (void *, hatrack_hash_t *,
						void *, bool *);
typedef bool            (*hatrack_putcond_func)(void *, hatrack_hash_t *,
						void *);
typedef void *          (*hatrack_remove_func) (void *, hatrack_hash_t *,
						bool *);
typedef void            (*hatrack_delete_func) (void *);
typedef uint64_t        (*hatrack_len_func)    (void *);
typedef hatrack_view_t *(*hatrack_view_func)   (void *, uint64_t *);

typedef struct {
    hatrack_init_func    init;
    hatrack_get_func     get;
    hatrack_put_func     put;
    hatrack_putcond_func putcond;
    hatrack_remove_func  remove;
    hatrack_delete_func  delete;
    hatrack_len_func     len;
    hatrack_view_func    view;
} hatrack_vtable_t;


typedef struct {
    hatrack_vtable_t     vtable;
    void                *htable;
} testhat_t;

testhat_t *testhat_new(char *);

static inline void *
testhat_get(testhat_t *self, hatrack_hash_t *hv, bool *found)
{
    return (*self->vtable.get)(self->htable, hv, found);
}

static inline void *
testhat_put(testhat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    return (*self->vtable.put)(self->htable, hv, item, found);        
}

static inline bool
testhat_put_if_empty(testhat_t *self, hatrack_hash_t *hv, void *item)
{
    return (*self->vtable.putcond)(self->htable, hv, item);
}

static inline void *
testhat_remove(testhat_t *self, hatrack_hash_t *hv, bool *found)
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
testhat_view(testhat_t *self, uint64_t *num_items)
{
    return (*self->vtable.view)(self->htable, num_items);
}

#endif
