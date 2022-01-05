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

#include "hatvtable.h"
#include "refhat.h"
#include "duncecap.h"
#include "swimcap.h"
#include "newshat.h"
#include "ballcap.h"
#include "hihat.h"
#include "oldhat.h"
#include "lohat.h"
#include "lohat-a.h"
#include "lohat-b.h"
#include "witchhat.h"
#include "woolhat.h"
#include "tophat.h"

typedef struct {
    hatrack_vtable_t vtable;
    void            *htable;
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

static inline void *
testhat_replace(testhat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    return (*self->vtable.replace)(self->htable, hv, item, found);
}

static inline bool
testhat_add(testhat_t *self, hatrack_hash_t *hv, void *item)
{
    return (*self->vtable.add)(self->htable, hv, item);
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
testhat_view(testhat_t *self, uint64_t *num_items, bool sort)
{
    return (*self->vtable.view)(self->htable, num_items, sort);
}

#endif
