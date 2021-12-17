/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           lowhat.h
 *  Description:    Linearizeable, Ordered, Wait-free HAsh Table (LOWHAT)
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __TESTHAT_H__
#define __TESTHAT_H__

// Pull in the various implementations.

#include "refhat0.h"
#include "hihat1.h"
#include "lowhat0.h"
#include "lowhat1.h"
#include "lowhat2.h"

typedef enum : uint64_t
{
    HATRACK_NONE = 0,
    HIHAT_1,  // Not linearizable.
    LOWHAT_0, // Keeps unordered buckets, slowest sorts.
    LOWHAT_1, // Keeps semi-ordered buckets and sorts quickly when needed.
    LOWHAT_2, // Keeps mostly-ordered buckets, sorting quickest when needed.
    REFHAT_0  // Unordered buckets, single-threaded only.
} hatrack_table_type_t;

void *testhat_new(hatrack_table_type_t);

static inline void *
testhat_get(void *self, hatrack_hash_t *hv, bool *found)
{
    hatrack_vtable_t *table = (hatrack_vtable_t *)self;

    return (*table->get)(self, hv, found);
}

static inline void *
testhat_put(void           *self,
            hatrack_hash_t *hv,
            void           *item,
            bool            ifempty,
            bool           *found)
{
    hatrack_vtable_t *table = (hatrack_vtable_t *)self;

    return (*table->put)(self, hv, item, ifempty, found);
}

static inline void *
testhat_remove(void *self, hatrack_hash_t *hv, bool *found)
{
    hatrack_vtable_t *table = (hatrack_vtable_t *)self;

    return (*table->remove)(self, hv, found);
}

static inline void
testhat_delete(void *self)
{
    hatrack_vtable_t *table = (hatrack_vtable_t *)self;

    (*table->delete)(self);

    return;
}

static inline uint64_t
testhat_len(void *self)
{
    hatrack_vtable_t *table = (hatrack_vtable_t *)self;

    return (*table->len)(self);
}

static inline hatrack_view_t *
testhat_view(void *self, uint64_t *num_items)
{
    hatrack_vtable_t *table = (hatrack_vtable_t *)self;

    return (*table->view)(self, num_items);
}

#endif
