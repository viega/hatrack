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
    LOWHAT_NONE = 0,
    HIHAT_1,  // Not linearizable.
    LOWHAT_0, // Keeps unordered buckets, slowest sorts.
    LOWHAT_1, // Keeps semi-ordered buckets and sorts quickly when needed.
    LOWHAT_2, // Keeps mostly-ordered buckets, sorting quickest when needed.
    REFHAT_0  // Unordered buckets, single-threaded only.
} lowhat_table_type_t;

void *testhat_new(lowhat_table_type_t);

static inline void *
testhat_get(void *self, lowhat_hash_t *hv, bool *found)
{
    lowhat_vtable_t *table = (lowhat_vtable_t *)self;

    return (*table->get)(self, hv, found);
}

static inline void *
testhat_put(void *self, lowhat_hash_t *hv, void *item, bool ifempty, bool *found)
{
    lowhat_vtable_t *table = (lowhat_vtable_t *)self;

    return (*table->put)(self, hv, item, ifempty, found);
}

static inline void *
testhat_remove(void *self, lowhat_hash_t *hv, bool *found)
{
    lowhat_vtable_t *table = (lowhat_vtable_t *)self;

    return (*table->remove)(self, hv, found);
}

static inline void
testhat_delete(void *self)
{
    lowhat_vtable_t *table = (lowhat_vtable_t *)self;

    (*table->delete)(self);

    return;
}

static inline uint64_t
testhat_len(void *self)
{
    lowhat_vtable_t *table = (lowhat_vtable_t *)self;

    return (*table->len)(self);
}

static inline lowhat_view_t *
testhat_view(void *self, uint64_t *num_items)
{
    lowhat_vtable_t *table = (lowhat_vtable_t *)self;

    return (*table->view)(self, num_items);
}

#endif
