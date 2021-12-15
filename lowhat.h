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

#ifndef __LOWHAT_H__
#define __LOWHAT_H__

// Pull in the various implementations.

#include "lowhat1.h"

typedef enum : uint64_t
{
    LOWHAT_NONE = 0,
    LOWHAT_1, // Keeps semi-ordered buckets and sorts quickly when needed.
    LOWHAT_2, // Keeps mostly-ordered buckets, sorting quickest when needed.
    LOWHAT_0  // Keeps unordered buckets, slowest sorts.
} lowhat_table_type_t;

lowhat_t *lowhat_new(lowhat_table_type_t);

static inline void *
lowhat_get(lowhat_t *self, lowhat_hash_t *hv, bool *found)
{
    return (*self->vtable.get)(self, hv, found);
}

static inline void *
lowhat_put(lowhat_t      *self,
           lowhat_hash_t *hv,
           void          *item,
           bool           ifempty,
           bool          *found)
{
    return (*self->vtable.put)(self, hv, item, ifempty, found);
}

static inline void *
lowhat_remove(lowhat_t *self, lowhat_hash_t *hv, bool *found)
{
    return (*self->vtable.remove)(self, hv, found);
}

static inline void
lowhat_delete(lowhat_t *self)
{
    (*self->vtable.delete)(self);

    return;
}

static inline uint64_t
lowhat_len(lowhat_t *self)
{
    return (*self->vtable.len)(self);
}

static inline lowhat_view_t *
lowhat_view(lowhat_t *self, uint64_t *num_items)
{
    return (*self->vtable.view)(self, num_items);
}

#endif
