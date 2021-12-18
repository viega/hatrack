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
testhat_put(testhat_t      *self,
            hatrack_hash_t *hv,
            void           *item,
            bool            ifempty,
            bool           *found)
{
    return (*self->vtable.put)(self->htable, hv, item, ifempty, found);
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
