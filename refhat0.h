/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           refhat0.h
 *  Description:    A reference hashtable that only works single-threaded.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __REFHAT0_H__
#define __REFHAT0_H__

#include "lowhat_common.h"

typedef struct {
    lowhat_hash_t hv;
    void         *item;
    bool          deleted;
#ifndef LOWHAT_DONT_SORT
    uint64_t epoch;
#endif
} refhat0_bucket_t;

typedef struct {
    lowhat_vtable_t   vtable;
    uint64_t          last_slot;
    uint64_t          threshold;
    uint64_t          used_count;
    uint64_t          item_count;
    refhat0_bucket_t *buckets;
#ifndef LOWHAT_DONT_SORT
    uint64_t next_epoch;
#endif
} refhat0_t;

extern const lowhat_vtable_t refhat0_vtable;

void           refhat0_init(refhat0_t *);
void          *refhat0_get(refhat0_t *, lowhat_hash_t *, bool *);
void          *refhat0_put(refhat0_t *, lowhat_hash_t *, void *, bool *);
bool           refhat0_put_if_empty(refhat0_t *, lowhat_hash_t *, void *);
void          *refhat0_remove(refhat0_t *, lowhat_hash_t *, bool *);
void           refhat0_delete(refhat0_t *);
uint64_t       refhat0_len(refhat0_t *);
lowhat_view_t *refhat0_view(refhat0_t *, uint64_t *);

#endif
