/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           hihat1.h
 *  Description:
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __HIHAT1_H__
#define __HIHAT1_H__

#include "lowhat_common.h"

// The info field consists of the following:
//
// - The high bit signals whether this is a used item or not.
// - The next big signals whether we're migrating.
// - The third most significant bit signals that migration is done.
// - The fourth signals that the bucket is not only empty, but deleted.
//
// - The rest is currently an structure-specific epoch counter // to
//   help with sorting (note there are no consistency guarantees the
//   way there are with lowhat).

typedef struct {
    void    *item;
    uint64_t info;
} hihat1_record_t;

typedef struct {
    _Atomic lowhat_hash_t   hv;
    _Atomic hihat1_record_t record;
} hihat1_bucket_t;

enum : uint64_t
{
    HIHAT_F_USED   = 0x8000000000000000,
    HIHAT_F_MOVING = 0x4000000000000000,
    HIHAT_F_MOVED  = 0x2000000000000000,
    HIHAT_F_RMD    = 0x1000000000000000,
    HIHAT_F_MASK   = 0x8fffffffffffffff
};

typedef struct hihat1_store_st hihat1_store_t;

struct hihat1_store_st {
    uint64_t                  last_slot;
    uint64_t                  threshold;
    _Atomic uint64_t          used_count;
    _Atomic uint64_t          del_count;
    _Atomic(hihat1_store_t *) store_next;
    hihat1_bucket_t           buckets[];
};

typedef struct {
    lowhat_vtable_t           vtable;
    uint64_t                  epoch;
    _Atomic(hihat1_store_t *) store_current;
} hihat1_t;

// clang-format off
void           hihat1_init(hihat1_t *);
void          *hihat1_get(hihat1_t *, lowhat_hash_t *, bool *);
void          *hihat1_put(hihat1_t *, lowhat_hash_t *, void *, bool, bool *);
void          *hihat1_remove(hihat1_t *, lowhat_hash_t *, bool *);
void           hihat1_delete(hihat1_t *);
uint64_t       hihat1_len(hihat1_t *);
lowhat_view_t *hihat1_view(hihat1_t *, uint64_t *);
// clang-format on

extern const lowhat_vtable_t hihat1_vtable;

#endif
