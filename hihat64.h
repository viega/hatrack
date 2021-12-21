/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           hihat64.h
 *  Description:
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __HIHAT64_H__
#define __HIHAT64_H__

#include "hatrack_common.h"

typedef struct {
    void *item;
} hihat64_record_t;

typedef struct {
    alignas(32) _Atomic(hihat64_record_t *) record;
    _Atomic uint64_t h1;
#ifdef HIHAT64_USE_FULL_HASH
    _Atomic uint64_t h2;
#endif
} hihat64_bucket_t;

enum : uint64_t
{
    HIHAT64_F_MOVING = 0x0000000000000001,
    HIHAT64_F_MOVED  = 0x0000000000000002,
    HIHAT64_F_USED   = 0x0000000000000004,
    HIHAT64_F_MASK   = 0xfffffffffffffff8
};

typedef struct hihat64_store_st hihat64_store_t;

struct hihat64_store_st {
    alignas(32) uint64_t last_slot;
    uint64_t                   threshold;
    _Atomic uint64_t           used_count;
    _Atomic uint64_t           del_count;
    _Atomic(hihat64_store_t *) store_next;
    hihat64_bucket_t           buckets[];
};

typedef struct {
    alignas(32) _Atomic(hihat64_store_t *) store_current;
} hihat64_t;

// clang-format off

void            hihat64_init(hihat64_t *);
void           *hihat64_get(hihat64_t *, hatrack_hash_t *, bool *);
void           *hihat64_put(hihat64_t *, hatrack_hash_t *, void *, bool,
			    bool *);
void           *hihat64_remove(hihat64_t *, hatrack_hash_t *, bool *);
void            hihat64_delete(hihat64_t *);
uint64_t        hihat64_len(hihat64_t *);
hatrack_view_t *hihat64_view(hihat64_t *, uint64_t *);

// clang-format on

#endif
