/*
 * Copyright © 2021 John Viega
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *  Name:           hihat1.h
 *  Description:    Half-Interesting HAsh Table.
 *                  This is a lock-free hash table, with wait-free
 *                  read operations. This table allows for you to
 *                  recover the approximate order when getting a view,
 *                  but does not guarantee the consistency of that view.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __HIHAT1_H__
#define __HIHAT1_H__

#include "hatrack_common.h"

/* hihat1_record_t
 *
 * hihat records are atomically compare-and-swapped.
 *
 * The item field is the item passed to the hash table, usually
 * a key : value pair or some sort.
 *
 * The top two bits of the info field are used for carrying status
 * information about migrations. The rest is an epoch field, used for
 * sorting. If the epoch portion is zero, then it indicates the item
 * either has not been set, or has been deleted.
 *
 * Note that there are no consistency guarantees the way there are
 * with lohat.
 *
 * The high bit signals whether this is a used item or not (HIHAT_F_MOVING).
 * The next big signals whether we're migrating (HIHAT_F_MOVED).
 */
typedef struct {
    void    *item;
    uint64_t info;
} hihat1_record_t;

enum : uint64_t
{
    HIHAT_F_MOVING   = 0x8000000000000000,
    HIHAT_F_MOVED    = 0x4000000000000000,
    HIHAT_EPOCH_MASK = 0x3fffffffffffffff
};

typedef struct {
    _Atomic hatrack_hash_t  hv;
    _Atomic hihat1_record_t record;
} hihat1_bucket_t;

typedef struct hihat1_store_st hihat1_store_t;

// clang-format off
struct hihat1_store_st {
    alignas(8)
    uint64_t                   last_slot;
    uint64_t                   threshold;
    _Atomic uint64_t           used_count;
    _Atomic uint64_t           item_count;
    _Atomic(hihat1_store_t *)  store_next;
    alignas(16)
    hihat1_bucket_t            buckets[];
};

typedef struct {
    alignas(8)
    _Atomic(hihat1_store_t *) store_current;
    uint64_t                  epoch;
} hihat1_t;


void            hihat1_init    (hihat1_t *);
void           *hihat1_get     (hihat1_t *, hatrack_hash_t *, bool *);
void           *hihat1_put     (hihat1_t *, hatrack_hash_t *, void *, bool *);
void           *hihat1_replace (hihat1_t *, hatrack_hash_t *, void *, bool *);
bool            hihat1_add     (hihat1_t *, hatrack_hash_t *, void *);
void           *hihat1_remove  (hihat1_t *, hatrack_hash_t *, bool *);
void            hihat1_delete  (hihat1_t *);
uint64_t        hihat1_len     (hihat1_t *);
hatrack_view_t *hihat1_view    (hihat1_t *, uint64_t *, bool);

void            hihat1a_init   (hihat1_t *);
void           *hihat1a_get    (hihat1_t *, hatrack_hash_t *, bool *);
void           *hihat1a_put    (hihat1_t *, hatrack_hash_t *, void *, bool *);
void           *hihat1a_replace(hihat1_t *, hatrack_hash_t *, void *, bool *);
bool            hihat1a_add    (hihat1_t *, hatrack_hash_t *, void *);
void           *hihat1a_remove (hihat1_t *, hatrack_hash_t *, bool *);
void            hihat1a_delete (hihat1_t *);
uint64_t        hihat1a_len    (hihat1_t *);
hatrack_view_t *hihat1a_view   (hihat1_t *, uint64_t *, bool);

#endif
