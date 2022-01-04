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
 *  Name:           oldhat.h
 *  Description:    Half-Interesting HAsh Table w/ single-word CAS only.
 *
 *                  This is somewhat like hihat1, except that we do
 *                  not use a double-word Compare-And-Swap, as it is
 *                  not natively available on all architectures (C11
 *                  atomics imitate this with locks, if not available,
 *                  but may warn about slow performance).
 *
 *                  This constraint does lead to some real differences
 *                  in the algorithm, particularly:
 *
 *               1) Since we can't swap in a record consisting of two
 *                  64-bit values (one value being an item, and the
 *                  other being an epoch and flags), we instead swap
 *                  in a pointer to a record.
 *
 *                  We use mmm to allocate these records, and use the
 *                  global mmm "epoch" value for the epoch value,
 *                  which gets placed in a hidden mmm header (see
 *                  mmm.h and mmm.c).  And, we then steal bits from
 *                  the pointer to the record to track status (see
 *                  the flag enumeration below).
 *
 *                  This, of course, means that we need to worry about
 *                  memory management of these records, now.
 *
 *               2) We need to handle hash comparisons differently. We
 *                  can either assume that 64 bits of hash value is
 *                  enough for identity, in which case, we only use
 *                  the first 64 bits of any hatrack_hash_t passed in,
 *                  or we write out the hash value in two chunks.
 *
 *
 *                  Beyond those items, the algorithms are otherwise
 *                  similar.  In our comments, we try to limit
 *                  ourselves to the differences between the
 *                  algorithms, so it is good to start with hihat1
 *                  before attempting to wrap your head around this
 *                  version.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __OLDHAT_H__
#define __OLDHAT_H__

#include "hatrack_common.h"

typedef struct {
    alignas(8) hatrack_hash_t hv;
    void *item;
    bool  moving;
    bool  moved;
    bool  used;
} oldhat_record_t;

typedef struct oldhat_store_st oldhat_store_t;

struct oldhat_store_st {
    alignas(16) uint64_t last_slot;
    uint64_t                  threshold;
    _Atomic uint64_t          used_count;
    _Atomic(oldhat_store_t *) store_next;
    alignas(16) _Atomic(oldhat_record_t *) buckets[];
};

typedef struct {
    alignas(16) _Atomic(oldhat_store_t *) store_current;
    _Atomic uint64_t item_count;

} oldhat_t;

void            oldhat_init(oldhat_t *);
void           *oldhat_get(oldhat_t *, hatrack_hash_t *, bool *);
void           *oldhat_put(oldhat_t *, hatrack_hash_t *, void *, bool *);
void           *oldhat_replace(oldhat_t *, hatrack_hash_t *, void *, bool *);
bool            oldhat_add(oldhat_t *, hatrack_hash_t *, void *);
void           *oldhat_remove(oldhat_t *, hatrack_hash_t *, bool *);
void            oldhat_delete(oldhat_t *);
uint64_t        oldhat_len(oldhat_t *);
hatrack_view_t *oldhat_view(oldhat_t *, uint64_t *, bool);

#endif
