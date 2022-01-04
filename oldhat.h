/*
 * Copyright © 2022 John Viega
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
 *  Name:           oldhat.c
 *  Description:    Old, Legacy, Dated Hardware-Acceptable Table
 *
 *                  This table stays away from 128-bit compare-and
 *                  swap operations.  It does so by keeping all bucket
 *                  information in a single structure, and only ever
 *                  CASing a pointer to said structure.
 *
 *                  The net result is we require a lot of dynamic
 *                  memory allocation.
 *
 *  Author:         John Viega, john@zork.org
 *
 * TODO: Add a cleanup handler to retire all the old store records.
 * TODO: Fix the hihat / witchhat expected_use = 0 assignment location.
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
