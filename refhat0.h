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
 *  Name:           refhat0.h
 *  Description:    A reference hashtable that only works single-threaded.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __REFHAT0_H__
#define __REFHAT0_H__

#include "hatrack_common.h"

// clang-format off
typedef struct {
    hatrack_hash_t hv;
    void          *item;
    bool           deleted;
    uint64_t       epoch;
} refhat0_bucket_t;

typedef struct {
    alignas(8)
    uint64_t          last_slot;
    uint64_t          threshold;
    uint64_t          used_count;
    uint64_t          item_count;
    refhat0_bucket_t *buckets;
    uint64_t          next_epoch;
} refhat0_t;

void            refhat0_init   (refhat0_t *);
void           *refhat0_get    (refhat0_t *, hatrack_hash_t *, bool *);
void           *refhat0_put    (refhat0_t *, hatrack_hash_t *, void *, bool *);
void           *refhat0_replace(refhat0_t *, hatrack_hash_t *, void *, bool *);
bool            refhat0_add    (refhat0_t *, hatrack_hash_t *, void *);
void           *refhat0_remove (refhat0_t *, hatrack_hash_t *, bool *);
void            refhat0_delete (refhat0_t *);
uint64_t        refhat0_len    (refhat0_t *);
hatrack_view_t *refhat0_view   (refhat0_t *, uint64_t *, bool);

//clang-format on

#endif
