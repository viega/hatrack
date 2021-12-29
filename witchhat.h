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
 *  Name:           witchhat.h
 *  Description:    Waiting I Trully Cannot Handle
 *
 *                  This is a lock-free, and wait freehash table,
 *                  without consistency / full ordering.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __WITCHHAT_H__
#define __WITCHHAT_H__

#include "hatrack_common.h"

typedef struct {
    void    *item;
    uint64_t info;
} witchhat_record_t;

enum : uint64_t
{
    WITCHHAT_F_USED   = 0x8000000000000000,
    WITCHHAT_F_MOVING = 0x4000000000000000,
    WITCHHAT_F_MOVED  = 0x2000000000000000,
    WITCHHAT_F_RMD    = 0x1000000000000000,
    WITCHHAT_F_MASK   = 0x8fffffffffffffff
};

typedef struct {
    _Atomic hatrack_hash_t    hv;
    _Atomic witchhat_record_t record;
} witchhat_bucket_t;

typedef struct witchhat_store_st witchhat_store_t;

// clang-format off
struct witchhat_store_st {
    alignas(8)
    uint64_t                    last_slot;
    uint64_t                    threshold;
    _Atomic uint64_t            used_count;
    _Atomic uint64_t            del_count;
    _Atomic(witchhat_store_t *) store_next;
    alignas(16)
    witchhat_bucket_t           buckets[];
};

typedef struct {
    alignas(8)
    _Atomic(witchhat_store_t *) store_current;
    _Atomic uint64_t            help_needed;
            uint64_t            epoch;

} witchhat_t;


void            witchhat_init  (witchhat_t *);
void           *witchhat_get   (witchhat_t *, hatrack_hash_t *, bool *);
void           *witchhat_put   (witchhat_t *, hatrack_hash_t *, void *, bool *);
bool            witchhat_add   (witchhat_t *, hatrack_hash_t *, void *);
void           *witchhat_remove(witchhat_t *, hatrack_hash_t *, bool *);
void            witchhat_delete(witchhat_t *);
uint64_t        witchhat_len   (witchhat_t *);
hatrack_view_t *witchhat_view  (witchhat_t *, uint64_t *, bool);

#endif
