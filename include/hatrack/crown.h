/*
 * Copyright © 2021-2022 John Viega
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
 *  Name:           crown.h
 *  Description:    Crown Really Overcomplicates Witchhat Now
 *
 *                  Crown is a slight modification of witchhat-- it
 *                  changes the probing strategy for buckets.
 *
 *                  Refer to crown.c for implementation notes.
 *
 *  Author: John Viega, john@zork.org
 */

#ifndef __CROWN_H__
#define __CROWN_H__

#include <hatrack/hatrack_common.h>

#ifdef HATRACK_32_BIT_HOP_TABLE


#define CROWN_HOME_BIT  0x80000000

typedef uint32_t hop_t;

#define CLZ(n) __builtin_clzl(n)

#else

#define CROWN_HOME_BIT  0x8000000000000000

typedef uint64_t hop_t;
    
#define CLZ(n) __builtin_clzll(n)

#endif

typedef struct {
    void    *item;
    uint64_t info;
} crown_record_t;

enum64(crown_flag_t,
       CROWN_F_MOVING   = 0x8000000000000000,
       CROWN_F_MOVED    = 0x4000000000000000,
       CROWN_F_INITED   = 0x2000000000000000,            
       CROWN_EPOCH_MASK = 0x1fffffffffffffff);

typedef struct {
    _Atomic hatrack_hash_t hv;
    _Atomic crown_record_t record;
    
#ifdef HATRACK_32_BIT_HOP_TABLE
    _Atomic uint32_t       neighbor_map;
#else
    _Atomic uint64_t       neighbor_map;
#endif
} crown_bucket_t;

typedef struct crown_store_st crown_store_t;

// clang-format off
struct crown_store_st {
    alignas(8)
    uint64_t                 last_slot;
    uint64_t                 threshold;
    _Atomic uint64_t         used_count;    
    _Atomic(crown_store_t *) store_next;
    _Atomic bool             claimed;
    alignas(16)
    crown_bucket_t           buckets[];
};

typedef struct {
    alignas(8)
    _Atomic(crown_store_t *) store_current;
    _Atomic uint64_t         item_count;
    _Atomic uint64_t         help_needed;
            uint64_t         next_epoch;

} crown_t;


crown_t        *crown_new        (void);
crown_t        *crown_new_size   (char);
void            crown_init       (crown_t *);
void            crown_init_size  (crown_t *, char);
void            crown_cleanup    (crown_t *);
void            crown_delete     (crown_t *);
void           *crown_get        (crown_t *, hatrack_hash_t, bool *);
void           *crown_put        (crown_t *, hatrack_hash_t, void *, bool *);
void           *crown_replace    (crown_t *, hatrack_hash_t, void *, bool *);
bool            crown_add        (crown_t *, hatrack_hash_t, void *);
void           *crown_remove     (crown_t *, hatrack_hash_t, bool *);
uint64_t        crown_len        (crown_t *);
hatrack_view_t *crown_view       (crown_t *, uint64_t *, bool);
hatrack_view_t *crown_view_fast  (crown_t *, uint64_t *, bool);
hatrack_view_t *crown_view_slow  (crown_t *, uint64_t *, bool);

/* These need to be non-static because tophat and hatrack_dict both
 * need them, so that they can call in without a second call to
 * MMM. But, they should be considered "friend" functions, and not
 * part of the public API.
 */
crown_store_t    *crown_store_new    (uint64_t);
void             *crown_store_get    (crown_store_t *, hatrack_hash_t, bool *);
void             *crown_store_put    (crown_store_t *, crown_t *,
				      hatrack_hash_t, void *, bool *, uint64_t);
void             *crown_store_replace(crown_store_t *, crown_t *,
				      hatrack_hash_t, void *, bool *, uint64_t);
bool              crown_store_add    (crown_store_t *, crown_t *,
				      hatrack_hash_t, void *, uint64_t);
void             *crown_store_remove (crown_store_t *, crown_t *,
				      hatrack_hash_t, bool *, uint64_t);

#endif
