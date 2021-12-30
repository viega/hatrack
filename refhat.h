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
 *  Name:           refhat.h
 *  Description:    A reference hashtable that only works single-threaded.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __REFHAT_H__
#define __REFHAT_H__

#include "hatrack_common.h"

// clang-format off
typedef struct {
    hatrack_hash_t hv;
    void          *item;
    bool           deleted;
    uint64_t       epoch;
} refhat_bucket_t;

typedef struct {

    uint64_t          last_slot;
    uint64_t          threshold;
    uint64_t          used_count;
    uint64_t          item_count;
    refhat_bucket_t *buckets;
    uint64_t          next_epoch;
} refhat_t;

typedef struct {
    alignas(8)    
    uint64_t           last_slot;
    uint64_t           threshold;
    uint64_t           used_count;
    uint64_t           item_count;
    refhat_bucket_t  *buckets;
    uint64_t           next_epoch;

    // These are additions to support tophat. They're not used w/in
    // refhat, but are used by tophat.
    pthread_mutex_t    mutex;
    uint64_t           thread_id;
    // This is used to recover the original tophat instance, when
    // we're dealing w/ a refhat and realize we need to switch to
    // another table type.
    void              *backref;
} refhat1_t;


void            refhat_init   (refhat_t *);
void           *refhat_get    (refhat_t *, hatrack_hash_t *, bool *);
void           *refhat_put    (refhat_t *, hatrack_hash_t *, void *, bool *);
void           *refhat_replace(refhat_t *, hatrack_hash_t *, void *, bool *);
bool            refhat_add    (refhat_t *, hatrack_hash_t *, void *);
void           *refhat_remove (refhat_t *, hatrack_hash_t *, bool *);
void            refhat_delete (refhat_t *);
uint64_t        refhat_len    (refhat_t *);
hatrack_view_t *refhat_view   (refhat_t *, uint64_t *, bool);

//clang-format on

#endif
