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
 *  Name:           tiara.h
 *
 *  Description:    This Is A Rediculous Acronym.
 *
 *                  This is roughly in the hihat family, but uses
 *                  64-bit hash values, which we do not generally
 *                  recommend. However, it allows us to show off an
 *                  algorithm that requires only a single
 *                  compare-and-swap per core operation.
 *
 *                  There are a few other differences between this and
 *                  the rest of the hihat family:
 *
 *                  1) We do NOT keep an epoch value.  It would
 *                  require more space, which defeats the purpose.
 *
 *                  2) We still need two status bits, which normally
 *                     we'd steal from the epoch, or, in the lohat
 *                     family, from a pointer to a dynamically
 *                     allocated record.  We've already weakened the
 *                     collision resistance of the hash to an amount
 *                     that I'm not truly comfortable with, so instead
 *                     we steal it from the item field, meaning that
 *                     you CANNOT store integers in here, without
 *                     shifting them up at least two bits.
 *
 *                  We obviously could do a lot better if we could CAS
 *                  more at once, and it's not particularly clear to
 *                  me why modern architectures won't just let you do
 *                  atomic loads and CASs on entire cache lines.
 *
 *                  Unless there's a good reason, hopefully we'll see
 *                  that happen some day!
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __TIARA_H__
#define __TIARA_H__

#include <hatrack/hatrack_common.h>

enum64(tiara_flag_t,
       TIARA_F_MOVING   = 0x0000000000000001,
       TIARA_F_MOVED    = 0x0000000000000002,
       TIARA_F_DELETED  = 0x0000000000000004,
       TIARA_F_ALL      = TIARA_F_MOVING | TIARA_F_MOVED | TIARA_F_DELETED);

typedef struct {
    uint64_t hv;
    void    *item;
} tiara_record_t;

typedef _Atomic(tiara_record_t) tiara_bucket_t;

typedef struct tiara_store_st tiara_store_t;

struct tiara_store_st {
    alignas(8)
    uint64_t                   last_slot;
    uint64_t                   threshold;
    _Atomic uint64_t           used_count;
    _Atomic(tiara_store_t *)   store_next;
    alignas(16)
    tiara_bucket_t             buckets[];
};


typedef struct {
    alignas(8)
    _Atomic(tiara_store_t *) store_current;
    _Atomic uint64_t         item_count;
} tiara_t;


tiara_t        *tiara_new      (void);
tiara_t        *tiara_new_size (char);
void            tiara_init     (tiara_t *);
void            tiara_init_size(tiara_t *, char);
void            tiara_cleanup  (tiara_t *);
void            tiara_delete   (tiara_t *);
void           *tiara_get      (tiara_t *, hatrack_hash_t, bool *);
void           *tiara_put      (tiara_t *, hatrack_hash_t, void *, bool *);
void           *tiara_replace  (tiara_t *, hatrack_hash_t, void *, bool *);
bool            tiara_add      (tiara_t *, hatrack_hash_t, void *);
void           *tiara_remove   (tiara_t *, hatrack_hash_t, bool *);
uint64_t        tiara_len      (tiara_t *);
hatrack_view_t *tiara_view     (tiara_t *, uint64_t *, bool);

#endif
