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
 *  Name:           refhat.c
 *  Description:    A reference hashtable that only works single-threaded.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "refhat.h"

static void refhat_migrate(refhat_t *);

void
refhat_init(refhat_t *self)
{
    uint64_t size;

    size             = HATRACK_MIN_SIZE;
    self->last_slot  = size - 1;
    self->threshold  = hatrack_compute_table_threshold(size);
    self->used_count = 0;
    self->item_count = 0;
    self->next_epoch = 0;
    self->buckets    = (refhat_bucket_t *)calloc(size, sizeof(refhat_bucket_t));
}

void *
refhat_get(refhat_t *self, hatrack_hash_t *hv, bool *found)
{
    uint64_t         bix;
    uint64_t         i;
    refhat_bucket_t *cur;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (cur->deleted) {
                if (found) {
                    *found = false;
                }
                return NULL;
            }
            if (found) {
                *found = true;
            }
            return cur->item;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (found) {
                *found = false;
            }
            return NULL;
        }
        bix = (bix + 1) & self->last_slot;
    }
    __builtin_unreachable();
}

void *
refhat_put(refhat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    uint64_t         bix;
    uint64_t         i;
    refhat_bucket_t *cur;
    void            *ret;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (cur->deleted) {
                cur->item    = item;
                cur->deleted = false;
                cur->epoch   = self->next_epoch++;
                self->item_count++;

                if (found) {
                    *found = false;
                }
                return NULL;
            }
            ret       = cur->item;
            cur->item = item;
            if (found) {
                *found = true;
            }
            return ret;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (self->used_count + 1 == self->threshold) {
                refhat_migrate(self);
                return refhat_put(self, hv, item, found);
            }
            self->used_count++;
            self->item_count++;
            cur->hv    = *hv;
            cur->item  = item;
            cur->epoch = self->next_epoch++;

            if (found) {
                *found = false;
            }
            return NULL;
        }
        bix = (bix + 1) & self->last_slot;
    }
    __builtin_unreachable();
}

void *
refhat_replace(refhat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    uint64_t         bix;
    uint64_t         i;
    refhat_bucket_t *cur;
    void            *ret;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (cur->deleted) {
                if (found) {
                    *found = false;
                }
                return NULL;
            }
            ret       = cur->item;
            cur->item = item;
            if (found) {
                *found = true;
            }
            return ret;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (found) {
                *found = false;
            }
        }
        bix = (bix + 1) & self->last_slot;
    }
    __builtin_unreachable();
}

bool
refhat_add(refhat_t *self, hatrack_hash_t *hv, void *item)
{
    uint64_t         bix;
    uint64_t         i;
    refhat_bucket_t *cur;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (cur->deleted) {
                cur->item    = item;
                cur->deleted = false;
                cur->epoch   = self->next_epoch++;
                self->item_count++;

                return true;
            }
            return false;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (self->used_count + 1 == self->threshold) {
                refhat_migrate(self);
                return refhat_add(self, hv, item);
            }
            self->used_count++;
            self->item_count++;
            cur->hv    = *hv;
            cur->item  = item;
            cur->epoch = self->next_epoch++;

            return true;
        }
        bix = (bix + 1) & self->last_slot;
    }
    __builtin_unreachable();
}

void *
refhat_remove(refhat_t *self, hatrack_hash_t *hv, bool *found)
{
    uint64_t         bix;
    uint64_t         i;
    refhat_bucket_t *cur;
    void            *ret;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (cur->deleted) {
                if (found) {
                    *found = false;
                }
                return NULL;
            }

            ret          = cur->item;
            cur->deleted = true;
            --self->item_count;

            if (found) {
                *found = true;
            }
            return ret;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (found) {
                *found = false;
            }
            return NULL;
        }
        bix = (bix + 1) & self->last_slot;
    }
    __builtin_unreachable();
}

void
refhat_delete(refhat_t *self)
{
    free(self->buckets);
    free(self);
}

uint64_t
refhat_len(refhat_t *self)
{
    return self->item_count;
}

hatrack_view_t *
refhat_view(refhat_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t  *view;
    hatrack_view_t  *p;
    refhat_bucket_t *cur;
    refhat_bucket_t *end;

    view = (hatrack_view_t *)malloc(sizeof(hatrack_view_t) * self->item_count);
    p    = view;
    cur  = self->buckets;
    end  = cur + (self->last_slot + 1);

    while (cur < end) {
        if (cur->deleted || hatrack_bucket_unreserved(&cur->hv)) {
            cur++;
            continue;
        }
        p->hv         = cur->hv;
        p->item       = cur->item;
        p->sort_epoch = cur->epoch;

        p++;
        cur++;
    }

    *num = self->item_count;

    if (sort) {
        qsort(view,
              self->item_count,
              sizeof(hatrack_view_t),
              hatrack_quicksort_cmp);
    }

    return view;
}

static void
refhat_migrate(refhat_t *self)
{
    refhat_bucket_t *new_buckets;
    refhat_bucket_t *cur;
    refhat_bucket_t *target;
    uint64_t         new_size;
    uint64_t         new_last_slot;
    uint64_t         i, n, bix;

    new_size      = hatrack_new_size(self->last_slot, self->item_count + 1);
    new_last_slot = new_size - 1;

    new_buckets = (refhat_bucket_t *)calloc(new_size, sizeof(refhat_bucket_t));
    for (n = 0; n <= self->last_slot; n++) {
        cur = &self->buckets[n];
        if (cur->deleted || hatrack_bucket_unreserved(&cur->hv)) {
            continue;
        }
        bix = hatrack_bucket_index(&cur->hv, new_last_slot);
        for (i = 0; i < new_size; i++) {
            target = &new_buckets[bix];
            if (hatrack_bucket_unreserved(&target->hv)) {
                target->hv.w1 = cur->hv.w1;
                target->hv.w2 = cur->hv.w2;
                target->item  = cur->item;
                break;
            }
            bix = (bix + 1) & new_last_slot;
        }
    }
    free(self->buckets);

    self->used_count = self->item_count;
    self->buckets    = new_buckets;
    self->last_slot  = new_size - 1;
    self->threshold  = hatrack_compute_table_threshold(new_size);
}
