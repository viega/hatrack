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
 *  Name:           swimcap.c
 *  Description:    Single WrIter, Multiple-read, Crappy, Albeit Parallel.
 *
 *                  This uses a per-data structure lock that writers hold
 *                  for their entire operation.
 *
 *                  Readers also use the lock, but only for a minimal
 *                  amount of time-- enough time to grab a pointer to
 *                  the current store, and to increment a reference
 *                  count in that store.  The lock does not need to
 *                  be held when readers exit.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "swimcap.h"

// clang-format off
static swimcap_store_t *swimcap_store_new   (uint64_t);
static void            *swimcap_store_get   (swimcap_store_t *,
					     hatrack_hash_t *, bool *);
static void            *swimcap_store_put   (swimcap_store_t *, swimcap_t *,
					     hatrack_hash_t *, void *, bool *);
static bool             swimcap_store_add   (swimcap_store_t *, swimcap_t *, 
					     hatrack_hash_t *, void *);
static void            *swimcap_store_remove(swimcap_store_t *, swimcap_t *,
					     hatrack_hash_t *, bool *);
static void             swimcap_migrate     (swimcap_t *);
// clang-format on

/* These macros clean up swimcap_view() to make it more readable.  It
 * uses the configuration variable SWIMCAP_INCONSISTENT_VIEW_IS_OKAY
 * to select whether the function should act as a reader or a writer.
 *
 * By default, we register as a reader, and live with potential
 * inconsistency.
 */
#ifdef SWIMCAP_INCONSISTENT_VIEW_IS_OKAY
#define swimcap_viewer_enter(self)       swimcap_reader_enter(self)
#define swimcap_viewer_exit(self, store) swimcap_reader_exit(store)
#else
static inline swimcap_store_t *
swimcap_viewer_enter(swimcap_t *self)
{
    if (pthread_mutex_lock(&self->write_mutex)) {
        abort();
    }

    return self->store;
}

static inline void
swimcap_viewer_exit(swimcap_t *self, swimcap_store_t *unused)
{
    if (pthread_mutex_unlock(&self->write_mutex)) {
        abort();
    }
}
#endif

void
swimcap_init(swimcap_t *self)
{
    swimcap_store_t *store;
    store            = swimcap_store_new(HATRACK_MIN_SIZE);
    self->store      = store;
    self->item_count = 0;
    self->next_epoch = 0;

    pthread_mutex_init(&self->write_mutex, NULL);

    return;
}

void *
swimcap_get(swimcap_t *self, hatrack_hash_t *hv, bool *found)
{
    void            *ret;
    swimcap_store_t *store;

    store = swimcap_reader_enter(self);
    ret   = swimcap_store_get(store, hv, found);
    swimcap_reader_exit(store);

    return ret;
}

void *
swimcap_put(swimcap_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    if (pthread_mutex_lock(&self->write_mutex)) {
        abort();
    }

    ret = swimcap_store_put(self->store, self, hv, item, found);

    if (pthread_mutex_unlock(&self->write_mutex)) {
        abort();
    }

    return ret;
}

bool
swimcap_add(swimcap_t *self, hatrack_hash_t *hv, void *item)
{
    bool ret;
    if (pthread_mutex_lock(&self->write_mutex)) {
        abort();
    }

    ret = swimcap_store_add(self->store, self, hv, item);

    if (pthread_mutex_unlock(&self->write_mutex)) {
        abort();
    }

    return ret;
}

void *
swimcap_remove(swimcap_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    if (pthread_mutex_lock(&self->write_mutex)) {
        abort();
    }

    ret = swimcap_store_remove(self->store, self, hv, found);

    if (pthread_mutex_unlock(&self->write_mutex)) {
        abort();
    }

    return ret;
}

void
swimcap_delete(swimcap_t *self)
{
    // Wait for all the readers to exit the store.
    while (atomic_load(&self->store->readers))
        ;
    pthread_mutex_destroy(&self->write_mutex);
    free(self->store);
    free(self);

    return;
}

uint64_t
swimcap_len(swimcap_t *self)
{
    return self->item_count;
}

hatrack_view_t *
swimcap_view(swimcap_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t   *view;
    swimcap_store_t  *store;
    hatrack_view_t   *p;
    swimcap_bucket_t *cur;
    swimcap_bucket_t *end;
    uint64_t          count;
    uint64_t          last_slot;
    uint64_t          alloc_len;

    store     = swimcap_viewer_enter(self);
    last_slot = store->last_slot;
    alloc_len = sizeof(hatrack_view_t) * (last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = store->buckets;
    end       = cur + (last_slot + 1);
    count     = 0;

    while (cur < end) {
        if (cur->deleted || hatrack_bucket_unreserved(&cur->hv)) {
            cur++;
            continue;
        }
        p->hv         = cur->hv;
        p->item       = cur->item;
        p->sort_epoch = cur->epoch;
        count++;
        p++;
        cur++;
    }

    *num = count;

    if (!count) {
        free(view);
        swimcap_viewer_exit(self, store);
        return NULL;
    }

    view = (hatrack_view_t *)realloc(view, sizeof(hatrack_view_t) * count);

    if (sort) {
        qsort(view, count, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

    swimcap_viewer_exit(self, store);
    return view;
}

static void *
swimcap_store_get(swimcap_store_t *self, hatrack_hash_t *hv, bool *found)
{
    uint64_t          bix;
    uint64_t          last_slot;
    uint64_t          i;
    swimcap_bucket_t *cur;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
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
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static swimcap_store_t *
swimcap_store_new(uint64_t size)
{
    swimcap_store_t *ret;

    ret            = (swimcap_store_t *)calloc(1,
                                    sizeof(swimcap_store_t)
                                        + size * sizeof(swimcap_bucket_t));
    ret->last_slot = size - 1;
    ret->threshold = hatrack_compute_table_threshold(size);

    return ret;
}

static void *
swimcap_store_put(swimcap_store_t *self,
                  swimcap_t       *top,
                  hatrack_hash_t  *hv,
                  void            *item,
                  bool            *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    swimcap_bucket_t *cur;
    void             *ret;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (cur->deleted) {
                cur->item    = item;
                cur->deleted = false;
                cur->epoch   = top->next_epoch++;
                top->item_count++;

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
                swimcap_migrate(top);
                return swimcap_store_put(top->store, top, hv, item, found);
            }
            self->used_count++;
            top->item_count++;
            cur->hv    = *hv;
            cur->item  = item;
            cur->epoch = top->next_epoch++;

            if (found) {
                *found = false;
            }
            return NULL;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static bool
swimcap_store_add(swimcap_store_t *self,
                  swimcap_t       *top,
                  hatrack_hash_t  *hv,
                  void            *item)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    swimcap_bucket_t *cur;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (cur->deleted) {
                cur->item    = item;
                cur->deleted = false;
                cur->epoch   = top->next_epoch++;
                top->item_count++;

                return true;
            }
            return false;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (self->used_count + 1 == self->threshold) {
                swimcap_migrate(top);
                return swimcap_store_add(top->store, top, hv, item);
            }
            self->used_count++;
            top->item_count++;
            cur->hv    = *hv;
            cur->item  = item;
            cur->epoch = top->next_epoch++;

            return true;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static void *
swimcap_store_remove(swimcap_store_t *self,
                     swimcap_t       *top,
                     hatrack_hash_t  *hv,
                     bool            *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    swimcap_bucket_t *cur;
    void             *ret;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
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
            --top->item_count;

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
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static void
swimcap_migrate(swimcap_t *self)
{
    swimcap_store_t  *cur_store;
    swimcap_store_t  *new_store;
    swimcap_bucket_t *cur;
    swimcap_bucket_t *target;
    uint64_t          new_size;
    uint64_t          cur_last_slot;
    uint64_t          new_last_slot;
    uint64_t          i, n, bix;

    cur_store     = self->store;
    cur_last_slot = cur_store->last_slot;

    new_size = hatrack_new_size(cur_store->last_slot, swimcap_len(self) + 1);

    new_last_slot = new_size - 1;
    new_store     = swimcap_store_new(new_size);

    for (n = 0; n <= cur_last_slot; n++) {
        cur = &cur_store->buckets[n];
        if (cur->deleted || hatrack_bucket_unreserved(&cur->hv)) {
            continue;
        }
        bix = hatrack_bucket_index(&cur->hv, new_last_slot);
        for (i = 0; i < new_size; i++) {
            target = &new_store->buckets[bix];
            if (hatrack_bucket_unreserved(&target->hv)) {
                target->hv.w1 = cur->hv.w1;
                target->hv.w2 = cur->hv.w2;
                target->item  = cur->item;
                target->epoch = cur->epoch;
                break;
            }
            bix = (bix + 1) & new_last_slot;
        }
    }

    new_store->used_count = self->item_count;
    self->store           = new_store;

    // Wait for all the readers to exit the store.
    while (atomic_load(&cur_store->readers))
        ;
    free(cur_store);

    return;
}
