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
 *  Name:           swimcap2.c
 *  Description:    Single WrIter, Multiple-read, Crappy, Albeit Parallel, v2.
 *
 *                  This uses a per-data structure lock that writers hold
 *                  for their entire operation.
 *
 *                  In this version, readers do NOT use the lock;
 *                  in fact, they are fully wait free.
 *
 *                  Instead, we use an epoch-based memory management
 *                  scheme on our current data store, to make sure that
 *                  a store cannot be deleted while we are reading it,
 *                  even if a resize has completed.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "swimcap2.h"

// clang-format off
static swimcap2_store_t *swimcap2_store_new    (uint64_t);
static void             *swimcap2_store_get    (swimcap2_store_t *,
						hatrack_hash_t *, bool *);
static void             *swimcap2_store_put    (swimcap2_store_t *,
						swimcap2_t *, hatrack_hash_t *,
						void *, bool *);
static void             *swimcap2_store_replace(swimcap2_store_t *,
						swimcap2_t *, hatrack_hash_t *,
						void *, bool *);
static bool              swimcap2_store_add    (swimcap2_store_t *,
						swimcap2_t *, hatrack_hash_t *,
						void *);
static void             *swimcap2_store_remove (swimcap2_store_t *,
						swimcap2_t *, hatrack_hash_t *,
						bool *);
static void              swimcap2_migrate     (swimcap2_t *);
// clang-format on

void
swimcap2_init(swimcap2_t *self)
{
    swimcap2_store_t *store = swimcap2_store_new(HATRACK_MIN_SIZE);
    self->item_count        = 0;
    self->store             = store;
    pthread_mutex_init(&self->write_mutex, NULL);

    return;
}

void *
swimcap2_get(swimcap2_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = swimcap2_store_get(self->store, hv, found);
    mmm_end_op();

    return ret;
}

void *
swimcap2_put(swimcap2_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    if (pthread_mutex_lock(&self->write_mutex)) {
        abort();
    }

    ret = swimcap2_store_put(self->store, self, hv, item, found);

    if (pthread_mutex_unlock(&self->write_mutex)) {
        abort();
    }
    mmm_end_op();

    return ret;
}

void *
swimcap2_replace(swimcap2_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    if (pthread_mutex_lock(&self->write_mutex)) {
        abort();
    }

    ret = swimcap2_store_replace(self->store, self, hv, item, found);

    if (pthread_mutex_unlock(&self->write_mutex)) {
        abort();
    }
    mmm_end_op();

    return ret;
}

bool
swimcap2_add(swimcap2_t *self, hatrack_hash_t *hv, void *item)
{
    bool ret;

    mmm_start_basic_op();
    if (pthread_mutex_lock(&self->write_mutex)) {
        abort();
    }

    ret = swimcap2_store_add(self->store, self, hv, item);

    if (pthread_mutex_unlock(&self->write_mutex)) {
        abort();
    }
    mmm_end_op();

    return ret;
}

void *
swimcap2_remove(swimcap2_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    if (pthread_mutex_lock(&self->write_mutex)) {
        abort();
    }

    ret = swimcap2_store_remove(self->store, self, hv, found);

    if (pthread_mutex_unlock(&self->write_mutex)) {
        abort();
    }
    mmm_end_op();

    return ret;
}

/* Make sure there are definitely no more writers before doing this;
 * the behavior for destroying a mutex that's in use is undefined.
 * Generally, if anyone is waiting on the mutex, they might hang
 * indefinitely.
 */
void
swimcap2_delete(swimcap2_t *self)
{
    pthread_mutex_destroy(&self->write_mutex);
    mmm_retire(self->store);
    free(self);

    return;
}

uint64_t
swimcap2_len(swimcap2_t *self)
{
    return self->item_count;
}

hatrack_view_t *
swimcap2_view(swimcap2_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t     *view;
    swimcap2_store_t   *store;
    swimcap2_contents_t contents;
    hatrack_view_t     *p;
    swimcap2_bucket_t  *cur;
    swimcap2_bucket_t  *end;
    uint64_t            count;
    uint64_t            last_slot;
    uint64_t            alloc_len;

    mmm_start_basic_op();
    store     = self->store;
    last_slot = store->last_slot;
    alloc_len = sizeof(hatrack_view_t) * (last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = store->buckets;
    end       = cur + (last_slot + 1);
    count     = 0;

    while (cur < end) {
        contents = atomic_read(&cur->contents);
        if (!(contents.info & SWIMCAP2_F_USED)) {
            cur++;
            continue;
        }
        p->hv         = cur->hv;
        p->item       = contents.item;
        p->sort_epoch = contents.info & ~SWIMCAP2_F_USED;
        count++;
        p++;
        cur++;
    }

    *num = count;
    if (!count) {
        free(view);
        mmm_end_op();
        return NULL;
    }

    view = (hatrack_view_t *)realloc(view, sizeof(hatrack_view_t) * count);

    if (sort) {
        qsort(view, count, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

    mmm_end_op();
    return view;
}

static swimcap2_store_t *
swimcap2_store_new(uint64_t size)
{
    swimcap2_store_t *ret;
    uint64_t          alloc_len;

    alloc_len = sizeof(swimcap2_store_t);
    alloc_len += size * sizeof(swimcap2_bucket_t);
    ret            = (swimcap2_store_t *)mmm_alloc_committed(alloc_len);
    ret->last_slot = size - 1;
    ret->threshold = hatrack_compute_table_threshold(size);

    return ret;
}

static void *
swimcap2_store_get(swimcap2_store_t *self, hatrack_hash_t *hv, bool *found)
{
    uint64_t            bix;
    uint64_t            last_slot;
    uint64_t            i;
    swimcap2_bucket_t  *cur;
    swimcap2_contents_t contents;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            // It's possible the hash has been written, but no item
            // has yet, so we need to load atomically, then make sure
            // there's something to return.
            contents = atomic_read(&cur->contents);
            if (contents.info & SWIMCAP2_F_USED) {
                if (found) {
                    *found = true;
                }
                return contents.item;
            }
            else {
                if (found) {
                    *found = false;
                }
                return NULL;
            }
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

static void *
swimcap2_store_put(swimcap2_store_t *self,
                   swimcap2_t       *top,
                   hatrack_hash_t   *hv,
                   void             *item,
                   bool             *found)
{
    uint64_t            bix;
    uint64_t            i;
    uint64_t            last_slot;
    swimcap2_bucket_t  *cur;
    swimcap2_contents_t contents;
    void               *ret;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            contents = atomic_load(&cur->contents);
            if (contents.info & SWIMCAP2_F_DELETED) {
                if (found) {
                    *found = false;
                }
                contents.info = top->next_epoch++ | SWIMCAP2_F_USED;
                ret           = NULL;
                top->item_count++;
            }
            else {
                if (found) {
                    *found = true;
                }
                ret = contents.item;
            }

            contents.item = item;
            atomic_store(&cur->contents, contents);

            return ret;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (self->used_count + 1 == self->threshold) {
                swimcap2_migrate(top);
                return swimcap2_store_put(top->store, top, hv, item, found);
            }
            self->used_count++;
            top->item_count++;
            cur->hv       = *hv;
            contents.item = item;
            contents.info = top->next_epoch++ | SWIMCAP2_F_USED;
            atomic_store(&cur->contents, contents);

            if (found) {
                *found = false;
            }
            return NULL;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static void *
swimcap2_store_replace(swimcap2_store_t *self,
                       swimcap2_t       *top,
                       hatrack_hash_t   *hv,
                       void             *item,
                       bool             *found)
{
    uint64_t            bix;
    uint64_t            i;
    uint64_t            last_slot;
    swimcap2_bucket_t  *cur;
    swimcap2_contents_t contents;
    void               *ret;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            contents = atomic_read(&cur->contents);

            if (contents.info & SWIMCAP2_F_DELETED) {
                if (found) {
                    *found = false;
                }
                return NULL;
            }
            ret           = contents.item;
            contents.item = item;

            atomic_store(&cur->contents, contents);

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

static bool
swimcap2_store_add(swimcap2_store_t *self,
                   swimcap2_t       *top,
                   hatrack_hash_t   *hv,
                   void             *item)
{
    uint64_t            bix;
    uint64_t            i;
    uint64_t            last_slot;
    swimcap2_bucket_t  *cur;
    swimcap2_contents_t contents;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            contents = atomic_read(&cur->contents);
            if (contents.info & SWIMCAP2_F_USED) {
                return false;
            }
            contents.item = item;
            contents.info = top->next_epoch++ | SWIMCAP2_F_USED;
            top->item_count++;
            atomic_store(&cur->contents, contents);
            return true;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (self->used_count + 1 == self->threshold) {
                swimcap2_migrate(top);
                return swimcap2_store_add(top->store, top, hv, item);
            }
            self->used_count++;
            top->item_count++;
            cur->hv       = *hv;
            contents.item = item;
            contents.info = top->next_epoch++ | SWIMCAP2_F_USED;
            atomic_store(&cur->contents, contents);

            return true;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

void *
swimcap2_store_remove(swimcap2_store_t *self,
                      swimcap2_t       *top,
                      hatrack_hash_t   *hv,
                      bool             *found)
{
    uint64_t            bix;
    uint64_t            i;
    uint64_t            last_slot;
    swimcap2_bucket_t  *cur;
    swimcap2_contents_t contents;
    void               *ret;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            contents = atomic_read(&cur->contents);
            if (contents.info & SWIMCAP2_F_DELETED) {
                if (found) {
                    *found = false;
                }
                return NULL;
            }

            ret           = contents.item;
            contents.info = SWIMCAP2_F_DELETED;
            atomic_store(&cur->contents, contents);
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
swimcap2_migrate(swimcap2_t *self)
{
    swimcap2_store_t   *cur_store;
    swimcap2_store_t   *new_store;
    swimcap2_bucket_t  *cur;
    swimcap2_bucket_t  *target;
    swimcap2_contents_t contents;
    uint64_t            new_size;
    uint64_t            cur_last_slot;
    uint64_t            new_last_slot;
    uint64_t            i, n, bix;

    cur_store     = self->store;
    cur_last_slot = cur_store->last_slot;
    new_size      = hatrack_new_size(cur_last_slot, swimcap2_len(self) + 1);
    new_last_slot = new_size - 1;
    new_store     = swimcap2_store_new(new_size);

    for (n = 0; n <= cur_last_slot; n++) {
        cur      = &cur_store->buckets[n];
        contents = atomic_read(&cur->contents);
        if ((contents.info == SWIMCAP2_F_DELETED)
            || hatrack_bucket_unreserved(&cur->hv)) {
            continue;
        }
        bix = hatrack_bucket_index(&cur->hv, new_last_slot);
        for (i = 0; i < new_size; i++) {
            target = &new_store->buckets[bix];
            if (hatrack_bucket_unreserved(&target->hv)) {
                target->hv = cur->hv;
                atomic_store(&target->contents, contents);
                break;
            }
            bix = (bix + 1) & new_last_slot;
        }
    }

    new_store->used_count = self->item_count;
    self->store           = new_store;

    mmm_retire(cur_store);

    return;
}
