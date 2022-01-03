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
 *  Name:           newshat.c
 *  Description:    Now Everyone Writes Simultaneously (HAsh Table)
 *
 *                  Uses pthread locks on a per-bucket basis, and
 *                  allows multiple simultaneous writers, except when
 *                  performing table migration.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "newshat.h"

// clang-format off

// Not static, because tophat needs to call it, but nonetheless, don't
// stick it in our public prototypes.
       newshat_store_t *newshat_store_new    (uint64_t);
static void             newshat_store_delete (newshat_store_t *);
static void            *newshat_store_get    (newshat_store_t *, newshat_t *,
					      hatrack_hash_t *, bool *);
static void            *newshat_store_put    (newshat_store_t *, newshat_t *,
					      hatrack_hash_t *, void *,
					      bool *);
static void            *newshat_store_replace(newshat_store_t *, newshat_t *,
					      hatrack_hash_t *, void *,
					      bool *);
static bool             newshat_store_add    (newshat_store_t *, newshat_t *,
					      hatrack_hash_t *, void *);
static void            *newshat_store_remove (newshat_store_t *, newshat_t *,
					      hatrack_hash_t *, bool *);
static newshat_store_t *newshat_store_migrate(newshat_store_t *, newshat_t *);

// clang-format on

void
newshat_init(newshat_t *self)
{
    newshat_store_t *store = newshat_store_new(HATRACK_MIN_SIZE);
    self->item_count       = 0;
    self->next_epoch       = 1; // 0 is reserved for empty buckets.
    self->store            = store;
    pthread_mutex_init(&self->migrate_mutex, NULL);

    return;
}

void *
newshat_get(newshat_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = newshat_store_get(self->store, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
newshat_put(newshat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = newshat_store_put(self->store, self, hv, item, found);
    mmm_end_op();

    return ret;
}

void *
newshat_replace(newshat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = newshat_store_replace(self->store, self, hv, item, found);
    mmm_end_op();

    return ret;
}

bool
newshat_add(newshat_t *self, hatrack_hash_t *hv, void *item)
{
    bool ret;

    mmm_start_basic_op();
    ret = newshat_store_add(self->store, self, hv, item);
    mmm_end_op();

    return ret;
}

void *
newshat_remove(newshat_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = newshat_store_remove(self->store, self, hv, found);
    mmm_end_op();

    return ret;
}

void
newshat_delete(newshat_t *self)
{
    mmm_retire(self->store);
    if (pthread_mutex_destroy(&self->migrate_mutex)) {
        abort();
    }
    free(self);

    return;
}

uint64_t
newshat_len(newshat_t *self)
{
    return self->item_count;
}

hatrack_view_t *
newshat_view(newshat_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t   *view;
    newshat_store_t  *store;
    hatrack_view_t   *p;
    newshat_bucket_t *cur;
    newshat_bucket_t *end;
    newshat_record_t  contents;
    uint64_t          count;
    uint64_t          last_slot;
    uint64_t          alloc_len;

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
        if (pthread_mutex_lock(&cur->mutex)) {
            abort();
        }
        contents = atomic_read(&cur->contents);

        if (!contents.info) {
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            cur++;
            continue;
        }
        p->hv         = cur->hv;
        p->item       = contents.item;
        p->sort_epoch = contents.info;
        if (pthread_mutex_unlock(&cur->mutex)) {
            abort();
        }
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

newshat_store_t *
newshat_store_new(uint64_t size)
{
    newshat_store_t *ret;
    uint64_t         i;
    uint64_t         alloc_len;

    alloc_len = sizeof(newshat_store_t) + size * sizeof(newshat_bucket_t);
    ret       = (newshat_store_t *)mmm_alloc_committed(alloc_len);

    mmm_add_cleanup_handler(ret, (void (*)(void *))newshat_store_delete);

    ret->last_slot = size - 1;
    ret->threshold = hatrack_compute_table_threshold(size);

    for (i = 0; i <= ret->last_slot; i++) {
        pthread_mutex_init(&ret->buckets[i].mutex, NULL);
    }

    return ret;
}

static void
newshat_store_delete(newshat_store_t *self)
{
    uint64_t i;

    for (i = 0; i <= self->last_slot; i++) {
        pthread_mutex_destroy(&self->buckets[i].mutex);
    }
}

static void *
newshat_store_get(newshat_store_t *self,
                  newshat_t       *top,
                  hatrack_hash_t  *hv,
                  bool            *found)
{
    uint64_t          bix;
    uint64_t          last_slot;
    uint64_t          i;
    newshat_bucket_t *cur;
    newshat_record_t  contents;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];

        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (pthread_mutex_lock(&cur->mutex)) {
                abort();
            }
            contents = atomic_read(&cur->contents);

            if (!contents.info) {
                if (found) {
                    *found = false;
                }
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return NULL;
            }
            if (found) {
                *found = true;
            }
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return contents.item;
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
newshat_store_put(newshat_store_t *self,
                  newshat_t       *top,
                  hatrack_hash_t  *hv,
                  void            *item,
                  bool            *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    newshat_bucket_t *cur;
    newshat_record_t  contents;
    void             *ret;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
check_bucket_again:
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (pthread_mutex_lock(&cur->mutex)) {
                abort();
            }
            if (cur->migrated) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return newshat_store_put(top->store, top, hv, item, found);
            }

            contents = atomic_read(&cur->contents);

            if (!contents.info) {
                contents.item = item;
                contents.info = top->next_epoch++;
                atomic_store(&cur->contents, contents);
                top->item_count++;
                if (found) {
                    *found = false;
                }
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return NULL;
            }
            ret           = contents.item;
            contents.item = item;
            // Note that, since we're overwriting something that was already
            // here, we don't need to update the epoch or the USED flag.
            atomic_store(&cur->contents, contents);
            if (found) {
                *found = true;
            }
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return ret;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (pthread_mutex_lock(&cur->mutex)) {
                abort();
            }
            if (cur->migrated) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return newshat_store_put(top->store, top, hv, item, found);
            }
            if (!hatrack_bucket_unreserved(&cur->hv)) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                goto check_bucket_again;
            }
            if (self->used_count == self->threshold) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                self = newshat_store_migrate(self, top);
                return newshat_store_put(self, top, hv, item, found);
            }
            self->used_count++;
            top->item_count++;
            cur->hv       = *hv;
            contents.item = item;
            contents.info = top->next_epoch++;
            atomic_store(&cur->contents, contents);
            if (found) {
                *found = false;
            }
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return NULL;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static void *
newshat_store_replace(newshat_store_t *self,
                      newshat_t       *top,
                      hatrack_hash_t  *hv,
                      void            *item,
                      bool            *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    newshat_bucket_t *cur;
    newshat_record_t  contents;
    void             *ret;

    last_slot = self->last_slot;

    bix = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (pthread_mutex_lock(&cur->mutex)) {
                abort();
            }
            if (cur->migrated) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return newshat_store_put(top->store, top, hv, item, found);
            }
            contents = atomic_read(&cur->contents);
            if (!contents.info) {
                if (found) {
                    *found = false;
                }
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return NULL;
            }
            ret           = contents.item;
            contents.item = item;
            // Note that, since we're overwriting something that was already
            // here, we don't need to update the epoch or the USED flag.
            atomic_store(&cur->contents, contents);
            if (found) {
                *found = true;
            }
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
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

bool
newshat_store_add(newshat_store_t *self,
                  newshat_t       *top,
                  hatrack_hash_t  *hv,
                  void            *item)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    newshat_bucket_t *cur;
    newshat_record_t  contents;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
check_bucket_again:
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (pthread_mutex_lock(&cur->mutex)) {
                abort();
            }
            if (cur->migrated) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return newshat_store_add(top->store, top, hv, item);
            }
            contents = atomic_read(&cur->contents);
            if (!contents.info) {
                contents.item = item;
                contents.info = top->next_epoch++;
                atomic_store(&cur->contents, contents);
                top->item_count++;
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return true;
            }
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return false;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (pthread_mutex_lock(&cur->mutex)) {
                abort();
            }
            if (cur->migrated) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return newshat_store_add(top->store, top, hv, item);
            }
            if (!hatrack_bucket_unreserved(&cur->hv)) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                goto check_bucket_again;
            }
            if (self->used_count == self->threshold) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                self = newshat_store_migrate(self, top);
                return newshat_store_add(self, top, hv, item);
            }
            self->used_count++;
            top->item_count++;
            cur->hv       = *hv;
            contents.item = item;
            contents.info = top->next_epoch++;
            atomic_store(&cur->contents, contents);
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return true;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

void *
newshat_store_remove(newshat_store_t *self,
                     newshat_t       *top,
                     hatrack_hash_t  *hv,
                     bool            *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    newshat_bucket_t *cur;
    newshat_record_t  contents;
    void             *ret;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (found) {
                *found = false;
            }
            return NULL;
        }
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (pthread_mutex_lock(&cur->mutex)) {
                abort();
            }
            if (cur->migrated) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return newshat_store_remove(top->store, top, hv, found);
            }
            contents = atomic_read(&cur->contents);
            if (!contents.info) {
                if (found) {
                    *found = false;
                }
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return NULL;
            }

            ret           = contents.item;
            contents.info = 0; // No epoch, empty bucket.

            atomic_store(&cur->contents, contents);
            --top->item_count;

            if (found) {
                *found = true;
            }
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return ret;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static newshat_store_t *
newshat_store_migrate(newshat_store_t *store, newshat_t *top)
{
    newshat_store_t  *new_store;
    newshat_bucket_t *cur;
    newshat_bucket_t *target;
    newshat_record_t  contents;
    uint64_t          new_size;
    uint64_t          cur_last_slot;
    uint64_t          new_last_slot;
    uint64_t          i, n, bix;
    uint64_t          items_to_migrate = 0;

    if (pthread_mutex_lock(&top->migrate_mutex)) {
        abort();
    }
    if (store != top->store) {
        // Someone else migrated it, and now we can go finish our
        // write.
        new_store = top->store;
        if (pthread_mutex_unlock(&top->migrate_mutex)) {
            abort();
        }
        return new_store;
    }
    cur_last_slot = store->last_slot;

    for (n = 0; n <= cur_last_slot; n++) {
        cur      = &store->buckets[n];
        contents = atomic_read(&cur->contents);

        if (pthread_mutex_lock(&cur->mutex)) {
            abort();
        }
        if (contents.info) {
            items_to_migrate++;
        }
    }

    new_size      = hatrack_new_size(cur_last_slot, items_to_migrate + 1);
    new_last_slot = new_size - 1;
    new_store     = newshat_store_new(new_size);

    for (n = 0; n <= cur_last_slot; n++) {
        cur      = &store->buckets[n];
        contents = atomic_read(&cur->contents);
        if (!contents.info) {
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
        cur->migrated = true;
    }

    new_store->used_count = top->item_count;
    top->store            = new_store;

    for (n = 0; n <= cur_last_slot; n++) {
        cur = &store->buckets[n];
        if (pthread_mutex_unlock(&cur->mutex)) {
            abort();
        }
    }

    mmm_retire(store);
    if (pthread_mutex_unlock(&top->migrate_mutex)) {
        abort();
    }

    return new_store;
}
