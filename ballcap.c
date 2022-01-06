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
 *  Name:           ballcap.c
 *  Description:    Besides a Lot of Locking, Clearly Awesomely Parallel
 *
 *                  Uses pthread locks on a per-bucket basis, and
 *                  allows multiple simultaneous writers, except when
 *                  performing table migration.
 *
 *                  Also uses our strategy from Lohat to ensure we can
 *                  provide a fully consistent ordered view of the hash
 *                  table.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "ballcap.h"

// clang-format off

       ballcap_store_t *ballcap_store_new    (uint64_t);
static void            *ballcap_store_get    (ballcap_store_t *, ballcap_t *,
					      hatrack_hash_t *, bool *);
static void            *ballcap_store_put    (ballcap_store_t *, ballcap_t *,
					      hatrack_hash_t *, void *, bool *);
static void            *ballcap_store_replace(ballcap_store_t *, ballcap_t *,
					      hatrack_hash_t *, void *, bool *);
static bool             ballcap_store_add    (ballcap_store_t *, ballcap_t *,
					      hatrack_hash_t *, void *);
static void            *ballcap_store_remove (ballcap_store_t *, ballcap_t *,
					      hatrack_hash_t *, bool *);
static ballcap_store_t *ballcap_store_migrate(ballcap_store_t *, ballcap_t *);

// clang-format on

void
ballcap_init(ballcap_t *self)
{
    ballcap_store_t *store = ballcap_store_new(HATRACK_MIN_SIZE);
    self->item_count       = 0;
    self->next_epoch       = 0;
    self->store            = store;
    pthread_mutex_init(&self->migrate_mutex, NULL);

    return;
}

void *
ballcap_get(ballcap_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = ballcap_store_get(self->store, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
ballcap_put(ballcap_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = ballcap_store_put(self->store, self, hv, item, found);
    mmm_end_op();

    return ret;
}

void *
ballcap_replace(ballcap_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = ballcap_store_replace(self->store, self, hv, item, found);
    mmm_end_op();

    return ret;
}

bool
ballcap_add(ballcap_t *self, hatrack_hash_t *hv, void *item)
{
    bool ret;

    mmm_start_basic_op();
    ret = ballcap_store_add(self->store, self, hv, item);
    mmm_end_op();

    return ret;
}

void *
ballcap_remove(ballcap_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = ballcap_store_remove(self->store, self, hv, found);
    mmm_end_op();

    return ret;
}

void
ballcap_delete(ballcap_t *self)
{
    uint64_t          i;
    ballcap_bucket_t *bucket;

    for (i = 0; i <= self->store->last_slot; i++) {
        bucket = &self->store->buckets[i];

        if (bucket->record) {
            mmm_retire_unused(bucket->record);
        }
    }

    mmm_retire_unused(self->store);

    if (pthread_mutex_destroy(&self->migrate_mutex)) {
        abort();
    }

    free(self);

    return;
}

static void
ballcap_store_delete(ballcap_store_t *self)
{
    uint64_t i;

    for (i = 0; i <= self->last_slot; i++) {
        pthread_mutex_destroy(&self->buckets[i].mutex);
    }
}

uint64_t
ballcap_len(ballcap_t *self)
{
    return self->item_count;
}

hatrack_view_t *
ballcap_view(ballcap_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t   *view;
    ballcap_store_t  *store;
    ballcap_record_t *record;
    hatrack_view_t   *p;
    ballcap_bucket_t *cur;
    ballcap_bucket_t *end;
    uint64_t          count;
    uint64_t          last_slot;
    uint64_t          alloc_len;
    uint64_t          target_epoch;
    uint64_t          sort_epoch;

    target_epoch = mmm_start_linearized_op();

    store     = self->store;
    last_slot = store->last_slot;
    alloc_len = sizeof(hatrack_view_t) * (last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = store->buckets;
    end       = cur + (last_slot + 1);
    count     = 0;

    while (cur < end) {
        if (hatrack_bucket_unreserved(&cur->hv)) {
            cur++;
            continue;
        }
        if (pthread_mutex_lock(&cur->mutex)) {
            abort();
        }
        record = cur->record;

        // Look for the most recent record written BEFORE or during
        // the epoch we're targeting for linearization.
        while (record) {
            sort_epoch = mmm_get_write_epoch(record);
            if (sort_epoch <= target_epoch) {
                break;
            }
            record = record->next;
        }

        if (!record || sort_epoch > target_epoch || record->deleted) {
            pthread_mutex_unlock(&cur->mutex);
            cur++;
            continue;
        }

        p->item       = record->item;
        p->sort_epoch = mmm_get_create_epoch(record);

        pthread_mutex_unlock(&cur->mutex);
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

ballcap_store_t *
ballcap_store_new(uint64_t size)
{
    ballcap_store_t *ret;
    uint64_t         i;
    uint64_t         len;

    len = sizeof(ballcap_store_t) + size * sizeof(ballcap_bucket_t);
    ret = (ballcap_store_t *)mmm_alloc_committed(len);

    mmm_add_cleanup_handler(ret, (void (*)(void *))ballcap_store_delete);

    ret->last_slot = size - 1;
    ret->threshold = hatrack_compute_table_threshold(size);

    for (i = 0; i <= ret->last_slot; i++) {
        pthread_mutex_init(&ret->buckets[i].mutex, NULL);
    }

    return ret;
}

static void *
ballcap_store_get(ballcap_store_t *self,
                  ballcap_t       *top,
                  hatrack_hash_t  *hv,
                  bool            *found)
{
    uint64_t          bix;
    uint64_t          last_slot;
    uint64_t          i;
    ballcap_bucket_t *cur;

    last_slot = self->last_slot;

    bix = hatrack_bucket_index(hv, last_slot);
    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (pthread_mutex_lock(&cur->mutex)) {
                abort();
            }
            if (!cur->record || cur->record->deleted) {
                if (found) {
                    *found = false;
                }
                pthread_mutex_unlock(&cur->mutex);
                return NULL;
            }
            if (found) {
                *found = true;
            }
            pthread_mutex_unlock(&cur->mutex);
            return cur->record->item;
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
ballcap_store_put(ballcap_store_t *self,
                  ballcap_t       *top,
                  hatrack_hash_t  *hv,
                  void            *item,
                  bool            *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    ballcap_bucket_t *cur;
    ballcap_record_t *record;
    ballcap_record_t *old_record;
    void             *ret;

    last_slot = self->last_slot;

    bix = hatrack_bucket_index(hv, last_slot);

    record       = mmm_alloc(sizeof(ballcap_record_t));
    record->item = item;

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
check_bucket_again:
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (pthread_mutex_lock(&cur->mutex)) {
                abort();
            }
            if (cur->migrated) {
                pthread_mutex_unlock(&cur->mutex);
                mmm_retire_unused(record);
                return ballcap_store_put(top->store, top, hv, item, found);
            }
            old_record = cur->record;
            /* Because we're using locks, there should always be a
             * record here. We may have to revisit this in the future
             * if we decide to handle killed threads that are holding
             * locks when killed.
             */
            if (old_record->deleted) {
                ret = NULL;
                if (found) {
                    *found = false;
                }
                top->item_count++;
            }
            else {
                ret = old_record->item;
                if (found) {
                    *found = true;
                }
                record->next = cur->record;
                // Since we're overwriting a pre-existing record, we should
                // inherit its creation time, in terms of our sort order.
                mmm_copy_create_epoch(record, cur->record);
            }
            cur->record = record;
            mmm_commit_write(record);
            mmm_retire(old_record);
            pthread_mutex_unlock(&cur->mutex);

            return ret;
        }

        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (pthread_mutex_lock(&cur->mutex)) {
                abort();
            }
            if (cur->migrated) {
                pthread_mutex_unlock(&cur->mutex);
                mmm_retire_unused(record);
                return ballcap_store_put(top->store, top, hv, item, found);
            }
            if (!hatrack_bucket_unreserved(&cur->hv)) {
                pthread_mutex_unlock(&cur->mutex);
                goto check_bucket_again;
            }
            if (self->used_count == self->threshold) {
                pthread_mutex_unlock(&cur->mutex);

                mmm_retire_unused(record);
                self = ballcap_store_migrate(self, top);
                return ballcap_store_put(self, top, hv, item, found);
            }
            self->used_count++;
            top->item_count++;
            cur->hv = *hv;
            ret     = NULL;
            if (found) {
                *found = false;
            }

            cur->record = record;
            mmm_commit_write(record);
            pthread_mutex_unlock(&cur->mutex);
            return ret;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static void *
ballcap_store_replace(ballcap_store_t *self,
                      ballcap_t       *top,
                      hatrack_hash_t  *hv,
                      void            *item,
                      bool            *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    ballcap_bucket_t *cur;
    ballcap_record_t *record;
    void             *ret;

    last_slot = self->last_slot;

    bix = hatrack_bucket_index(hv, last_slot);

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
                pthread_mutex_unlock(&cur->mutex);
                return ballcap_store_replace(top->store, top, hv, item, found);
            }
            /* Because we're using locks, there should always be a
             * record here. We may have to revisit this in the future
             * if we decide to handle killed threads that are holding
             * locks when killed.
             */
            if (cur->record->deleted) {
                if (found) {
                    *found = false;
                }
                return NULL;
            }

            record          = mmm_alloc(sizeof(ballcap_record_t));
            record->item    = item;
            record->deleted = false;

            ret = cur->record->item;
            if (found) {
                *found = true;
            }
            // Since we're overwriting a pre-existing record, we should
            // inherit its creation time, in terms of our sort order.
            mmm_copy_create_epoch(record, cur->record);
            record->next = cur->record;
            cur->record  = record;
            mmm_commit_write(record);
            mmm_retire(record->next);
            pthread_mutex_unlock(&cur->mutex);

            return ret;
        }

        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

bool
ballcap_store_add(ballcap_store_t *self,
                  ballcap_t       *top,
                  hatrack_hash_t  *hv,
                  void            *item)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    ballcap_bucket_t *cur;
    ballcap_record_t *record;

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
                pthread_mutex_unlock(&cur->mutex);
                return ballcap_store_add(top->store, top, hv, item);
            }
            if (!cur->record->deleted) {
                pthread_mutex_unlock(&cur->mutex);
                return false;
            }
            goto fill_record;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (pthread_mutex_lock(&cur->mutex)) {
                abort();
            }
            if (cur->migrated) {
                pthread_mutex_unlock(&cur->mutex);
                return ballcap_store_add(top->store, top, hv, item);
            }
            if (!hatrack_bucket_unreserved(&cur->hv)) {
                pthread_mutex_unlock(&cur->mutex);
                goto check_bucket_again;
            }
            if (self->used_count == self->threshold) {
                pthread_mutex_unlock(&cur->mutex);
                self = ballcap_store_migrate(self, top);
                return ballcap_store_add(self, top, hv, item);
            }
            self->used_count++;
            top->item_count++;
            cur->hv = *hv;

fill_record:
            record          = mmm_alloc(sizeof(ballcap_record_t));
            record->item    = item;
            record->next    = cur->record;
            record->deleted = false;
            cur->record     = record;
            mmm_commit_write(record);
            if (record->next) {
                mmm_retire(record->next);
            }
            pthread_mutex_unlock(&cur->mutex);

            return true;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

void *
ballcap_store_remove(ballcap_store_t *self,
                     ballcap_t       *top,
                     hatrack_hash_t  *hv,
                     bool            *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    ballcap_bucket_t *cur;
    ballcap_record_t *record;
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
                pthread_mutex_unlock(&cur->mutex);
                return ballcap_store_remove(top->store, top, hv, found);
            }
            if (cur->record->deleted) {
                if (found) {
                    *found = false;
                }
                pthread_mutex_unlock(&cur->mutex);
                return NULL;
            }

            ret             = cur->record->item;
            record          = mmm_alloc(sizeof(ballcap_record_t));
            record->item    = NULL;
            record->next    = cur->record;
            record->deleted = true;
            cur->record     = record;

            mmm_commit_write(record);
            mmm_retire(record->next);

            --top->item_count;

            if (found) {
                *found = true;
            }
            pthread_mutex_unlock(&cur->mutex);
            return ret;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static ballcap_store_t *
ballcap_store_migrate(ballcap_store_t *store, ballcap_t *top)
{
    ballcap_store_t  *new_store;
    ballcap_bucket_t *cur;
    ballcap_bucket_t *target;
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
        pthread_mutex_unlock(&top->migrate_mutex);
        return new_store;
    }
    cur_last_slot = store->last_slot;

    for (n = 0; n <= cur_last_slot; n++) {
        cur = &store->buckets[n];
        if (pthread_mutex_lock(&cur->mutex)) {
            abort();
        }
        if (cur->hv.w1 && cur->hv.w2 && !cur->record->deleted) {
            items_to_migrate++;
        }
    }

    new_size      = hatrack_new_size(cur_last_slot, items_to_migrate + 1);
    new_last_slot = new_size - 1;
    new_store     = ballcap_store_new(new_size);

    for (n = 0; n <= cur_last_slot; n++) {
        cur           = &store->buckets[n];
        cur->migrated = true;

        if (hatrack_bucket_unreserved(&cur->hv)) {
            continue;
        }
        if (cur->record->deleted) {
            mmm_retire(cur->record);
            continue;
        }
        bix = hatrack_bucket_index(&cur->hv, new_last_slot);
        for (i = 0; i < new_size; i++) {
            target = &new_store->buckets[bix];
            if (hatrack_bucket_unreserved(&target->hv)) {
                target->hv     = cur->hv;
                target->record = cur->record;
                break;
            }
            bix = (bix + 1) & new_last_slot;
        }
    }

    /* Once we install a new store, new writers may come into that new
     * store. If we really care about being fair, we can lock all the
     * buckets in the new store, unlock all the buckets in this store,
     * then go and free up the new store.  But we don't do that here.
     */
    new_store->used_count = top->item_count;
    top->store            = new_store;

    for (n = 0; n <= cur_last_slot; n++) {
        cur = &store->buckets[n];
        pthread_mutex_unlock(&cur->mutex);
    }

    mmm_retire(store);
    pthread_mutex_unlock(&top->migrate_mutex);

    return new_store;
}
