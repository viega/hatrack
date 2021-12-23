/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           newshat.c
 *  Description:    Single WrIter, Multiple-read, Crappy, Albeit Parallel.
 *
 *  Author:         John Viega, john@zork.org
 *
 */
#include "newshat.h"

// clang-format off

static newshat_store_t *newshat_store_new         (uint64_t);
static void             newshat_store_delete      (newshat_store_t *);
static void             newshat_store_migrate(newshat_store_t *store, newshat_t *top);
#if 0
static void            *newshat_store_get         (newshat_store_t *,
						   newshat_t *,
						   hatrack_hash_t *,
						   bool *);
static void            *newshat_store_put         (newshat_store_t *,
						   newshat_t *,
						   hatrack_hash_t *,
						   void *,
						   bool *);
static bool             newshat_store_put_if_empty(newshat_store_t *,
						   newshat_t *,
						   hatrack_hash_t *,
						   void *);
static void            *newshat_store_remove      (newshat_store_t *,
						   newshat_t *,
						   hatrack_hash_t *,
						   bool *);
static newshat_store_t *newshat_store_migrate     (newshat_store_t *,
						   newshat_t *);
static hatrack_view_t  *newshat_store_view        (newshat_store_t *,
						   newshat_t *,
						   uint64_t,
						   uint64_t *);
#endif

// clang-format on

void
newshat_init(newshat_t *self)
{
    newshat_store_t *store = newshat_store_new(1 << HATRACK_MIN_SIZE_LOG);
    self->item_count       = 0;
    self->store            = store;
    pthread_mutex_init(&self->migrate_mutex, NULL);

    return;
}

void *
newshat_get(newshat_t *self, hatrack_hash_t *hv, bool *found)
{
    newshat_store_t  *store;
    uint64_t          bix;
    uint64_t          last_slot;
    uint64_t          i;
    newshat_bucket_t *cur;

    mmm_start_basic_op();
    store     = self->store;
    last_slot = store->last_slot;

    bix = hatrack_bucket_index(hv, last_slot);
    for (i = 0; i <= last_slot; i++) {
        cur = &store->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (cur->deleted) {
                if (found) {
                    *found = false;
                }
                mmm_end_op();
                return NULL;
            }
            if (found) {
                *found = true;
            }
            mmm_end_op();
            return cur->item;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (found) {
                *found = false;
            }
            mmm_end_op();
            return NULL;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

void *
newshat_base_put(newshat_t      *self,
                 hatrack_hash_t *hv,
                 void           *item,
                 bool            ifempty,
                 bool           *found)
{
    bool bool_ret;

    if (ifempty) {
        bool_ret = newshat_put_if_empty(self, hv, item);

        return (void *)bool_ret;
    }

    return newshat_put(self, hv, item, found);
}

void *
newshat_put(newshat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    newshat_bucket_t *cur;
    newshat_store_t  *store;
    void             *ret;

    mmm_start_basic_op();
again_after_migration:
    store     = self->store;
    last_slot = store->last_slot;

    bix = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &store->buckets[bix];
check_bucket_again:
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (pthread_mutex_lock(&cur->write_mutex)) {
                abort();
            }
            if (cur->migrated) {
                pthread_mutex_unlock(&cur->write_mutex);
                goto again_after_migration;
            }
            if (cur->deleted) {
                cur->item    = item;
                cur->deleted = false;
                self->item_count++;
                if (found) {
                    *found = false;
                }
                pthread_mutex_unlock(&cur->write_mutex);
                mmm_end_op();
                return NULL;
            }
            ret       = cur->item;
            cur->item = item;
            if (found) {
                *found = true;
            }
            pthread_mutex_unlock(&cur->write_mutex);
            mmm_end_op();
            return ret;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (pthread_mutex_lock(&cur->write_mutex)) {
                abort();
            }
            if (cur->migrated) {
                pthread_mutex_unlock(&cur->write_mutex);
                goto again_after_migration;
            }
            if (!hatrack_bucket_unreserved(&cur->hv)) {
                pthread_mutex_unlock(&cur->write_mutex);
                goto check_bucket_again;
            }
            if (store->used_count == store->threshold) {
                pthread_mutex_unlock(&cur->write_mutex);
                newshat_store_migrate(store, self);
                goto again_after_migration;
            }
            store->used_count++;
            self->item_count++;
            cur->hv   = *hv;
            cur->item = item;
            if (found) {
                *found = false;
            }
            pthread_mutex_unlock(&cur->write_mutex);
            mmm_end_op();
            return NULL;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

bool
newshat_put_if_empty(newshat_t *self, hatrack_hash_t *hv, void *item)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    newshat_bucket_t *cur;
    newshat_store_t  *store;

    mmm_start_basic_op();
again_after_migration:
    store     = self->store;
    last_slot = store->last_slot;
    bix       = hatrack_bucket_index(hv, store->last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &store->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (pthread_mutex_lock(&cur->write_mutex)) {
                abort();
            }
            if (cur->migrated) {
                pthread_mutex_unlock(&cur->write_mutex);
                goto again_after_migration;
            }
            if (cur->deleted) {
                cur->item    = item;
                cur->deleted = false;
                self->item_count++;
                pthread_mutex_unlock(&cur->write_mutex);
                mmm_end_op();
                return true;
            }
            pthread_mutex_unlock(&cur->write_mutex);
            mmm_end_op();
            return false;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (pthread_mutex_lock(&cur->write_mutex)) {
                abort();
            }
            if (cur->migrated) {
                pthread_mutex_unlock(&cur->write_mutex);
                goto again_after_migration;
            }
            if (store->used_count == store->threshold) {
                pthread_mutex_unlock(&cur->write_mutex);
                newshat_store_migrate(store, self);
                goto again_after_migration;
            }
            store->used_count++;
            self->item_count++;
            cur->hv   = *hv;
            cur->item = item;
            pthread_mutex_unlock(&cur->write_mutex);
            mmm_end_op();
            return true;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

void *
newshat_remove(newshat_t *self, hatrack_hash_t *hv, bool *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    newshat_bucket_t *cur;
    newshat_store_t  *store;
    void             *ret;

    mmm_start_basic_op();
again_after_migration:
    store     = self->store;
    last_slot = store->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &store->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (pthread_mutex_lock(&cur->write_mutex)) {
                abort();
            }
            if (cur->migrated) {
                pthread_mutex_unlock(&cur->write_mutex);
                goto again_after_migration;
            }
            if (cur->deleted) {
                if (found) {
                    *found = false;
                }
                pthread_mutex_unlock(&cur->write_mutex);
                mmm_end_op();
                return NULL;
            }

            ret          = cur->item;
            cur->deleted = true;
            --self->item_count;

            if (found) {
                *found = true;
            }
            pthread_mutex_unlock(&cur->write_mutex);
            mmm_end_op();
            return ret;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (found) {
                *found = false;
            }
            pthread_mutex_unlock(&cur->write_mutex);
            mmm_end_op();
            return NULL;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
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
newshat_view(newshat_t *self, uint64_t *num)
{
    hatrack_view_t  *view;
    newshat_store_t *store;

    hatrack_view_t   *p;
    newshat_bucket_t *cur;
    newshat_bucket_t *end;
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
        if (cur->deleted || hatrack_bucket_unreserved(&cur->hv)) {
            cur++;
            continue;
        }
        p->hv         = cur->hv;
        p->item       = cur->item;
        p->sort_epoch = mmm_get_write_epoch(cur);
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

#ifndef HATRACK_DONT_SORT
    qsort(view, count, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
#endif

    mmm_end_op();

    return view;
}

static newshat_store_t *
newshat_store_new(uint64_t size)
{
    newshat_store_t *ret;
    uint64_t         i;

    ret = (newshat_store_t *)mmm_alloc_committed(
        sizeof(newshat_store_t) + size * sizeof(newshat_bucket_t));
    mmm_add_cleanup_handler(ret, (void (*)(void *))newshat_store_delete);
    ret->last_slot = size - 1;
    ret->threshold = hatrack_compute_table_threshold(size);

    for (i = 0; i <= ret->last_slot; i++) {
        pthread_mutex_init(&ret->buckets[i].write_mutex, NULL);
    }

    return ret;
}

static void
newshat_store_delete(newshat_store_t *self)
{
    uint64_t i;

    for (i = 0; i <= self->last_slot; i++) {
        pthread_mutex_destroy(&self->buckets[i].write_mutex);
    }
}

static void
newshat_store_migrate(newshat_store_t *store, newshat_t *top)
{
    newshat_store_t  *new_store;
    newshat_bucket_t *cur;
    newshat_bucket_t *target;
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
        pthread_mutex_unlock(&top->migrate_mutex);
        return;
    }
    cur_last_slot = store->last_slot;

    for (n = 0; n <= cur_last_slot; n++) {
        cur = &store->buckets[n];
        if (pthread_mutex_lock(&cur->write_mutex)) {
            abort();
        }
        if (cur->hv.w1 && cur->hv.w2 && !cur->deleted) {
            items_to_migrate++;
        }
    }

    new_size      = hatrack_new_size(cur_last_slot, items_to_migrate + 1);
    new_last_slot = new_size - 1;
    new_store     = newshat_store_new(new_size);

    for (n = 0; n <= cur_last_slot; n++) {
        cur = &store->buckets[n];
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
        pthread_mutex_unlock(&cur->write_mutex);
    }

    mmm_retire(store);
    pthread_mutex_unlock(&top->migrate_mutex);

    return;
}
