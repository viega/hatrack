/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           swimcap2.c
 *  Description:    Single WrIter, Multiple-read, Crappy, Albeit Parallel.
 *
 *  Author:         John Viega, john@zork.org
 *
 */
#include "swimcap2.h"

// clang-format off

static void  swimcap2_migrate(swimcap2_t *);

// clang-format on

static swimcap2_store_t *
swimcap2_new_store(uint64_t size)
{
    swimcap2_store_t *ret;

    ret = (swimcap2_store_t *)mmm_alloc_committed(
        sizeof(swimcap2_store_t) + size * sizeof(swimcap2_bucket_t));
    ret->last_slot = size - 1;
    ret->threshold = hatrack_compute_table_threshold(size);
    return ret;
}

void
swimcap2_init(swimcap2_t *self)
{
    swimcap2_store_t *store = swimcap2_new_store(1 << HATRACK_MIN_SIZE_LOG);
    self->item_count        = 0;
    self->store             = store;
    pthread_mutex_init(&self->write_mutex, NULL);

    return;
}

void *
swimcap2_get(swimcap2_t *self, hatrack_hash_t *hv, bool *found)
{
    swimcap2_store_t  *store;
    uint64_t           bix;
    uint64_t           last_slot;
    uint64_t           i;
    swimcap2_bucket_t *cur;

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
swimcap2_put(swimcap2_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    uint64_t           bix;
    uint64_t           i;
    uint64_t           last_slot;
    swimcap2_bucket_t *cur;
    swimcap2_store_t  *store;
    void              *ret;

    mmm_start_basic_op();
    if (pthread_mutex_lock(&self->write_mutex)) {
        abort();
    }
again_after_migration:
    store     = self->store;
    last_slot = store->last_slot;

    bix = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &store->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (cur->deleted) {
                cur->item    = item;
                cur->deleted = false;
                self->item_count++;
                if (found) {
                    *found = false;
                }
                if (pthread_mutex_unlock(&self->write_mutex)) {
                    abort();
                }
                mmm_end_op();
                return NULL;
            }
            ret       = cur->item;
            cur->item = item;
            if (found) {
                *found = true;
            }
            if (pthread_mutex_unlock(&self->write_mutex)) {
                abort();
            }
            mmm_end_op();
            return ret;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (store->used_count + 1 == store->threshold) {
                swimcap2_migrate(self);
                goto again_after_migration;
            }
            store->used_count++;
            self->item_count++;
            cur->hv   = *hv;
            cur->item = item;
            if (found) {
                *found = false;
            }

            if (pthread_mutex_unlock(&self->write_mutex)) {
                abort();
            }
            mmm_end_op();
            return NULL;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

bool
swimcap2_put_if_empty(swimcap2_t *self, hatrack_hash_t *hv, void *item)
{
    uint64_t           bix;
    uint64_t           i;
    uint64_t           last_slot;
    swimcap2_bucket_t *cur;
    swimcap2_store_t  *store;

    mmm_start_basic_op();
    if (pthread_mutex_lock(&self->write_mutex)) {
        abort();
    }

again_after_migration:
    store     = self->store;
    last_slot = store->last_slot;
    bix       = hatrack_bucket_index(hv, store->last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &store->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (cur->deleted) {
                cur->item    = item;
                cur->deleted = false;
                self->item_count++;
                if (pthread_mutex_unlock(&self->write_mutex)) {
                    abort();
                }
                mmm_end_op();
                return true;
            }
            if (pthread_mutex_unlock(&self->write_mutex)) {
                abort();
            }
            mmm_end_op();
            return false;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (store->used_count + 1 == store->threshold) {
                swimcap2_migrate(self);
                goto again_after_migration;
            }
            store->used_count++;
            self->item_count++;
            cur->hv   = *hv;
            cur->item = item;
            if (pthread_mutex_unlock(&self->write_mutex)) {
                abort();
            }
            mmm_end_op();
            return true;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

void *
swimcap2_remove(swimcap2_t *self, hatrack_hash_t *hv, bool *found)
{
    uint64_t           bix;
    uint64_t           i;
    uint64_t           last_slot;
    swimcap2_bucket_t *cur;
    swimcap2_store_t  *store;
    void              *ret;

    mmm_start_basic_op();
    if (pthread_mutex_lock(&self->write_mutex))
        abort();

    store     = self->store;
    last_slot = store->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &store->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (cur->deleted) {
                if (found) {
                    *found = false;
                }
                if (pthread_mutex_unlock(&self->write_mutex)) {
                    abort();
                }
                mmm_end_op();
                return NULL;
            }

            ret          = cur->item;
            cur->deleted = true;
            --self->item_count;

            if (found) {
                *found = true;
            }
            if (pthread_mutex_unlock(&self->write_mutex)) {
                abort();
            }
            mmm_end_op();
            return ret;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (found) {
                *found = false;
            }
            if (pthread_mutex_unlock(&self->write_mutex)) {
                abort();
            }
            mmm_end_op();
            return NULL;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

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
swimcap2_view(swimcap2_t *self, uint64_t *num)
{
    hatrack_view_t   *view;
    swimcap2_store_t *store;

    hatrack_view_t    *p;
    swimcap2_bucket_t *cur;
    swimcap2_bucket_t *end;
    uint64_t           count;
    uint64_t           last_slot;

    mmm_start_basic_op();
    store     = self->store;
    last_slot = store->last_slot;
    view      = (hatrack_view_t *)malloc(sizeof(hatrack_view_t)
                                    * (store->last_slot + 1));
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

static void
swimcap2_migrate(swimcap2_t *self)
{
    swimcap2_store_t  *cur_store;
    swimcap2_store_t  *new_store;
    swimcap2_bucket_t *cur;
    swimcap2_bucket_t *target;
    uint64_t           new_size;
    uint64_t           cur_last_slot;
    uint64_t           new_last_slot;
    uint64_t           i, n, bix;

    cur_store     = self->store;
    cur_last_slot = cur_store->last_slot;

    new_size = (cur_last_slot + 1) << 2;
    if (self->item_count > new_size / 2) {
        new_size <<= 1;
    }

    new_last_slot = new_size - 1;
    new_store     = swimcap2_new_store(new_size);

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

void *
swimcap2_base_put(swimcap2_t     *self,
                  hatrack_hash_t *hv,
                  void           *item,
                  bool            ifempty,
                  bool           *found)
{
    bool bool_ret;

    if (ifempty) {
        bool_ret = swimcap2_put_if_empty(self, hv, item);

        return (void *)bool_ret;
    }

    return swimcap2_put(self, hv, item, found);
}
