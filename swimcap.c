/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           swimcap.c
 *  Description:    Single WrIter, Multiple-read, Crappy, Albeit Parallel.
 *
 *  Author:         John Viega, john@zork.org
 *
 */
#include "swimcap.h"

// clang-format off

static void  swimcap_migrate(swimcap_t *);

// clang-format on

static swimcap_store_t *
swimcap_new_store(uint64_t size)
{
    swimcap_store_t *ret = (swimcap_store_t *)calloc(
        1,
        sizeof(swimcap_store_t) + size * sizeof(swimcap_bucket_t));
    ret->last_slot = size - 1;
    ret->threshold = hatrack_compute_table_threshold(size);
    return ret;
}

void
swimcap_init(swimcap_t *self)
{
    swimcap_store_t *store = swimcap_new_store(1 << HATRACK_MIN_SIZE_LOG);
    self->item_count       = 0;
#ifndef HATRACK_DONT_SORT
    self->next_epoch = 0;
#endif
    self->store = store;
    pthread_mutex_init(&self->write_mutex, NULL);

    return;
}

void *
swimcap_get(swimcap_t *self, hatrack_hash_t *hv, bool *found)
{
    swimcap_store_t  *store;
    uint64_t          bix;
    uint64_t          last_slot;
    uint64_t          i;
    swimcap_bucket_t *cur;

    store     = swimcap_reader_enter(self);
    last_slot = store->last_slot;

    bix = hatrack_bucket_index(hv, last_slot);
    for (i = 0; i <= last_slot; i++) {
        cur = &store->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (cur->deleted) {
                if (found) {
                    *found = false;
                }
                swimcap_reader_exit(store);
                return NULL;
            }
            if (found) {
                *found = true;
            }
            swimcap_reader_exit(store);
            return cur->item;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (found) {
                *found = false;
            }
            swimcap_reader_exit(store);
            return NULL;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

void *
swimcap_put(swimcap_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    swimcap_bucket_t *cur;
    swimcap_store_t  *store;
    void             *ret;

    //    if (found) abort();
    if (pthread_mutex_lock(&self->write_mutex))
        abort();
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

#ifndef HATRACK_DONT_SORT
                cur->epoch = self->next_epoch++;
#endif

                if (found) {
                    *found = false;
                }
                if (pthread_mutex_unlock(&self->write_mutex)) {
                    abort();
                }
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
            return ret;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (store->used_count + 1 == store->threshold) {
                swimcap_migrate(self);
                goto again_after_migration;
            }
            store->used_count++;
            self->item_count++;
            cur->hv   = *hv;
            cur->item = item;

#ifndef HATRACK_DONT_SORT
            cur->epoch = self->next_epoch++;
#endif

            if (found) {
                *found = false;
            }

            if (pthread_mutex_unlock(&self->write_mutex)) {
                abort();
            }
            return NULL;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

bool
swimcap_put_if_empty(swimcap_t *self, hatrack_hash_t *hv, void *item)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    swimcap_bucket_t *cur;
    swimcap_store_t  *store;

    if (pthread_mutex_lock(&self->write_mutex))
        abort();

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

#ifndef HATRACK_DONT_SORT
                cur->epoch = self->next_epoch++;
#endif
                if (pthread_mutex_unlock(&self->write_mutex)) {
                    abort();
                }
                return true;
            }
            if (pthread_mutex_unlock(&self->write_mutex)) {
                abort();
            }
            return false;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (store->used_count + 1 == store->threshold) {
                swimcap_migrate(self);
                goto again_after_migration;
            }
            store->used_count++;
            self->item_count++;
            cur->hv   = *hv;
            cur->item = item;

#ifndef HATRACK_DONT_SORT
            cur->epoch = self->next_epoch++;
#endif

            if (pthread_mutex_unlock(&self->write_mutex)) {
                abort();
            }
            return true;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

void *
swimcap_remove(swimcap_t *self, hatrack_hash_t *hv, bool *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    swimcap_bucket_t *cur;
    swimcap_store_t  *store;
    void             *ret;

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
            return ret;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (found) {
                *found = false;
            }
            if (pthread_mutex_unlock(&self->write_mutex)) {
                abort();
            }
            return NULL;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

void
swimcap_delete(swimcap_t *self)
{
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
swimcap_view(swimcap_t *self, uint64_t *num)
{
    hatrack_view_t  *view;
    swimcap_store_t *store;

    hatrack_view_t   *p;
    swimcap_bucket_t *cur;
    swimcap_bucket_t *end;
    uint64_t          count;
    uint64_t          last_slot;

    if (pthread_mutex_lock(&self->write_mutex))
        abort();
    store     = self->store;
    last_slot = store->last_slot;
    view = (hatrack_view_t *)malloc(sizeof(hatrack_view_t) * store->used_count);
    p    = view;
    cur  = store->buckets;
    end  = cur + (last_slot + 1);
    count = 0;

    while (cur < end) {
        if (cur->deleted || hatrack_bucket_unreserved(&cur->hv)) {
            cur++;
            continue;
        }
        p->hv   = cur->hv;
        p->item = cur->item;
#ifdef HATRACK_DONT_SORT
        p->sort_epoch = 0; // No sort info.
#else
        p->sort_epoch = cur->epoch;
#endif
        count++;
        p++;
        cur++;
    }

    *num = count;
    view = (hatrack_view_t *)realloc(view, sizeof(hatrack_view_t) * count);

#ifndef HATRACK_DONT_SORT
    qsort(view, count, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
#endif

    if (pthread_mutex_unlock(&self->write_mutex))
        abort();

    return view;
}

#include <assert.h>

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

    new_size = (cur_last_slot + 1) << 2;
    if (self->item_count > new_size / 2) {
        new_size <<= 1;
    }

    assert(!(new_size & (new_size - 1)));

    new_last_slot = new_size - 1;
    new_store     = swimcap_new_store(new_size);

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

    while (atomic_load(&cur_store->readers))
        ;
    free(cur_store);

    return;
}

void *
swimcap_base_put(swimcap_t      *self,
                 hatrack_hash_t *hv,
                 void           *item,
                 bool            ifempty,
                 bool           *found)
{
    bool bool_ret;

    if (ifempty) {
        bool_ret = swimcap_put_if_empty(self, hv, item);

        return (void *)bool_ret;
    }

    return swimcap_put(self, hv, item, found);
}
