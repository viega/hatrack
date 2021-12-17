/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           refhat0.c
 *  Description:    A reference hashtable that only works single-threaded.
 *
 *  Author:         John Viega, john@zork.org
 *
 */
#include "refhat0.h"

// clang-format off

static void  refhat0_migrate(refhat0_t *);
static void *refhat0_base_put(refhat0_t *, hatrack_hash_t *, void *, bool,
			      bool *);

#ifndef HATRACK_DONT_SORT
int refhat0_quicksort_cmp(const void *, const void *);
#endif

const hatrack_vtable_t refhat0_vtable = {
    .init   = (hatrack_init_func)refhat0_init,
    .get    = (hatrack_get_func)refhat0_get,
    .put    = (hatrack_put_func)refhat0_base_put,
    .remove = (hatrack_remove_func)refhat0_remove,
    .delete = (hatrack_delete_func)refhat0_delete,
    .len    = (hatrack_len_func)refhat0_len,
    .view   = (hatrack_view_func)refhat0_view
};

// clang-format on

void
refhat0_init(refhat0_t *self)
{
    uint64_t size;

    size             = 1 << HATRACK_MIN_SIZE_LOG;
    self->last_slot  = size - 1;
    self->threshold  = hatrack_compute_table_threshold(size);
    self->used_count = 0;
    self->item_count = 0;
    self->buckets = (refhat0_bucket_t *)calloc(size, sizeof(refhat0_bucket_t));

#ifndef HATRACK_DONT_SORT
    self->next_epoch = 0;
#endif
}

void *
refhat0_get(refhat0_t *self, hatrack_hash_t *hv, bool *found)
{
    uint64_t          bix = hatrack_bucket_index(hv, self->last_slot);
    uint64_t          i;
    refhat0_bucket_t *cur;

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
refhat0_put(refhat0_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    uint64_t          bix = hatrack_bucket_index(hv, self->last_slot);
    uint64_t          i;
    refhat0_bucket_t *cur;
    void             *ret;

    for (i = 0; i <= self->last_slot; i++) {
        cur = &self->buckets[bix];
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
                refhat0_migrate(self);
                return refhat0_put(self, hv, item, found);
            }
            self->used_count++;
            self->item_count++;
            cur->hv   = *hv;
            cur->item = item;

#ifndef HATRACK_DONT_SORT
            cur->epoch = self->next_epoch++;
#endif

            if (found) {
                *found = false;
            }
            return NULL;
        }
        bix = (bix + 1) & self->last_slot;
    }
    __builtin_unreachable();
}

bool
refhat0_put_if_empty(refhat0_t *self, hatrack_hash_t *hv, void *item)
{
    uint64_t          bix = hatrack_bucket_index(hv, self->last_slot);
    uint64_t          i;
    refhat0_bucket_t *cur;

    for (i = 0; i <= self->last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (cur->deleted) {
                cur->item    = item;
                cur->deleted = false;
                self->item_count++;

#ifndef HATRACK_DONT_SORT
                cur->epoch = self->next_epoch++;
#endif

                return true;
            }
            return false;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (self->used_count + 1 == self->threshold) {
                refhat0_migrate(self);
                return refhat0_put_if_empty(self, hv, item);
            }
            self->used_count++;
            self->item_count++;
            cur->hv   = *hv;
            cur->item = item;

#ifndef HATRACK_DONT_SORT
            cur->epoch = self->next_epoch++;
#endif

            return true;
        }
        bix = (bix + 1) & self->last_slot;
    }
    __builtin_unreachable();
}

void *
refhat0_remove(refhat0_t *self, hatrack_hash_t *hv, bool *found)
{
    uint64_t          bix = hatrack_bucket_index(hv, self->last_slot);
    uint64_t          i;
    refhat0_bucket_t *cur;
    void             *ret;

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
refhat0_delete(refhat0_t *self)
{
    free(self->buckets);
    free(self);
}

uint64_t
refhat0_len(refhat0_t *self)
{
    return self->item_count;
}

hatrack_view_t *
refhat0_view(refhat0_t *self, uint64_t *num)
{
    hatrack_view_t   *view;
    hatrack_view_t   *p;
    refhat0_bucket_t *cur;
    refhat0_bucket_t *end;

    view = (hatrack_view_t *)malloc(sizeof(hatrack_view_t) * self->item_count);
    p    = view;
    cur  = self->buckets;
    end  = cur + (self->last_slot + 1);

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
        p++;
        cur++;
    }

    *num = self->item_count;

#ifndef HATRACK_DONT_SORT
    qsort(view,
          self->item_count,
          sizeof(hatrack_view_t),
          refhat0_quicksort_cmp);
#endif

    return view;
}

static void
refhat0_migrate(refhat0_t *self)
{
    refhat0_bucket_t *new_buckets;
    refhat0_bucket_t *cur;
    refhat0_bucket_t *target;
    uint64_t          new_size;
    uint64_t          new_last_slot;
    uint64_t          i, n, bix;

    new_size = (self->last_slot + 1) << 2;
    if (self->item_count > new_size / 2) {
        new_size <<= 1;
    }

    new_last_slot = new_size - 1;

    new_buckets
        = (refhat0_bucket_t *)calloc(new_size, sizeof(refhat0_bucket_t));
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

static void *
refhat0_base_put(refhat0_t      *self,
                 hatrack_hash_t *hv,
                 void           *item,
                 bool            ifempty,
                 bool           *found)
{
    bool bool_ret;

    if (ifempty) {
        bool_ret = refhat0_put_if_empty(self, hv, item);

        return (void *)bool_ret;
    }

    return refhat0_put(self, hv, item, found);
}

#ifndef HATRACK_DONT_SORT
int
refhat0_quicksort_cmp(const void *bucket1, const void *bucket2)
{
    hatrack_view_t *item1 = (hatrack_view_t *)bucket1;
    hatrack_view_t *item2 = (hatrack_view_t *)bucket2;

    return item1->sort_epoch - item2->sort_epoch;
}

#endif
