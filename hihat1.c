#include "hihat1.h"

// clang-format off

static hihat1_store_t  *hihat1_store_new(uint64_t);
static void            *hihat1_store_get(hihat1_store_t *, hihat1_t *,
					 hatrack_hash_t *, bool *);
static void            *hihat1_store_put(hihat1_store_t *, hihat1_t *,
					 hatrack_hash_t *, void *, bool *);
static bool             hihat1_store_put_if_empty(hihat1_store_t *,
						  hihat1_t *,
						  hatrack_hash_t *,
						  void *);
static void            *hihat1_store_remove(hihat1_store_t *, hihat1_t *,
					    hatrack_hash_t *, bool *);
static hihat1_store_t *hihat1_store_migrate(hihat1_store_t *, hihat1_t *);
static hatrack_view_t   *hihat1_store_view(hihat1_store_t *, hihat1_t *,
					   uint64_t *);

// clang-format on

void
hihat1_init(hihat1_t *self)
{
    hihat1_store_t *store = hihat1_store_new(1 << HATRACK_MIN_SIZE_LOG);

    self->epoch = 0;
    atomic_store(&self->store_current, store);
}

void *
hihat1_get(hihat1_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = hihat1_store_get(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
hihat1_put(hihat1_t       *self,
           hatrack_hash_t *hv,
           void           *item,
           bool            ifempty,
           bool           *found)
{
    void *ret;
    bool  bool_ret;

    mmm_start_basic_op();
    if (ifempty) {
        bool_ret
            = hihat1_store_put_if_empty(self->store_current, self, hv, item);
        mmm_end_op();

        return (void *)bool_ret;
    }

    ret = hihat1_store_put(self->store_current, self, hv, item, found);
    mmm_end_op();

    return ret;
}

void *
hihat1_remove(hihat1_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = hihat1_store_remove(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void
hihat1_delete(hihat1_t *self)
{
    hihat1_store_t *store = atomic_load(&self->store_current);

    mmm_retire(store);
    free(self);
}

uint64_t
hihat1_len(hihat1_t *self)
{
    return self->store_current->used_count - self->store_current->del_count;
}

// This version cannot be linearized.
hatrack_view_t *
hihat1_view(hihat1_t *self, uint64_t *num_items)
{
    hatrack_view_t *ret;

    mmm_start_basic_op();
    ret = hihat1_store_view(self->store_current, self, num_items);
    mmm_end_op();

    return ret;
}

static hihat1_store_t *
hihat1_store_new(uint64_t size)
{
    hihat1_store_t *store;
    uint64_t        alloc_len;

    alloc_len = sizeof(hihat1_store_t) + sizeof(hihat1_bucket_t) * size;
    store     = (hihat1_store_t *)mmm_alloc_committed(alloc_len);

    store->last_slot  = size - 1;
    store->threshold  = hatrack_compute_table_threshold(size);
    store->used_count = ATOMIC_VAR_INIT(0);
    store->del_count  = ATOMIC_VAR_INIT(0);
    store->store_next = ATOMIC_VAR_INIT(NULL);

    return store;
}

static void *
hihat1_store_get(hihat1_store_t *self,
                 hihat1_t       *top,
                 hatrack_hash_t *hv1,
                 bool           *found)
{
    uint64_t         bix = hatrack_bucket_index(hv1, self->last_slot);
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat1_bucket_t *bucket;
    hihat1_record_t  record;

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2    = atomic_load(&bucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            goto not_found;
        }
        if (!hatrack_hashes_eq(hv1, &hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        record = atomic_load(&bucket->record);
        if (record.info & HIHAT_F_USED) {
            if (found) {
                *found = true;
            }
            return record.item;
        }
        break;
    }
not_found:
    if (found) {
        *found = false;
    }
    return NULL;
}

static void *
hihat1_store_put(hihat1_store_t *self,
                 hihat1_t       *top,
                 hatrack_hash_t *hv1,
                 void           *item,
                 bool           *found)
{
    void            *old_item;
    uint64_t         bix = hatrack_bucket_index(hv1, self->last_slot);
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat1_bucket_t *bucket;
    hihat1_record_t  record;
    hihat1_record_t  candidate;

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2.w1 = 0;
        hv2.w2 = 0;
        if (!LCAS(&bucket->hv, &hv2, *hv1, HIHAT1_CTR_BUCKET_ACQUIRE)) {
            if (!hatrack_hashes_eq(hv1, &hv2)) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
        else {
            if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
                return hihat1_store_put(hihat1_store_migrate(self, top),
                                        top,
                                        hv1,
                                        item,
                                        found);
            }
        }
        goto found_bucket;
    }
    return hihat1_store_put(hihat1_store_migrate(self, top),
                            top,
                            hv1,
                            item,
                            found);

found_bucket:
    record = atomic_load(&bucket->record);
    if (record.info & HIHAT_F_MOVING) {
        return hihat1_store_put(hihat1_store_migrate(self, top),
                                top,
                                hv1,
                                item,
                                found);
    }

    if (found) {
        if (record.info & HIHAT_F_USED) {
            *found = true;
        }
        else {
            *found = false;
        }
    }

    old_item       = record.item;
    candidate.item = item;
    candidate.info = top->epoch++ | HIHAT_F_USED;

    if (LCAS(&bucket->record, &record, candidate, HIHAT1_CTR_REC_INSTALL)) {
        if (record.info & HIHAT_F_RMD) {
            atomic_fetch_sub(&self->del_count, 1);
        }
        return old_item;
    }

    return item;
}

static bool
hihat1_store_put_if_empty(hihat1_store_t *self,
                          hihat1_t       *top,
                          hatrack_hash_t *hv1,
                          void           *item)
{
    uint64_t         bix = hatrack_bucket_index(hv1, self->last_slot);
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat1_bucket_t *bucket;
    hihat1_record_t  record;
    hihat1_record_t  candidate;

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2.w1 = 0;
        hv2.w2 = 0;
        if (!LCAS(&bucket->hv, &hv2, *hv1, HIHAT1_CTR_BUCKET_ACQUIRE)) {
            if (!hatrack_hashes_eq(hv1, &hv2)) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
        else {
            if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
                return hihat1_store_put_if_empty(
                    hihat1_store_migrate(self, top),
                    top,
                    hv1,
                    item);
            }
        }
        goto found_bucket;
    }
    return hihat1_store_put_if_empty(hihat1_store_migrate(self, top),
                                     top,
                                     hv1,
                                     item);

found_bucket:
    record = atomic_load(&bucket->record);
    if (record.info & HIHAT_F_MOVING) {
        return hihat1_store_put_if_empty(hihat1_store_migrate(self, top),
                                         top,
                                         hv1,
                                         item);
    }
    if (record.info & HIHAT_F_USED) {
        return false;
    }

    candidate.item = item;
    candidate.info = top->epoch++ | HIHAT_F_USED;

    if (LCAS(&bucket->record, &record, candidate, HIHAT1_CTR_REC_INSTALL)) {
        if (record.info & HIHAT_F_RMD) {
            atomic_fetch_sub(&self->del_count, 1);
        }
        return true;
    }

    return false;
}

static void *
hihat1_store_remove(hihat1_store_t *self,
                    hihat1_t       *top,
                    hatrack_hash_t *hv1,
                    bool           *found)
{
    void            *old_item;
    uint64_t         bix = hatrack_bucket_index(hv1, self->last_slot);
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat1_bucket_t *bucket;
    hihat1_record_t  record;
    hihat1_record_t  candidate;

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2    = atomic_load(&bucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            break;
        }
        if (!hatrack_hashes_eq(hv1, &hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        goto found_bucket;
    }

    if (found) {
        *found = false;
    }

    return NULL;

found_bucket:
    record = atomic_load(&bucket->record);
    if (record.info & HIHAT_F_MOVING) {
        return hihat1_store_remove(hihat1_store_migrate(self, top),
                                   top,
                                   hv1,
                                   found);
    }
    if (!(record.info & HIHAT_F_USED)) {
        if (found) {
            *found = false;
        }

        return NULL;
    }

    old_item       = record.item;
    candidate.item = NULL;
    candidate.info = HIHAT_F_RMD;

    if (LCAS(&bucket->record, &record, candidate, HIHAT1_CTR_DEL)) {
        atomic_fetch_add(&self->del_count, 1);

        if (found) {
            *found = true;
        }
        return old_item;
    }

    if (found) {
        *found = false;
    }
    return NULL;
}

static hihat1_store_t *
hihat1_store_migrate(hihat1_store_t *self, hihat1_t *top)
{
    hihat1_store_t  *new_store;
    hihat1_store_t  *candidate_store;
    uint64_t         new_size;
    hihat1_bucket_t *bucket;
    hihat1_bucket_t *new_bucket;
    hihat1_record_t  record;
    hihat1_record_t  candidate_record;
    hihat1_record_t  expected_record;
    hatrack_hash_t   expected_hv;
    hatrack_hash_t   hv;
    uint64_t         i, j;
    uint64_t         bix;
    uint64_t         new_used      = 0;
    uint64_t         expected_used = ~0;

    // Quickly run through every history bucket, and mark any bucket
    // that doesn't already have F_MOVING set.  Note that the CAS
    // could fail due to some other updater, so we keep CASing until
    // we know it was successful.
    for (i = 0; i <= self->last_slot; i++) {
        bucket                = &self->buckets[i];
        record                = atomic_load(&bucket->record);
        candidate_record.info = record.info | HIHAT_F_MOVING;
        candidate_record.item = record.item;

        do {
            if (record.info & HIHAT_F_MOVING) {
                break;
            }
        } while (!LCAS(&bucket->record,
                       &record,
                       candidate_record,
                       HIHAT1_CTR_F_MOVING));

        if (record.info & HIHAT_F_USED) {
            new_used++;
        }
    }

    new_store = atomic_load(&self->store_next);

    // If we couldn't acquire a store, try to install one. If we fail, free it.
    if (!new_store) {
        new_size        = hatrack_new_size(self->last_slot, new_used);
        candidate_store = hihat1_store_new(new_size);
        // This helps address a potential race condition, where
        // someone could drain the table after resize, having
        // us swap in the wrong length.
        atomic_store(&candidate_store->used_count, ~0);

        if (!LCAS(&self->store_next,
                  &new_store,
                  candidate_store,
                  HIHAT1_CTR_NEW_STORE)) {
            mmm_retire_unused(candidate_store);
        }
        else {
            new_store = candidate_store;
        }
    }

    // At this point, we're sure that any late writers will help us
    // with the migration. Therefore, we can go through each item,
    // and, if it's not fully migrated, we can attempt to migrate it.

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[i];
        record = atomic_load(&bucket->record);

        if (record.info & HIHAT_F_MOVED) {
            continue;
        }

        // If the bucket has been rm'd, or has never been used...
        if ((record.info & HIHAT_F_RMD) || !(record.info & HIHAT_F_USED)) {
            candidate_record.info = record.info | HIHAT_F_MOVED;
            candidate_record.item = record.item;
            LCAS(&bucket->record,
                 &record,
                 candidate_record,
                 HIHAT1_CTR_F_MOVED1);
            continue;
        }

        hv  = atomic_load(&bucket->hv);
        bix = hatrack_bucket_index(&hv, new_store->last_slot);

        for (j = 0; j <= new_store->last_slot; j++) {
            new_bucket     = &new_store->buckets[bix];
            expected_hv.w1 = 0;
            expected_hv.w2 = 0;
            if (!LCAS(&new_bucket->hv,
                      &expected_hv,
                      hv,
                      HIHAT1_CTR_MIGRATE_HV)) {
                if (!hatrack_hashes_eq(&expected_hv, &hv)) {
                    bix = (bix + 1) & new_store->last_slot;
                    continue;
                }
            }
            break;
        }

        candidate_record.info = record.info & HIHAT_F_MASK;
        candidate_record.item = record.item;
        expected_record.info  = 0;
        expected_record.item  = NULL;

        LCAS(&new_bucket->record,
             &expected_record,
             candidate_record,
             HIHAT1_CTR_MIG_REC);
        candidate_record.info = record.info | HIHAT_F_MOVED;
        LCAS(&bucket->record, &record, candidate_record, HIHAT1_CTR_F_MOVED2);
    }

    LCAS(&new_store->used_count,
         &expected_used,
         new_used,
         HIHAT1_CTR_LEN_INSTALL);

    if (LCAS(&top->store_current, &self, new_store, HIHAT1_CTR_STORE_INSTALL)) {
        mmm_retire(self);
    }

    return new_store;
}

static hatrack_view_t *
hihat1_store_view(hihat1_store_t *self, hihat1_t *top, uint64_t *num)
{
    hatrack_view_t  *view;
    hatrack_view_t  *p;
    hatrack_hash_t   hv;
    hihat1_bucket_t *cur;
    hihat1_bucket_t *end;
    hihat1_record_t  record;
    uint64_t         num_items;
    uint64_t         alloc_len;

    alloc_len = sizeof(hatrack_view_t) * (self->last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = self->buckets;
    end       = cur + (self->last_slot + 1);

    while (cur < end) {
        hv            = atomic_load(&cur->hv);
        record        = atomic_load(&cur->record);
        p->hv         = hv;
        p->item       = record.item;
        p->sort_epoch = record.info & HIHAT_F_MASK;

        p++;
        cur++;
    }

    num_items = p - view;
    *num      = num_items;

    if (!num_items) {
        free(view);
        return NULL;
    }

    view = realloc(view, num_items * sizeof(hatrack_view_t));

    // Unordered buckets should be in random order, so quicksort is a
    // good option.
    qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);

    return view;
}
