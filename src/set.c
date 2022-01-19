/*
 * Copyright © 2022 John Viega
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
 *  Name:           set.c
 *  Description:    Higher-level set based on woolhat.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

static hatrack_hash_t hatrack_set_get_hash_value(hatrack_set_t *, void *);
static void hatrack_set_record_eject(woolhat_record_t *, hatrack_set_t *);
static int  hatrack_set_hv_sort_cmp(const void *, const void *);
static int  hatrack_set_epoch_sort_cmp(const void *, const void *);

hatrack_set_t *
hatrack_set_new(uint32_t item_type)
{
    hatrack_set_t *ret;

    ret = (hatrack_set_t *)malloc(sizeof(hatrack_set_t));

    hatrack_set_init(ret, item_type);

    return ret;
}

void
hatrack_set_init(hatrack_set_t *self, uint32_t item_type)
{
    woolhat_init(&self->woolhat_instance);

    switch (item_type) {
    case HATRACK_DICT_KEY_TYPE_INT:
    case HATRACK_DICT_KEY_TYPE_REAL:
    case HATRACK_DICT_KEY_TYPE_CSTR:
    case HATRACK_DICT_KEY_TYPE_PTR:
    case HATRACK_DICT_KEY_TYPE_OBJ_INT:
    case HATRACK_DICT_KEY_TYPE_OBJ_REAL:
    case HATRACK_DICT_KEY_TYPE_OBJ_CSTR:
    case HATRACK_DICT_KEY_TYPE_OBJ_PTR:
    case HATRACK_DICT_KEY_TYPE_OBJ_CUSTOM:
        self->item_type = item_type;
        break;
    default:
        abort();
    }

    self->hash_info.offsets.hash_offset  = 0;
    self->hash_info.offsets.cache_offset = HATRACK_DICT_NO_CACHE;
    self->free_handler                   = NULL;
    self->pre_return_hook                = NULL;

    return;
}

void
hatrack_set_cleanup(hatrack_set_t *self)
{
    uint64_t           i;
    woolhat_store_t   *store;
    woolhat_history_t *bucket;
    hatrack_hash_t     hv;
    woolhat_record_t  *record;
    hatrack_mem_hook_t handler;

    if (self->free_handler) {
        handler = (hatrack_mem_hook_t)self->free_handler;
        store   = atomic_load(&self->woolhat_instance.store_current);

        for (i = 0; i <= store->last_slot; i++) {
            bucket = &store->hist_buckets[i];
            hv     = atomic_load(&bucket->hv);

            if (hatrack_bucket_unreserved(hv)) {
                continue;
            }

            record = atomic_load(&bucket->head);

            if (!record || record->deleted) {
                continue;
            }
            (*handler)(self, record->item);
        }
    }

    woolhat_cleanup(&self->woolhat_instance);

    return;
}

void
hatrack_set_delete(hatrack_set_t *self)
{
    hatrack_set_cleanup(self);

    free(self);

    return;
}

void
hatrack_set_set_hash_offset(hatrack_set_t *self, int32_t offset)
{
    self->hash_info.offsets.hash_offset = offset;

    return;
}

void
hatrack_set_set_cache_offset(hatrack_set_t *self, int32_t offset)
{
    self->hash_info.offsets.cache_offset = offset;

    return;
}

void
hatrack_set_set_custom_hash(hatrack_set_t *self, hatrack_hash_func_t func)
{
    self->hash_info.custom_hash = func;

    return;
}

void
hatrack_set_set_free_handler(hatrack_set_t *self, hatrack_mem_hook_t func)
{
    self->free_handler = func;

    woolhat_set_cleanup_func(&self->woolhat_instance,
                             (mmm_cleanup_func)hatrack_set_record_eject,
                             self);
    return;
}

void
hatrack_set_set_return_hook(hatrack_set_t *self, hatrack_mem_hook_t func)
{
    self->pre_return_hook = func;

    return;
}

bool
hatrack_set_contains(hatrack_set_t *self, void *item)
{
    bool ret;

    woolhat_get(&self->woolhat_instance,
                hatrack_set_get_hash_value(self, item),
                &ret);

    return ret;
}

bool
hatrack_set_put(hatrack_set_t *self, void *item)
{
    bool ret;

    woolhat_put(&self->woolhat_instance,
                hatrack_set_get_hash_value(self, item),
                item,
                &ret);

    return ret;
}

bool
hatrack_set_add(hatrack_set_t *self, void *item)
{
    return woolhat_add(&self->woolhat_instance,
                       hatrack_set_get_hash_value(self, item),
                       item);
}

bool
hatrack_set_remove(hatrack_set_t *self, void *item)
{
    bool ret;

    woolhat_remove(&self->woolhat_instance,
                   hatrack_set_get_hash_value(self, item),
                   &ret);

    return ret;
}

static inline void *
hatrack_set_items_base(hatrack_set_t *self, uint64_t *num, bool sort)
{
    hatrack_set_view_t *view;
    void              **ret;
    uint64_t            i;
    uint64_t            epoch;

    epoch = mmm_start_linearized_op();

    view = woolhat_view_epoch(&self->woolhat_instance, num, epoch);
    ret  = malloc(sizeof(void *) * *num);

    if (sort) {
        qsort(view,
              *num,
              sizeof(hatrack_set_view_t),
              hatrack_set_epoch_sort_cmp);
    }

    if (self->pre_return_hook) {
        for (i = 0; i < *num; i++) {
            ret[i] = view[i].item;

            (*self->pre_return_hook)(self, view[i].item);
        }
    }
    else {
        for (i = 0; i < *num; i++) {
            ret[i] = view[i].item;
        }
    }

    mmm_end_op();

    free(view);

    return (void *)ret;
}

void *
hatrack_set_items(hatrack_set_t *self, uint64_t *num)
{
    return hatrack_set_items_base(self, num, false);
}

void *
hatrack_set_items_sort(hatrack_set_t *self, uint64_t *num)
{
    return hatrack_set_items_base(self, num, true);
}

/* hatrack_set_is_eq(A, B)
 *
 * Compares two sets for equality at a moment in time, by comparing
 * first the number of items in the set, and then (if equal), by
 * comparing the individual items.
 *
 * We compare hash values to test for equality.
 */
bool
hatrack_set_is_eq(hatrack_set_t *set1, hatrack_set_t *set2)
{
    bool                ret;
    uint64_t            epoch;
    uint64_t            num1;
    uint64_t            num2;
    hatrack_set_view_t *view1;
    hatrack_set_view_t *view2;
    uint64_t            i;

    epoch = mmm_start_linearized_op();

    view1 = woolhat_view_epoch(&set1->woolhat_instance, &num1, epoch);
    view2 = woolhat_view_epoch(&set2->woolhat_instance, &num2, epoch);

    if (num2 != num1) {
        ret = false;
        goto finished;
    }

    qsort(view1, num1, sizeof(hatrack_set_view_t), hatrack_set_hv_sort_cmp);
    qsort(view2, num2, sizeof(hatrack_set_view_t), hatrack_set_hv_sort_cmp);

    for (i = 0; i < num2; i++) {
        /* view2's item must appear in view1, so we keep scanning
         * view1, until we get to the end of view1, as long as
         * view1's hash is less than view2's.
         */
        if (!hatrack_hashes_eq(view1[i].hv, view2[i].hv)) {
            ret = false;
            goto finished;
        }
    }

    ret = true;

finished:
    mmm_end_op();

    free(view1);
    free(view2);

    return ret;
}

/* hatrack_set_is_superset(A, B, proper)
 *
 * Returns true if set A is a superset of set B.
 *
 * If proper is true, this will return false when sets are equal;
 * otherwise, it will return true.
 */
bool
hatrack_set_is_superset(hatrack_set_t *set1, hatrack_set_t *set2, bool proper)
{
    bool                ret;
    uint64_t            epoch;
    uint64_t            num1;
    uint64_t            num2;
    hatrack_set_view_t *view1;
    hatrack_set_view_t *view2;
    uint64_t            i, j;

    epoch = mmm_start_linearized_op();

    view1 = woolhat_view_epoch(&set1->woolhat_instance, &num1, epoch);
    view2 = woolhat_view_epoch(&set2->woolhat_instance, &num2, epoch);

    if (num2 > num1) {
        ret = false;
        goto finished;
    }

    qsort(view1, num1, sizeof(hatrack_set_view_t), hatrack_set_hv_sort_cmp);
    qsort(view2, num2, sizeof(hatrack_set_view_t), hatrack_set_hv_sort_cmp);

    j = 0;

    for (i = 0; i < num2; i++) {
        /* view2's item must appear in view1, so we keep scanning
         * view1, until we get to the end of view1, as long as
         * view1's hash is less than view2's.
         */
        while (true) {
            if (hatrack_hashes_eq(view1[j].hv, view2[i].hv)) {
                break;
            }

            // If view1's hash is larger, then it's not in view2.
            if (hatrack_hash_gt(view1[j].hv, view2[i].hv)) {
                ret = false;
                goto finished;
            }

            // If we're at the end of view1, but there are still
            // items in view2, then we're not a superset.
            if (j == num1) {
                ret = false;
                goto finished;
            }

            j++;
        }
    }

    if (proper && (num1 == num2)) {
        ret = false;
    }
    else {
        ret = true;
    }

finished:
    mmm_end_op();

    free(view1);
    free(view2);

    return ret;
}

/* hatrack_set_is_subset(A, B, proper)
 *
 * Returns true if set A is a subset of set B.
 *
 * If proper is true, this will return false when sets are equal;
 * otherwise, it will return true.
 */
bool
hatrack_set_is_subset(hatrack_set_t *set1, hatrack_set_t *set2, bool proper)
{
    return hatrack_set_is_superset(set2, set1, proper);
}

/* hatrack_set_is_disjoint(A, B)
 *
 * Returns true if, at the moment of call (as defined by our epoch),
 * the two sets do not share any items.
 *
 * If one of the sets is empty, this will always return true.
 */
bool
hatrack_set_is_disjoint(hatrack_set_t *set1, hatrack_set_t *set2)
{
    bool                ret;
    uint64_t            epoch;
    uint64_t            num1;
    uint64_t            num2;
    hatrack_set_view_t *view1;
    hatrack_set_view_t *view2;
    uint64_t            i, j;

    epoch = mmm_start_linearized_op();

    view1 = woolhat_view_epoch(&set1->woolhat_instance, &num1, epoch);
    view2 = woolhat_view_epoch(&set2->woolhat_instance, &num2, epoch);

    qsort(view1, num1, sizeof(hatrack_set_view_t), hatrack_set_hv_sort_cmp);
    qsort(view2, num2, sizeof(hatrack_set_view_t), hatrack_set_hv_sort_cmp);

    i = 0;
    j = 0;

    while (true) {
        if (i == num1 || j == num2) {
            break;
        }
        if (hatrack_hashes_eq(view1[i].hv, view2[j].hv)) {
            ret = false;
            goto finished;
        }
        if (hatrack_hash_gt(view1[i].hv, view2[j].hv)) {
            j++;
        }
        else {
            i++;
        }
    }

    ret = true;

finished:
    mmm_end_op();

    free(view1);
    free(view2);

    return ret;
}

/* hatrack_set_difference(A, B)
 *
 * Returns a new set that consists of A - B, at the moment in time
 * of the call (as defined by the epoch).
 */
hatrack_set_t *
hatrack_set_difference(hatrack_set_t *set1, hatrack_set_t *set2)
{
    hatrack_set_t      *ret;
    uint64_t            epoch;
    uint64_t            num1;
    uint64_t            num2;
    hatrack_set_view_t *view1;
    hatrack_set_view_t *view2;
    uint64_t            i, j;

    if (set1->item_type != set2->item_type) {
        abort();
    }

    ret   = hatrack_set_new(set1->item_type);
    epoch = mmm_start_linearized_op();

    view1 = woolhat_view_epoch(&set1->woolhat_instance, &num1, epoch);
    view2 = woolhat_view_epoch(&set2->woolhat_instance, &num2, epoch);

    qsort(view1, num1, sizeof(hatrack_set_view_t), hatrack_set_hv_sort_cmp);
    qsort(view2, num2, sizeof(hatrack_set_view_t), hatrack_set_hv_sort_cmp);

    i = 0;
    j = 0;

    while (true) {
        if (i == num1 || j == num2) {
            break;
        }

        if (hatrack_hashes_eq(view1[i].hv, view2[i].hv)) {
            i++;
            j++;
            continue; // Not in result set.
        }

        // Next hash comes from the set on the rhs; we ignore it.
        if (hatrack_hash_gt(view1[i].hv, view2[j].hv)) {
            j++;
            continue;
        }

        if (set1->pre_return_hook) {
            (*set1->pre_return_hook)(set1, view1[i].item);
        }

        woolhat_put(&ret->woolhat_instance, view1[i].hv, view1[i].item, NULL);
        i++;
    }

    mmm_end_op();

    free(view1);
    free(view2);

    return ret;
}

/* hatrack_set_union(A, B)
 *
 * Returns a new set that consists of all the items from both sets, at
 * the moment in time of the call (as defined by the epoch).
 */

hatrack_set_t *
hatrack_set_union(hatrack_set_t *set1, hatrack_set_t *set2)
{
    hatrack_set_t      *ret;
    uint64_t            epoch;
    uint64_t            num1;
    uint64_t            num2;
    hatrack_set_view_t *view1;
    hatrack_set_view_t *view2;
    uint64_t            i, j;

    if (set1->item_type != set2->item_type) {
        abort();
    }

    ret   = hatrack_set_new(set1->item_type);
    epoch = mmm_start_linearized_op();

    view1 = woolhat_view_epoch(&set1->woolhat_instance, &num1, epoch);
    view2 = woolhat_view_epoch(&set2->woolhat_instance, &num2, epoch);

    qsort(view1, num1, sizeof(hatrack_set_view_t), hatrack_set_epoch_sort_cmp);
    qsort(view2, num2, sizeof(hatrack_set_view_t), hatrack_set_epoch_sort_cmp);

    /* Here we're going to add from each array based on the insertion
     * epoch, to preserve insertion ordering.
     */
    i = 0;
    j = 0;

    while ((i < num1) && (j < num2)) {
        if (view1[i].sort_epoch < view2[j].sort_epoch) {
	    
            if (woolhat_add(&ret->woolhat_instance, view1[i].hv, view1[i].item)
		&& set1->pre_return_hook) {
		(*set1->pre_return_hook)(set1, view1[i].item);
	    }
            i++;
        }
        else {
            if (woolhat_add(&ret->woolhat_instance, view2[j].hv, view2[j].item)
		&& set2->pre_return_hook) {
                (*set2->pre_return_hook)(set2, view2[j].item);
            }

            j++;
        }
    }

    while (i < num1) {
	if (woolhat_add(&ret->woolhat_instance, view1[i].hv, view1[i].item) 
	    && set1->pre_return_hook) {
	    (*set1->pre_return_hook)(set1, view1[i].item);
	}
        i++;
    }

    while (j < num2) {
	if (woolhat_add(&ret->woolhat_instance, view2[j].hv, view2[j].item)
	    && set2->pre_return_hook) {
            (*set2->pre_return_hook)(set2, view2[j].item);
        }
        j++;
    }

    mmm_end_op();

    free(view1);
    free(view2);

    return ret;
}

/* hatrack_set_intersection(A, B)
 *
 * Returns a new set that consists of only the items that exist in
 * both sets at the time of the call (as defined by the epoch).
 *
 * This does NOT currently preserve insertion ordering the way that
 * hatrack_set_union() does. It could, if we first mark what gets
 * copied and what doesn't, then re-sort based on original epoch.
 *
 * But, meh.
 *
 * The basic algorithm is to sort both views by hash value, then
 * march through them in tandem.
 *
 * If we see two hash values are equal, add the item to the new set
 * and advance to the next item in both sets.
 *
 * Otherwise, the item with the lower hash value definitely is NOT
 * in the intersection (since the items are sorted by hash value).
 * Advance to the next item in that view.
 *
 * Once one view ends, there are no more items in the intersection.
 */
hatrack_set_t *
hatrack_set_intersection(hatrack_set_t *set1, hatrack_set_t *set2)
{
    hatrack_set_t      *ret;
    uint64_t            epoch;
    uint64_t            num1;
    uint64_t            num2;
    hatrack_set_view_t *view1;
    hatrack_set_view_t *view2;
    uint64_t            i, j;

    if (set1->item_type != set2->item_type) {
        abort();
    }

    ret   = hatrack_set_new(set1->item_type);
    epoch = mmm_start_linearized_op();

    view1 = woolhat_view_epoch(&set1->woolhat_instance, &num1, epoch);
    view2 = woolhat_view_epoch(&set2->woolhat_instance, &num2, epoch);

    qsort(view1, num1, sizeof(hatrack_set_view_t), hatrack_set_hv_sort_cmp);
    qsort(view2, num2, sizeof(hatrack_set_view_t), hatrack_set_hv_sort_cmp);

    i = 0;
    j = 0;

    while ((i < num1) && (j < num2)) {
        if (hatrack_hashes_eq(view1[i].hv, view2[j].hv)) {
            if (set1->pre_return_hook) {
                (*set1->pre_return_hook)(set1, view1[i].item);
            }

            woolhat_add(&ret->woolhat_instance, view1[i].hv, view1[i].item);
            i++;
            j++;
            continue;
        }

        if (hatrack_hash_gt(view1[i].hv, view2[j].hv)) {
            j++;
        }

        else {
            i++;
        }
    }

    mmm_end_op();
    free(view1);
    free(view2);

    return ret;
}

/* hatrack_set_disjunction(A, B)
 *
 * Returns a new set that contains items in set A that did not exist
 * in set B, PLUS the items in set B that did not exist in set A.
 *
 * Like intersection, this does not currently preserve intersection
 * order.
 *
 * The algorithm here is to sort by hash value, then go through in
 * tandem. If the item at the current index in one view has a lower
 * hash value than the item in the current index of the other view,
 * then that item is part of the disjunction.
 */
hatrack_set_t *
hatrack_set_disjunction(hatrack_set_t *set1, hatrack_set_t *set2)
{
    hatrack_set_t      *ret;
    uint64_t            epoch;
    uint64_t            num1;
    uint64_t            num2;
    hatrack_set_view_t *view1;
    hatrack_set_view_t *view2;
    uint64_t            i, j;

    if (set1->item_type != set2->item_type) {
        abort();
    }

    ret   = hatrack_set_new(set1->item_type);
    epoch = mmm_start_linearized_op();

    view1 = woolhat_view_epoch(&set1->woolhat_instance, &num1, epoch);
    view2 = woolhat_view_epoch(&set2->woolhat_instance, &num2, epoch);

    qsort(view1, num1, sizeof(hatrack_set_view_t), hatrack_set_hv_sort_cmp);
    qsort(view2, num2, sizeof(hatrack_set_view_t), hatrack_set_hv_sort_cmp);

    i = 0;
    j = 0;

    while ((i < num1) && (j < num2)) {
        if (hatrack_hashes_eq(view1[i].hv, view2[j].hv)) {
            i++;
            j++;
            continue;
        }

        if (hatrack_hash_gt(view1[i].hv, view2[j].hv)) {
            if (set2->pre_return_hook) {
                (*set2->pre_return_hook)(set2, view2[j].item);
            }

            woolhat_add(&ret->woolhat_instance, view2[j].hv, view2[j].item);
            j++;
        }

        else {
            if (set1->pre_return_hook) {
                (*set1->pre_return_hook)(set1, view1[i].item);
            }

            woolhat_add(&ret->woolhat_instance, view1[i].hv, view1[i].item);
            i++;
        }
    }

    mmm_end_op();
    free(view1);
    free(view2);

    return ret;
}

static hatrack_hash_t
hatrack_set_get_hash_value(hatrack_set_t *self, void *key)
{
    hatrack_hash_t hv;
    int32_t        offset;
    uint8_t       *loc_to_hash;

    switch (self->item_type) {
    case HATRACK_DICT_KEY_TYPE_OBJ_CUSTOM:
        return (*self->hash_info.custom_hash)(key);

    case HATRACK_DICT_KEY_TYPE_INT:
        return hash_int((uint64_t)key);

    case HATRACK_DICT_KEY_TYPE_REAL:
        return hash_double(*(double *)key);

    case HATRACK_DICT_KEY_TYPE_CSTR:
        return hash_cstr((char *)key);

    case HATRACK_DICT_KEY_TYPE_PTR:
        return hash_pointer(key);

    default:
        break;
    }

    offset = self->hash_info.offsets.cache_offset;

    if (offset != (int32_t)HATRACK_DICT_NO_CACHE) {
        hv = *(hatrack_hash_t *)(((uint8_t *)key) + offset);

        if (!hatrack_bucket_unreserved(hv)) {
            return hv;
        }
    }

    loc_to_hash = (uint8_t *)key;

    if (self->hash_info.offsets.hash_offset) {
        loc_to_hash += self->hash_info.offsets.hash_offset;
    }

    switch (self->item_type) {
    case HATRACK_DICT_KEY_TYPE_OBJ_INT:
        hv = hash_int((uint64_t)loc_to_hash);
        break;
    case HATRACK_DICT_KEY_TYPE_OBJ_REAL:
        hv = hash_double(*(double *)loc_to_hash);
        break;
    case HATRACK_DICT_KEY_TYPE_OBJ_CSTR:
        hv = hash_cstr((char *)loc_to_hash);
        break;
    case HATRACK_DICT_KEY_TYPE_OBJ_PTR:
        hv = hash_pointer(loc_to_hash);
        break;
    default:
        abort();
    }

    if (offset != (int32_t)HATRACK_DICT_NO_CACHE) {
        *(hatrack_hash_t *)(((uint8_t *)key) + offset) = hv;
    }

    return hv;
}

static void
hatrack_set_record_eject(woolhat_record_t *record, hatrack_set_t *set)
{
    hatrack_mem_hook_t handler;

    handler = (hatrack_mem_hook_t)set->free_handler;

    (*handler)(set, record->item);

    return;
}

static int
hatrack_set_hv_sort_cmp(const void *b1, const void *b2)
{
    hatrack_set_view_t *item1;
    hatrack_set_view_t *item2;

    item1 = (hatrack_set_view_t *)b1;
    item2 = (hatrack_set_view_t *)b2;

    if (hatrack_hash_gt(item1->hv, item2->hv)) {
        return 1;
    }

    if (hatrack_hashes_eq(item1->hv, item2->hv)) {
        abort(); // Shouldn't happen; hash entries should be unique.
    }

    return -1;
}

static int
hatrack_set_epoch_sort_cmp(const void *b1, const void *b2)
{
    hatrack_set_view_t *item1;
    hatrack_set_view_t *item2;

    item1 = (hatrack_set_view_t *)b1;
    item2 = (hatrack_set_view_t *)b2;

    return item1->sort_epoch - item2->sort_epoch;
}
