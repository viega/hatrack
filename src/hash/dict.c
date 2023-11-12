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
 *  Name:           dict.c
 *  Description:    High-level dictionary based on Crown.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

// clang-format off
static hatrack_hash_t hatrack_dict_get_hash_value(hatrack_dict_t *, void *);
static void           hatrack_dict_record_eject  (hatrack_dict_item_t *,
						  hatrack_dict_t *);

hatrack_dict_t *
hatrack_dict_new(uint32_t key_type)
{
    hatrack_dict_t *ret;

    ret = (hatrack_dict_t *)malloc(sizeof(hatrack_dict_t));

    hatrack_dict_init(ret, key_type);

    return ret;
}

void
hatrack_dict_init(hatrack_dict_t *self, uint32_t key_type)
{
    crown_init(&self->crown_instance);

    switch (key_type) {
    case HATRACK_DICT_KEY_TYPE_INT:
    case HATRACK_DICT_KEY_TYPE_REAL:
    case HATRACK_DICT_KEY_TYPE_CSTR:
    case HATRACK_DICT_KEY_TYPE_PTR:
    case HATRACK_DICT_KEY_TYPE_OBJ_INT:
    case HATRACK_DICT_KEY_TYPE_OBJ_REAL:
    case HATRACK_DICT_KEY_TYPE_OBJ_CSTR:
    case HATRACK_DICT_KEY_TYPE_OBJ_PTR:
    case HATRACK_DICT_KEY_TYPE_OBJ_CUSTOM:
        self->key_type = key_type;
        break;
    default:
        abort();
    }

    self->hash_info.offsets.hash_offset  = 0;
    self->hash_info.offsets.cache_offset = HATRACK_DICT_NO_CACHE;
    self->free_handler                   = NULL;
    self->key_return_hook                = NULL;
    self->val_return_hook                = NULL;
    self->slow_views                     = false;

    return;
}

void
hatrack_dict_cleanup(hatrack_dict_t *self)
{
    uint64_t           i;
    crown_store_t  *store;
    crown_bucket_t *bucket;
    hatrack_hash_t     hv;
    crown_record_t  record;

    if (self->free_handler) {
        store = atomic_load(&self->crown_instance.store_current);

        for (i = 0; i <= store->last_slot; i++) {
            bucket = &store->buckets[i];
            hv     = atomic_load(&bucket->hv);

            if (hatrack_bucket_unreserved(hv)) {
                continue;
            }

            record = atomic_load(&bucket->record);

            if (!record.info) {
                continue;
            }

            (*self->free_handler)(self, record.item);
        }
    }

    mmm_retire(atomic_load(&self->crown_instance.store_current));

    return;
}

void
hatrack_dict_delete(hatrack_dict_t *self)
{
    hatrack_dict_cleanup(self);

    free(self);

    return;
}

void
hatrack_dict_set_hash_offset(hatrack_dict_t *self, int32_t offset)
{
    self->hash_info.offsets.hash_offset = offset;

    return;
}

void
hatrack_dict_set_cache_offset(hatrack_dict_t *self, int32_t offset)
{
    self->hash_info.offsets.cache_offset = offset;

    return;
}

void
hatrack_dict_set_custom_hash(hatrack_dict_t *self, hatrack_hash_func_t func)
{
    self->hash_info.custom_hash = func;

    return;
}

void
hatrack_dict_set_free_handler(hatrack_dict_t *self, hatrack_mem_hook_t func)
{
    self->free_handler = func;

    return;
}

void
hatrack_dict_set_key_return_hook(hatrack_dict_t *self, hatrack_mem_hook_t func)
{
    self->key_return_hook = func;

    return;
}

void
hatrack_dict_set_val_return_hook(hatrack_dict_t *self, hatrack_mem_hook_t func)
{
    self->val_return_hook = func;

    return;
}

void
hatrack_dict_set_consistent_views(hatrack_dict_t *self, bool value)
{
    self->slow_views = value;

    return;
}

void
hatrack_dict_set_sorted_views(hatrack_dict_t *self, bool value)
{
    self->sorted_views = value;

    return;
}

bool
hatrack_dict_get_consistent_views(hatrack_dict_t *self)
{
    return self->slow_views;
}

bool
hatrack_dict_get_sorted_views(hatrack_dict_t *self)
{
    return self->sorted_views;
}

void *
hatrack_dict_get(hatrack_dict_t *self, void *key, bool *found)
{
    hatrack_hash_t       hv;
    hatrack_dict_item_t *item;
    crown_store_t    *store;

    
    hv = hatrack_dict_get_hash_value(self, key);

    mmm_start_basic_op();

    store = atomic_read(&self->crown_instance.store_current);
    item  = crown_store_get(store, hv, found);

    if (!item) {
        if (found) {
            *found = false;
        }

	mmm_end_op();
        return NULL;
    }

    if (found) {
        *found = true;
    }

    if (self->val_return_hook) {
	(*self->val_return_hook)(self, item->value);
    }
    
    mmm_end_op();
    
    return item->value;
}

/*
 * Because we are going to protect our dict_item allocations with mmm,
 * and we don't want to double-call MMM: it will replace our
 * reservation, and also end our reservation before we want it.
 *
 * We could do two layers of MMM, but instead we just lift it out here,
 * and skip directly to the crown_store() calls.
 */
void
hatrack_dict_put(hatrack_dict_t *self, void *key, void *value)
{
    hatrack_hash_t       hv;
    hatrack_dict_item_t *new_item;
    hatrack_dict_item_t *old_item;
    crown_store_t    *store;

    hv = hatrack_dict_get_hash_value(self, key);

    mmm_start_basic_op();

    new_item        = mmm_alloc_committed(sizeof(hatrack_dict_item_t));
    new_item->key   = key;
    new_item->value = value;
    store           = atomic_read(&self->crown_instance.store_current);

    old_item = crown_store_put(store,
                                  &self->crown_instance,
                                  hv,
                                  new_item,
                                  NULL,
                                  0);

    if (old_item) {
        if (self->free_handler) {
            mmm_add_cleanup_handler(old_item,
                                    (mmm_cleanup_func)hatrack_dict_record_eject,
                                    self);
        }

        mmm_retire(old_item);
    }

    mmm_end_op();

    return;
}

bool
hatrack_dict_replace(hatrack_dict_t *self, void *key, void *value)
{
    hatrack_hash_t       hv;
    hatrack_dict_item_t *new_item;
    hatrack_dict_item_t *old_item;
    crown_store_t    *store;

    hv = hatrack_dict_get_hash_value(self, key);

    mmm_start_basic_op();

    new_item        = mmm_alloc_committed(sizeof(hatrack_dict_item_t));
    new_item->key   = key;
    new_item->value = value;
    store           = atomic_read(&self->crown_instance.store_current);

    old_item = crown_store_put(store,
                                  &self->crown_instance,
                                  hv,
                                  new_item,
                                  NULL,
                                  0);

    if (old_item) {
        if (self->free_handler) {
            mmm_add_cleanup_handler(old_item,
                                    (mmm_cleanup_func)hatrack_dict_record_eject,
                                    self);
        }

        mmm_retire(old_item);
        mmm_end_op();

        return true;
    }

    mmm_retire_unused(new_item);
    mmm_end_op();

    return false;
}

bool
hatrack_dict_add(hatrack_dict_t *self, void *key, void *value)
{
    hatrack_hash_t       hv;
    hatrack_dict_item_t *new_item;
    crown_store_t    *store;

    hv = hatrack_dict_get_hash_value(self, key);

    mmm_start_basic_op();

    new_item        = mmm_alloc_committed(sizeof(hatrack_dict_item_t));
    new_item->key   = key;
    new_item->value = value;
    store           = atomic_read(&self->crown_instance.store_current);

    if (crown_store_add(store, &self->crown_instance, hv, new_item, 0)) {
        mmm_end_op();

        return true;
    }

    mmm_retire_unused(new_item);
    mmm_end_op();

    return false;
}

bool
hatrack_dict_remove(hatrack_dict_t *self, void *key)
{
    hatrack_hash_t       hv;
    hatrack_dict_item_t *old_item;
    crown_store_t    *store;

    hv = hatrack_dict_get_hash_value(self, key);

    mmm_start_basic_op();

    store = atomic_read(&self->crown_instance.store_current);
    old_item
        = crown_store_remove(store, &self->crown_instance, hv, NULL, 0);

    if (old_item) {
        if (self->free_handler) {
            mmm_add_cleanup_handler(old_item,
                                    (mmm_cleanup_func)hatrack_dict_record_eject,
                                    self);
        }

        mmm_retire(old_item);
        mmm_end_op();

        return true;
    }

    mmm_end_op();

    return false;
}

static hatrack_dict_key_t *
hatrack_dict_keys_base(hatrack_dict_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t      *view;
    hatrack_dict_key_t  *ret;
    hatrack_dict_item_t *item;
    uint64_t             alloc_len;
    uint32_t             i;

    mmm_start_basic_op();

    if (self->slow_views) {
	view = crown_view_slow(&self->crown_instance, num, sort);
    }
    else {
	view = crown_view_fast(&self->crown_instance, num, sort);
    }
    
    alloc_len = sizeof(hatrack_dict_key_t) * *num;
    ret       = (hatrack_dict_key_t *)malloc(alloc_len);

    if (self->key_return_hook) {
	for (i = 0; i < *num; i++) {
	    item   = (hatrack_dict_item_t *)view[i].item;
	    ret[i] = item->key;
	    
	    (*self->key_return_hook)(self, item->key);	
	}	
    }
    else {
	for (i = 0; i < *num; i++) {
	    item   = (hatrack_dict_item_t *)view[i].item;
	    ret[i] = item->key;
	}
    }

    mmm_end_op();

    free(view);

    return ret;
}

static hatrack_dict_value_t *
hatrack_dict_values_base(hatrack_dict_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t       *view;
    hatrack_dict_value_t *ret;
    hatrack_dict_item_t  *item;
    uint64_t              alloc_len;
    uint32_t              i;

    mmm_start_basic_op();

    if (self->slow_views) {
	view = crown_view_slow(&self->crown_instance, num, sort);
    }
    else {
	view = crown_view_fast(&self->crown_instance, num, sort);
    }

    alloc_len = sizeof(hatrack_dict_value_t) * *num;
    ret       = (hatrack_dict_value_t *)malloc(alloc_len);

    if (self->val_return_hook) {
	for (i = 0; i < *num; i++) {
	    item   = (hatrack_dict_item_t *)view[i].item;
	    ret[i] = item->value;
	    
	    (*self->val_return_hook)(self, item->value);		    
	}
    } else {
	for (i = 0; i < *num; i++) {
	    item   = (hatrack_dict_item_t *)view[i].item;
	    ret[i] = item->value;
	}
    }

    mmm_end_op();
    
    free(view);

    return ret;
}

static hatrack_dict_item_t *
hatrack_dict_items_base(hatrack_dict_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t      *view;
    hatrack_dict_item_t *ret;
    hatrack_dict_item_t *item;
    uint64_t             alloc_len;
    uint32_t             i;

    mmm_start_basic_op();
    
    if (self->slow_views) {
	view = crown_view_slow(&self->crown_instance, num, sort);
    }
    else {
	view = crown_view_fast(&self->crown_instance, num, sort);
    }

    alloc_len = sizeof(hatrack_dict_item_t) * *num;
    ret       = (hatrack_dict_item_t *)malloc(alloc_len);

    for (i = 0; i < *num; i++) {
	item         = (hatrack_dict_item_t *)view[i].item;
	ret[i].key   = item->key;
	ret[i].value = item->value;
	
	if (self->key_return_hook) {
	    (*self->key_return_hook)(self, item->key);
	}
	if (self->val_return_hook) {
	    (*self->val_return_hook)(self, item->value);
	}
    }

    mmm_end_op();
    
    free(view);

    return ret;
}

hatrack_dict_key_t *
hatrack_dict_keys(hatrack_dict_t *self, uint64_t *num)
{
    return hatrack_dict_keys_base(self, num, self->sorted_views);
}

hatrack_dict_value_t *
hatrack_dict_values(hatrack_dict_t *self, uint64_t *num)
{
    return hatrack_dict_values_base(self, num, self->sorted_views);
}

hatrack_dict_item_t *
hatrack_dict_items(hatrack_dict_t *self, uint64_t *num)
{
    return hatrack_dict_items_base(self, num, self->sorted_views);
}

hatrack_dict_key_t *
hatrack_dict_keys_sort(hatrack_dict_t *self, uint64_t *num)
{
    return hatrack_dict_keys_base(self, num, true);
}

hatrack_dict_value_t *
hatrack_dict_values_sort(hatrack_dict_t *self, uint64_t *num)
{
    return hatrack_dict_values_base(self, num, true);
}

hatrack_dict_item_t *
hatrack_dict_items_sort(hatrack_dict_t *self, uint64_t *num)
{
    return hatrack_dict_items_base(self, num, true);
}

hatrack_dict_key_t *
hatrack_dict_keys_nosort(hatrack_dict_t *self, uint64_t *num)
{
    return hatrack_dict_keys_base(self, num, false);
}

hatrack_dict_value_t *
hatrack_dict_values_nosort(hatrack_dict_t *self, uint64_t *num)
{
    return hatrack_dict_values_base(self, num, false);
}

hatrack_dict_item_t *
hatrack_dict_items_nosort(hatrack_dict_t *self, uint64_t *num)
{
    return hatrack_dict_items_base(self, num, false);
}

static hatrack_hash_t
hatrack_dict_get_hash_value(hatrack_dict_t *self, void *key)
{
    hatrack_hash_t hv;
    int32_t        offset;
    uint8_t       *loc_to_hash;

    switch (self->key_type) {
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

    switch (self->key_type) {
    case HATRACK_DICT_KEY_TYPE_OBJ_INT:
        hv = hash_int((uint64_t)loc_to_hash);
        break;
    case HATRACK_DICT_KEY_TYPE_OBJ_REAL:
        hv = hash_double(*(double *)loc_to_hash);
        break;
    case HATRACK_DICT_KEY_TYPE_OBJ_CSTR:
        hv = hash_cstr(*(char **)loc_to_hash);
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
hatrack_dict_record_eject(hatrack_dict_item_t *record,
			  hatrack_dict_t *dict)
{
    hatrack_mem_hook_t callback;

    callback = dict->free_handler;
    
    (*callback)(dict, record);

    return;
}
