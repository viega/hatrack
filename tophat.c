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
 *  Name:           tophat.c
 *
 *  Description:    Adaptive hash table that starts off fast, but
 *                  migrates to a multi-reader / multi-writer
 *                  implementation once the table is accessed by
 *                  multiple threads simultaneously.
 *
 *                  See tophat.h for an overview.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "tophat.h"

// clang-format off
static void             tophat_init_base     (tophat_t *, bool);
static void            *tophat_st_get        (refhat_a_t *, hatrack_hash_t,
					      bool *);
static void            *tophat_st_put        (refhat_a_t *, hatrack_hash_t,
					      void *, bool *);
static void            *tophat_st_replace    (refhat_a_t *, hatrack_hash_t,
					      void *, bool *);
static bool            tophat_st_add         (refhat_a_t *, hatrack_hash_t,
					      void *);
static void           *tophat_st_remove      (refhat_a_t *, hatrack_hash_t,
					      bool *);
static void            tophat_st_delete      (refhat_a_t *);
static void            tophat_st_delete_store(refhat_a_t *);
static uint64_t        tophat_st_len         (refhat_a_t *);
static hatrack_view_t *tophat_st_view        (refhat_a_t *, uint64_t *, bool);

#ifdef TOPHAT_USE_LOCKING_ALGORITHMS

#include "ballcap.h"
#include "newshat.h"

static void tophat_migrate_to_ballcap (tophat_t *, refhat_a_t *);
static void tophat_migrate_to_newshat (tophat_t *, refhat_a_t *);

extern ballcap_store_t *ballcap_store_new(uint64_t);
extern newshat_store_t *newshat_store_new(uint64_t);

// Virtual call tables for the two locking algorithms.
static hatrack_vtable_t cst_vtable = {
    .init    = (hatrack_init_func)ballcap_init,
    .get     = (hatrack_get_func)ballcap_get,
    .put     = (hatrack_put_func)ballcap_put,
    .replace = (hatrack_replace_func)ballcap_replace,    
    .add     = (hatrack_add_func)ballcap_add,    
    .remove  = (hatrack_remove_func)ballcap_remove,
    .delete  = (hatrack_delete_func)ballcap_delete,
    .len     = (hatrack_len_func)ballcap_len,
    .view    = (hatrack_view_func)ballcap_view
};

static hatrack_vtable_t fast_vtable = {
    .init    = (hatrack_init_func)newshat_init,
    .get     = (hatrack_get_func)newshat_get,
    .put     = (hatrack_put_func)newshat_put,
    .replace = (hatrack_replace_func)newshat_replace,    
    .add     = (hatrack_add_func)newshat_add,    
    .remove  = (hatrack_remove_func)newshat_remove,
    .delete  = (hatrack_delete_func)newshat_delete,
    .len     = (hatrack_len_func)newshat_len,
    .view    = (hatrack_view_func)newshat_view
};

#else
static void tophat_migrate_to_woolhat (tophat_t *, refhat_a_t *);
static void tophat_migrate_to_witchhat(tophat_t *, refhat_a_t *);


extern woolhat_store_t  *woolhat_store_new (uint64_t);
extern witchhat_store_t *witchhat_store_new(uint64_t);

// Virtual call tables for the two wait-free algorithms.
static hatrack_vtable_t cst_vtable = {
    .init    = (hatrack_init_func)woolhat_init,
    .get     = (hatrack_get_func)woolhat_get,
    .put     = (hatrack_put_func)woolhat_put,
    .replace = (hatrack_replace_func)woolhat_replace,    
    .add     = (hatrack_add_func)woolhat_add,
    .remove  = (hatrack_remove_func)woolhat_remove,
    .delete  = (hatrack_delete_func)woolhat_delete,
    .len     = (hatrack_len_func)woolhat_len,
    .view    = (hatrack_view_func)woolhat_view
};

static hatrack_vtable_t fast_vtable = {
    .init    = (hatrack_init_func)witchhat_init,
    .get     = (hatrack_get_func)witchhat_get,
    .put     = (hatrack_put_func)witchhat_put,
    .replace = (hatrack_replace_func)witchhat_replace,    
    .add     = (hatrack_add_func)witchhat_add,
    .remove  = (hatrack_remove_func)witchhat_remove,
    .delete  = (hatrack_delete_func)witchhat_delete,
    .len     = (hatrack_len_func)witchhat_len,
    .view    = (hatrack_view_func)witchhat_view
};

#endif

// Virtual call table for our single-threaded vtable
static hatrack_vtable_t st_vtable = {
    .get     = (hatrack_get_func)tophat_st_get,
    .put     = (hatrack_put_func)tophat_st_put,
    .replace = (hatrack_replace_func)tophat_st_replace,    
    .add     = (hatrack_add_func)tophat_st_add,
    .remove  = (hatrack_remove_func)tophat_st_remove,
    .delete  = (hatrack_delete_func)tophat_st_delete,
    .len     = (hatrack_len_func)tophat_st_len,
    .view    = (hatrack_view_func)tophat_st_view
};


static inline void
tophat_migrate(tophat_t *self)
{
    tophat_algo_info_t implementation;

    // This could have swapped out while we were waiting for the lock.
    // Check the implementation's vtable to see if it's still refhat.
    implementation = atomic_read(&self->implementation);

    if (implementation.vtable != &st_vtable) {
	return;
    }
    
#ifdef TOPHAT_USE_LOCKING_ALGORITHMS
    if (self->flags & TOPHAT_F_CONSISTENT_VIEWS) {
	tophat_migrate_to_ballcap(self, (refhat_a_t *)implementation.htable);
    } else {
	tophat_migrate_to_newshat(self, (refhat_a_t *)implementation.htable);
    }
#else
    if (self->flags & TOPHAT_F_CONSISTENT_VIEWS) {
	tophat_migrate_to_woolhat(self, (refhat_a_t *)implementation.htable);
    } else {
	tophat_migrate_to_witchhat(self, (refhat_a_t *)implementation.htable);
    }
#endif
}

void
tophat_init_cst(tophat_t *self)
{
    tophat_init_base(self, true);
    
    return;
}

void
tophat_init_fast(tophat_t *self)
{
    tophat_init_base(self, false);

    return;
}

void *
tophat_get(tophat_t *self, hatrack_hash_t hv, bool *found)
{
    void               *ret;
    tophat_algo_info_t  info;
    
    mmm_start_basic_op();
    info   = atomic_read(&self->implementation);
    ret    = (*info.vtable->get)(info.htable, hv, found);
    mmm_end_op();

    return ret;
}

void *
tophat_put(tophat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void               *ret;
    tophat_algo_info_t  info;
    
    mmm_start_basic_op();
    info   = atomic_read(&self->implementation);
    ret    = (*info.vtable->put)(info.htable, hv, item, found);
    mmm_end_op();

    return ret;
}

void *
tophat_replace(tophat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void               *ret;
    tophat_algo_info_t  info;
    
    mmm_start_basic_op();
    info   = atomic_read(&self->implementation);
    ret    = (*info.vtable->replace)(info.htable, hv, item, found);
    mmm_end_op();

    return ret;
}

bool
tophat_add(tophat_t *self, hatrack_hash_t hv, void *item)
{
    bool                ret;
    tophat_algo_info_t  info;
    
    mmm_start_basic_op();
    info   = atomic_read(&self->implementation);
    ret    = (*info.vtable->add)(info.htable, hv, item);
    mmm_end_op();

    return ret;
}

void *
tophat_remove(tophat_t *self, hatrack_hash_t hv, bool *found)
{
    void               *ret;
    tophat_algo_info_t  info;
    
    mmm_start_basic_op();
    info   = atomic_read(&self->implementation);
    ret    = (*info.vtable->remove)(info.htable, hv, found);
    mmm_end_op();

    return ret;
}

void
tophat_delete(tophat_t *self) {
    tophat_algo_info_t info;

    info = atomic_read(&self->implementation);
    (info.vtable->delete)(info.htable);
    free(self);
}

uint64_t
tophat_len(tophat_t *self)
{
    uint64_t            ret;
    tophat_algo_info_t  info;
    
    mmm_start_basic_op();
    info   = atomic_read(&self->implementation);
    ret    = (*info.vtable->len)(info.htable);
    mmm_end_op();

    return ret;
}

hatrack_view_t *
tophat_view(tophat_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t     *ret;
    tophat_algo_info_t  info;    

    mmm_start_linearized_op();
    info = atomic_read(&self->implementation);
    ret  = (*info.vtable->view)(info.htable, num, sort);
    mmm_end_op();

    return ret;
}

static void
tophat_init_base(tophat_t *self, bool cst)
{
    uint64_t           alloc_len;
    refhat_a_t         *initial_table;
    tophat_algo_info_t info;    
    
    alloc_len     = sizeof(refhat_a_t);

    initial_table = (refhat_a_t *)mmm_alloc_committed(alloc_len);
    info.htable   = initial_table;
    info.vtable   = &st_vtable;
    
    refhat_a_init((refhat_a_t *)initial_table);
    initial_table->backref   = (void *)self;
    
    atomic_store(&initial_table->readers, 0);
    

    pthread_mutex_init(&initial_table->mutex, NULL);
    atomic_store(&self->implementation, info);

    if (cst) {
	self->flags = TOPHAT_F_CONSISTENT_VIEWS;
    }
    else {
	self->flags = 0;
    }

    return;
}

static void *
tophat_st_get(refhat_a_t *rhobj, hatrack_hash_t hv, bool *found)
{
    void *ret;

    atomic_fetch_add(&rhobj->readers, 1);
    ret = refhat_a_get((refhat_a_t *)rhobj, hv, found);
    atomic_fetch_sub(&rhobj->readers, 1);

    return ret;
    
}

static void *
tophat_st_put(refhat_a_t *rhobj, hatrack_hash_t hv, void *item, bool *found)
{
    void *ret;
    
    if(pthread_mutex_trylock(&rhobj->mutex)) {
	pthread_mutex_lock(&rhobj->mutex);
	tophat_migrate((tophat_t *)rhobj->backref);
	pthread_mutex_unlock(&rhobj->mutex);	
	return tophat_put((tophat_t *)rhobj->backref, hv, item, found);
    }

    ret = refhat_a_put((refhat_a_t *)rhobj, hv, item, found);
    
    pthread_mutex_unlock(&rhobj->mutex);

    return ret;
}

static void *
tophat_st_replace(refhat_a_t *rhobj, hatrack_hash_t hv, void *item, bool *found)
{
    void *ret;

    if (pthread_mutex_trylock(&rhobj->mutex)) {
	tophat_migrate((tophat_t *)rhobj->backref);
	pthread_mutex_unlock(&rhobj->mutex);	
	return tophat_replace((tophat_t *)rhobj->backref, hv, item, found);
    }

    ret = refhat_a_replace((refhat_a_t *)rhobj, hv, item, found);
    
    pthread_mutex_unlock(&rhobj->mutex);

    return ret;
}

static bool
tophat_st_add(refhat_a_t *rhobj, hatrack_hash_t hv, void *item)
{
    bool ret;

    if (pthread_mutex_lock(&rhobj->mutex)) {
	tophat_migrate((tophat_t *)rhobj->backref);
	pthread_mutex_unlock(&rhobj->mutex);
	return tophat_add((tophat_t *)rhobj->backref, hv, item);
    }
    
    ret = refhat_a_add((refhat_a_t *)rhobj, hv, item);
    
    pthread_mutex_unlock(&rhobj->mutex);

    return ret;
}

static void *
tophat_st_remove(refhat_a_t *rhobj, hatrack_hash_t hv, bool *found)
{
    void *ret;

    if (pthread_mutex_lock(&rhobj->mutex)) {    
	tophat_migrate((tophat_t *)rhobj->backref);
	pthread_mutex_unlock(&rhobj->mutex);
	return tophat_remove((tophat_t *)rhobj->backref, hv, found);
    }

    ret = refhat_a_remove((refhat_a_t *)rhobj, hv, found);
    
    pthread_mutex_unlock(&rhobj->mutex);    

    return ret;
}

static void
tophat_st_delete(refhat_a_t *rhobj)
{
    mmm_add_cleanup_handler(rhobj, (void (*)(void *))tophat_st_delete_store);
    mmm_retire(rhobj);
}

static void
tophat_st_delete_store(refhat_a_t *rhobj)
{
    free(rhobj->buckets);
    pthread_mutex_destroy(&rhobj->mutex);
}

static uint64_t
tophat_st_len(refhat_a_t *rhobj) {
    uint64_t ret;
    
    pthread_mutex_lock(&rhobj->mutex);
    ret = refhat_a_len((refhat_a_t *)rhobj);
    pthread_mutex_unlock(&rhobj->mutex);

    return ret;
}

static hatrack_view_t *
tophat_st_view(refhat_a_t *rhobj, uint64_t *num, bool sort) {
    hatrack_view_t *ret;
    
    pthread_mutex_lock(&rhobj->mutex);
    ret = refhat_a_view((refhat_a_t *)rhobj, num, sort);
    pthread_mutex_unlock(&rhobj->mutex);

    return ret;
}

#ifdef TOPHAT_USE_LOCKING_ALGORITHMS

static void
tophat_migrate_to_ballcap(tophat_t *tophat, refhat_a_t *rhobj)
{
    ballcap_t         *new_table;
    uint64_t           i, n, bix, record_len;
    refhat_a_bucket_t   *cur;
    ballcap_bucket_t  *target;
    ballcap_record_t  *record;
    tophat_algo_info_t implementation;

    new_table        = (ballcap_t *)malloc(sizeof(ballcap_t));
    new_table->store = ballcap_store_new(rhobj->last_slot + 1);
    record_len       = sizeof(ballcap_record_t);

    for (n = 0; n <= rhobj->last_slot; n++) {
	cur = &rhobj->buckets[n];
	if (cur->deleted || hatrack_bucket_unreserved(cur->hv)) {
	    continue;
	}
	bix = hatrack_bucket_index(cur->hv, rhobj->last_slot);	
	for (i = 0; i <= rhobj->last_slot; i++) {
	    target = &new_table->store->buckets[bix];
	    
	    if (hatrack_bucket_unreserved(target->hv)) {	    
		    target->hv       = cur->hv;
		    record           = mmm_alloc_committed(record_len);
		    record->item     = cur->item;
		    target->migrated = false;
		    target->record   = record;
		    mmm_set_create_epoch(record, cur->epoch);
		    break;
	    }
	    bix = (bix + 1) & rhobj->last_slot;
	}
    }

    new_table->store->used_count = rhobj->item_count;
    new_table->item_count        = rhobj->item_count;
    new_table->next_epoch        = rhobj->next_epoch;
    implementation.htable        = new_table;
    implementation.vtable        = &cst_vtable;
    
    pthread_mutex_init(&new_table->migrate_mutex, NULL);
    
    atomic_store(&tophat->implementation, implementation);

    while (rhobj->readers)
	;
    tophat_st_delete(rhobj);
}

static void
tophat_migrate_to_newshat(tophat_t *tophat, refhat_a_t *rhobj)
{
    newshat_t         *new_table;
    uint64_t           n, i, bix;
    refhat_a_bucket_t   *cur;
    newshat_bucket_t  *target;
    tophat_algo_info_t implementation;

    new_table        = (newshat_t *)malloc(sizeof(newshat_t));
    new_table->store = newshat_store_new(rhobj->last_slot + 1);

    for (n = 0; n <= rhobj->last_slot; n++) {
	cur = &rhobj->buckets[n];
	if (cur->deleted || hatrack_bucket_unreserved(cur->hv)) {
	    continue;
	}
	bix = hatrack_bucket_index(cur->hv, rhobj->last_slot);
	for (i = 0; i <= rhobj->last_slot; i++) {
	    target = &new_table->store->buckets[bix];
	    
	    if (hatrack_bucket_unreserved(target->hv)) {
		target->hv       = cur->hv;
		target->item     = cur->item;
		target->epoch    = cur->epoch;
		target->migrated = false;
		break;
	    }
	    bix = (bix + 1) & rhobj->last_slot;
	}
    }

    new_table->store->used_count = rhobj->item_count;
    new_table->item_count        = rhobj->item_count;
    new_table->next_epoch        = rhobj->next_epoch;
    
    pthread_mutex_init(&new_table->migrate_mutex, NULL);
    
    implementation.htable = new_table;
    implementation.vtable = &fast_vtable;
    
    atomic_store(&tophat->implementation, implementation);

    while (rhobj->readers)
	;
    tophat_st_delete(rhobj);    
}

#else

static void
tophat_migrate_to_woolhat(tophat_t *tophat, refhat_a_t *rhobj)
{
    woolhat_t          *new_table;
    uint64_t            n, i, bix, record_len;
    hatrack_hash_t      hv;
    refhat_a_bucket_t  *cur;
    refhat_a_record_t   old_record;
    woolhat_history_t  *target;
    woolhat_record_t   *cur_record;
    tophat_algo_info_t  implementation;

    new_table                = (woolhat_t *)malloc(sizeof(woolhat_t));
    new_table->store_current = woolhat_store_new(rhobj->last_slot + 1);
    record_len               = sizeof(woolhat_record_t);
    atomic_store(&new_table->help_needed, 0);

    for (n = 0; n <= rhobj->last_slot; n++) {
	cur = &rhobj->buckets[n];
	
	if (hatrack_bucket_unreserved(cur->hv)) {
	    continue;
	}

	old_record = atomic_read(&cur->record);

	if (!old_record.epoch) {
	    continue;
	}

	bix = hatrack_bucket_index(cur->hv, rhobj->last_slot);
	for (i = 0; i <= rhobj->last_slot; i++) {
	    target = &new_table->store_current->hist_buckets[bix];
	    hv     = atomic_load(&target->hv);
	    
	    if (hatrack_bucket_unreserved(hv)) {
		cur_record         = (woolhat_record_t *)mmm_alloc(record_len);
		cur_record->item   = old_record.item;
		cur_record->next   = hatrack_pflag_set(NULL, WOOLHAT_F_USED);
		mmm_set_create_epoch(cur_record, old_record.epoch);
		atomic_store(&target->hv, cur->hv);
		atomic_store(&target->head, cur_record);
		break;
	    }
	    bix = (bix + 1) & rhobj->last_slot;
	}
    }
    new_table->store_current->used_count = rhobj->item_count;
    implementation.htable                = new_table;
    implementation.vtable                = &cst_vtable;
    
    atomic_store(&tophat->implementation, implementation);

    while (rhobj->readers)
	;
    tophat_st_delete(rhobj);
}

static void
tophat_migrate_to_witchhat(tophat_t *tophat, refhat_a_t *rhobj)
{
    witchhat_t        *new_table;
    witchhat_bucket_t *new_bucket;
    witchhat_record_t  new_record;
    witchhat_record_t  expected_record;
    hatrack_hash_t     expected_hv;
    refhat_a_bucket_t *cur;
    uint64_t           i, n, bix;
    refhat_a_record_t  record;
    tophat_algo_info_t implementation;    
    
    new_table                = (witchhat_t *)malloc(sizeof(witchhat_t));
    new_table->store_current = witchhat_store_new(rhobj->last_slot + 1);
    new_table->next_epoch    = 1;

    for (n = 0; n <= rhobj->last_slot; n++) {
	cur = &rhobj->buckets[n];
	
	if (hatrack_bucket_unreserved(cur->hv)) {
	    continue;
	}

	record = atomic_read(&cur->record);

	if (!record.epoch) {
	    continue;
	}

	bix = hatrack_bucket_index(cur->hv, rhobj->last_slot);

	for (i = 0; i <= rhobj->last_slot; i++) {
	    new_bucket  = &new_table->store_current->buckets[bix];
	    expected_hv = atomic_read(&new_bucket->hv);
	    
	    if (hatrack_bucket_unreserved(expected_hv)) {
		if (CAS(&new_bucket->hv, &expected_hv, cur->hv)) {
		    break;
		}
	    }
	    if (!hatrack_hashes_eq(expected_hv, cur->hv)) {
		bix = (bix + 1) & new_table->store_current->last_slot;
		continue;
	    }
	    break;
	}

	new_record.info       = record.epoch;
	new_record.item       = record.item;
	expected_record.info  = 0;
	expected_record.item  = NULL;

	CAS(&new_bucket->record, &expected_record, new_record);
	
    }
    implementation.htable = new_table;
    implementation.vtable = &fast_vtable;

    atomic_store(&new_table->help_needed, 0);
    atomic_store(&tophat->implementation, implementation);

    while (rhobj->readers)
	;
    tophat_st_delete(rhobj);
}

#endif
