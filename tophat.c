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
 *  Name:           tophat.c
 *  Description:    Adaptive hash table that starts off fast, but
 *                  migrates to a multi-reader / multi-writer
 *                  implementation once the table is accessed by
 *                  multiple threads simultaneously.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "tophat.h"

// clang-format off
static void             tophat_init_base         (tophat_t *, bool);
static void            *tophat_st_get            (refhat1_t *, hatrack_hash_t *,
						  bool *);
static void            *tophat_st_put            (refhat1_t *, hatrack_hash_t *,
						  void *, bool *);
static void            *tophat_st_replace        (refhat1_t *, hatrack_hash_t *,
						  void *, bool *);
static bool            tophat_st_add             (refhat1_t *, hatrack_hash_t *,
						  void *);
static void           *tophat_st_remove          (refhat1_t *, hatrack_hash_t *,
						  bool *);
static void            tophat_st_delete          (refhat1_t *);
static void            tophat_st_delete_store    (refhat1_t *);
static uint64_t        tophat_st_len             (refhat1_t *);
static hatrack_view_t *tophat_st_view            (refhat1_t *, uint64_t *,
						  bool);

#define TOPHAT_USE_LOCKING_ALGORITHMS
#ifdef TOPHAT_USE_LOCKING_ALGORITHMS

#include "ballcap.h"
#include "newshat.h"

static void tophat_migrate_to_ballcap (tophat_t *, refhat1_t *);
static void tophat_migrate_to_newshat (tophat_t *, refhat1_t *);

extern ballcap_store_t *ballcap_store_new(uint64_t);
extern newshat_store_t *newshat_store_new(uint64_t);

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
static void tophat_migrate_to_woolhat (tophat_t *, refhat1_t *);
static void tophat_migrate_to_witchhat(tophat_t *, refhat1_t *);

static hatrack_vtable_t cst_vtable = {
n    .init    = (hatrack_init_func)woolhat_init,
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

#endif

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
	tophat_migrate_to_ballcap(self, (refhat1_t *)implementation.htable);
    } else {
	tophat_migrate_to_newshat(self, (refhat1_t *)implementation.htable);
    }
#else
    if (self->flags & TOPHAT_F_CONSISTENT_VIEWS) {
	tophat_migrate_to_woolhat(self, (refhat1_t *)implementation.htable);
    } else {
	tophat_migrate_to_witchhat(self, (refhat1_t *)implementation.htable);
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
tophat_get(tophat_t *self, hatrack_hash_t *hv, bool *found)
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
tophat_put(tophat_t *self, hatrack_hash_t *hv, void *item, bool *found)
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
tophat_replace(tophat_t *self, hatrack_hash_t *hv, void *item, bool *found)
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
tophat_add(tophat_t *self, hatrack_hash_t *hv, void *item)
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
tophat_remove(tophat_t *self, hatrack_hash_t *hv, bool *found)
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
    refhat1_t         *initial_table;
    tophat_algo_info_t info;    
    
    alloc_len     = sizeof(refhat1_t);

    initial_table = (refhat1_t *)mmm_alloc_committed(alloc_len);
    info.htable   = initial_table;
    info.vtable   = &st_vtable;
    
    refhat_init((refhat_t *)initial_table);
    initial_table->backref   = (void *)self;

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
tophat_st_get(refhat1_t *rhobj, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    pthread_mutex_lock(&rhobj->mutex);
    ret = refhat_get((refhat_t *)rhobj, hv, found);
    pthread_mutex_unlock(&rhobj->mutex);
    
    return ret;
}

static void *
tophat_st_put(refhat1_t *rhobj, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;
    
    if(pthread_mutex_trylock(&rhobj->mutex)) {
	pthread_mutex_lock(&rhobj->mutex);
	tophat_migrate((tophat_t *)rhobj->backref);
	pthread_mutex_unlock(&rhobj->mutex);	
	return tophat_put((tophat_t *)rhobj->backref, hv, item, found);
    }

    ret = refhat_put((refhat_t *)rhobj, hv, item, found);
    
    pthread_mutex_unlock(&rhobj->mutex);

    return ret;
}

static void *
tophat_st_replace(refhat1_t *rhobj, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    if (pthread_mutex_trylock(&rhobj->mutex)) {
	tophat_migrate((tophat_t *)rhobj->backref);
	pthread_mutex_unlock(&rhobj->mutex);	
	return tophat_replace((tophat_t *)rhobj->backref, hv, item, found);
    }

    ret = refhat_replace((refhat_t *)rhobj, hv, item, found);
    
    pthread_mutex_unlock(&rhobj->mutex);

    return ret;
}

static bool
tophat_st_add(refhat1_t *rhobj, hatrack_hash_t *hv, void *item)
{
    bool ret;

    if (pthread_mutex_lock(&rhobj->mutex)) {
	tophat_migrate((tophat_t *)rhobj->backref);
	pthread_mutex_unlock(&rhobj->mutex);
	return tophat_add((tophat_t *)rhobj->backref, hv, item);
    }
    
    ret = refhat_add((refhat_t *)rhobj, hv, item);
    
    pthread_mutex_unlock(&rhobj->mutex);

    return ret;
}

static void *
tophat_st_remove(refhat1_t *rhobj, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    if (pthread_mutex_lock(&rhobj->mutex)) {    
	tophat_migrate((tophat_t *)rhobj->backref);
	pthread_mutex_unlock(&rhobj->mutex);
	return tophat_remove((tophat_t *)rhobj->backref, hv, found);
    }

    ret = refhat_remove((refhat_t *)rhobj, hv, found);
    
    pthread_mutex_unlock(&rhobj->mutex);    

    return ret;
}

static void
tophat_st_delete(refhat1_t *rhobj)
{
    mmm_add_cleanup_handler(rhobj, (void (*)(void *))tophat_st_delete_store);
    mmm_retire(rhobj);
}

static void
tophat_st_delete_store(refhat1_t *rhobj)
{
    free(rhobj->buckets);
    pthread_mutex_destroy(&rhobj->mutex);
}

static uint64_t
tophat_st_len(refhat1_t *rhobj) {
    uint64_t ret;
    
    pthread_mutex_lock(&rhobj->mutex);
    ret = refhat_len((refhat_t *)rhobj);
    pthread_mutex_unlock(&rhobj->mutex);

    return ret;
}

static hatrack_view_t *
tophat_st_view(refhat1_t *rhobj, uint64_t *num, bool sort) {
    hatrack_view_t *ret;
    
    pthread_mutex_lock(&rhobj->mutex);
    ret = refhat_view((refhat_t *)rhobj, num, sort);
    pthread_mutex_unlock(&rhobj->mutex);

    return ret;
}

#ifdef TOPHAT_USE_LOCKING_ALGORITHMS

static void
tophat_migrate_to_ballcap(tophat_t *tophat, refhat1_t *rhobj)
{
    ballcap_t         *new_table;
    uint64_t           i, n, bix;
    refhat_bucket_t   *cur;
    ballcap_bucket_t  *target;
    ballcap_record_t  *record;
    tophat_algo_info_t implementation;

    new_table        = (ballcap_t *)malloc(sizeof(ballcap_t));
    new_table->store = ballcap_store_new(rhobj->last_slot + 1);

    for (n = 0; n <= rhobj->last_slot; n++) {
	cur = &rhobj->buckets[n];
	if (cur->deleted || hatrack_bucket_unreserved(&cur->hv)) {
	    continue;
	}
	bix = hatrack_bucket_index(&cur->hv, rhobj->last_slot);	
	for (i = 0; i <= rhobj->last_slot; i++) {
	    target = &new_table->store->buckets[bix];
	    
	    if (hatrack_bucket_unreserved(&target->hv)) {	    
		    target->hv       = cur->hv;
		    record           = mmm_alloc_committed(sizeof(ballcap_record_t));
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
    pthread_mutex_init(&new_table->migrate_mutex, NULL);
    implementation.htable = new_table;
    implementation.vtable = &cst_vtable;
    atomic_store(&tophat->implementation, implementation);
    tophat_st_delete(rhobj);
}

static void
tophat_migrate_to_newshat(tophat_t *tophat, refhat1_t *rhobj)
{
    newshat_t         *new_table;
    uint64_t           n, i, bix;
    refhat_bucket_t   *cur;
    newshat_bucket_t  *target;
    tophat_algo_info_t implementation;

    new_table        = (newshat_t *)malloc(sizeof(newshat_t));
    new_table->store = newshat_store_new(rhobj->last_slot + 1);

    for (n = 0; n <= rhobj->last_slot; n++) {
	cur = &rhobj->buckets[n];
	if (cur->deleted || hatrack_bucket_unreserved(&cur->hv)) {
	    continue;
	}
	bix = hatrack_bucket_index(&cur->hv, rhobj->last_slot);
	for (i = 0; i <= rhobj->last_slot; i++) {
	    target = &new_table->store->buckets[bix];
	    
	    if (hatrack_bucket_unreserved(&target->hv)) {
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
    tophat_st_delete(rhobj);    
}

#else

    
    epoch                        = atomic_read(&mmm_epoch);
    
    if (epoch < rhobj->next_epoch) {
	atomic_fetch_add(&mmm_epoch, rhobj->next_epoch - epoch);
    }

static void
tophat_migrate_to_woolhat(tophat_t *tophat, refhat1_t *rhobj)
{
    woolhat_t         *new_table;
    uint64_t           n, i, bix;
    refhat_bucket_t   *cur;
    woolhat_bucket_t  *target;
    tophat_algo_info_t implementation;
    
}

static void
tophat_migrate_to_witchhat(tophat_t *tophat, refhat1_t *rhobj)
{
}

#endif
