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

#include <hatrack.h>

// clang-format off
static void             tophat_init_base     (tophat_t *);
static void		tophat_st_migrate    (tophat_st_ctx_t *);

// The migration algorithms can be selected dynamically, but
// at the time of initialization of the tophat_t object.
static void *tophat_migrate_to_newshat (tophat_t *);
static void *tophat_migrate_to_ballcap (tophat_t *);
static void *tophat_migrate_to_witchhat(tophat_t *);
static void *tophat_migrate_to_woolhat (tophat_t *);

/* These are meant to be "friend" methods; private to others, but we
 * need access to them. So they're not in the .h files, just re-declared
 * as extern methods here.
 *
 * We simply dispatch to the right migration method based on the
 * dst_type field, set at initialization time.
 *
 * We also use witchhat_store_new(), but dict.c uses a bunch of the
 * witchhat_store functions as well, so they got lifted to witchhat.h.
 */
extern newshat_store_t  *newshat_store_new (uint64_t);
extern ballcap_store_t  *ballcap_store_new (uint64_t);
extern woolhat_store_t  *woolhat_store_new (uint64_t);

static inline void *
tophat_migrate(tophat_t *self)
{
    switch (self->dst_type) {
    case TOPHAT_T_FAST_LOCKING:
	return tophat_migrate_to_newshat(self);
    case TOPHAT_T_FAST_WAIT_FREE:
	return tophat_migrate_to_witchhat(self);
    case TOPHAT_T_CONSISTENT_LOCKING:
	return tophat_migrate_to_ballcap(self);
    case TOPHAT_T_CONSISTENT_WAIT_FREE:
	return tophat_migrate_to_woolhat(self);
    default:
	__builtin_unreachable();
    }
}    

tophat_t *
tophat_new_fast_mx(void)
{
    tophat_t *ret;

    ret = (tophat_t *)malloc(sizeof(tophat_t));

    tophat_init_fast_mx(ret);

    return ret;
}

tophat_t *
tophat_new_fast_wf(void)
{
    tophat_t *ret;

    ret = (tophat_t *)malloc(sizeof(tophat_t));

    tophat_init_fast_wf(ret);

    return ret;
}

tophat_t *
tophat_new_cst_mx(void)
{
    tophat_t *ret;

    ret = (tophat_t *)malloc(sizeof(tophat_t));

    tophat_init_cst_mx(ret);

    return ret;
}

tophat_t *
tophat_new_cst_wf(void)
{
    tophat_t *ret;

    ret = (tophat_t *)malloc(sizeof(tophat_t));

    tophat_init_cst_wf(ret);

    return ret;
}

/* If we've migrated to a multi-threaded table, then the
 * single-threaded implementation is already cleaned up, except for
 * deallocating the mutex.
 * 
 * Similarly, if we never migrate, then there's nothing there to clean
 * up.
 */
void
tophat_cleanup(tophat_t *self)
{
    if (atomic_load(&self->mt_table)) {
	(*self->mt_vtable.delete)(self->mt_table);
    }
    else {
	mmm_retire(self->st_table->buckets);
	mmm_retire(self->st_table);
    }
    
    pthread_mutex_destroy(&self->mutex);

    return;
}

void
tophat_delete(tophat_t *self)
{
    tophat_cleanup(self);
    free(self);

    return;
}

/* The different initialization functions simply set up the right
 * virtual call table for the multi-threaded instance (in case it's
 * needed), and takes note of which table type it will be migrating
 * to.
 */
void
tophat_init_fast_mx(tophat_t *self)
{
    tophat_init_base(self);
    
    self->dst_type          = TOPHAT_T_FAST_LOCKING;
    self->mt_vtable.init    = (hatrack_init_func)newshat_init;
    self->mt_vtable.get     = (hatrack_get_func)newshat_get;
    self->mt_vtable.put     = (hatrack_put_func)newshat_put;
    self->mt_vtable.replace = (hatrack_replace_func)newshat_replace;
    self->mt_vtable.add     = (hatrack_add_func)newshat_add;
    self->mt_vtable.remove  = (hatrack_remove_func)newshat_remove;
    self->mt_vtable.delete  = (hatrack_delete_func)newshat_delete;
    self->mt_vtable.len     = (hatrack_len_func)newshat_len;
    self->mt_vtable.view    = (hatrack_view_func)newshat_view;

    return;
}

void
tophat_init_fast_wf(tophat_t *self)
{
    tophat_init_base(self);
    
    self->dst_type          = TOPHAT_T_FAST_WAIT_FREE;
    self->mt_vtable.init    = (hatrack_init_func)witchhat_init;
    self->mt_vtable.get     = (hatrack_get_func)witchhat_get;
    self->mt_vtable.put     = (hatrack_put_func)witchhat_put;
    self->mt_vtable.replace = (hatrack_replace_func)witchhat_replace;
    self->mt_vtable.add     = (hatrack_add_func)witchhat_add;
    self->mt_vtable.remove  = (hatrack_remove_func)witchhat_remove;
    self->mt_vtable.delete  = (hatrack_delete_func)witchhat_delete;
    self->mt_vtable.len     = (hatrack_len_func)witchhat_len;
    self->mt_vtable.view    = (hatrack_view_func)witchhat_view;

    return;
}

void
tophat_init_cst_mx(tophat_t *self)
{
    tophat_init_base(self);
    
    self->dst_type          = TOPHAT_T_CONSISTENT_LOCKING;
    self->mt_vtable.init    = (hatrack_init_func)ballcap_init;
    self->mt_vtable.get     = (hatrack_get_func)ballcap_get;
    self->mt_vtable.put     = (hatrack_put_func)ballcap_put;
    self->mt_vtable.replace = (hatrack_replace_func)ballcap_replace;
    self->mt_vtable.add     = (hatrack_add_func)ballcap_add;
    self->mt_vtable.remove  = (hatrack_remove_func)ballcap_remove;
    self->mt_vtable.delete  = (hatrack_delete_func)ballcap_delete;
    self->mt_vtable.len     = (hatrack_len_func)ballcap_len;
    self->mt_vtable.view    = (hatrack_view_func)ballcap_view;

    return;
}

void
tophat_init_cst_wf(tophat_t *self)
{
    tophat_init_base(self);
    
    self->dst_type          = TOPHAT_T_CONSISTENT_WAIT_FREE;
    self->mt_vtable.init    = (hatrack_init_func)woolhat_init;
    self->mt_vtable.get     = (hatrack_get_func)woolhat_get;
    self->mt_vtable.put     = (hatrack_put_func)woolhat_put;
    self->mt_vtable.replace = (hatrack_replace_func)woolhat_replace;    
    self->mt_vtable.add     = (hatrack_add_func)woolhat_add;
    self->mt_vtable.remove  = (hatrack_remove_func)woolhat_remove;
    self->mt_vtable.delete  = (hatrack_delete_func)woolhat_delete;
    self->mt_vtable.len     = (hatrack_len_func)woolhat_len;
    self->mt_vtable.view    = (hatrack_view_func)woolhat_view;

    return;
}

void *
tophat_get(tophat_t *self, hatrack_hash_t hv, bool *found) {
    void               *mt_table;
    uint64_t            bix, i;
    tophat_st_ctx_t    *ctx;
    tophat_st_bucket_t *cur;
    tophat_st_record_t  record;

    /* The high-level approach here is to see if we're using a
     * multi-threaded table, and dispatch to it, if so.
     *
     * If not, we need to protect our reads of the underlying table
     * store, via mmm. If the underlying language implementation can
     * enforce single-threaded access until the threading system
     * starts, then this is unnecessary-- here, we are assuming that
     * our implementation is responsible for detecting concurrent
     * access.
     *
     * We do the detection in writer-threads, and do it in a way that
     * actually supports mutiple readers and a single, concurrent
     * writer.
     *
     * Note that we call the mmm wrappers whether or not we need to
     * use them. They're cheap enough that it doesn't seem to much
     * matter from a performance perspective (mmm_start_basic_op()
     * just loads the current epoch and our thread ID, using the later
     * to index into an array to store the epoch; mmm_end_op() stores
     * a constant value in the same place).
     *
     * If we don't do this, we need to complicate the logic and load
     * mt_table twice to avoid a race condition.
     */

    mmm_start_basic_op();

    mt_table = atomic_read(&self->mt_table);
    
    if (mt_table) {
	mmm_end_op();
	
	return (*self->mt_vtable.get)(mt_table, hv, found);
    }

    /* Note that the call to mmm_start_basic_op() guaranteed that, if
     * mt_table was NULL, we will be safe to read self->st_table.
     * That's because mmm_retire() won't get called by the migrating
     * thread until AFTER it sets mt_table. So if we read that NULL,
     * then we know a concurrent write thread will respect our
     * reservation, and not free the single threaded table out from
     * under us.
     */
    ctx = self->st_table;

    /* From this point down, the implementation is basically the same
     * as in refhat, except for the calls to mmm_end_op(), and the
     * somewhat different data structure layout so that we can
     * atomically read the item and the epoch in one read, just in
     * case there are readers running concurrently with a writer.
     */
    bix = hatrack_bucket_index(hv, ctx->last_slot);

    for (i = 0; i <= ctx->last_slot; i++) {
	cur = &ctx->buckets[bix];
	
	if (hatrack_hashes_eq(hv, cur->hv)) {
	    record = atomic_read(&cur->record);
	    
	    if (!record.epoch) {
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
	    
	    return record.item;
	}
	
	if (hatrack_bucket_unreserved(cur->hv)) {
	    if (found) {
		*found = false;
	    }
	    
	    mmm_end_op();
	    
	    return NULL;	    
	}
	
	bix = (bix + 1) & ctx->last_slot;
    }
    __builtin_unreachable();
}

void *
tophat_put(tophat_t *self, hatrack_hash_t hv, void *item, bool *found) {
    void               *mt_table;
    tophat_st_ctx_t    *ctx;
    uint64_t            bix;
    uint64_t            i;
    tophat_st_bucket_t *cur;
    tophat_st_record_t  record;
    void               *ret;


    /* Unlike with readers, we use a lock to prevent multiple
     * simultaneous writers in the single-threaded implementation.
     *
     * Obviously, once we've migrated, we do not want to use this lock
     * on write operations. Therefore, we attempt to load mt_table
     * right away, and only lock if it's not initialized.
     *
     * Of course, it could end up initialized while we're waiting on
     * the lock, so we need to check again once the lock is acquired.
     *
     * As with the read operation above, there's really no reason for
     * this extra complexity if the language is willing to perform
     * migrations from within the threading subsystem's initilization
     * process, making all of this overhead go away (even though it's
     * already exceptionally small).
     */
    mt_table = atomic_load(&self->mt_table);

    if (mt_table) {
	return (*self->mt_vtable.put)(mt_table, hv, item, found);
    }

    if (pthread_mutex_trylock(&self->mutex)) {
	/* If we get here, we found someone else had the write-lock.
	 * Once they're done, we'll first see if someone else
	 * completed the migration, by trying to read mt_table (any
	 * thread that came before us and noticed a migration was
	 * necessary will have set it, before releasing the lock).  If
	 * not, we'll do a migration, and then retry in the new table.
	 */
	if (pthread_mutex_lock(&self->mutex)) {
	    abort();
	}
	
	mt_table = atomic_read(&self->mt_table);
	
	if (!mt_table) {
	    mt_table = tophat_migrate(self);
	}
	
	if (pthread_mutex_unlock(&self->mutex)) {
	    abort();
	}
	
	mt_table = atomic_load(&self->mt_table);
	
	return (*self->mt_vtable.put)(mt_table, hv, item, found);
    } 
    /* Here we successfully acquired the lock, so we didn't detect
     * multiple concurrent writers, so we can proceed with our write
     * without any worries; no migration to a different table type can
     * begin until after we yield the lock.
     *
     * This is semantically identical to refhat, except for the calls
     * to pthread_mutex_unlock(), and the differtent data structure
     * layout to ensure readers can run in parallel (we store the
     * epoch and item as one unit, via atomic_store()).
     */
    ctx = self->st_table;    
    bix = hatrack_bucket_index(hv, ctx->last_slot);

    for (i = 0; i <= ctx->last_slot; i++) {
	cur = &ctx->buckets[bix];
	
	if (hatrack_hashes_eq(hv, cur->hv)) {
	    record = atomic_read(&cur->record);
	    
	    if (!record.epoch) {
		record.item  = item;
		record.epoch = ctx->next_epoch++;
		ctx->item_count++;

		atomic_store(&cur->record, record);

		if (found) {
		    *found = false;
		}
		
		if (pthread_mutex_unlock(&self->mutex)) {
		    abort();
		}
		
		return NULL;
	    }
	    ret         = record.item;
	    record.item = item;
	    
	    atomic_store(&cur->record, record);

	    if (found) {
		*found = true;
	    }
	    
	    if (pthread_mutex_unlock(&self->mutex)) {
		abort();
	    }
	    
	    return ret;
	}
	if (hatrack_bucket_unreserved(cur->hv)) {
	    if (ctx->used_count + 1 == ctx->threshold) {
		tophat_st_migrate(ctx);
		
		if (pthread_mutex_unlock(&self->mutex)) {
		    abort();
		}
		
		return tophat_put(self, hv, item, found);
	    }
	    
	    ctx->used_count++;
	    ctx->item_count++;

	    cur->hv = hv;
	    record.item = item;
	    record.epoch = ctx->next_epoch++;

	    atomic_store(&cur->record, record);

	    if (found) {
		*found = false;
	    }
	    
	    if (pthread_mutex_unlock(&self->mutex)) {
		abort();
	    }
	    
	    return NULL;
	}
	bix = (bix + 1) & ctx->last_slot;
    }

    __builtin_unreachable();
}

// See tophat_put for notes on the overall approach.
void *
tophat_replace(tophat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void               *mt_table;
    tophat_st_ctx_t    *ctx;
    uint64_t            bix;
    uint64_t            i;
    tophat_st_bucket_t *cur;
    tophat_st_record_t  record;
    void               *ret;

    mt_table = atomic_load(&self->mt_table);

    if (mt_table) {
	return (*self->mt_vtable.replace)(mt_table, hv, item, found);
    }

    if (pthread_mutex_trylock(&self->mutex)) {
	if (pthread_mutex_lock(&self->mutex)) {
	    abort();
	}
	
	mt_table = atomic_read(&self->mt_table);
	
	if (!mt_table) {
	    mt_table = tophat_migrate(self);
	}
	
	if (pthread_mutex_unlock(&self->mutex)) {
	    abort();
	}
	
	mt_table = atomic_load(&self->mt_table);
	
	return (*self->mt_vtable.replace)(mt_table, hv, item, found);
    }

    ctx = self->st_table;
    bix = hatrack_bucket_index(hv, ctx->last_slot);

    for (i = 0; i <= ctx->last_slot; i++) {
        cur = &ctx->buckets[bix];
	
        if (hatrack_hashes_eq(hv, cur->hv)) {
            record = atomic_read(&cur->record);
	    
            if (!record.epoch) {
	    empty:
                if (found) {
                    *found = false;
                }
		
		if (pthread_mutex_unlock(&self->mutex)) {
		    abort();
		}
		
                return NULL;
            }
	    
            ret         = record.item;
            record.item = item;

            atomic_store(&cur->record, record);

            if (found) {
                *found = true;
            }
	    
	    if (pthread_mutex_unlock(&self->mutex)) {
		abort();
	    }
	    
            return ret;
        }
	
        if (hatrack_bucket_unreserved(cur->hv)) {
	    goto empty;
        }
	
        bix = (bix + 1) & ctx->last_slot;
    }
    __builtin_unreachable();
}

// See tophat_put for notes on the overall approach.
bool
tophat_add(tophat_t *self, hatrack_hash_t hv, void *item)
{
    void               *mt_table;
    tophat_st_ctx_t    *ctx;
    uint64_t            bix;
    uint64_t            i;
    tophat_st_bucket_t *cur;
    tophat_st_record_t  record;
    

    mt_table = atomic_load(&self->mt_table);

    if (mt_table) {
	return (*self->mt_vtable.add)(mt_table, hv, item);
    }

    if (pthread_mutex_trylock(&self->mutex)) {
	if (pthread_mutex_lock(&self->mutex)) {
	    abort();
	}
	
	mt_table = atomic_read(&self->mt_table);
	
	if (!mt_table) {
	    mt_table = tophat_migrate(self);
	}
	
	if (pthread_mutex_unlock(&self->mutex)) {
	    abort();
	}
	
	mt_table = atomic_load(&self->mt_table);
	
	return (*self->mt_vtable.add)(mt_table, hv, item);
    }

    ctx = self->st_table;
    bix = hatrack_bucket_index(hv, ctx->last_slot);

    for (i = 0; i <= ctx->last_slot; i++) {
        cur = &ctx->buckets[bix];
	
        if (hatrack_hashes_eq(hv, cur->hv)) {
            record = atomic_read(&cur->record);
	    
            if (!record.epoch) {
                record.item  = item;
                record.epoch = ctx->next_epoch++;

                atomic_store(&cur->record, record);

                ctx->item_count++;
		
		if (pthread_mutex_unlock(&self->mutex)) {
		    abort();
		}
		
                return true;
            }
	    if (pthread_mutex_unlock(&self->mutex)) {
		abort();
	    }
	    
            return false;
        }
	
        if (hatrack_bucket_unreserved(cur->hv)) {
            if (ctx->used_count + 1 == ctx->threshold) {
                tophat_st_migrate(ctx);
		
		if (pthread_mutex_unlock(&self->mutex)) {
		    abort();
		}
		
                return tophat_add(self, hv, item);
            }
	    
            ctx->used_count++;
            ctx->item_count++;
	    
            cur->hv      = hv;
            record.item  = item;
            record.epoch = ctx->next_epoch++;

            atomic_store(&cur->record, record);
	    
	    if (pthread_mutex_unlock(&self->mutex)) {
		abort();
	    }
	    
            return true;
        }
	
        bix = (bix + 1) & ctx->last_slot;
    }
    __builtin_unreachable();
 }

// See tophat_put for notes on the overall approach.
void *
tophat_remove(tophat_t *self, hatrack_hash_t hv, bool *found)
{
    void               *mt_table;
    tophat_st_ctx_t    *ctx;
    uint64_t            bix;
    uint64_t            i;
    tophat_st_bucket_t *cur;
    tophat_st_record_t  record;
    void               *ret;
    

    mt_table = atomic_load(&self->mt_table);

    if (mt_table) {
	return (*self->mt_vtable.remove)(mt_table, hv, found);
    }

    if (pthread_mutex_trylock(&self->mutex)) {
	if (pthread_mutex_lock(&self->mutex)) {
	    abort();
	}
	
	mt_table = atomic_read(&self->mt_table);
	
	if (!mt_table) {
	    mt_table = tophat_migrate(self);
	}
	
	if (pthread_mutex_unlock(&self->mutex)) {
	    abort();
	}
	
	mt_table = atomic_load(&self->mt_table);
	
	return (*self->mt_vtable.remove)(mt_table, hv, found);
    }

    ctx = self->st_table;
    bix = hatrack_bucket_index(hv, ctx->last_slot);

    for (i = 0; i <= ctx->last_slot; i++) {
        cur = &ctx->buckets[bix];
	
        if (hatrack_hashes_eq(hv, cur->hv)) {
            record = atomic_read(&cur->record);
	    
            if (!record.epoch) {
                if (found) {
                    *found = false;
                }
		
		if (pthread_mutex_unlock(&self->mutex)) {
		    abort();
		}
		
                return NULL;
            }

            // No need to write over the item pointer; we won't
            // ever access it if epoch == 0.
            ret          = record.item;
            record.epoch = 0;

            atomic_store(&cur->record, record);

            --ctx->item_count;

            if (found) {
                *found = true;
            }
	    
	    if (pthread_mutex_unlock(&self->mutex)) {
		abort();
	    }
	    
            return ret;
        }
        if (hatrack_bucket_unreserved(cur->hv)) {
            if (found) {
                *found = false;
            }
	    
	    if (pthread_mutex_unlock(&self->mutex)) {
		abort();
	    }
	    
	    return NULL;
        }
	
        bix = (bix + 1) & ctx->last_slot;
    }
    __builtin_unreachable();
}

uint64_t
tophat_len(tophat_t *self)
{
    void            *mt_table;
    uint64_t         ret;

    /* In case mt_table isn't found, protect our ability to read into
     * st_table by creating an mmm_reservation.
     *
     * Again, this works because the migration function won't retire
     * self->st_table until mt_table is set. So as long as we get our
     * reservation in before checking mt_table, we're guaranteed that,
     * if mt_table is NULL, we will be able to read st_tbale.
     */
    mmm_start_basic_op();
    
    mt_table = atomic_load(&self->mt_table);
    
    if (mt_table) {
	mmm_end_op();
	
	return (*self->mt_vtable.len)(mt_table);	    
    }
    
    ret = self->st_table->item_count;
    mmm_end_op();
    
    return ret;
}

hatrack_view_t *
tophat_view(tophat_t *self, uint64_t *num, bool sort)
{
    tophat_st_ctx_t    *ctx;
    void               *mt_table;
    hatrack_view_t     *view;
    hatrack_view_t     *p;
    tophat_st_record_t  record;
    tophat_st_bucket_t *cur;
    tophat_st_bucket_t *end;
    uint64_t            alloc_len;
    uint64_t            n;
 

    /* The view operation being a reader, we wrap our single-threaded
     * activity using mmm_start_basic_op() and mmm_end_op() to protect
     * against the underlying single threaded hash table (or its
     * buckets) being deleted while we are using it.
     *
     * Otherwise, the single-threaded code is algorithmically
     * identical to refhat (though laid out a bit differently, since
     * we atomically load the epoch and item together).
     */
    mmm_start_basic_op();
    
    mt_table = atomic_load(&self->mt_table);
    
    if (mt_table) {
	mmm_end_op();
	
	return (*self->mt_vtable.view)(mt_table, num, sort);
    }
    
    ctx = self->st_table;

    /* Allow for concurrent writes by resizing down.  Upper
     * bound for buckets needed is the table size.
     * The variable n will track the actual size used.
     */
    alloc_len = sizeof(hatrack_view_t) * (ctx->last_slot + 1);
    n         = 0;
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = ctx->buckets;
    end       = cur + (ctx->last_slot + 1);

    while (cur < end) {
        if (hatrack_bucket_unreserved(cur->hv)) {
            cur++;
            continue;
        }

        record = atomic_read(&cur->record);

        if (!record.epoch) {
            cur++;
            continue;
        }

        p->item       = record.item;
        p->sort_epoch = record.epoch;

	n++;
        p++;
        cur++;
    }

    *num = n;

    if (sort) {
        qsort(view, n, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }
    
    mmm_end_op();

    return view;
}

/* tophat tables all start out in single-threaded mode. So we just
 * allocate the single-threaded implementation.
 */
static void
tophat_init_base(tophat_t *self)
{
    uint64_t            size;
    uint64_t            alloc_len;
    tophat_st_ctx_t    *table;
    
    alloc_len         = sizeof(tophat_st_ctx_t);
    table             = (tophat_st_ctx_t *)mmm_alloc_committed(alloc_len);
    self->st_table    = table;
    self->mt_table    = NULL;
    size              = HATRACK_MIN_SIZE; 
    alloc_len         = sizeof(tophat_st_bucket_t) * size;
    table->last_slot  = size - 1;
    table->threshold  = hatrack_compute_table_threshold(size);
    table->next_epoch = 1; // 0 is reserved for deleted.
    table->buckets    = (tophat_st_bucket_t *)mmm_alloc_committed(alloc_len);

    pthread_mutex_init(&self->mutex, NULL);

    return;
}

/* This migration function is the one used by single-threaded
 * instances to migrate stores when we're staying single-threaded.
 *
 * Per above, it's identical to refhat's implementation, except for
 * the atomic reading / writing of the item/epoch, and the slightly
 * different data structure layout that results.
 */
static void
tophat_st_migrate(tophat_st_ctx_t *ctx)
{
    tophat_st_bucket_t *new_buckets;
    tophat_st_bucket_t *cur_bucket;
    tophat_st_bucket_t *new_bucket;
    tophat_st_record_t  record;
    uint64_t            bucket_size;
    uint64_t            num_buckets;
    uint64_t            new_last_slot;
    uint64_t            size;
    uint64_t            i, n, bix;

    bucket_size   = sizeof(tophat_st_bucket_t);
    num_buckets   = hatrack_new_size(ctx->last_slot, ctx->item_count + 1);
    new_last_slot = num_buckets - 1;
    size          = num_buckets * bucket_size;
    new_buckets   = (tophat_st_bucket_t *)mmm_alloc_committed(size);

    for (n = 0; n <= ctx->last_slot; n++) {
        cur_bucket = &ctx->buckets[n];

        if (hatrack_bucket_unreserved(cur_bucket->hv)) {
            continue;
        }

        record = atomic_read(&cur_bucket->record);

        if (!record.epoch) {
            continue;
        }

        bix = hatrack_bucket_index(cur_bucket->hv, new_last_slot);
	
        for (i = 0; i < num_buckets; i++) {
            new_bucket = &new_buckets[bix];
	    
            if (hatrack_bucket_unreserved(new_bucket->hv)) {
                new_bucket->hv     = cur_bucket->hv;
                new_bucket->record = record;
                break;
            }
	    
            bix = (bix + 1) & new_last_slot;
        }
    }
    
    mmm_retire(ctx->buckets);

    ctx->used_count = ctx->item_count;
    ctx->buckets    = new_buckets;
    ctx->last_slot  = new_last_slot;
    ctx->threshold  = hatrack_compute_table_threshold(num_buckets);

    return;
}

/* Remember that we already have a lock at this point. So the
 * migration is fairly straightforward, and will look not unlike the
 * newshat->newshat migration.
 */
static void *
tophat_migrate_to_newshat(tophat_t *self)
{
    tophat_st_ctx_t    *ctx;        // essentially, the current table.
    newshat_t          *new_table;
    tophat_st_bucket_t *cur_bucket; // pointer to the st bucket
    newshat_bucket_t   *new_bucket; // pointer to the newshat bucket
    tophat_st_record_t  cur_record;
    newshat_record_t    new_record;
    uint64_t            n, i, bix;
    

    ctx                      = self->st_table;
    new_table                = (newshat_t *)malloc(sizeof(newshat_t));
    new_table->store_current = newshat_store_new(ctx->last_slot + 1);

    for (n = 0; n <= ctx->last_slot; n++) {
	cur_bucket = &ctx->buckets[n];
	
	if (hatrack_bucket_unreserved(cur_bucket->hv)) {
	    continue;
	}
	
	cur_record = atomic_read(&cur_bucket->record);
	
	if (!cur_record.epoch) {
	    continue;
	}
	
	bix    = hatrack_bucket_index(cur_bucket->hv, ctx->last_slot);
	
	for (i = 0; i <= ctx->last_slot; i++) {
	    new_bucket = &new_table->store_current->buckets[bix];

	    /* Note that the implementation of our st buckets and
	     * newshat's are compatible, but due to the atomics, it's
	     * pretty challenging to get the cast to work, without
	     * significant uglification.
	     *
	     * Just go ahead and copy the data; the compiler will
	     * probably be able to optimize this away, and if not,
	     * it's a small bit of one-time overhead when we migrate
	     * anyway; it should amortize out.
	     */
	    if (hatrack_bucket_unreserved(new_bucket->hv)) {
		new_bucket->hv       = cur_bucket->hv;
		new_record.item      = cur_record.item;
		new_record.epoch     = cur_record.epoch;
		new_bucket->record   = new_record;
		new_bucket->migrated = false;
		break;
	    }
	    
	    bix = (bix + 1) & ctx->last_slot;
	}
    }

    new_table->store_current->used_count = ctx->item_count;
    new_table->item_count                = ctx->item_count;
    new_table->next_epoch                = ctx->next_epoch;
    
    pthread_mutex_init(&new_table->migrate_mutex, NULL);
    
    atomic_store(&self->mt_table, new_table);

    // Now that mt_table is set, we can retire the st implementation.    
    mmm_retire(ctx->buckets);
    mmm_retire(ctx);

    return (void *)new_table;
}

/* Unlike witchhat's migration function, we're doing this from the
 * comfort of a single thread, so don't need to do any CAS operations;
 * we can perform direct stores.
 */
static void *
tophat_migrate_to_witchhat(tophat_t *self)
{
    tophat_st_ctx_t    *ctx;
    witchhat_t         *new_table;
    tophat_st_bucket_t *cur_bucket;
    witchhat_bucket_t  *new_bucket;
    hatrack_hash_t      hv;
    tophat_st_record_t  cur_record;
    witchhat_record_t   new_record;    
    uint64_t            i, n, bix;

    ctx                      = self->st_table;
    new_table                = (witchhat_t *)malloc(sizeof(witchhat_t));
    new_table->store_current = witchhat_store_new(ctx->last_slot + 1);
    new_table->next_epoch    = ctx->next_epoch;
    new_table->item_count    = ctx->item_count;

    for (n = 0; n <= ctx->last_slot; n++) {
	cur_bucket = &ctx->buckets[n];
	
	if (hatrack_bucket_unreserved(cur_bucket->hv)) {
	    continue;
	}
	
	cur_record = atomic_read(&cur_bucket->record);
	
	if (!cur_record.epoch) {
	    continue;
	}
	
	bix = hatrack_bucket_index(cur_bucket->hv, ctx->last_slot);

	for (i = 0; i <= ctx->last_slot; i++) {
	    new_bucket = &new_table->store_current->buckets[bix];
	    hv         = atomic_read(&new_bucket->hv);
	    
	    if (hatrack_bucket_unreserved(hv)) {
		atomic_store(&new_bucket->hv, cur_bucket->hv);
		break;
	    }
	    
	    if (hatrack_hashes_eq(hv, cur_bucket->hv)) {
		break;
	    }
	    
	    bix = (bix + 1) & new_table->store_current->last_slot;
	    continue;
	}

	/* Data structure layout is compatable.
	 *
	 * Witchhat does steal two bits from MSB of the second word
	 * for status, but they will definitely be zero in the source
	 * (unless you've used a full 62 bits of epoch space, which is
	 * not even remotely realistic).
	 *
	 * Still, per above, we make a local copy into new_record,
	 * instead of just casting... just because it's challenging
	 * enough to get the cast to work, that it really makes the
	 * code ugly.
	 */
	new_record.item = cur_record.item;
	new_record.info = cur_record.epoch;
	
	atomic_store(&new_bucket->record, new_record);
    }
    
    atomic_store(&new_table->help_needed, 0);
    atomic_store(&new_table->store_current->used_count, ctx->item_count);
    atomic_store(&self->mt_table, new_table);

    // Now that mt_table is set, we can retire the st implementation.
    mmm_retire(ctx->buckets);
    mmm_retire(ctx);

    return (void *)new_table;
}

static void *
tophat_migrate_to_ballcap(tophat_t *self)
{
    tophat_st_ctx_t    *ctx;
    ballcap_t          *new_table;
    tophat_st_bucket_t *cur_bucket;
    ballcap_bucket_t   *new_bucket;
    tophat_st_record_t  cur_record;
    ballcap_record_t   *new_record;
    uint64_t            i, n, bix, record_len;


    ctx                      = self->st_table;
    new_table                = (ballcap_t *)malloc(sizeof(ballcap_t));
    new_table->store_current = ballcap_store_new(ctx->last_slot + 1);
    record_len               = sizeof(ballcap_record_t);

    for (n = 0; n <= ctx->last_slot; n++) {
	cur_bucket = &ctx->buckets[n];
	
	if (hatrack_bucket_unreserved(cur_bucket->hv)) {
	    continue;
	}

	cur_record = atomic_read(&cur_bucket->record);
	
	if (!cur_record.epoch) {
	    continue;
	}
	
	bix = hatrack_bucket_index(cur_bucket->hv, ctx->last_slot);
	
	for (i = 0; i <= ctx->last_slot; i++) {
	    new_bucket = &new_table->store_current->buckets[bix];
	    
	    if (hatrack_bucket_unreserved(new_bucket->hv)) {	    
		    new_bucket->hv       = cur_bucket->hv;
		    new_record           = mmm_alloc_committed(record_len);
		    new_record->item     = cur_record.item;
		    new_bucket->migrated = false;
		    new_bucket->record   = new_record;
		    mmm_set_create_epoch(new_record, cur_record.epoch);
		    break;
	    }
	    
	    bix = (bix + 1) & ctx->last_slot;
	}
    }

    new_table->store_current->used_count = ctx->item_count;
    new_table->item_count                = ctx->item_count;
    new_table->next_epoch                = ctx->next_epoch;
    
    pthread_mutex_init(&new_table->migrate_mutex, NULL);

    atomic_store(&self->mt_table, new_table);

    // Now that mt_table is set, we can retire the st implementation.
    mmm_retire(ctx->buckets);
    mmm_retire(ctx);

    return (void *)new_table;
}

/*
 * This follows the logic of the woolhat migration, but since we're
 * running the initial migration with a lock on writers, we have the
 * luxury of direct stores, instead of compare-and-swap operations.
 */
static void *
tophat_migrate_to_woolhat(tophat_t *self)
{
    tophat_st_ctx_t    *ctx;    
    woolhat_t          *new_table;
    tophat_st_bucket_t *cur_bucket;
    woolhat_history_t  *new_bucket;
    hatrack_hash_t      hv;
    tophat_st_record_t  cur_record;
    woolhat_record_t   *new_record;
    uint64_t            n, i, bix, record_len;

    ctx                      = self->st_table;
    new_table                = (woolhat_t *)malloc(sizeof(woolhat_t));
    new_table->store_current = woolhat_store_new(ctx->last_slot + 1);
    record_len               = sizeof(woolhat_record_t);
    new_table->cleanup_func  = NULL;
    new_table->cleanup_aux   = NULL;
    
    atomic_store(&new_table->help_needed, 0);

    for (n = 0; n <= ctx->last_slot; n++) {
	cur_bucket = &ctx->buckets[n];
	
	if (hatrack_bucket_unreserved(cur_bucket->hv)) {
	    continue;
	}

	cur_record = atomic_read(&cur_bucket->record);

	if (!cur_record.epoch) {
	    continue;
	}

	bix = hatrack_bucket_index(cur_bucket->hv, ctx->last_slot);
	
	for (i = 0; i <= ctx->last_slot; i++) {
	    new_bucket = &new_table->store_current->hist_buckets[bix];
	    hv         = atomic_load(&new_bucket->hv);
	    
	    if (hatrack_bucket_unreserved(hv)) {
		new_record
		    = (woolhat_record_t *)mmm_alloc_committed(record_len);
		new_record->item   = cur_record.item;
		new_record->next   = NULL;
		
		mmm_set_create_epoch(new_record, cur_record.epoch);
		atomic_store(&new_bucket->hv, cur_bucket->hv);
		atomic_store(&new_bucket->head, new_record);
		
		break;
	    }
	    
	    bix = (bix + 1) & ctx->last_slot;
	}
    }
    
    new_table->store_current->used_count = ctx->item_count;
    
    if (mmm_epoch < ctx->next_epoch) {
	atomic_store(&mmm_epoch, ctx->next_epoch);
    }

    atomic_store(&self->mt_table, new_table);

    // Now that mt_table is set, we can retire the st implementation.
    mmm_retire(ctx->buckets);
    mmm_retire(ctx);

    return (void *)new_table;
}

