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
 *  Name:           crown.c
 *  Description:    Crown Really Overcomplicates Witchhat Now
 *
 *                  Crown is a slight modification of witchhat-- it
 *                  changes the probing strategy for buckets.
 *
 *                  The idea is loosely adapted the "hopscotch" hash
 *                  probing algorithm.  Hopscotch itself is a cool
 *                  mash-up of multiple techniques, but we cannot use
 *                  it as-is in our lock-free and wait-free
 *                  algorithms, because it requires moving buckets (we
 *                  could conceptually use the whole approach if we
 *                  add locks per-newshat).
 *
 *                  The core trick is that we use an extra bitfield in
 *                  each bucket to "cache" linear probing information,
 *                  so that we can skip checking lots of buckets that
 *                  are going to be a waste of time.
 *
 *                  Essentially, for any item that hashes to a bucket
 *                  at address N, that bucket will have a bitfield
 *                  cache... by default this is 64 bits, though 32
 *                  bits is supported via compiler flag.
 *
 *                  Let's number the bits from left to right, starting
 *                  at 0. If, in bucket N, bit 0 is set, then an hash
 *                  value that hashed to bucket N has reserved bucket
 *                  N.  If the bit 1 is set, then some hash value that
 *                  our hash function mapped to bucket N has been
 *                  stored at the location N + 1.
 *
 *                  The only reason we'd store such a value at N + 1
 *                  is if bucket N was already reserved.  Note that it
 *                  might not have been reserved by a value that
 *                  hashed to N... it could have been, for instance,
 *                  N-3.
 *
 *                  And, naturally, if a hash bit x has a value of
 *                  zero 0, it indicates that NO value hashing to
 *                  location N is stored at (N + x) % table_size.
 *
 *                  The cool thing is, if we have multiple bits set to
 *                  1, we know that we probed the buckets in between,
 *                  and those buckets are in use by hashes that
 *                  originally hashed to some OTHER bucket.
 *
 *                  So, if we can cheaply identify bits from left to
 *                  right that are set to one, we can probe ONLY to
 *                  those locations, until we run out of bits to
 *                  probe.
 *
 *                  And indeed, most architectures have a "CLZ"
 *                  instruction that "counts leading zeros".  We just
 *                  have to load a copy the cached neighborhood map,
 *                  look at the leftmost bit, then remove it from our
 *                  copy, and repeat until we either find our item, or
 *                  until we run out of cached places to look.
 *
 *                  Once our cache runs out, we resume linear probing
 *                  like normal.
 *
 *                  Operations that reserve buckets set the cache once
 *                  they've successfully reserved a bucket, but before
 *                  they have completed any other operation.
 *
 *                  There are a few things we need to mind, however:
 *
 *                  1) Different architectures give different values
 *                     for CLZ(0). This is easy to avoid by stopping
 *                     when the cache is zero.
 *
 *                  2) If we aren't careful, we could get in a race
 *                     condition in setting the cache, leading to
 *                     duplicate entries in the hash table.
 *
 *                     Consider, for example, two threads, A and B,
 *                     both trying to write a *new* entry with the
 *                     same hash value H, at approximately the same
 *                     time.
 *
 *                     Let's assume that the cache is clear, and that
 *                     A starts out a little bit ahead of B.  It sees
 *                     the cache is clear, and probes until it finds
 *                     an empty bucket.  Let's say that it also
 *                     successfully installs H into whatever bucket
 *                     was found via probing.
 *
 *                     A now needs to write the cache information
 *                     out. But let's assume the worst-- that A gets
 *                     suspended right before it does that.
 *
 *                     During A's downtime, thread C swiftly comes
 *                     along, with a different hash value, V.  But V
 *                     happens to hash to the same bucket that H does.
 *
 *                     It probes through the bucket that thread A
 *                     managed to reserve, and keeps going.  Let's say
 *                     it ends up in the very next bucket, and
 *                     SUCCESSFULLY updates the cache.
 *
 *                     Now thread B comes along, looking to write an
 *                     item associated with the hash value H.  It
 *                     loads the neighborhood map, which has C's
 *                     changes, but not A's changes.
 *
 *                     Thread A now wakes up, and updates the map. But
 *                     B's copy doesn't have the right bit set, so B
 *                     will skip over the bucket A used, and write H
 *                     to a SECOND bucket.  That... is bad.
 *
 *                     There are two different ways we can address this:
 *
 *                     1) We could force puts and adds to use full
 *                        linear probing.  It's irrelevent if replace
 *                        / remove operations race with an insert-- we
 *                        can just (conceptually) linearize contending
 *                        calls before the insertion.
 *
 *                     2) We could force puts and adds that end up
 *                        probing beyond the cache to test each store
 *                        hash value beyond the cache, to see if the
 *                        entry would naturally hash to the same
 *                        bucket as it.
 *
 *                        In cases where it does, it will "help"
 *                        update the cache, and not proceed until the
 *                        cache is correct.
 *
 *                     I suspect approach #2 will be more performant,
 *                     but currently I am going to implement this both
 *                     ways (selectable via compiler flag).
 *
 *                     For those curious, the new code below
 *                     definitely adds a few loops. But they are all
 *                     very clearly bounded with small upper bounds,
 *                     often by the number of bits in the cache.
 *
 *                     As a result, like witchhat, this algorithm is
 *                     fully wait-free.
 *
 *                     Note that our comments only document
 *                     differences from witchhat.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include <hatrack.h>

// clang-format off

// Most of the store functions are needed by other modules, for better
// or worse, so we lifted their prototypes into the header.
static crown_store_t  *crown_store_migrate(crown_store_t *, crown_t *);
static inline bool     crown_help_required(uint64_t);
static inline bool     crown_need_to_help (crown_t *);

crown_t *
crown_new(void)
{
    crown_t *ret;

    ret = (crown_t *)malloc(sizeof(crown_t));

    crown_init(ret);

    return ret;
}

crown_t *
crown_new_size(char size)
{
    crown_t *ret;

    ret = (crown_t *)malloc(sizeof(crown_t));

    crown_init_size(ret, size);

    return ret;
}

void
crown_init(crown_t *self)
{
    crown_init_size(self, HATRACK_MIN_SIZE_LOG);

    return;
}

void
crown_init_size(crown_t *self, char size)
{
    crown_store_t *store;
    uint64_t       len;

    if (size > (ssize_t)(sizeof(intptr_t) * 8)) {
	abort();
    }

    if (size < HATRACK_MIN_SIZE_LOG) {
	abort();
    }

    len              = 1 << size;
    store            = crown_store_new(len);
    self->next_epoch = 1;
    
    atomic_store(&self->store_current, store);
    atomic_store(&self->item_count, 0);

    return;
}

void
crown_cleanup(crown_t *self)
{
    mmm_retire(atomic_load(&self->store_current));

    return;
}

void
crown_delete(crown_t *self)
{
    crown_cleanup(self);
    free(self);

    return;
}

void *
crown_get(crown_t *self, hatrack_hash_t hv, bool *found)
{
    void           *ret;
    crown_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);
    ret   = crown_store_get(store, hv, found);
    
    mmm_end_op();

    return ret;
}

void *
crown_put(crown_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void           *ret;
    crown_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);
    ret   = crown_store_put(store, self, hv, item, found, 0);
    
    mmm_end_op();

    return ret;
}

void *
crown_replace(crown_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void           *ret;
    crown_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);
    ret   = crown_store_replace(store, self, hv, item, found, 0);
    
    mmm_end_op();

    return ret;
}

bool
crown_add(crown_t *self, hatrack_hash_t hv, void *item)
{
    bool            ret;
    crown_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);    
    ret   = crown_store_add(store, self, hv, item, 0);
    
    mmm_end_op();

    return ret;
}

void *
crown_remove(crown_t *self, hatrack_hash_t hv, bool *found)
{
    void           *ret;
    crown_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);    
    ret   = crown_store_remove(store, self, hv, found, 0);
    
    mmm_end_op();

    return ret;
}

uint64_t
crown_len(crown_t *self)
{
    return atomic_read(&self->item_count);
}

hatrack_view_t *
crown_view(crown_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t *ret;
    
    mmm_start_basic_op();
    
    ret = crown_view_fast(self, num, sort);

    mmm_end_op();

    return ret;
}

/* This is the witchhat version.  We do not invoke mmm here; the dict
 *  class wraps this operation in mmm.
 */
hatrack_view_t *
crown_view_fast(crown_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t *view;
    hatrack_view_t *p;
    crown_bucket_t *cur;
    crown_bucket_t *end;
    crown_record_t  record;
    uint64_t        num_items;
    uint64_t        alloc_len;
    crown_store_t  *store;

    store     = atomic_read(&self->store_current);
    alloc_len = sizeof(hatrack_view_t) * (store->last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = store->buckets;
    end       = cur + (store->last_slot + 1);

    while (cur < end) {
        record        = atomic_read(&cur->record);
        p->sort_epoch = record.info & CROWN_EPOCH_MASK;

	if (!p->sort_epoch) {
	    cur++;
	    continue;
	}
	
        p->item = record.item;
	
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

    if (sort) {
	qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

    return view;
}

/* This is modified to copy the store first, ensuring a consistent view.
 * But it's much slower, since we're doing a LOT of extra work.
 *
 * In practice, seems to be 2x slower for sorted views, and 10x slower
 * for unsorted views.  Probably could get that down a little bit by
 * sorting the store and yielding from the store, but not enough to
 * matter.
 */
hatrack_view_t *
crown_view_slow(crown_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t *view;
    hatrack_view_t *p;
    crown_bucket_t *cur;
    crown_bucket_t *end;
    crown_record_t  record;
    uint64_t        num_items;
    uint64_t        alloc_len;
    crown_store_t  *store;
    bool            expected;
    
    while (true) {
	store    = atomic_read(&self->store_current);
	expected = false;

	if (CAS(&store->claimed, &expected, true)) {
	    break;
	}
	crown_store_migrate(store, self);
    }

    crown_store_migrate(store, self);

    alloc_len = sizeof(hatrack_view_t) * (store->last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = store->buckets;
    end       = cur + (store->last_slot + 1);

    while (cur < end) {
        record        = atomic_read(&cur->record);
        p->sort_epoch = record.info & CROWN_EPOCH_MASK;

	if (!p->sort_epoch) {
	    cur++;
	    continue;
	}
	
        p->item       = record.item;
	
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

    if (sort) {
	qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }
    
    mmm_retire(store);

    return view;
}

crown_store_t *
crown_store_new(uint64_t size)
{
    crown_store_t *store;
    uint64_t       alloc_len;

    alloc_len = sizeof(crown_store_t) + sizeof(crown_bucket_t) * size;
    store     = (crown_store_t *)mmm_alloc_committed(alloc_len);

    store->last_slot  = size - 1;
    store->threshold  = hatrack_compute_table_threshold(size);

    return store;
}

void *
crown_store_get(crown_store_t *self, hatrack_hash_t hv1, bool *found)
{
    uint64_t        bix;
    uint64_t        i;
    hatrack_hash_t  hv2;
    crown_bucket_t *bucket;
    crown_record_t  record;
    hop_t           map;

    /* Once we get the index of our initial bucket, the first thing
     * we're going to do is load up the "neighborhood map". If there
     * are bits set in this value, they represent buckets where a get
     * might live.  
     *
     * Per above, any zero bits to the left of the rightmost one bit
     * represent buckets we don't even need to examime.
     *
     * Note that we set i = -1 here (actually, MAXINT, but C doesn't
     * care), for reasons that should become clear after the first
     * loop.
     */
    bix = hatrack_bucket_index(hv1, self->last_slot);
    map = atomic_read(&self->buckets[bix].neighbor_map);
    i   = -1;

    /* CLZ stands for "count leading zeros."  
     * 
     * This function returns the bit position corresponding to the
     * first bucket we should jump to, in order to look for an
     * entry. Any leading zeros constitute buckets that definitely do
     * NOT contain the entry (as long as there is at least one item in
     * this bitfield).
     */
    while (map) {
	i      = CLZ(map);
	bucket = &self->buckets[(bix + i) & self->last_slot];
	hv2    = atomic_read(&bucket->hv);

	if (hatrack_hashes_eq(hv1, hv2)) {
	    record = atomic_read(&bucket->record);
	    if (record.info & CROWN_EPOCH_MASK) {
		if (found) {
		    *found = true;
		}
		return record.item;
	    }
	    else {
		goto not_found;
	    }
	}

	/* If the entry we just looked at was some other entry, we need
	 * to remove the corresponding bit from our local copy of the map,
	 * so that we don't end up in an infinite loop!
	 *
	 * CROWN_HOME_BIT is a constant, and it corresponds to turning
	 * on the leftmost bit of the bitfield, and ONLY that bit.
	 *
	 * To remove the correct bit from the map, we position
	 * CROWN_HOME_BIT by shifting it left by the result of CLZ(map),
	 * and then taking the complement of the bits, before the AND
	 * operation.
	 */
	map &= ~(CROWN_HOME_BIT >> i);
    }

    /* If we get here, we exhausted our linear probe result cache, and
     * none of the cached items had the right hash value. Therefore,
     * we need to fall back on good ol' fashioned linear probing.
     *
     * However, we can start the probing at the bucket index that's
     * one past i... since i is the last index we looked at when using
     * the cache.
     *
     * That's why we set i to -1 (well, MAXINT) above-- it indicates
     * that we didn't have anything in the cache, and we still add one
     * to it here so that we properly look at the "next" item.
     *
     * After we properly set i, the rest of this function is identical
     * to the witchhat implementation.
     *
     * Generally, the cache will cause us to look at a very small
     * number of buckets, even in a pretty full hash table.  And often
     * we will only need to look at one bucket beyond the cache.
     *
     * This caching helps us more and more, the more loaded the hash
     * table gets. 
     *
     * Note that, when we have a pretty empty hash table, the overhead
     * of loading the empty bitfield will be slightly higher than a
     * straight linear probe, as the direct probe will give an answer
     * in a bucket or two, without the extra loading and testing.
     *
     * We may be able to improve performance more by changing our
     * table metrics to wait longer to resize, to require more items
     * to resize up, and to resize down more readily, though we have
     * not explored that yet, and don't plan to do so in the near
     * future.
     */
    i++;
    bix = (bix + i) & self->last_slot;

    for (; i < self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2    = atomic_read(&bucket->hv);
	
        if (hatrack_bucket_unreserved(hv2)) {
            goto not_found;
        }
	
        if (!hatrack_hashes_eq(hv1, hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        record = atomic_read(&bucket->record);
	
        if (record.info & CROWN_EPOCH_MASK) {
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

/* Our put operation is a little more challenging than most of our
 * other operations:
 *
 * 1) We need to keep track of the initial bucket that we hash to, so
 *    that, IF we end up acquiring a new bucket, we can update the probing
 *    cache.
 *
 * 2) We need to avoid the race condition discussed at the comment at
 *    the top. 
 *
 *  As we mentioned above, there are two ways we could handle the
 *  second issue: a) Forcing linear probing for put (and add)
 *  operations; and b) Implement a "helping" mechanism for updating
 *  the cache.
 *
 *  Option a) would make the code base fairly simple-- it would be the
 *  same as witchhat's put operation, except for updating the
 *  cache. Option b) is more complicated, but almost certainly more
 *  efficient for any reasonable table sizes.
 *
 *  Here, we currently do "all of the above", making the resulting
 *  code even more complicated!  By default we use option B, but if
 *  you compile with HATRACK_FULL_LINEAR_PROBES defined, you'll get
 *  option A, which I wouldn't recommend for general purpose use, but
 *  would be fine if you know your table is going to be sparsely
 *  populated.
 */
void *
crown_store_put(crown_store_t  *self,
		crown_t        *top,
		hatrack_hash_t  hv1,
		void           *item,
		bool           *found,
		uint64_t        count)
{
    void           *old_item;
    bool            new_item;
    uint64_t        bix;
    uint64_t        i;
    hatrack_hash_t  hv2;
    crown_bucket_t *bucket;
    crown_bucket_t *orig_bucket;
    crown_record_t  record;
    crown_record_t  candidate;
    hop_t           map;
    hop_t           new_map;
    hop_t           bit_to_set;
    
#ifndef HATRACK_FULL_LINEAR_PROBES
    uint64_t        orig_index;
#endif    

    bix         = hatrack_bucket_index(hv1, self->last_slot);
    orig_bucket = &self->buckets[bix];

#ifndef HATRACK_FULL_LINEAR_PROBES
    /* When we're not linear probing, we first iterate through the
     * buckets that are sitting in the linear probe cache.  The logic
     * here is identical to the logic in "get", other than the fact
     * that, if we find a bucket with the right hash value, we then do
     * the things appropriate for our operation...
     */
    i          = -1;
    map        = atomic_read(&orig_bucket->neighbor_map);
    orig_index = bix;

    while (map) {
	i = CLZ(map);
	bucket = &self->buckets[(bix + i) & self->last_slot];
	hv2    = atomic_read(&bucket->hv);

	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}

	map &= ~(CROWN_HOME_BIT >> i);
    }

    i++;
    bix = (bix + i) & self->last_slot;
    
#else
    i = 0;
#endif    

    for (; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
	hv2    = atomic_read(&bucket->hv);
	
	if (hatrack_bucket_unreserved(hv2)) {
	    if (CAS(&bucket->hv, &hv2, hv1)) {
		if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
		    goto migrate_and_retry;
		}

		map        = atomic_read(&orig_bucket->neighbor_map);
		bit_to_set = CROWN_HOME_BIT >> i;
		
		do {
		    new_map = map | bit_to_set;
		} while (!CAS(&orig_bucket->neighbor_map, &map, new_map));
		
		goto found_bucket;
	    }
	}
	
	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}

#ifndef HATRACK_FULL_LINEAR_PROBES
	/* This code addresses the race condition discussed at the top
	 * of this file. As we linear probe past the end of our cache,
	 * when we see a hash value that is *not* ours, we check to
	 * see if the current bucket SHOULD have a one bit set in our
	 * probing cache.
	 *
	 * If the answer is yes, then we "help", and keep trying to
	 * set the bit, until we know it's been successfully set
	 * (whether by us, or another thread).
	 *
	 * We do this before we probe the next bucket. In this way, we
	 * eliminate the chance of a race condition.
	 */
	if (hatrack_bucket_index(hv2, self->last_slot) == orig_index) {
	    map        = atomic_read(&orig_bucket->neighbor_map);
	    bit_to_set = CROWN_HOME_BIT >> i;
	    
	    while (!(map & bit_to_set)) {
		new_map = map | bit_to_set;
		CAS(&orig_bucket->neighbor_map, &map, new_map);
	    }
	}
#endif	
	
	bix = (bix + 1) & self->last_slot;
	continue;
    }
	    
    // The rest of this operation is identical to Witchhat.    
 migrate_and_retry:
    count = count + 1;
    if (crown_help_required(count)) {
	HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);
	
	atomic_fetch_add(&top->help_needed, 1);
	
	self     = crown_store_migrate(self, top);
	old_item = crown_store_put(self, top, hv1, item, found, count);
	
	atomic_fetch_sub(&top->help_needed, 1);
	
	return old_item;
    }
    
    self = crown_store_migrate(self, top);
    return crown_store_put(self, top, hv1, item, found, count);

 found_bucket:
    record = atomic_read(&bucket->record);
    
    if (record.info & CROWN_F_MOVING) {
	goto migrate_and_retry;
    }

    if (record.info & CROWN_EPOCH_MASK) {
	if (found) {
	    *found = true;
	}
	
	old_item       = record.item;
	new_item       = false;
	candidate.info = record.info;
    }
    else {
	if (found) {
	    *found = false;
	}
	
	old_item       = NULL;
	new_item       = true;
	candidate.info = CROWN_F_INITED | top->next_epoch++;
    }

    candidate.item = item;

    if (CAS(&bucket->record, &record, candidate)) {
        if (new_item) {
            atomic_fetch_add(&top->item_count, 1);
        }
	
        return old_item;
    }

    if (record.info & CROWN_F_MOVING) {
	goto migrate_and_retry;
    }

    if (!new_item) {
	if (atomic_read(&self->used_count) >= self->threshold) {
	    crown_store_migrate(self, top);
	}
    }

    return item;
}

void *
crown_store_replace(crown_store_t    *self,
		       crown_t       *top,
		       hatrack_hash_t hv1,
		       void          *item,
		       bool          *found,
		       uint64_t       count)
{
    void           *ret;
    uint64_t        bix;
    uint64_t        i;
    hatrack_hash_t  hv2;
    crown_bucket_t *bucket;
    crown_record_t  record;
    crown_record_t  candidate;
    hop_t           map;    

    bix = hatrack_bucket_index(hv1, self->last_slot);
    map = atomic_read(&self->buckets[bix].neighbor_map);
    i   = -1;

    /* Since replace never acquires a bucket, it is not subject to the
     * potential race condition that the put and add operations must
     * deal with.  
     *
     * Therefore, the algorithm for finding a bucket is basically the
     * same as with the get operation.
     */
    while (map) {
	i      = CLZ(map);
	bucket = &self->buckets[(bix + i) & self->last_slot];
	hv2    = atomic_read(&bucket->hv);

	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}
	
	map &= ~(CROWN_HOME_BIT >> i);
    }

    i++;
    bix = (bix + i) & self->last_slot;
    
    for (; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
	hv2    = atomic_read(&bucket->hv);
	
	if (hatrack_bucket_unreserved(hv2)) {
	    goto not_found;
	}
	
	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}
	
	bix = (bix + 1) & self->last_slot;
	continue;
    }

 not_found:
    if (found) {
	*found = false;
    }
    return NULL;

 found_bucket:
    record = atomic_read(&bucket->record);
    
    if (record.info & CROWN_F_MOVING) {
    migrate_and_retry:
	count = count + 1;
	
	if (crown_help_required(count)) {
	    HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);
	    
	    atomic_fetch_add(&top->help_needed, 1);
	    self = crown_store_migrate(self, top);
	    ret  = crown_store_replace(self, top, hv1, item, found, count);
	    
	    atomic_fetch_sub(&top->help_needed, 1);
	    
	    return ret;
	}
	
	self = crown_store_migrate(self, top);
	return crown_store_replace(self, top, hv1, item, found, count);
    }

    if (!(record.info & CROWN_EPOCH_MASK)) {
	goto not_found;
    }

    candidate.item = item;
    candidate.info = record.info;

    if(!CAS(&bucket->record, &record, candidate)) {
	if (record.info & CROWN_F_MOVING) {
	    goto migrate_and_retry;
	}
	
	goto not_found;
    }
    
    if (found) {
	*found = true;
    }

    if (atomic_read(&self->used_count) >= self->threshold) {
	crown_store_migrate(self, top);
    }    

    return record.item;
}

/* Because the add operation always acquires a new bucket when
 * successful, we need to protect against the same race condition we
 * protected against in the put operation.
 *
 * The bucket logic implementation is basically identical. Please
 * see above for exposition. 
 */
bool
crown_store_add(crown_store_t    *self,
		   crown_t       *top,
		   hatrack_hash_t hv1,
		   void          *item,
		   uint64_t       count)
{
    uint64_t        bix;
    uint64_t        i;
    hatrack_hash_t  hv2;
    crown_bucket_t *bucket;
    crown_bucket_t *orig_bucket;    
    crown_record_t  record;
    crown_record_t  candidate;
    hop_t           map;
    hop_t           new_map;
    hop_t           bit_to_set;
    
#ifndef HATRACK_FULL_LINEAR_PROBES
    uint64_t        orig_index;
#endif

    bix = hatrack_bucket_index(hv1, self->last_slot);
    orig_bucket = &self->buckets[bix];

#ifndef HATRACK_FULL_LINEAR_PROBES
    i          = -1;
    map        = atomic_read(&orig_bucket->neighbor_map);
    orig_index = bix;

    while (map) {
	i          = CLZ(map);
	bucket     = &self->buckets[(bix + i) & self->last_slot];
	hv2        = atomic_read(&bucket->hv);

	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}
	
	map &= ~(CROWN_HOME_BIT >> i);
    }

    i++;
    bix = (bix + i) & self->last_slot;
    
#else
    i = 0;
#endif
    
    for (; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
	hv2    = atomic_read(&bucket->hv);
	
	if (hatrack_bucket_unreserved(hv2)) {
	    if (CAS(&bucket->hv, &hv2, hv1)) {
		if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
		    goto migrate_and_retry;
		}
		
		map        = atomic_read(&orig_bucket->neighbor_map);
		bit_to_set = CROWN_HOME_BIT >> i;
		
		do {
		    new_map = map | bit_to_set;
		} while (!CAS(&orig_bucket->neighbor_map, &map, new_map));
		
		goto found_bucket;
	    }
	}
	
	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}

#ifndef HATRACK_FULL_LINEAR_PROBES	
	if (hatrack_bucket_index(hv2, self->last_slot) == orig_index) {
	    map = atomic_read(&orig_bucket->neighbor_map);
	    bit_to_set = CROWN_HOME_BIT >> i;
	    
	    while (!(map & bit_to_set)) {
		new_map = map | bit_to_set;
		CAS(&orig_bucket->neighbor_map, &map, new_map);
	    }
	}
#endif	

	bix = (bix + 1) & self->last_slot;
	continue;
    }

 migrate_and_retry:
    count = count + 1;
    if (crown_help_required(count)) {
	bool ret;

	HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);
	
	atomic_fetch_add(&top->help_needed, 1);
	
	self = crown_store_migrate(self, top);
	ret  = crown_store_add(self, top, hv1, item, count);
	
	atomic_fetch_sub(&top->help_needed, 1);

	return ret;
    }
    
    self = crown_store_migrate(self, top);
    return crown_store_add(self, top, hv1, item, count);

found_bucket:
    record = atomic_read(&bucket->record);
    if (record.info & CROWN_F_MOVING) {
	goto migrate_and_retry;
    }
    
    if (record.info & CROWN_EPOCH_MASK) {
        return false;
    }

    candidate.item = item;
    candidate.info = CROWN_F_INITED | top->next_epoch++;

    if (CAS(&bucket->record, &record, candidate)) {
	atomic_fetch_add(&top->item_count, 1);
        return true;
    }
    
    if (record.info & CROWN_F_MOVING) {
	goto migrate_and_retry;
    }

    return false;
}

/* As with "replace", this operation is safe to use the cache without
 * doing the extra work to guard against a race condition.  As a
 * result, the bucket acquisition logic is the same as with replace,
 * and effectively identical to "get".  Please see above for details.
 */
void *
crown_store_remove(crown_store_t *self,
		   crown_t       *top,
		   hatrack_hash_t hv1,
		   bool          *found,
		   uint64_t       count)
{
    void           *old_item;
    uint64_t        bix;
    uint64_t        i;
    hop_t           map;
    hatrack_hash_t  hv2;
    crown_bucket_t *bucket;
    crown_record_t  record;
    crown_record_t  candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);
    map = atomic_read(&self->buckets[bix].neighbor_map);
    i   = -1;

    while (map) {
	i      = CLZ(map);
	bucket = &self->buckets[(bix + i) & self->last_slot];
	hv2    = atomic_read(&bucket->hv);

	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}
	
	map &= ~(CROWN_HOME_BIT >> i);
    }

    i++;
    bix = (bix + i) & self->last_slot;
    
    for (; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2    = atomic_read(&bucket->hv);
	
        if (hatrack_bucket_unreserved(hv2)) {
	    goto not_found;
        }
	
        if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
        }
	
	bix = (bix + 1) & self->last_slot;
	continue;
    }

 not_found:
    if (found) {
        *found = false;
    }

    return NULL;

found_bucket:
    record = atomic_read(&bucket->record);
    if (record.info & CROWN_F_MOVING) {
    migrate_and_retry:
	count = count + 1;
	
	if (crown_help_required(count)) {
	    HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);
	    atomic_fetch_add(&top->help_needed, 1);
	    self     = crown_store_migrate(self, top);
	    old_item = crown_store_remove(self, top, hv1, found, count);
	    atomic_fetch_sub(&top->help_needed, 1);
	    return old_item;
	}
	
	self = crown_store_migrate(self, top);
	return crown_store_remove(self, top, hv1, found, count);
    }
    
    if (!(record.info & CROWN_EPOCH_MASK)) {
	goto not_found;
    }

    old_item       = record.item;
    candidate.item = NULL;
    candidate.info = CROWN_F_INITED;

    if (CAS(&bucket->record, &record, candidate)) {
        atomic_fetch_sub(&top->item_count, 1);

        if (found) {
            *found = true;
        }

	if (atomic_read(&self->used_count) >= self->threshold) {
	    crown_store_migrate(self, top);
	}
	
        return old_item;
    }
    
    if (record.info & CROWN_F_MOVING) {
	goto migrate_and_retry;
    }

    goto not_found;
}

/* Often when we migrate, we are growing the table. This probing
 * technique is less excellent the more sparsely populated the table
 * is.
 * 
 * So, it stands to reason that linear probing could be better, at
 * least for the early part of the migration.
 *
 * Here, we use a preprocessor macro to control whether or not we
 * should use the caches in the new table's buckets, or not:
 * HATRACK_SKIP_ON_MIGRATIONS (skip in the sense of use the cache to
 * skip probing we can safely avoid).
 *
 * We do NOT have a dynamic option here. And, we control it seprately
 * from the similar macro above... particularly because we currently
 * default to having put / add use the cache, but migrate go without.
 *
 * Note that the migration operation does NOT suffer from the race
 * condition we discuss up at the front. That's because our migrations
 * consists of a deterministic set of operations on the new table,
 * that are all guaranteed to happen in-order.  
 *
 * ALL threads that write to the new store during migration will be
 * trying to write the exact same things in the exact same order.
 */
static crown_store_t *
crown_store_migrate(crown_store_t *self, crown_t *top)
{
    crown_store_t  *new_store;
    crown_store_t  *candidate_store;
    uint64_t        new_size;
    crown_bucket_t *bucket;
    crown_bucket_t *new_bucket;
    crown_bucket_t *map_bucket;
    crown_record_t  record;
    crown_record_t  candidate_record;
    crown_record_t  expected_record;
    hatrack_hash_t  expected_hv;
    hatrack_hash_t  hv;
    uint64_t        i, j;
    uint64_t        bix;
    uint64_t        new_used;
    uint64_t        expected_used;
    hop_t           map;
    hop_t           new_map;

#ifdef HATRACK_SKIP_ON_MIGRATIONS
    uint64_t        original_bix;
#endif    

    new_used  = 0;
    new_store = atomic_read(&top->store_current);
    
    if (new_store != self) {
	return new_store;
    }

    for (i = 0; i <= self->last_slot; i++) {
        bucket                = &self->buckets[i];
        record                = atomic_read(&bucket->record);
        candidate_record.item = record.item;

	if (record.info & CROWN_F_MOVING) {
	    
	    if (record.info & CROWN_EPOCH_MASK) {
		new_used++;
	    }
	    continue;
	}
	    
	OR2X64L(&bucket->record, CROWN_F_MOVING);

	record = atomic_read(&bucket->record);

	if (record.info & CROWN_EPOCH_MASK) {
	    new_used++;
	} else {
	    OR2X64L(&bucket->record, CROWN_F_MOVED); 
	}
    }

    new_store = atomic_read(&self->store_next);

    if (!new_store) {
	if (crown_need_to_help(top)) {
	    new_size = (self->last_slot + 1) << 1;
	}
	else {
	    new_size        = hatrack_new_size(self->last_slot, new_used);
	}
	
        candidate_store = crown_store_new(new_size);
	
        if (!CAS(&self->store_next, &new_store, candidate_store)) {
            mmm_retire_unused(candidate_store);
        }
        else {
            new_store = candidate_store;
        }
    }

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[i];
        record = atomic_read(&bucket->record);

        if (record.info & CROWN_F_MOVED) {
            continue;
        }

        hv         = atomic_read(&bucket->hv);
        bix        = hatrack_bucket_index(hv, new_store->last_slot);
	map_bucket = &new_store->buckets[bix];

#ifdef HATRACK_SKIP_ON_MIGRATIONS
	original_bix = bix;
	map          = atomic_read(&map_bucket->neighbor_map);
	j            = -1;

	while (map) {
	    uint64_t ix;
	    
	    j           = CLZ(map);
	    ix          = (original_bix + j) & new_store->last_slot;
	    new_bucket  = &new_store->buckets[ix];
	    expected_hv = atomic_read(&new_bucket->hv);
	    if (hatrack_hashes_eq(hv, expected_hv)) {
		goto found_bucket;
	    }

	    map &= ~(CROWN_HOME_BIT >> j);
	}

	j++;
	bix = (original_bix + j) & new_store->last_slot;
#else
	j = 0;
#endif
	
	for (; j <= new_store->last_slot; j++) {
            new_bucket     = &new_store->buckets[bix];
	    expected_hv    = atomic_read(&new_bucket->hv);
	    
	    if (hatrack_bucket_unreserved(expected_hv)) {
		if (CAS(&new_bucket->hv, &expected_hv, hv)) {
		    map        = atomic_read(&map_bucket->neighbor_map);
		    new_map    = map | (CROWN_HOME_BIT >> j);
		    CAS(&map_bucket->neighbor_map, &map, new_map);
		    
		    break;
		}
	    }
	    
	    if (!hatrack_hashes_eq(expected_hv, hv)) {
		bix = (bix + 1) & new_store->last_slot;
		continue;
            }
	    
            break;
        }

#ifdef HATRACK_SKIP_ON_MIGRATIONS
    found_bucket:
#endif	
        candidate_record.info = record.info & CROWN_EPOCH_MASK;
        candidate_record.item = record.item;
        expected_record.info  = 0;
        expected_record.item  = NULL;

        CAS(&new_bucket->record,
	     &expected_record,
	     candidate_record
	   );

	OR2X64L(&bucket->record, CROWN_F_MOVED);
    }

    expected_used = 0;
    
    CAS(&new_store->used_count,
         &expected_used,
         new_used
       );

    if (CAS(&top->store_current,
	     &self,
	     new_store
	   )) {
	if (!self->claimed) {
	    mmm_retire(self);
	}
    }

    return top->store_current;
}

static inline bool
crown_help_required(uint64_t count)
{
    if (count == HATRACK_RETRY_THRESHOLD) {
	return true;
    }
    
    return false;
}


static inline bool
crown_need_to_help(crown_t *self) {
    return (bool)atomic_read(&self->help_needed);
}
