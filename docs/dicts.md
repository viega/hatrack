# Wait-Free O(1) dictionaries

The default Hatrack dictionary is a fast, wait-free hash table, supporting multiple concurent readers and writers.

It is the first wait-free hash table fully backed by a single resizable array, without any sort of tree structure. Previous work in this space provided a wait-free associative interface[^1], but kept an internal tree structure that gives the algorithm an O(log n) lower bound on inserts and lookups. A more recent paper [^2] provides a wait-free hash table based on a universal construction, using extendible hashing, which does allow for O(1) operations, but keeps two tiers of buckets, a trie and 'actual' buckets.  Unfortunately, besides the extra indirection, the buckets in this scheme generally will not maintain memory locality as the hash table grows (the original use case for extendible hashing was file systems[^3]).

An unpublished hash table by Cliff Click from 2007 is nevertheless widely adopted in the Java standard library. It is lock-free, despite an early claim of wait freedom that was retracted.

However, Click's hash table is backed by an array, making it a true hash table. Hash tables benefit from amortized O(1) operations as long as their load factor (the ratio of records to table size) remains below 1, because the cost of operations can be bounded by a function of the load factor, independent of the values of n and m [^4].

The Hatrack dictionary is a true hash table like Click's, giving amortized O(1) operations. Yet, it is simple and truly wait-free, and address memory management issues for languages without full automatic garbage collection.

We additionally have a variant that caches linear probing results that can speed up hash tables nearing their load factor, though the additional constant overhead imposes a small fixed penalty, making it slightly less efficient when hash tables are not at high load.

As we document our algorithm below, we will focus on the following items:

1. How to support safe memory reclaimation without garbage collection in the face of overwrites and other issues due to parallelism.

2. Explaining the algorithms for the core operations, and arguing for correctness by identifying linearization points for the operations.

3. Showing wait freedom by examining every loop in the algorithm, and arguring how they will all end in finite time, even in the face of contention.


Core to this algorithm is our technique for growing and shrinking the backing store as the table needs to be resized. This migration operation is itself wait-free.  We discuss the migration operation, including how we keep it from impacting the wait freedom of other operations [here](migration.md).

## Memory Management

In non-garbage collected languages, we have two different concerns for memory management. First is the safe reclaimation of dynamically allocated memory within our data structure. Second is being able to make memory safety guarantees for users of the hash table that are independent of the user's memory management strategy.

### Internal store safety

The first problem, memory safety for data structures, is well addressed in the literature. We use a simple epoch-based memory reclaimation scheme internally[^5], as it simple, easy to implement correctly, and far more efficient than popular alternatives (e.g., Hazard Pointers and reference counting).

First note that the only dynamic allocation in our dictionary is for the underlying backing store. When we migrate to a different backing store (for instance, when growing the table), slow threads may still have references to the old store, and we need those references to stay live through the life of their operation (Additionally, we also need to ensure correct semantics when multiple stores are being used in parallel, but we consider that problem below).

With memory-based reclaimation, data structure operations are bookended with two calls:

- One that reads a current, monotonically increasing epoch, and stores that epoch in an array where each thread in the system has a single storage location.  We will call the epoch a 'reservation', and the array the reservation array.

- One that 'removes' the reservation, writing into the thread's slot in the reservation array an indication that there is no reservation.

In the simplest version of this scheme, we do nothing special during memory allocation. However, at some point, a thread will know that a memory object is no longer useful, *once all other threads that might be using the object are done with it*.

Specifically, the thread must have passed the linearization point at which the object should no longer be visible to threads who do not already have a reference to it.

At this point, instead of a deallocation operation (which might lead to other threads accessing freed memory), the thread calls a `retire` operation:

1. The thread marks the memory allocation with the epoch in which it was 'retired'.
2. The thread places the allocation on a thread-local 'retirement' list.

3. Periodically (usually after every `n` retirements, where `n` is an arbitrary but typically small number), the thread scans its retirement list, looking for objects it can reclaim.  We call this the 'empty' operation, which is detailed below.

The value of the epoch counter increments with every `m` allocations.  Considerations for the values of `m` and `n` are discussed below, but we currently bump `m` with every allocation, primarily to make analysis easier for ourselves.

The basic idea behind the algorithm is that, before retired objects can be freed, a thread will make sure that the epoch in which it was retired is LOWER than any active reservation by any thread.

That means the `empty` operation scans the reservation array, taking note of the lowest active reservation. It then scans its retirement list and frees any memory object whose retirement epoch is below that threshold, confident that no threads could possibly be accessing the memory cell.

This algorithm allows us to linearize all memory accesses for allocations using the system, based on the epoch.  The actual reclaimation of memory will happen at some epoch AFTER the retirement epoch: because the current thread has a reservation that is necessarily lower than or equal to the retirement epoch, it will not free anything it retires until a future operation in which its reservation is for a higher epoch than the retirement epoch.

Similarly, any thread that makes a reservation will see it honored. Even in the case where the thread suspends right after reading out an epoch X, but before writing X to the reservation array, the thread's reads will be safe, no matter how long the pause.  To consider why, impagine the thread writes out the epoch X during epoch X + N.  Any memory retired after thread X but before thread X + N cannot be referenced, because it will have been retired, meaning only threads that already have a reference can continue to use it. Any reference our thread now manages to acquire, even those retired in epoch X + N are guaranteed to be alive at least through the next epoch. And, in fact, since we've successfully written out X before taking any reference, any memory locations we see cannot be freed until some point after we yield our reservation.

The potenential downside to this scheme occurs when threads fail to remove their reservation when operating on a data structure, as it will cause any memory allocated after that thread's reservation takes effect to go unfreed in perpituity. That's one reason why such schemes are most valuable for data structure operations only, where we can take care to make sure we protect each operation, and that our operations are generally short-lived.

Crashed threads tend not to be a problem on Posix systems-- thread crashes generally result in a full process crash. But thread cancelation can be an issue. Threads can certainly detect crashes and cancelations and make sure to clean up, or other threads can have access to retirement lists (potentially by a threadsafe multi-user shared queue).

### Safety of user data

Due to the fact that most higher level languages have full garbage collection, there is little attention in the literature paid to the user's burden on memory management in situations where multiple threads may read from and mutate a shared data structure when that data structure holds dynamically allocated objects.

However, even in situations where users are reference counting, parallel data structures can lead to memory safety issues, even if the data structure itself is safe.  Particularly, just making sure to increase a reference count when storing a memory object, and decreasing the reference count when removing the object is not sufficient to provide safety.

For instance, consider two threads, T1 and T2, both of whom wish to operate on a data object O currently stored in a dictionary using the hash key K. Imagine that the only current copy of O is in the hash table, so its reference count is currently 1.

T1 intends to look up the item, and will immediately increment the reference count, once the reference to the object has been returned from our get() function. 

T2, on the other hand, intends to remove the entry from the hash table. The following worst-case scenario can easily happen:

1. T1's get() operation succeeds before T2's remove() operation begins.

2. However, T1 gets suspended after get() exits, but before the user code to increase O's reference count.

3. T2's remove() completes, and T2 decreases the reference count on O, causing the data to be freed immediately.

4. T1 wakes up, and attempts to incref O, which is a use-after-free error.

While epoch-based memory management schemes could address this problem, it is not suitable for memory objects with long lifetimes since, as we saw above, long reservations prevent the entire system from reclaiming memory.

Hatrack addresses this problem by allowing users to register two callbacks on a per-data structure basis:

- An 'ejection' callback, that, if registered, gets called with the object O at some point after the time when the hash table has stopped storing a reference to an object.

- A 'return' callback, that gets called BEFORE a get() operation returns.

The way these callbacks are implemented removes the race condition.  Here's how it works:

1. When an ejection handler is registered, the dictionary wraps every store S by creating a new data object (S'), using our epoch-based memory management system.  The hash table actually stores S'.

2. When an object is being overwritten or otherwise deleted (for instance, when the hash table itself is deleted, and we are cleaning up items remaining in the table), we call our 'retire' function on the memory management wrapper.  Importantly, we do NOT notify the calling thread that the object is ready to be considered ejected from the hash table.

3. At some point when our epoch-based memory management system determines that no other thread could possibly be accessing S', it will go to free S', at which point, it will also call the 'ejection' callback, passing in S as a parameter.  The intention is that users should be able to safely decrease their reference count or free at this point (or do any other appropriate memory management).

4. When a value is about to be returned from the table to the user (e.g., via a get() operation, or a view() operation), we call the 'return' callback, passing in S.  We do this before we call end_op() in our epoch-based memory manager, which we do before returning S from the get() function.

This scheme ensures that any get() operation has a safe oportunity to take appropriate action on data obejcts, such as by increasing a reference count, before the data structure will ever notify a competing thread about the object being ejected.

To illustrate, let's revisit the above example, where T1 does not complete its reference count before T2 frees the object.  How does that work in our scheme?

- T2 comes in parallel to T1, with a request to remove data object O from its current slot in the hash table.

- The hash table will actually retrieve O' from the slot, and 'retire' O'.  At this point, it returns 'success' to T2.  Since this all happens in an epoch protected by T2's reservation, O' will not actually be freed until some future epoch.

- T1's get() operation reads O' from the current cell, under the projection of our epoch-based memory manager.

- At some point before get() calls end_op() in the memory manager, it will invoke the registered 'return' callback.

- T1 will increment a reference count, or take any other appropriate measure to preserve its ability to read O in the future.

- T1 will then yield its reservation with the memory manager.

- At some future point, T2's thread will, when running the empty() operation, decide it is safe to free O'. At this point, T2 will call the ejection callback.

This scheme works because T2 does not retire S' until after it has passed the linearization point that removed S' from the table.  If T1's get() operation also has a reference, then it necessarily made a reservation for an epoch that is equal to, or earlier than, that linearization point.

Therefore, even if T1 is very slow, and T2 completes several more operations before T1 wakes up and finishes, T1's active reservation will prevent T2 from calling the ejection callback.

Notably, this approach does not require us to keep reservation lifetimes beyond the natural lifetime of the underlying operations.

## Core table operations

In this section, we review the core hash table operations, and demonstrate evidence for their correctness in the face of arbitrary parallelism.

As we describe the algorithms, we will elide the memory management aspect, and skip to the part where the operation is safely reading from a store.  That is, we can assume that our topmost operations have a wrapper around them like this:

```
arbitrary_op_wrapper(table, ...):
   start_op()  # Write an epoch reservation
   store = table->store
   arbitrary_op(store, ..., table)
   end_op()    # Remove the epoch reservation
```

We will assume that operations work out of the store variable, except during resizes, where the new store needs to be installed into the top-level data structure.  Additionally, when a thread participates in a resize, we assume it updates its pointer to the current store with the newly acquired store.

Note that every core operation consists of two phases.  The first we call 'bucket acquisition', which here means identifying the appropriate cell in the array for performing the second half of the operation.  The second phase is the 'core operation', which may be a 'get', or a mutation operation.

For bucket acquisition, we rely upon 'lazy deletions'[^4], in the case where buckets are deleted, as opposed to re-arranging buckets to minimize probing.

Particularly, we impose a constraint that, in any given backing store, once a bucket (cell) has been 'acquired' by a particular hash key, the bucket may never be associated with any other hash key.

That means, if a value with a particular hash key is deleted, the bucket is not used again unless, in the future of that store, a new item is inserted under the same hash key.

However, deleted cells are considered in part of the table's load factor, and can help trigger a store migration, at which point deleted cells are NOT migrated. Again, the migration process is [discussed here](migration.md).

This lazy deletion approach was also taken by Click in his hash table, and is in many respects an "obvious" approach for two reasons:

1. A lock-free or wait-free implementation that moves buckets in an active hash table is possible, but it is clearly complicated to do in a safe manner.[^6]

2. When hash keys cannot move, there will be very little contention among threads operating on buckets (there will only be contention when such threads are operating on the same bucket at the same time). That seems far preferable to the potential contention that would come from having to syncrhonize moves across multiple cells.

As a result, we need to worry about the correctness of our bucket acquisition logic, particularly given the case where different operations with different hash values may be competing to install themselves in the same bucket at the same time.

With our core operations, we need to consider the correctness of those operations in the face of competing operations. Particularly, when we go through a multi-phase process to write data, we need to be confident in our linearization points, and of correct semantics in the face of multiple concurrent operations in the same bucket.

Hatrack's dictionary supports the following core operations:

- **get(K)** which retrieves the item associated with a hash key K, if any.

- **put(K, V)**, which guarantees that it will store the value V, associated to the hash key K, whether or not the key previously had an associated item in the table (ultimately ejecting the old value, if present).

- **replace(K, V)**, which stores the value of V, but only if there is currently a value already associated with the key in the table, at the time of the operation.

- **add(K, V)**, which stores the value of V, but only if there is NO value already associated with the key in the table.

- **remove(K)**, which removes the value V associated with the key, if there is one.

- **view(B)**, which provides a 'view' of all the hash table buckets, with the option for a fully consistent view (the boolean parameter B).

### Hash values and keys

Most hash table interfaces accept two data objects, a "Key" and a "Value".  They calculate a hash value based on the key only, and then store both the key and the associated value in a bucket.

Our high-level dictionary interface looks like this, but we have implemented multiple low-level tables that have a much more primitive interface, in that they take only a "Hash Value" and an item.  We do not require the hash value to be computed near the time of the operation, nor do we require it to be a function of the key, nor do we require the key to even be stored:  The user of the low-level interface can choose to store both the key and the value in the 'item' field, based on their preference.

The only strong requirement we put on the hash value is that it be 128-bits. Ideally, this itself would be a randomly keyed universal hash, but in practice the preference seems to be for faster, non-keyed functions.  Certainly, if the hash function selection is poor, performance could suffer dramatically.

We require 128 bits so that, assuming the selection of a competent hash function, the odds of any two keys yielding the same hash key is so low as to be a non-issue. That allows us to ignore such collisions safely.

Particularly, the bounds of the birthday paradox tells us that, given an ideal hash function, one can add about 26 trillion items to the table, with a probability of any two items having the same hash value being about 1 in 1 trillion.

In contrast, with 64 bit keys, the probabilty of a collision goes up to 1% at a mere 190 million items in the table, making it a very real concern, making a (potentially costly) secondary comparison imperitive.

### Bucket layout

In our core algorithm, buckets consist of the following:

- A 128-bit hash value, `hv`.
- A 64-bit field `item` that may be any value.
- Three state bits.
- A 61 bit 'nonce' that is unique for every new insertion in the table.

The three state bits are primarily there to support our migration operation:

- The 'MOVING' bit signals to mutation operations that they must 'help' a migration, and then retry their mutation operation.
- The 'MOVED' bit signals to migrating threads that a particular cell is fully migrated, and they do not need to attempt to migrate it (this is an optimization, and not strictly necessary).
- The 'INITIALIZED' bit is set the first time that a bucket has something written to it, and is not removed for the life of the store. This bit is used to ensure that late migrators do not accidentially re-copy cells after they are deleted.

In this algorithm, deletion is denoted by erasing the nonce. Neither the INITIALIZED bit nor the nonce is strictly necessary for the algorithm-- if we add an additional state bit to specify whether a cell is deleted or not, we can do without EITHER the 'INITIALIZED' bit, or the nonce.

Note that the primary purpose of the nonce field is to approximate the insertion order for views, if people would like to attempt to present a view in near-insertion order. The nonce will not necessarily map to our linearized order when there is contention.

We do have another algorithm that allows for using the linearized insertion order for views, which we use in sets. We'll discuss this addition there. Primarily, we provide the approximation here, because we have the extra bits to do it. 

Specifically, we will atomically update buckets in two pieces, generally via a compare-and-swap operation:
1. The 128-bit hash
2. The item, state bits and nonce.

Another, alternative layout would be to keep the hash as 128 bits, skip the nonce, and 'steal' two to three bits from the 64-bit 'item' field for flags.  We will only strictly need two flag bits-- the third is an optimization for the migration process.

If we require all items placed in the hash table to be pointers, this scheme will work well; on modern architectures, memory allocations are eight-byte aligned by default, leaving three bits to 'steal'.

However, most modern architectures have an atomic 128-bit compare-and-swap operation, and it is approximately as efficient as a 64-bit compare-and-swap.

A second option would be to truncate the hash key to 64 bits, and do the pointer stealing trick, so that we can update the bucket with a single 128-bit compare-and-swap operation. But, not only does this have poor collision resistance properties (per above) that merits an additional identity check, but the performance benefit is not significant.  In particular, we have implemented this approach for comparative testing, and it only outperforms our core algorithm in limited cases, generally on very small table sizes.

A third option, for when 128-bit compare-and-swap operations are not available, would be to keep the bucket in dynamically allocated memory, and swap out the pointer, not the bucket contents, with a single update operation. This does require the state bits stealing from the pointer, instead of living in the bucket.

Again, we have implemented this version of the algorithm as well, and it actually tends to perform faster for read operations, but slower for mutations (due to the extra memory management necessary).

We will not focus on those variants here, but those algorithms are document in the source code (the single 128-bit algorithm in `hash/tiara.c` and the '64-bit CAS only' algorithm in `hash/oldhat.c`).

### Bucket acquisition

The bucket acquisition operation consists of two variants, one that will always acquire a bucket, even if there is not currently a bucket associated with the hash value (used for put and add operations), and one that will return ⊥ if there is not currently a bucket associated with the hash value (used for get, replace and remove operations).

Our primary goals for bucket acquisition are:

1. To make sure that, if a bucket in a store has become associated with a key, no other bucket in that store can ever be associated with the key.

2. That, if a thread is looking to acquire a bucket that is already associated with a key, they will always acquire the correct bucket.

To show that we meet our goals, we will identify the linearization points of bucket acquisition, and demonstrate how, once one thread has acquired a bucket in a store, no other thread searching with the same hash key will be able to acquire a different bucket.

First, we will look at the variant of operation used for when we might insert an item, and thus need to acquire a bucket if no bucket currently associated with the hash key is present in the table. This version of the operation will always return an index associated with the acquired bucket.

The bucket acquisition operation takes a hash value H and table size S as an input, and then does the following:

1. Computes I, an initial index into the array by computing the H % S (as is standard with hash tables).[^7]

2. Reads the hash value stored in bucket I.

3. If the stored hash value in the current bucket is the same as H, then the bucket has already been acquired, and we return I.

4. If the stored hash value in the current bucket is different than ours, then we do the following:
    a. If we have checked every bucket in the table, and found them all to be full, we perform a migration, then re-try the insertion from the beginning.

    b. Otherwise, we compute I = (I + 1) % S and jump to step 2.

5. If there is NO stored hash value in the current bucket, then we first check the store's counter of number of acquired buckets. If our acquiring the bucket would put us above our target load factor (75% in our implementation), then we call our resize function, and then re-try the operation.

6. Otherwise, we attempt to acquire the bucket we believe to be empty, trying to associate slot I with the hash value H. We do this with an atomic compare-and-swap operation, attempting to write H into the `hv` field, comparing it to an empty (all zero) bucket, and replacing it with H if the operation is successful.

7. If the compare-and-swap operation succeeds, we atomically increment our store's counter of acquired buckets, and return I.

8. If the operation fails, we jump to step 3 (without incrementing I).

When the compare-and-swap fails, it can only be because some parallel thread was successful in acquiring the bucket for their key. It could be a thread with a different hash value, or one with the same hash value as us.  This is why we don't increment I-- we need to re-check the same bucket.  And, we jump back to step 3 instead of step 2, because the compare-and-swap operation, when it fails, will load the value of the hash value in the bucket at the time of our compare-and-swap operation.

Note that, in this scheme, H = 0 is not allowed. However, since we are using 128-bit hash keys, in practice, we need not concern ourselves with selecting an algorithm that cannot produce 0 as an output, since a well-selected function would have approximately a 2^-128 chance of generating a zero value.  That's a 1 in 3.4 x 10^38, so small as to never be a reasonable concern.

For the version of bucket acquisition where we wish to return failure if a bucket has not yet been acquired, the first four steps are identical. Then, if we get to step 5 (the bucket is empty), or if we get to the point where we have checked every bucket, then we know the hash key in question has not acquired a bucket yet, and we simply return ⊥.  This variant will never trigger a resize operation, clearly.

In this version, the read in step 2 becomes the linearization point, whether we eventually read a value from a bucket's `hv` field that is identical to H, or whether we find an empty `hv` (and thus an empty, unacquired bucket, which proves that H has not been associated with a bucket yet).  If we determine a miss, the linearization point is the read of the final bucket.

#### Correctness

Were the hash table single threaded, this algorithm would clearly meet our correctness conditions, because the compare-and-swap never fails, and we never delete keys from the table, making this algorithm the simple, traditional linear probing algorithm, that can successfully determine whether or not a hash value is currently associated with a bucket.

We need only consider correctness in the face of contention. The only contention occurs when trying to apply the compare-and-swap operation in step 5.

Success in step 5 is the linearization point for bucket acquisition, making it easy for threads to always agree on the state of the table.

Let's consider any two arbitrary threads T1 and T2 are competing, attempting to install hash values H1 and H2, where H1 and H2 may or may not be identical.

We can easily iterate through the possible cases:

1. T1 successfully installs H1, and H1 ≠ H2
2. T1 successfully installs H1, and H1 = H2
3. T2 successfully installs H2, and H1 ≠ H2
4. T2 successfully installs H2, and H1 = H2

Note that, both threads compare against an empty bucket, and we disallow the empty bucket as a hash key. That means, the empty bucket's value cannot be the same as H1 or H2 (Or, in a more practical implementation is the same with such miniscule odds as to never be a concern in the lifetime of the universe).

As a result, only one compare-and-swap operation can possibly succeed, since all other compare-and-swaps will expect an empty bucket (i.e., empty value for the `hv` field), but see a hash key.

In cases 1 and 3, the 'winning' thread successfully acquires its bucket, and any future thread will consistently acquire the same bucket through simple linear probing, regardless of any parallel threads, since they will never need to perform a compare-and-swap operation as part of their bucket acquisition.

The losing thread checks the value that it lost to, sees that this bucket is reserved for some other hash key, and continues its probing.

In cases 2 and 4, the 'losing' thread also checks the value when it loses, and sees that the current bucket is now the correct bucket, successfully acquiring the bucket.

Since our hashing and probing approach is deterministic and identical across all parties, since, there is no case where a successfully acquired bucket in a store changes value, and since there is no case where a thread will skip an empty bucket, there is no way for two threads to acquire different buckets for the same hash value.

The only exceptional condition occurs when acquiring a new bucket would exceed our allowable table load, in which case we invoke our migration algorithm.

Our migration algorithm effectively consists of every thread iterating through the old store, and re-inserting into the new store via an **add** operation, meaning all threads will use the exact same algorithm as above to acquire buckets in the new table, with a completely linear order to the **add**s.

At that point, we retry our operation, using the above algorithm in the new store. In all cases, by the time our operation is successful, we've used the above algorithm to acquire a bucket in a store that was big enough to accomodate the insertion.

#### Wait freedom

To determine that the above acquisition algorithm is wait-free, we must look at all loops and recursion, and show how they will complete in a finite, bounded number of steps.

There are only three things in the algorithm worth noting:

- The primary loop is a probing loop, clearly bounded by S (the size of the hash table). The index only ever increments by a single value in the arithmetical group defined by the table size, and stops when every bucket has been visited, triggering a resize.

- The CAS operation is *not* retried if failed. Any thread that fails will re-check the current bucket a single time, before continuing to probe. There can be at most one CAS operation per bucket in any given store.

- When a resize is triggered, this algorithm is recursively invoked.

Assume for the moment that the resize operation only ever doubles the size of the underlying store. It is clear that bucket acquisition will complete in some small finite number of steps, because eventually the table size will exceed the number of items that could ever be put into it, given the available computing power.  This number will be a small number, as well, given the logarithmic nature of doubling the store as we grow.

However, as you can see in our [discussion of the migration algorithm](migration.md), we may choose *not* to grow the store when we migrate, we may shrink or grow it.  However, as noted in that document, if any operation that is attempting to insert (and thus triggering bucket acquisition) fails a fixed number of times, it will trigger a help mechanism. When threads need help inserting, the resize will only ever double, ensuring a finite number of steps, bounded by the number of permitted failures before resizes notice the helping mechanism has been invoked, plus a logarithmic number of resizes.

There are no other instances of loops or recursion in this algorithm, and no other points where two threads may contend. Therefore, this algorithm is clearly wait-free.

### The **get** operation

Our get operation is exceptionally simple:

1. We attempt acquire a bucket using the conditional version, which returns  ⊥ if the bucket has not been acquired.

2. If the bucket acquisition algorithm returned  ⊥, then we return  ⊥ as well.

3. We atomically read the other 128 bits of data from the bucket.

4. We ignore the flags, and check the nonce. If the nonce is zero, this indicates that the item has either not been written yet, or has been written and subsequently deleted.  Either way, the item is not present in the table, and we return  ⊥.

5. Otherwise, we return the item found during the atomic read.[^8]

When a bucket has been acquired, the linearization point for this operation is the atomic read of the other 128 bits of data.

Note that a migration could be in progress, or even completed if a thread performing a 'get' suspends for a long time. However, the operation will always linearize to some time before the migration completes.

This algorithm has no contention, no looping beyond the bounded looping done by the bucket acquisition process, and no recursion.

Therefore, this operation is clearly wait-free.

It's important to understand that, while bucket acquisition is a precursor to mutation operations, it's possible for **get** operations to compete with mutation operations, and acquire a bucket that has never been written to, perhaps due to a **put** operation that is currently suspended.

The linearization point for the successful mutation will be when the 128 bits of state (including the item) is successfully installed, so if the read operation does not see a value when it does read, it will consider the item to not be present, even if it successfully acquired a bucket.

### The **put** operation

The put operation takes as inputs a hash value, as well as an item to store, and then does the following:

1. It acquires a bucket, using the non-conditional version of our bucket acquisition logic, where it is guaranteed to get an index into a current store.

2. It atomically reads the current state of the bucket.

3. If the current state indicates that a store migration is in place, it helps with the store, and then re-tries the operation in the new store.  Here, we count the number of retries, and signal for 'help' if we exceed a fixed threshold.

4. Since no migration is in progress, we select a nonce, by applying fetch-and-add to a value local to the data structure (the value starts at 1, and is monotonically increasing).

5. We apply our compare-and-swap operation, attempting to install new state consisting of the following:

    - The item
    - Our nonce
    - The INITIALIZED flag.

6. If the CAS operation is successful, we are done, and return[^9]

7. If the CAS operation fails, we check the read value:

    a. If the MOVING bit is set, we help migrate and then retry the entire operation, again invoking our help mechanism if necessary.
    b. Otherwise, some other operation mutated the bucket.  We consider ourselves to have been successfully 'installed', but immediately overwritten-- placing our insertion on our imaginary 'timeline' to the moment in time immediately preceding the success of the competing operation.[^10]

Our bucket acquisition algorithm requires that each hash value get a unique, consistent bucket, and that no bucket get reused for a different value throughout the life of the store.  Therefore, our operation must be changing the right bucket, so we only need worry ourselves with linearizability of mutations to the bucket.

Here, the linearization point is the CAS operation.  The choice we make in step 7b might seem unintuitive, and certainly we could keep retrying the CAS until we successfully complete the compare-and-swap, or until we are forced to help with a migration.  However:

1. Without an additional 'helping' mechanism, that would make our algorithm lock-free instead of wait free.

2. If we consider N threads with conflicting CAS operations, one of those threads will succeed, and N-1 will fail.  We can think of this as a virtual N-way tie, with a tie-breaker.  If we keep retrying, then N-1 threads will be delayed from the time where their operation could have been considered completed, potentially significantly.

3. This approach is certainly far more efficient in the face of contention.

The notion of a "tie" being something where we we pick a definitive order to break the tie should be comfortable for us-- we are essentially mapping from our linearization point to the conceptual 'line', in a well defined order, where CAS operations returning `true` are ordered AFTER CAS operations returning `false`.

Note that Hatrack does contain an alternative lock-free implementation that does a CAS-loop instead.  If keys are being selected randomly from a set, the performance difference is negligible; the wait-free operation is definitely faster when there is contention.

That is to say, that, if someone preferred different semantics, while the retry loop could be augmented with a help mechanism to make it wait-free, the overhead of the help mechanism is not likely to be worth the constant overhead in the real world.

In terms of our analysis of wait freedom, there is one loop (the bucket acquisition algorithm), two possible contention points (the CAS operation during bucket acquisition, and the CAS operation that writes out the new bucket value), and one possibility for recursion (table migration):

1. The bucket acquisition is wait-free, per discussion above, covering the loop and the first CAS operation.
2. The second CAS operation never retries, so there is no loop, and will thus complete after one operation, unless it fails due to a migration;
3. As we showed above, operations preempted due to a migration that are otherwise wait-free stay wait-free, as long as we use our helping mechanism, where we ensure the store doubles if threads need 'help' when a resize commences.

### The **replace** operation

This operation is simiar to the **put** operation, except that:

1. It uses the version of the bucket acquisition algorithm that can return ⊥. If bucket acquisition returns ⊥, then there is nothing to replace, so this algorithm also returns ⊥.

2. If, on atomic read of the bucket state, there is no item in the bucket (as defined by a zero-value in the nonce field), then we return ⊥.

- In difference #1, when we return ⊥, the linearization point is the atomic read of the `hv` field that is empty.

- In difference #2, when we return ⊥, the linearization point is the atomic read of the remainder of the bucket's state.

The semantics and linearization points are otherwise the same as with our **put** operation, making the argument for correct linearization and wait freedom obvious.

### The **add** operation

This operation is simiar to the **put** operation, except that:

- If, on atomic read of the bucket state, there *IS* an item in the bucket (as identified by a non-zero-value in the nonce field), then we return ⊥, as the constraint of the **add** operation is that it mutates only if it finds the bucket empty.

Here, when we return ⊥, the linearization point is the atomic read of the `hv` field that is empty.

The semantics and linearization points are otherwise the same as with our **put** operation, making the argument for correct linearization and wait freedom obvious.

### The **remove** operation

The semantics and analysis for this operation are identical to the **replace** operation, except that, instead of attempting to write an valid nonce and item into the bucket, we write a null item and a zero value in the nonce field.  Importantly, we still write the INITIALIZED flag (this is required for correctness in our migration algorithm).

### The **view** operation

Our consistent, linearizable view operation is described in our discussion of the migration algorithm.

In our implementation, we do allow a much faster view operation that will not necessarily give a view that is consistent with any linearized state of the table. This faster alternative iterates through each bucket reading the bucket state atomically.  This maintains a consistent linearization on a per-bucket basis, but not on a data structure basis, the way that our migration algorithm does.


--

[^1]: P. Laborde, S. Feldman, D. Dechev. A Wait-Free Hash Map. International Journal of Parallel Programming, 46(3) 2017.

[^2]: P. Fatourou, N. Kallimanis, T. Ropars. An Efficient Wait-free Resizable Hash Table. Symposium on Parallelism in Algorithms and Architectures, July 2018.

[^3]: R. Fagnin, J. Nievergelt, N. Pippenger, H.R. Strong. Extendible Hashing -- A Fast Access Method for Dynamic Files. ACM Transactions on Database Systems 4(3). Sept. 1979.

[^4]: P. Celis, J. Franco. The Analysis of Hashing with Lazy Deletions. Information Sciences: an International Journal 62(1-2) 1992.

[^5]: K Fraser. Practical Lock-Freedom. PhD Thesis, University of Cambridge 2004.

[^6]: Particularly, we could use a variant of our migration scheme where we first mark any relevent buckets involved in a move, so that they cannot be accessed without first helping to carry out the operation. To protect against late / slow threads, it would need to use an epoch scheme to ensure that no spurious operations occur.

[^7]: We actually add an additional constraint that table stores are always a power-of-two in size, particularly so that we can cheaply compute the modulus of H % S using H & (S-1). This is also a common optimization in hash tables.

[^8]: In our higher-level interface, we will also invoke the return callback, per above, to ensure callers get a chance to handle memory management.

[^9]: In our higher-level interface, we also do the dynamic memory management noted above, to make sure that an ejection callback will be invoked, when it is safe to do so.

[^10]: When dealing with dynamic memory management, we still need to treat the input item as data that needs to be ejected.