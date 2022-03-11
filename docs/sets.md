# Fast, wait-free sets

Hatrack's standard set type is based on a hash table that can support providing fully consistent views across multiple hash tables, so that operations like union, difference and intersection can happen at a precise, linearizable point, even when the timeline is shared between multiple data structures.

In describing the algorithm for this, we use the description of our [dictionary class](dicts.md) as a reference point. The hash table behind our sets is different in several ways, but several things such as the bucket acquisition logic are identicial. Here, we will focus on the differences, and argue the correctness of those differences.

## Understanding the challenges

The core challenge is to be able to provide a consistent, moment-in-time view of two different hash tables. We could certainly support that with a modest extension to our dictionary class.

Specifically, we could add an operation, call it `view2(D1, D2)`, which takes two dictionaries, and creates a consistent view of each, much as described in our other documentation. The only challenge is ensuring the two views can linearize to the same point in time, which is easily solved by setting the MOVING bits across both dictionaries before performing any copying in either dictionary.

However, the copying does defer any updates to the state of tables. That's acceptable if views are infrequent, but if set operations require frequent copying of large sets, then we should want a way to get a consistent view across two data structures, without having to add significant latency to mutation operations.

As another challenge, applications may want the items in their sets to reflect the linearized insertion order. While the Hatrack dictionary does write a nonce for every new insertion, to allow for recovery of an approximate insertion order, this order will not always match the linearized order of insertions when there is contention. 

## Structural differences from Hatrack

The Hatrack set addresses these problems both with some changes to our dictionary, and with some changes to our memory management system.

First, all records that may live in a bucket are stored in dynamically allocated memory. Records still contain 64-bit value for the item, and a 'nonce', though in this scheme the nonce will end up reflecting the linearized mutation ordering.  In this scheme, we call it the `committment epoch`, not a nonce. The flags involved in a migration are not part of the record in this scheme.  In addition, we add the following fields:

- A `next` pointer, showing the previous record that occupied a bucket, if any.
- A `deleted` field, which we will use to distinguish between deletions and uninitialized buckets.
- A `creation epoch`, that reflects the epoch in which a key was last added into the dictionary.  The intent is to capture Python semantics for dictionary and set sort ordering.  Specifically, if you insert a key X into a dictionary, then overwrite the value associated with X, the insertion ordering should not change.  However, if you insert X, delete it, then re-insert it, the creation epoch should get reset on the second insertion.

In the scheme for our sets, the *committment epoch* and *creation epoch* are part of our miniature memory manager (MMM), and so these fields actually live in a hidden accounting field, along with the *retirement epoch*.

Essentially, we keep a history of each bucket's contents in a linked list of records, but we only keep as much history as we might need to recover a consistent view in a particular epoch, as we will describe below.

We still need a state bit for migration (and ideally two, to help optimize the migration).  Instead of living in the record, these live in the bucket, which consists of the following fields:

- The 128-bit hash value `hv`, per our other dictionary.
- The 64-bit `head` value, which is a pointer to our topmost record for the bucket.
- A single bit `MOVING` to indicate that a migration is in progress.
- An optional bit `MOVED`, to indicate to migrators, that work on this bucket is completed, and doesn't need to be tried.

Note that we will apply atomic operations to the hash value, and to the remainder of the bucket. The two state bits can be stored in a 64-bit value while using a 128-bit compare-and-swap, or we can safely 'steal' the lowest two bits from our pointer to the top record.

## Changes to core operations

Our core operations stay as close to the semantics of our dictionary implementation as possible.

For instance, our bucket acquisition operations are completely identical to those in our core dictionary class.

The set migration algorithm is nearly identical. Even though our set's bucket contains a pointer to a record, instead of the actual value, the migration simply copies the pointer from the old store to the new store. The only difference is in how we process records we are NOT copying due to them being deleted from the table. There, the thread that successfully sets the 'MOVING' bit in the bucket is responsible for calling the `retire` operation on the deletion record (ensuring bucket contents only get freed once).

Remember that retirement will put the record on a list of cells to free later, but will keep the record available until there are no more threads that care about the epoch in which the record was retired.

Before we detail the semantics of the core operations, let's start with two 'helper' functions:

- `commit_write(record)` Given a record, it attempts to set the commitment epoch as follows:
    - It performs an atomic fetch-and-add of the global epoch.
    - It attempts ONCE to install this epoch into the `committment epoch` field, via a compare-and-swap operation.

- `set_creation_epoch(record)`. Given a record, it sets it's creation epoch, and potentially helps set the creation epoch of any record in the linked list below it, through recursive application of the algorithm. Particularly:
    - If the current record has no `next` pointer, then the creation epoch is set by copying the committment epoch (which we ensure will be set before this call).
    - If there is a `next` pointer, and the record that it points to is a deletion record, then we copy the current record's committment epoch into its creation epoch.
    - If the next record has a creation epoch set, we copy it into our creation epoch.
    - Otherwise, we apply this algorithm recursively to the record below, then copy the creation epoch of the record below into our creation epoch.

Note that, for this algorithm, the memory manager's epoch needs to be global to all isntances of the data structure, whereas, for our dictionaries, it could easily be local to the data structure.

Note that we will argue for the memory safety of the `set_creation_epoch()` function when we discuss our **view** operation, below.

### The **get** operation

The set's **get** function is nearly identical as well, except that, instead of returning the value stored in the bucket (which would be the top record in the bucket):

1. If it finds a record, and there is no committment epoch set in the record, it calls `commit_write(record)`.  This action is taken for any record, even if it is a deletion record.
2. It returns ⊥ if there is no record, or if the record is marked as a deletion record; and
3. After we atomically read the (non-hash value) contents of the bucket, we return bottom if the head pointer is NULL (no item has been inserted yet), or if the top record is a deletion record (i.e., has the deletion flag set).
4. Otherwise, we return the item field of the top record.

The linearization point for the **get** operation is still the atomic read of the contents, which in this version is bucket's head pointer (and flags). For this to be the case, it's important to know that, once a record is committed, its contents are non-mutable until it's retirement, at which point the only change will be in adding a retirement epoch.

This algorithm is clearly still wait-free.

### The **put** operation 
Bucket acquisition is again the same as with our dictionaries, but otherwise mutation operations are more complicated, in order to support view operations. Particularly, our linearization point is different.

Here are the semantics of the **put** operation, after a bucket is required:

1. We atomically load the current state of both the flags and the head pointer.

2. If record stored in the current head pointer does not yet have a committment epoch, we call `commit_write(record)`.

3. If a migration is in progress, we help with that, then retry in the newer store.

4. The operation allocates a new memory cell using our epoch-based memory manager.

5. The new memory cell's `item` field is set to the item passed into the operation.

6. The new cell's `next` pointer is set to the current value of `head` (which may be `NULL`, of course).

7. We attempt to install our new cell into the bucket's head pointer, via a compare-and-swap operation, comparing against the read value.

8. If the compare-and-swap fails due to a migration being in progress, we `retire` the unused memory cell, complete our migration, then retry the operation from the beginning.

9. If the compare-and-swap fails due to another thread succeeding, we take the same approach we did with our dictionary, in that we consider ourself to have been installed RIGHT before the thread that won.  Here, we must also `retire` the unused memory cell.

10. If our compare-and-swap succeeded, we call `commit_write()` on our own record.

11. We call the `set_creation_epoch()` helper on our own record.

12. Finally, we call the `retire()` operation on that previous record.

In this scheme, the compare-and-swap operation where we install our record is **not** our linearization point. The linearization point is when some thread successfully installs a write committment into the record.

This requires every thread that might want to look at this record to make sure that the committment is installed, and try to help install it, if it is not.

The fact that no item can be added to the linked list before the item below it has committed to an epoch, along with the fact that `commit_write()` always starts a new epoch, ensures that items on the list will be sorted from highest epoch, to lowest epoch.

The additional complexity in this algorithm relative to the algorithm for our dictionary is due to our desire to support a linearized view across sets, along with a sort order that maps to that linearization. However, our analysis is not vastly different.

Specifically:
- There are no additional loops in the algorithm.
- There is no additional recursion in the algorithm.
- While there is an additional CAS operation in the algorithm (via `commit_write()`), it is not used in a loop. Multiple threads can each try it a single time, but the only way the CAS can fail is if some thread succeeds.  Further threads will not attempt the CAS once it is successful.
- The commit_write() call becomes the new linearization point, and this is enforced by every operation accessing a record calling it if necessary, before accessing a cell.

As a result, we can consider the algorithm both wait-free (as long as we use the help mechanism discussed for when there's excessive table resizing; otherwise, we can consider it lock-free).

One thing that's important to note is that our 'committment epoch' is not ever the same as the epoch in which the cell was allocated, nor is the it ever same as when the CAS completes.

### Other mutation operations

The changes we make to **add** and **replace** should be fairly straightforward, given the semantics of the **put** operation.  The only things worth noting is that the *add* operation does not ever help with committment; if it determines there is a record in the bucket that isn't a deletion record, it returns ⊥.

The **replace** and **delete** operations return ⊥ if, when they read the bucket contents, the head pointer is null, or if the top record is a deletion record.

The **replace** and **delete** operations do help with epoch committments if needed.  The **delete** operation does not mutate any data in the record it replaces, it simply creates an otherwise empty record marked as a deletion, and commits to that.  Essentially, we treat **delete** as a replace operation, that writes out a record consisting of ⊥.

Note that the deletion record must also 'help' set the `creation` epoch for any cell that it replaces, to prevent use-after-free bugs (described below).

## The new *view* operation
As with all our operations, the *view* operation starts by registering a epoch reservation R0 with the memory management system.

It then reads the value of the global epoch into R1, which we will use to produce a linear view of the hash table.

It is important that the read and store of R0, and the read of R1 all happen in a sequentially consistent fashion.

It is important that we read the global epoch twice. Particularly, if our thread is very slow to write its reservation to the reservations array, items could get retired in that epoch, and even in future epochs, all before we complete our reservation. This can easily happen if our thread suspends at the wrong time, and other threads advance the epoch.

That is, once our reservation is in place in the global array, it ensures that no records from that epoch or after are removed, but it does **not** prevent the removal of records *before* the reservation was written.

For instance, if R0 is 100, but R1 is 102, the write to the reservation array could have happened in epoch 100, 101, or 102. If it happened in 100, then items retired in epoch 100 and epoch 101 may or may not have been reclaimed.

However, since R0 was written before the time that R1 was read (due to sequential consistency), and since we do not ever free items in the epoch in which they were retired, we can be confident that every record that is visible in epoch 102 will be visible at least until we remove our reservation.

To be clear, if we write out our reservation and read R1 very late in epoch 102, there may be a memory allocation with epoch 102 as a retirement epoch, but we are guaranteed that it will still be live.

The epoch R1 is the epoch that we will use to linearize our view operation[^1]. We are essentially going to go through each bucket, load the head, and scan until we find the record that was current in the epoch to which we were linearizing ourself.

Our read of epoch R1 is the linearization point for the view operation. We will return every cell that was both in the table at epoch R1, and has a committment epoch of R1 or lower.

Note that each epoch may have at most a single write committment as well.  And, there may be a lag in that committment, in the same way that there is when we write out our reservation to the global reservations array.

To that end, view operations will, just as write operations do, help finish writing committment epochs when first loading a record, before making any decisions about what action to take.

Specifically, the *view* operation iterates through cells from the start of the array, to the end, and takes the following steps for each cell:

1. It atomically loads the state.

2. If the state is empty (i.e., there is no value in the head record), it moves on to the next cell.

3. If the record's write committment is not set, then it calls `commit_write(record)`, ensuring that there will be a committment epoch by the time the call returns, whether this thread succeeds at setting it, or some other thread does.

4. If the current record's commit epoch is less than or equal to R1, and the record is a delete record, then we decide there was no active record in this field at our linearization point, and we move on to the next cell (going back to step 1).

5. If the current record's commit epoch is less than or equal to R1, we jump down to step 8.

6. If the record's `next` pointer is `NULL`, we decide there was no item in this cell, and move on to the next cell, again jumping back to step 1.

7. Otherwise, we follow the `next` pointer to get the address of the next record, loads that record, and jumps to step 3.

8. At this point, we've found the correct record, and our intent is to copy out the data we need for a proper view, that can be sorted based on our linearization.  To that end, we copy out the data in the record's `item` field.

9. Next, we need the insertion order. For this, we check the `creation epoch` field.  If it is there, we copy it and move on to the next bucket, jumping to step 1.

10. Otherwise, we call `set_creation_epoch()` on the current record, and copy out the value when done, moving on to the next cell.

We've argued above that the view operation is properly linearized. Here, correctness is more of a concern, particularly in regard to the memory safety.  Given that any record not installed in the head will be 'retired', we must be worried about whether our algorithm might possibly be able to attempt to read memory after it is freed.

To argue for safety of this construction, we need to show that we cannot dereference cells that might have been freed.

Any cell below the top cell of a record may have been retired. However, the retirement epoch will be no less than the committment epoch of the cell above it, since we commit to an epoch BEFORE we retire the cell we are replacing.

Therefore, if we read a cell whose committment epoch is above R1, if there is a `next` cell, that cell will necessarily be retired *after* R1, meaning our reservation guarantees the cell will be alive.

Once we find a cell whose committment epoch is equal to, or below R1, we may still descend further into the linked list, in order to recover the creation epoch (when calling `set_creation_epoch()`.

This too is safe. Consider the possible cases:

- We reach a cell where the creation epoch is not yet set, but has no `next` pointer. We know that this cell cannot be retired, because the cell that wrote the record above this one must help it complete, before it can retire it. Even if it does manage to retire it, before we finish our operation, the fact that it is alive when we look at it ensures that our reservation will keep it alive.

- We are visiting a cell without a creation epoch set, that does have a `next` pointer.  The current cell has not finished setting its own creation epoch. This means that the cell below will be live, and also means that the cell above it will not have retired this cell, as it must help it write out its creation epoch, first.

- We are visiting a cell where there is an creation epoch set. We can only reach such a cell if the cell above has not had its creation epoch set. Otherwise, we would have taken our creation epoch from the cell above. And, since the cell above has not finished updating its creation epoch by the time of our read of the node above, it certainly will not have retired this node by the time of our read of R1.

- We are visiting a cell that consists of a deletion record. Such a record will not have a 'creation' epoch at all, but will inform us that we should use the information from the node above, if any.  There must be a record above, otherwise we would not have begun a search. That record above must not be a deletion record, and must not have had an epoch set when we read it. Since non-deletion records must finish writing the creation epoch before retiring the record beneath them, the deletion record could not have been retired before our read of R1, if we are visiting it.

That covers every case, and therefore the algorithm will never descend into cells that may have been freed in the epoch before we read the value of R1.

Once the view operation has visited every cell, and copied out all information it needs to return in the view, then it calls `end_op()`, yielding its reservation.

This algorithm clearly maps mutations to a specific linearization point that we can use for views, in the face of all our core operations. It's worth considering how well the algorithm linearizes if there is also a migration in progress at an point in parallel with the view operation.

The view operation acquires the store pointer either during the epoch R1, or after.  If we acquire the OLD store, then there is no issue; we will see history from the lifetime of the store we've acquired, through at least R1.

If we acquire the new store, then our linearization point will actually map not to the epoch R1, but to the end of the migration process-- we will yield the contents of the store directly after the completion of the view operation.

## The **view2** operation

The **view2** operation is straightforward.  First, it reserves an epoch, then reads the current value of the epoch to use for linearization (R1). Since the reservation array is global, both tables will preserve all records needed to linearize to epoch R1.

The algorithm then copies out a view for each table, per the **view** algorithm above.

Then, the reservation can be yielded, and operations (including set operations), can be done based on the linearized views.

## Other modifications

This section describes slight modifications we can make to the algorithm, depending on our goals.

### History modifications

This algorithm essentially allows us to keep a history of all hash table modifications,from the present back through the a particular period of time, defined by the time at which the oldest epoch reservation took effect (that is, not the epoch of the reservation, but the epoch where the reservation's write into the array took effect).

We can use this approach to allow for iterators that are aware of changes to a table since the last item was yielded, depending on whatever semantics we like. For instance, we could imagine yielding items from the 'moment-in-time' view, then produce up-to-date deltas when the array depletes, or always yielding an item 'currently' in the array at a given epoch, but never one that has already been yielded.

Obviously, this has implications to any system using the memory manager, but we can have our reservations array be global only to sets where we might want this kind of view, not all our data structures.

We can also naturally support time-based replay history by extending our memory management system to not only wait until a particular epoch has passed since retirement, but to wait a particular amount of wall-clock time, then allow views and get operations to take a timestamp parameter.

For these schemes to be robust, they do require us to also copy deletion records during a migration, whenever the cells below the deletion record might be needed for such an operation.

### Faster sorting

We can keep a two-tier bucket structure, where bucket acquisition consists of first writing data in an array where cell indexes are given out first come, first served, via fetch-and-add, and then the hashed bucket location stores only a pointer to the associated cell index.  We write the hash key into the appropriate cell in both arrays.

This allows us to keep the cells in an order that will be much closer to the actual sort order than random. In fact, if there are no deletions, and no contention during writes, then the two would be identical.

This would allow us to use an insertion sort, whose complexity approaches O(n) the closer the array gets to sorted.

We implemented this in a version of this algorithm without the wait-free additions (called lohat-a in the repository).  The sort times are definitely better for small tables, and possibly for incredibly large tables, but the low-level optimzations around the system-supplied sort implementation makes it clearly faster for most table sizes once you get above about 2000 items (on my laptop anyway).  It argues that the constant multiple is much lower on the built-in O(n log n) average case sort than the sort that should approach O(n).  This was even true in lohat-b (since removed from the repo) where we had re-insertions acquire new buckets in the 'nearly ordered' array.

--

[^1]: Actually, as described below, we also need to acquire a pointer to the current store, so if a migration is happening in parallel, our linearization point may not map to the epoch R1. Specifically, if we acquire the migrated store, our linearization point will be the point that the migration linearizes to.