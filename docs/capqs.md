# Compare-And-Pop queues

In languages like C++ and Java, Vector abstrations are popular-- essentially flex arrays that additionally support stack-based operations (i.e., push and pop).

In Hatrack, we've provided efficient abstractions for flex arrays and for various types of queues, including stacks. Being able to combine those abstractions into a single data structure would give programmers more flexibility.

However, there are some important considerations. For instance, our stack abstraction, while backed by an array, can have unused gaps, making it unsutable for random access.  To combine our stack with flex arrays naively would require linear scans for operations that should be constant time.

In general, handling parallel stack operations without locking is challenging, even when backing the stack with arrays. That's simply because every operation is a mutation that not only impacts the logical size of the data structure, but can require a change in the underlying backing store.  If we then add in the constraint that the entire backing store should support constant-time array lookups, the other queue algorithms we've used in Hatrack are not appropriate, because they all allow contention to effectively create some dead space in the data structure.

Still, we want to be able to provide a flexible, general-purpose abstraction for those use cases where neither a flex array nor a stack alone are suitable. We just want to do it without a dramatic increase in the computational complexity of the algorithms.

To that end, we decided that a first pass on the problem could allow random access to remain fairly fast and constant-time, if we force size mutating operations to be explicitly ordered.  Our vision for this is as follows:

1. When a thread has an operation it would like to perform on the vector, it first enqueues it in an ordered FIFO.
2. It then looks at the last element in the FIFO (the next one to be removed), which may be a different operation.  The current thread attempts to execute this operation (other threads may be attempting to complete the same operation in parallel).
3. When the top queued operation is completed, it must be dequeued.
4. The current thread stops servicing operations in the queue once the item it originally enqueued completes successfully.

This approach makes it easy for threads to agree on a linearized order of operations, but is not without its challenges:

1. The approach effectively forces all operations that need to use the queue to execute in a linear fashion. This means our upper bound on performance for these operations should be lower than if we were doing the work single-threaded, since we are essentially serializing the work, and additionally have constant overhead in the synchronization.  Still, if the operations in question are relatively rare (as opposed to random-access operations), this might often be totally acceptable.

2. When multiple threads are attemping to carry out the same operation in parallel, it can be challenging algorithmically to make sure the work is carried out properly.  We could, instead, use locking here, which would make the operations much easier to design. Still, if we can get the cooperative wait-free algorithm right, our performance should end up better than it would using locking, for a variety of reasons.  

3. While it's easy to imagine a FIFO with a `peek` operation that allows multiple threads to see the current (top) operation at the same time, a traditional FIFO would suffer from race conditions in the dequeue operation.

Specifically, let's imagine we have n threads all attempting to complete some operation. They will all attempt to execute each part of the operation, because any other threads working on the operation might get suspended at any moment, and we want to ensure progress. That means, up to n threads may decide, at about the same time, that the current operation should be dequeued. We can imagine this leading to accidental dequeues of operations that have not yet actually been executed.

The Compare-And-Pop (CAP) queue is a solution to this third challenge. It provides an operation to peek at the next item in line for dequeuing (`capq_top`), and provides an operation to CONDITIONALLY dequeue, only if the item to be dequeued is the one that the caller expects it is dequeuing.

Much like with our [FIFO implementation](queue.md), we use an array to back our queue, with indicies into that array to represent the current location for enqueuing and dequeuing.

The enqueue operation itself is also similar, using fetch-and-add to assign enqueue slots, and those slots can be invalidated if the enqueuer is so slow that the dequeuing catches up to it. In such a case, the enqueuer retries, to ensure proper linearization.  And, as with our FIFO, if there is contention, the enqueuer can jump the enqueue pointer ahead exponentially, to help ensure enqueues happen in a small, bounded amount of time.

However, where our original queue also used fetch-and-add on the dequeue pointer, CAP queues use compare-and-swap. The former dequeue operation only fails if the queue is already empty (with the fetch-and-add being the linearization point). But in a CAP queue, the dequeue fails if the expected item has already been dequeued.

For general purpose use, the FIFO should be used when the dequeue operation is agnostic to what the top item is, it just needs to exclusively dequeue it. The CAP queue could be used as a primitive to build such a thing, but would add significant overhead, relative to our base FIFO.

The CAP queue is specifically for cases where threads need to conditionally dequeue, if and only if no other thread have dequeued the item first.

In this document, we'll focus on the algorithm for the `top()` operation and the `cap()` operation. As with most of our data structures, we use epoch-based memory management to ensure memory safety for the individual records.

## Representation

Cells in our capq are 128-bit values that we expect to update atomically via compare-and-swap operations.  64 bits of the cell are reserved for the user-defined value, and 64 bits are used for internal state.

For a cell's internal state, the four most significant bits are flags used in state tracking, and the remaining 60 bits are an 'epoch' field, a monotonically increasing value that ends up being critical for linearizing our dequeue operations.

To illustate, let's imagine that a thread thinks a cell is currently an active cell and that is additionally the top cell.  To dequeue, the thread will want to swap in some representation to indicate the cell is dequeued (see below).  This thread needs to ensure that it only completes the swap if the current cell contains an expected value.  Well, both the flags and the user-defined data item could be reused in multiple data items. The epoch field being unique, this is the core bit of identity we need to implement our scheme.

Therefore, the `top()` operation must return an epoch (in addition to the data item), and the `cap()` operation need only take the epoch as a parameter, and return either true or false.

The four flags we use are straightforward, and should be familiar from our other algorithms:

1. An ENQUEUED flag, indicating the item is currently enqueued.
2. A DEQUEUED flag, indicating the item was enqueued, but has since been dequeued.
3. A MOVING flag, indicating that the underlying backing store is being migrated.
4. A MOVED flag, indicating that the cell in question has been migrated successfully.

Note that, if an epoch is written into a cell, but neither ENQUEUED nor DEQUEUED is set, then it represents an INVALID cell.  Top operations will attempt to invalidate cells if they know some enqueuer has been given an epoch, but when they read, they see no evidence of the enqueuer successfully completing their enqueue operation.  If the invalidation is successful, the enqueuer will have to restart its operation.

As with our other algorithms, the MOVED flag is primarily an optimization to avoid unnecessary work when migrating the backing store. The considerations with migrating the backing store are mostly the same as with our core queue abstraction; see source code comments for more detail. It is worth noting that the migration needs to preserve epoch values across the backing store, due to the fact that they're returned to the user (other algorithms can often re-start epochs on a per-backing store basis).

Note that we do start the value of the epoch field at 1<<32, and increase the epoch by 1<<32 with each migration. This ensures that epochs are not reused.

## The `top()` operation

This operation conceptually returns three different pieces of information:

1. Whether there was any item enqueued at all, at the linearization point.
2. The item, if one was enqueued.
3. The unique epoch associated with this value, which must be passed to the `cap()` operation (as it is the value we're comparing, to ensure uniqueness).

In our implementation, we return a structure consisting of two 64-bit values. The first is the user-defined item, and the second is the epoch value. If (and only if) the queue is empty, the returned epoch will be 0. However, our implementation also takes an optional boolean reference parameter to indicate whether the queue was empty, to try to make it easier to use the API correctly.

First, the `top()` operation reads the value of the dequeue epoch, followed by the value of the enqueue epoch.  If the dequeue epoch isn't smaller than the enqueue index, then we know the queue was empty at the time of the read of the enqueue epoch, and return appropriately.  Similarly, if we go through all of the items up to the enqueue pointer we read, and there is nothing enqueued (due to skipping or us invalidating), then we know the queue was empty at some point after we started.

To test individual cells, We first load the value of the cell.  We then check to see if the epoch written into that cell is as expected, and if it is, what the state of the cell is.

If the epoch in the loaded cell is higher than our epoch, then our thread was very slow and needs to retry from the beginning (as the cell we thought was the top has been dequeued and potentially reenqueued).

If we otherwise find that the cell doesn't represent a queued value, there are multiple reasons this might be:

1. A `cap()` may have succeeded, but the dequeue index has not caught up.
2. If there was contention on a list with few items, the cell in question may have been skipped outright.
3. The enqueuing thread may be in progress, but too slow.

In the first case, the dequeue flag will be set, and we should return this value, because it was a valid top() at some point after we started our operation. While this might seem unintuitive, since we know that calls to cap() relying on such a value will always fail, it greatly simplifies our quest for wait freedom.

We don't need to be able to distinguish between the last two cases. We simply attempt to invalidate the cell, by writing in the dequeue epoch we read, with no flags set.  If we succeed in invalidating the cell, we again try to bump the dequeue epoch, then retry the operation.

If we fail to invalidate the cell, then we re-start the operation from the point where we initially loaded the contents of the cell, as it may now be properly enqueued.

### Progress guarantees for `top()`

The top() operation has a few contention points:

1. It can contend with slow enqueuers.  This mostly impacts the enqueuer (we invalidate cells, like we do with other algorithms), but we do end up re-trying our top operation, but we STOP if we find no item was ever successfully enqueued from the original dequeue point to the original enqueue point.

2. It can contend with other top operations moving to invalidate a slow writer. That's fine; any time we fail on invalidating a cell, we retry that cell once. If we see a successful enqueue, then great. If we see a successful invalidation, we move on to the next cell.  If we were so slow that the epoch is wrong, we bump a counter as part of ensuring wait freedom for this specific case (see below).

3. Our read can contend with other threads reading and writing so quickly, that the cell we originally were assigned to read from has since been re-written by a future write.

To deal with this situation, when we see that the data we read from a cell is stale in this way, we bump up a counter. If that counter hits a fixed threshold, it forces a doubling of the size of the array and a reset of the counter. Eventually, if the problem keeps happening, the underlying store will be so big that we couldn't possibly have such contention, thus ensuring wait freedom.

## The `cap()` dequeue operation

This operation is relatively straightforward, in that the input here is an epoch returned by `top()`, which means that, at some point, that epoch referred to a queued item that lived at the front of the queue (i.e., would be the next thing to dequeue).

First, we load the dequeue epoch, and the enqueue epoch. If the dequeue epoch has changed from the top() call, then we know the item has already been popped and we fail.

Otherwise, we load the cell we wish to pop.  We double-check the state, to see if it got updated before we could load, or if there is a store migration in progress (we act accordingly when those things happen).

We then try to CAS in an empty cell.  If that CAS succeeds, the `cap()` operation is successful, and if it doesn't succeed, the operation fails, unless a migration has started, in which case we retry after helping with the migration.

When successful, we can try to update the dequeue pointer, but this may not succeed, because other calls to `top()` may notice the invalid cell, and beat us to updating this pointer.

### Progress guarantees for `cap()`

The only contention here that leads to looping occurs when a store migration is in progress. In such a case, we bound the work we need to do by always growing the backing store by a power of two, and never shrinking it.  This makes the `cap()` operation trivially wait-free in and of itself.

However, it's very easy to use this function in a way that destroys wait freedom.  Particularly, our CAP queue implementation also provides a dequeue abstraction implemented like so:

```
void *
capq_dequeue(capq_t *self, bool *found)
{
    capq_top_t top;
    bool       f;

    while (true) {
	top = capq_top(self, &f);
	if (!f) {
	    return hatrack_not_found(found);
	}
	if (capq_cap(self, capq_extract_epoch(top.state))) {
	    return hatrack_found(found, top.item);
	}
    }
}
```

While the above code is, in fact, lock-free, it is definitely NOT wait free.  That is to say, the `cap()` operation itself is wait-free, but the fact that we use it in a loop that could theoretically never exit if we're incredibly unlucky, means that the operation we used `cap()` to implement is not wait free.


