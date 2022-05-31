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

1. A TOOSLOW flag, that indicates the epoch recordered in the cell is invalid, and that no item is present. The epoch slot was given out to an enqueuer, who did not manage to complete the enqueue before a dequeuer came along.  The enqueuer will have retried its operation.
2. A USED flag, indicating that there is an item enqueued, and that the current epoch is valid.  If this flag is NOT set, then the item associated with the epoch field has either been dequeued successfully, or was never enqueued (e.g., due to the enqueuer being too slow).
3. A MOVING flag, indicating that the underlying backing store is being migrated.
4. A MOVED flag, indicating that the cell in question has been migrated successfully.

As with our other algorithms, the MOVED flag is primarily an optimization to avoid unnecessary work when migrating the backing store. The considerations with migrating the backing store are essentially the same as with our core queue abstraction. It is worth noting that the migration needs to preserve epoch values across the backing store, due to the fact that they're returned to the user (other algorithms can often re-start epochs on a per-backing store basis).

Note that we do start the value of the epoch field at 1<<32, and increase the epoch by 1<<32 with each migration. This ensures that epochs are not reused, that epochs are always directly mapped to the underlying cell's index, and that 0 is not a valid epoch.

## The `top()` operation

This operation conceptually returns three different pieces of information:

1. Whether there was any item enqueued at all, at the linearization point.
2. The item, if one was enqueued.
3. The unique epoch associated with this value, which must be passed to the `cap()` operation (as it is the value we're comparing, to ensure uniqueness).

In our implementation, we return a structure consisting of two 64-bit values. The first is the user-defined item, and the second is the epoch value. If (and only if) the queue is empty, the returned epoch will be 0. However, our implementation also takes an optional boolean reference parameter to indicate whether the queue was empty, to try to make it easier to use the API correctly.

First, the `top()` operation reads the value of the dequeue index, followed by the value of the enqueue index.  If the dequeue index isn't smaller than the enqueue index, then we know the queue is empty, and return appropriately.

At this point, we load the value of the cell.  We then check to see if the epoch written into that cell is as expected, and if it is, what the state of the cell is.

If the epoch in the loaded cell is higher than our epoch, then our thread was very slow and needs to retry (as the cell we thought was the top has been dequeued and potentially reenqueued).

If, we find that the cell doesn't represent a queued value with the correct epoch, there are multiple reasons this might be:

1. A `cap()` may have succeeded, but the dequeue index has not caught up.
2. If there was contention on a list with few items, the cell in question may have been skipped outright.
3. The enqueuing thread may be in progress, but too slow.

We don't need to be able to distinguish between these cases, we can attempt to write in the 'too slow' marker; if we succeed in doing so, we retry the operation. Otherwise, we attempt to fix the dequeue index, by updating it via CAS, attempting to install an epoch value one higher than the epoch we read.

Whether or not this CAS succeeds, we simply retry the operation.

### Progress guarantees for `top()`

Any time where the `top()` operation has to retry, it is because the view of the dequeue index was wrong. Again, this could be because we were contending with either successful dequeues, because some enqueue turned out to be too slow, or because other dequeues invalidated enqueues, causing those enqueues to skip cells.  If the cell is invalid, there was no contention, and our progress is not impacted. Similarly, if we retry because we invalidated a cell, that impacts the enqueue operation's progress, but not our own.

Our progress is only truly impeded when dequeue operations that have successfully read the top dequeue. However, we note that, if we assume the caller doesn't try to guess epochs, then we know that any successful dequeue required a successful call to `top()`.  That means some thread is always able to guarantee progress, meaning this operation is at least lock free.

However, every time we re-start we might find a faster thread has successfully dequeued something.  If the queue never empties due to enqueuing, one thread can, in concept, spin forever, making the algorithm as described lock-free, not wait-free.

We need to do better if we want to use our CAPQ for implementing other wait-free operations-- our core operations need to be wait-free themselves.  That means, we have to be able to show some finite bound in which we produce a linearizable result.

To that end, we can make a simple adjustment.  Dequeues can leave behind the item, and just set a new state flag that indicates when a successful dequeue occurs.  Then, if a `top()` call fails some finite number of times due to dequeue contention, it can return the most recent dequeued value as the 'top'.

Essentially, this linearizes the `top()` operation before the `cap()` that beat it. It means the thread asking for the `top()` will get a value we expect to be stale-- but values going stale after `top()` is ALWAYS a possibility. We are only ensuring that any subsequent `cap()` from that thread is destined to fail.

So we might cause a bit of unnecessary work to occur, but can guarantee wait freedom.

Currently, we've not yet implemented this enhancement to the `top()` operation.

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


