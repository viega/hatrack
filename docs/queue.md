# Faster wait-free queues

There are several fast wait-free FIFO queues in the literature, most notably a fast queue from Yang and Mellor-Crummey[^1], which removed most contention inherent in previous FIFO implementations by leveraging Fetch-and Add.

I implemented a version that queue, and in doing so, realized I could make some simple, significant enhancements.

In this document, I'll give a brief overview of the core FIFO algorithms present in Hatrack. In particular, I will refer to three different queues:

- **YMCQ**, the original specification of the Yang / Mellor-Crummey queue.
- **FAAQ**, my original implementation of the *ymcq* queue, except with a simpler helping mechanism, and a different storage reclaimation scheme.
- **HQ**, The hatrack queue, which modifies the overall storage approach of **FAAQ**, resulting in a more performant algorithm.

## YMCQ overview

The *ymcq* algorithm adds a helping mechanism to a simple obstruction-free queue algorithm adapted from LCRQ[^2]:


```
enqueue(Q, x) {
  do t := FAA(&Q.T, 1);
  while (!CAS(&Q.C[t], ⊥, x));
}

dequeue(Q) {
  do h := FAA(&Q.H, 1);
  while (CAS(&Q.C[h], ⊥, ⊤) and Q.T > h);
  return (Q[h] = ⊤ ? ⊥ : Q.C[h]);
}
```

In the above pseudocode, The basic idea is that there is an infinite array that is fully initialized to some value that is otherwise an undefined state, which we represent with the bottom operator (⊥). Enqueuers get an index to write into, via fetch-and-add of a tail pointer, Q.T.  They expect the cell's value to be ⊥, and replace that via compare-and-swap.

Enqueuers do not compete with each other, however, they may compete with dequeuers.

Dequeuers also use fetch-and-add to find a cell to attempt to dequeue, using a head pointer, Q.H. The head pointer will not pass the tail pointer, but dequeuers that are faster than enqueuers must 'invalidate' the cell they're attempting to read, forcing enqueuers to restart their operation with a new index, to ensure that all threads see the expected ordering semantics.

In this algorithm, the invalidation occurs by attempting to CAS a special value, ⊤, into a cell.  If the enqueuer is slow, the dequeuer will know it, because its CAS operation will succeed.

If, on the other hand, the dequeuer's CAS operation failed, some previous enqueur must have succeeded.

It's easy to see that this algorithm could end up in a live-lock situation when Q.T == Q.H and the following events continually happen in the same order:

1. An enqueue operation successfully advances Q.T via fetch-and-add.
2. A new dequeue operation runs until it reads bottom from the cell.
3. The enqueue operation fails its CAS, restarting its operation.
4. the dequeue operation checks the value of Q.T, comparing it to its value.

The fact that failed CASes do not imply progress in some other thread makes this algorithm merely obstruction free (specifically, there is a dependency across two operations).

The *YMCQ* algorithm enhances the core algorithm in two ways:

1. It adds a helping mechanism that makes the algorithm wait free.
2. It adds a practical simulation of an infinite array.

The infinite array is approximated by keeping a linked list of fixed-size arrays.  If the enqueue operator finds the tail pointer pointing to the end of a segment, the algorithm installs a new segment, linking it to the previous segment. And, if all instances of the dequeue operator are done with a given segment, it can be reclaimed (or cleaned, then added back to the linked list).

We've seen in the resizing operation we've implemented for other algorithms how this kind of allocation can be done in a wait-free manner, even when multiple threads might content to create a new segment.

The *YMCQ* helping mechanism only triggers if an enqueue operation fails some fixed number of times. Once that happens, *YMCQ* uses a typical approach of adding a work request to an announcement array when help is needed; contending operations who notice the help request will all attempt to perform the same operation, until it is successful.

This approach works, but requires threads to scan for announcements of work, which generally requires either all possible contending threads knowing about each other (*YMCQ*'s approach), or scanning one memory cell per thread[^3].

This approach also can slow down more threads than necessary. When this help mechanism *does* get invoked, the cost can be a bit higher than we might like.

## FAAQ overview

*FAAQ* is a version of the original *YMCQ* algorithm, with the following changes:

1. Whereas *YMCQ* used hazard pointers to manage safe reclaimation of segments, we use epoch-based memory management, in the way described [in our migration algorithm](migration.md). Epoch-based schemes tend to be faster and produce simpler implementations, when compared to hazard pointers.

2. For our helping mechanism, every time an enqueue fails, it doubles the amount it fetches and adds. And, if help is needed when we need to create a new segment, we double the segment size, and keep doubling it until no thread needs help when we create our next segment. As a result, threads having a hard time enqueuing will eventually skip ahead so far that dequeuers will not be able to catch up quickly enough to compete. An equally effective alternative would be to slow down dequeuers with an exponential backoff algorithm.

3. Before we FAA the dequeue pointer, we read the dequeue pointer, and then the enqueue pointer. We simply return ⊥ (without the FAA) if the dequeue pointer is at, or past the enqueue pointer.

In practice, with #2, even a slight backoff greatly improves the odds of a push being successful. Given we have implemented a *YMCQ*-like help mechanism in our vector implementation and have seen that, when the help mechanism triggers, the overhead tends to end up high, our approach seems likely to be more efficient in practice, and is certainly much simpler to implement.

For change #3, this was motivated by a practical problem that I'm surprised was not noted in [^1]. Imagine an empty queue shared between two threads, one that produces items and enqueues them as fast as possible, and the other whose mission in life is to consume from the queue and then perform actions based on the value it dequeues.

If both threads are fairly scheduled, the dequeue operation will probably run many times while waiting for the first item to be enqueued, properly returning ⊥ every time.

Since every dequeue operation calls FAA, when the first item is finally ready to be enqueued, the enqueue operation may need to try MANY times to catch up to the dequeue pointer, where it can successfully write. And, in practice, while our enqueue operation searches for a spot, dequeue attempts will continue to push up the work level. Because of this problem, I was seeing that workloads with far more dequeue attempts than enqueue attempts would not complete in any reasonable time.  Our change is a simple way to keep the dequeue pointer from getting too far ahead of the enqueue pointer.

Note that the dequeue pointer can still pass the enqueue pointer. Let's imagine we have a queue with one item in it, and N threads all come in in parallel to dequeue the item. One will succeed, but all N will advance the dequeue pointer, forcing the next enqueue operation to fail N-1 times before finding a slot to write. However, the dequeue pointer can go no further, until an enqueuer is given a slot that leads the dequeue operation to believe that there might be an enqueued item.

This seemed like a better approach than synchronizing the head pointer via CAS, solely based on the emperical performance benefit of *YMCQ*'s fetch-and-add approach, relative to older CAS-based approaches.

## HQ motivation

As we were implementing *FAAQ*, we made particular note of two performance issues:

1. The FAA, while removing CAS contention that tends to slow down most multi-writer queues, still is a performance bottleneck. Since all threads need to synchronize around this value, requiring updates to be replicated across cores before proceeding, algorithms using FAA will eventually bottleneck here, meaning that more processors is unlikely to get us much more scalability when we operate on data structures as fast as possible.

2. Clearly there is a time/space trade-off, based on the segment size, as well as the rate of enqueue operations.

While we were not able to come up with an effective mechanism to address the first problem, we definitely spent a lot of time exploring solutions to the second problem.

First, I looked at segment reuse, whether via zeroing out segments, or allowing pushes to replace pops based on an epoch/nonce. That did not provide performance significant improvements.

Then, I thought about using our migration approach for other data structures to double the store size, when needed. But, if we only run through each segment once, that clearly was going to require store sizes to be proportional to some function of the number of enqueues, instead of some function of the maximum size of the queue.

To address that problem, I decided to treat the store as a fixed-size ring, where if the enqueue pointer ever catches up to the dequeue pointer, we need to double the store size, migrating the queue's contents, before finishing the enqueue operation.

The enqueue operation changes a bit, in that it must read the value currently in the cell it is assigned, and use that value in the compare-and-swap.

That change has had a significant impact on performance: *HQ* is significantly faster than *FAAQ*, usually 50%-100% faster.

Detecting whether a resize should occur is not difficult.  First, we keep a sequence number for enqueues and dequeues, that we convert into an array index via a modulus operation, when needed. When we fetch-and-add the enqueue pointer, we can then read the dequeue pointer, and if the distance between them is greater than the queue size, we need to migrate.

A more difficult challenge is preserving our linearization during a migration.  For instance, let's imagine that we have a queue size of 8, that the dequeue pointer is at 0, and that the enqueue pointer moves from 7 to 8, triggering a resize.  We show the state of cells below as follows:

- **E** for enqueued
- **M** for moving / marked
- **D** for dequeued

Everything at this point is enqueued, and unmarked:

| Index  |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |
|--------|-----|-----|-----|-----|-----|-----|-----|-----|
| State  |  E  |  E  |  E  |  E  |  E  |  E  |  E  |  E  |

Let's imagine that before our enqueue thread successfully starts marking, two dequeue requests come in, in parallel.  They'll be given index 0, and index 1, but those two threads will compete with our marking.  If the thread assigned index 1 goes faster, we might end up with the following queue state:

| Index  |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |
|--------|-----|-----|-----|-----|-----|-----|-----|-----|
| State  | ME  | MD  |  E  |  E  |  E  |  E  |  E  |  E  |

In previous algorithms, we've had threads pause to migrate, then restart their operations after the store moveis complete. If we were to do that here, we could end up yielding the item at index 1 in a way that breaks our linearization.

To be clear, the actual time our threads take to return once they pass their linearization point can be arbitrary, and that does not break our linearization. So, if we can map the linearization points of the first dequeue and second dequeue algorithmically and consistently to a timeline that maps to the array order, and no thread can ever see evidence that we violated that order, it doesn't matter if the thread dequeueing at index 1 returns quickly, while the other thread dequeing at index 0 stalls for a long time.

However, if we just blindly restart threads in the scenario of migration contention, then we can easily have an issue.  If T0 get assigned index 0, starts migrating, and gets suspended near the end, and T1 gets assigned index 1, doesn't see the migration flag and never suspends, it could conceptually run another dequeue operation that executes after the migration that returns the value that was in the old slot 0, which conceptually should have been returned first. Thread T1 clearly sees evidence of out-of-order execution.

Similarly, if the marking goes slowly, but a dequeue thread is going very quickly, we could mark cells 0 and 1, but have a single thread dequeue cells 2-6 before the cells are marked, which would return items in an order that would make it challenging for us to map our migration operation to a point in time on a timeline.

On the enqueue side, there are no issues here, because in marking cells from lowest to highest, while we may prevent enqueues from reaching their linearization point, the order they execute in does not matter until they reach that linearization point.

In general, we address these issues in two ways:

1. Before we begin marking buckets, we mark the dequeue pointer with a migration bit, and have dequeues check it, before beginning their operation. While this does not help us with slow dequeuers that have already received an index, it does ensure that no thread can dequeue multiple items successfully during the marking process.

2. We ensure that, if any one thread manages to dequeue, a spot such that there are enqueued, marked items with a lower associated dequeue sequence number, any thread with a lower dequeue sequence number will complete its operation out of the old store. Of course, the migration process will NOT copy such values into the new store.

Item #2 will require additional accounting (discussed below), especially because, while one thread may set the migration bit in the dequeue pointer, we still update it with fetch-and-add instead of compare-and-swap, and so dequeue threads cannot rely on the value they read to be in any way the 'lowest value' item to migrate.

A related subtle issue is that we can still have enqueues once we notice the buffer is full, in the following scenario:

1. Thread T1 gets an enqueue sequence number.
2. Thread T1 reads the current dequeue sequence number.
3. Thread T1 decides its time to migrate, but the scheduler suspends it before it kicks off the process by marking the dequeue sequence number.
4. An arbitrary number of dequeues happen quickly.
5. Thread T1 wakes up and marks the dequeue sequence number, but:
6. Several enqueue threads come in, who do NOT see a full queue.

Once any thread decides an expansion needs to happen, we do go through with the expansion, doubling the size, even if the number of enqueued items does shrink significantly before the migration properly kicks off.  Again, we just need to know where to start copying, and where to end copying.

The mechanism we use to preserve our linearization is to have enqueue and dequeue operations always write out their sequence number into the state of the bucket when they're performing an operation.

We then can identify the enqueued cell with the highest sequence number, knowing it is the last cell to be copied.

For the lowest sequence number, we can determine whether or not it needs to be copied based on whether there is any dequeued cell with a *higher* sequence number. If there is, then we start copying at the cell immediately after the dequeued cell with the highest sequence number.

We can easily and reliably locate these cells during the marking process, as our atomic fetch-and-or will prevent additional mutations, and give us an accurate view of the value of a cell at the time of the marking.

Note that, since we are using re-using cells in the buffer, and since the enqueue pointer might skip cells (as part of our approach to make the algorithm wait-free, per above), there can easily be cells that have a stored sequence number that is lower than would be allowable if the enqueue pointer only ever increased by one.

These will necessarily all be either cells that have never been written to at all (if they were skipped in the first set of enqueues, and no attempt has been made to dequeue), or cells that were either dequeued or otherwise invalidated, just with a very stale-looking sequence number.

Note that, with this scheme, as we migrate cells, we should assign 'new' sequence numbers in the new store, instead of copying over the old sequence numbers. We do this so that we can keep the constraint that sequence numbers written to a bucket will always be identical to the index of the bucket, when reduced modulo the store size. This makes life a lot easier as an implementer.

## Final Thoughts

**HQ** reusing its store as a fixed-size ring definitely complicates the core *YMCQ* algorithm on which it was based:

- Enqueues and dequeues have to anticipate seeing more states than just ⊥ and ⊤, which will require an extra read per operation: *YMCQ* and *FAAQ* can always blindly apply compare-and-swap.
- It is actually easy to be conservative in making sure writes never overwrite a dequeue in the ring, simply by loading the dequeue pointer AFTER reading the enqueue pointer. However, instead of keeping them as two separate pointers, we could have a more precice view if we keep them in the same 128-bit atomic value. This would potentially increase contention a bit, but this is actually the approach we take in our ring buffers (which are still quite performant).
- The only real challenge to maintaining our sequencing is making sure that dequeues always have a proper linearization when we do migrate stores. However, our addition of sequence numbers to the cell state makes it easy for dequeue threads to figure out whether they need to complete the request out of the old queue, or retry in the new queue: they just need to check if their sequence number is lower than the lowest sequence number that we migrate.

However, the extra accounting is the price of minimizing the amount of memory management we need to do. The *HQ* algorithm is significantly more performant as a result.

--
[^1]: C. Yank, J. Mellor-Crummey. A Wait-free Queue As Fast As Fetch-and-add. In proceedings of the 21st ACM SIGPLAN Symposium on Principlies and Practice of Parallel Programming, 2016.
[^2]: A. Morrison and Y. Afek. Fast, Concurrent Queues for x86 Processors. In Proceedings of PPoPP 2013.
[^3]: If threads can guarantee they stay pinned to a core for the operation, it can actually be done with only one cell per core.