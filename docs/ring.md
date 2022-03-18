# The first wait-free MPMC ring buffer

Ringer buffers are ubiquitous in high-performance computing, from video processing to network and kernel events. In those domains, it is often important to work inside fixed memory (or storage) constraints, to the point that dropping old data is a better choice.

In such high-performance environments, existing ring buffer implementations tend to fall into one of two categories:

1. They are single-producer, single-consumer, which allows them to be both fast and wait-free without too much difficulty, and without sacrificing on performance.

2. If they allow multi-producer multi-consumer (MPMC) access, they do not strictly provide a linearizable, "safe" ring buffer. Particularly, when doing in-memory logging, if one keeps a very large array, it would be impractical to have two threads writing to the same cell at once. However, it's certainly possible with the naive approach to have old cells sandwiched by new cells, and to read cells that have an invalid state (i.e., new data is in the process of overwriting old data in parallel to te read).  Hatrack itself has, since the beginning of developer, had an in-memory debugging facility that takes this approach.

Generally, one could straightforwardly support MPMC with a locking mechanism, but that would greatly reduce performance. Similarly, even were a lock-free MPMC ring buffer to exist, real applications might still prefer the SPSC approach, in fear of the dramatic performance dropoffs that are possible when there is high contention.

The literature does not yet have a true, MPMC ring buffer that is either lock-free, or wait-free. There was a single paper claiming to provide such a thing.[^1] However, their construct does NOT support dropping old data. Instead, when the internal buffer fills up, enqueue operations fail.

Dropping old data is a critical feature of ring buffers, the way they are used in the real world.  While people's definitions can vary, we do not consider such a construct a proper ring buffer; it is simply a fixed-size FIFO. The implementation may reuse memory cells, but that's an implementation detail that's irrelevent to the abstraction of an end-user-- if the buffer can fill up at the expense of our enqueue operation, our abstraction is a fixed-size FIFO.

Another issue with the referenced work is that they can only enqueue a small, fixed-size value that's atomically set. While this is good for enqueuing pointers in practice, it does not solve the problem of keeping a circular log in a fixed amount of space.

Hatrack, on the other hand, provides two algorithms that solve these problems:

- **Hatring**, a wait-free MPMC ring buffer that enqueues 64-bit values.
- **Logring**, MPMC ring buffer abstraction that can enqueue values of an arbitrary, but fixed, size.

## Ring Buffer definition

Before detailing our algorithms, we should formalize our definition and requirements for a ring buffer:

1. A ring buffer is an ordered set `S`, with a maximum size `n`, where each item is of a fixed length `l`.
2. Ring buffers support at least the following two operations:
  - `enqueue(I)` where `I` is an arbitrary item of length `l`.
  - `dequeue()`, which returns an item of length `l` or ⊥ if no such item is enqueued.
3. Ring buffers must have an upper bound of `O(n)` storage[^2].
4. The `enqueue` and `dequeue` operations should run in O(1) time, whenever possible.
5. Even in parallel environments, there shall always be a linearizable ordering of operations, such that no thread would ever be able to see an inconsistent mapping of operations to a timeline, under any circumstances.
6. If, when an enqueue operation linearizes, the queue was already of size `n`, the enqueue operation must implicitly dequeue the oldest enqueued item at its linearization point (where oldest is defined relative to our linearized ordering of operations.

Note that it may be valuable to have ring buffers support non-mutable read operations. Particularly, our implementations support a `view()` operation, which returns an ordered list of the items in the ring, without dequeueing any of the items.

Also note that, In a MPMC scenario, we may find it necessary in practice to accept a worst-case bound that is above O(1), but the typical case should be O(1) with a *very* low constant.

## An obstruction-free algorithm

Before describing our core algorithms, we start with an obstruction-free ring buffer implementation that enqueues a fixed-size item. Once we understand how this gives us correct semantics with a linearizable ordering, we will build on top of it, modifying the algorithm to be wait-free, minimizing contention for real-world scenarios.

First, let's look at the top-level operations, which abstract away some of the functionality into helper functions that we'll describe shortly.

```
of_enqueue(Q, I):
  while true:
    # Atomically read the queue's sequence numbers.  In this pseudocode,
    # we will use a convention that comma-separated values inside tuples
    # are operated on atomically together (e.g., in a 128-bit value via
    # FAA or CAS).
    (dseq, eseq) = (Q.dseq, Q.esq)
    
    # If enqueues are racing ahead of dequeues, try to keep dequeuers from
    # paying a large penalty to catch up to the back of the queue.
    if is_lagging(Q, dseq, eseq): try_to_fix_dseq(Q, eseq, dseq)
    
    (dseq, eseq) = atomic_increment_eseq(Q)
    if try_to_enqueue(Q, I, eseq):
         return ⊤ # Success
    else:
      continue
    
of_dequeue(Q):
  while true:
    (dseq, eseq) = (Q.dseq, Q.esq)
    if (dseq >= eseq): return ⊥ # Ring is empty.

    (dseq, eseq) = atomic_increment_dseq(Q)
    (state, cell_seq, val) = atomic_read(cell_address(Q, dseq))

    # If we find an sequence that we read from a cell is greater
    # than ours, we were *so* slow that we've been lapped, so will
    # need to retry in the top-level loop where we get a new index.
    while (cell_seq <= dseq):
       if (CAS(cell_address(Q, dseq), (state, cell_seq, val), (⊥, dseq, ⊥))):
           if dseq > cell_seq:
              if dseq + 1 == eseq:
	          # In this branch, We read the last possible enqueued cell, 
	          # and no item was enqueued there. 
	          return ⊥
	      else:
	          # In this branch, we can't declare the queue empty without
	          # retrying the top-level loop.
	          break

	    else: # due to the entry condition and successful CAS,
	          # we know that cell_seq == dseq if we get to this branch.
		  # Since we never have the head pointer skip the way we do
		  # with our normal FIFO, there must be an enqueued value.
                  return val
        else: # This branch when the CAS fails.  Retry, but see notes below.
	   continue

# Helper functions.

is_lagging(Q, dseq, eseq):
  if dseq + len(Q) < eseq: return true
  else: return false

try_to_fix_dseq(Q, eseq, dseq):
  target_dseq = (eseq + 1) - len(Q)
  CAS((Q.dseq, Q.eseq), (dseq, eseq), (target_dseq, eseq))

cell_address(Q, seq):
  return &Q->cells[seq % len(Q)]
  
try_to_enqueue(Q, I, eseq):
  (state, cell_seq, val) = atomic_read(cell_address(Q, eseq))
  
  if CAS(cell_address(Q, eseq), (state, cell_seq, val), (⊤, eseq I)):
     return true
     
  return false

# dseq and eseq are stored in a single integer; this function updates
# the value such that, when we recover dseq, it increments by one, assuming
# it is stored in the higher order bits, and that sizeof() returns a
# value measured in bytes.
atomic_increment_dseq(Q):
  return FAA((Q.dsec, Q.eseq), 1 << (sizeof(Q.eseq)*8)

# Increments eseq instead of dseq.
atomic_increment_eseq(Q):
  return FAA((Q.desq, Q.eseq), 1)

```

Note that the algorithm above is only obstruction free. While there are no locks:

1. Dequeues can keep retrying infinitely because they're competing with enqueues that are continually overwriting old values.
2. Enqueues can keep retrying indefinitely either because they're continually invalidated by a high volume of faster dequeuers, or because other enqueue operations are faster.

The fact that there is a dependency between operations that can keep one operation from making progress, makes this algorithm obstruction free. Again, we will show how to make this wait free below.

One of the key design choices in this ring is updating the sequence numbers for the next dequeue operation and the next enqueue operation in the same, atomically updated value. This can increase contention for that value, certainly. However, most of the time this value will be updated via fetch-and-add, instead of compare-and-swap.

The only time we do update the value via compare-and-swap is when we are trying to fix a lagging dequeue sequence number (i.e., one where the item with that sequence number has already been dropped from the ring buffer, due to new writes).

The reason we might want to 'fix' the lagging dequeue is because, when we don't, our dequeue operation might retry many, many times in a case where large amounts of data are written before the first dequeue operation comes in, which could happen with 'just in case' log entries, or other items that only get sampled occassionally.

Keeping the two sequence numbers in a single, atomically updatable memory cell and having enqueuers attempt to update it via compare-and-swap is critical to make sure that this 'fix' operation always either succeeds in bringing the dequeue sequence number to the right value, or fails outright.

If the CAS does fail, it could be because of the following scenarios:
1. A FAA from an enqueuer, which means that the dequeue sequence number isn't too horribly invalid (bounded by the maximum number of threads)
2. a FAA from a dequeuer, which means that, while dequeuers are trying to catch up, we are at least slowing ourselves down a tiny bit to give them a chance to dequeue something before we overwrite yet another entry; or
3. A CAS from another enqueuer, which means that the dequeue sequence number is currently close to the tail.

Note that threads do not ever re-try this operation. This is one of the areas where this algorithm could have livelock-- where enqueuers are fast enough that they get ahead of dequeue operations, but dequeue operations are not fast enough to catch up to the head, but their FAA operation is frequent enough to keep the CAS operation failing.

Unfortunately, without being able to atomically set both sequence numbers at once, there is no other obvious way to ensure safety when trying to make a big jump in the dequeue pointer, to help dequeuers catch up.

The linearization point of an enqueue operation is when it successfully enqueues a value via CAS (done inside the try_to_enqueue helper function). An enqueue operation can fail if a dequeuer has the same sequence number and 'invalidates' the slot (helping to preserve our linearization), or if the enqueue operation is suspended long enough that at least one other enqueue operation with a higher number has written there This means that we might also see the cell state's value as containing dequeues of higher sequence numbers, of course.

The linearization point for a successful dequeue is when we successfully read an enqueued value via CAS. When we return ⊥, there are two possible linearization points:

1. The original test to see if the dequeue sequence number is higher than the enqueue sequence number (before we bother to fetch-and-add); or
2. In the case where the last assigned cell does not contain a valid, enqueued value (i.e., due to a slow writer), the linearization point is the CAS where we invalidate that sequence number, returning no enqueued value.

Note that, if a user does enqueue dynamically allocated objects that need memory management, this algorithm can easily be extended to ensure that all such objects can be cleaned up.  For instance, in our ring buffer implementation *ringhat*, we allow the user to specify a per-buffer *ejection callback*-- a function pointer that gets called when objects are overwritten without being returned.

Note that, in such a scheme, there are two places where we need to invoke the ejection callback to ensure we do not miss an object:

1. When a queue successfully replaces one enqueued item with another (which happens at its linearization point; this is also the linearization point for the ejection of the value it replaces);

2. When a dequeue successfully completes a CAS, but finds it dequeued an item that has a lower sequence number. In such a case, the enqueue thread will get 'invalidated' and not see the previous item it would have overwritten. In this case, returning an item with a much lower sequence number would violate our linearization, but as the operation that actually has a reference to the value, it is our responsibility to invoke the ejection callback.

Additionally, when the dequeuer's CAS operation fails in the above algorithm, if the value it read to replace had the correct sequence number, then the buffer is considered full, and the CAS lost to a 'future' operation. Since the sequence number is correct though, we could absolutely return this value without violating our sequential mapping-- we essentially order operations in such a way that the dequeue happens immediately preceding the next item being enqueued.

However, if there is an ejection callback, the overwriting enqueue operation will have called it, passing in the data item, which would likely lead to a memory management issue. As a result, our final algorithm actually will return the item if and only if the queue does NOT have an ejection callback installed.

## Hatring

In this section, we will describe the changes to the obstruction-free algorithm presented above that bound the contention. Generally, we use exponential backoff at the contention point, which is a general way to make single contention points wait-free[^3] that typically has extremely minimal overhead in most practical applications.

In our [FIFO](queue.md), the only point where there is contention is when the dequeue sequence number catches up to the enqueue sequence number. The enqueue operation might need to retry, potentially an arbitrary number of times, if not for our 'helping' mechanism, which is to skip the enqueue sequence forward based on the severity of the problem, until the enqueue operation has a comfortable amount of time to complete before worrying about contention again.

Unfortunately, while the obstruction-free version of our ring algorithm has the same problem, the same solution is not going to work. That's due to our constraint that ring buffers must operate in a fixed amount of space that's linear to the maximum number of items we want to be able to enqueue at once.


And, our ring buffer has not one contention point, but three contention points:

1. The empty queue problem shared with our FIFO.

2. We can imagine a situation where we try to swing forward the dequeue sequence using CAS, in attempt to attempt to minimize the impact of lagging dequeues, yet enqueues are still too fast for dequeues, starving dequeues.  This could result in either continual readjustment of the head pointer, or situations where the enqueue CAS needs to retry due to dequeues competing (though any such event should lead to a small, finite number of retries).

3. In a ring, enqueue operations can complete continually with other enqueues, if the ring is small enough that enqueue operations frequently circle the ring in less time than it takes for suspended enqueues to wake up and finish.

Fortunately, we can address these problems straightforwardly, with exponential backoff technqiues:

First, our implementation use time-based exponential backoff on *enqueue* operations, which 'helps'in problems #2, (when dequeuers are starving), and for problem #3 (where writes dominate, but one write is not keeping up). When help is needed, helping threads sleep for a short amount of time, doubling the time each time help is still needed.  In practice, due to fair scheduling, it doesn't take a significant amount of backoff for contention to resolve. In fact, if we assume a benevolent scheduler, we can comfortably cap our backoff at a small value, and the algorithm will still be wait-free, due to the benevolent scheduler, per the definition in [^3].

For situations #2 and #3, enqueuers can make the determination that contention is a problem due to the queue being full and CASs failing.  If an enqueue operation fails to update the tail pointer, it is competing either with dequeuers overwhelmed by too many enqueues, or dealing with other enqueuers also trying to help dequeuers out.  In this case, exponential back-off gives dequeuers time to catch up; once they do, enqueuers no longer need to swing the tail pointer.

For problem #1, dequeuers can count that they're invalidating too many cells, and that enqueuers need help.  At that point, they can start their own 'exponential backoff', using the trick we used for stacks, where they read a cell multiple times before invalidating it (and go exponential on the number of times they 'read' before invalidating).  Personally, I prefer this approach to trying to guess what minimum time slice is appropriate. However, the structure of our enqueue algorithm made the time-based approach feel more natural.

## Logring: a ring buffer for arbitrary data lengths.

As motivated above, 64-bit values are not always appropriate in situations where we want to work in a fixed amount of memory, without dynamic allocations. Hatring does not work for such scenarios when the data size exceeds 64 bits, because it must atomically add and remove items in their entirety using compare-and-swap.

Logring allows the programmer to specify not only the number of entries in a ring buffer, but also the size of the value field via a parameter.

At the core of this algorithm is a slight enhancement of the hatring algorithm described above-- the enqueue and dequeue operations additionally return the sequence number associated with their operations.

We can use this enhanced hatring as a component to implement our more flexible ring abstraction.

The basic idea is that we have two circular buffers, one buffer name R that is N items long that is an actual ring (implemented using our enhanced hatring), and another circular named L that is not a proper linearized ring, itself, yet holds the actual log messages. The entries in R simply point to a spot in L; R is used to ensure proper linearization.

Threads scan L like a ring to get a spot to write, but if they notice an operation in progress on one of L's cells, they skip that cell, continuing until they find one that's not only safe for writing, but also guaranteednot to have an entry in R pointing into it.

To meet that requirement, L needs to have more entries than R. Specically, it needs to have at least L + MAX_NUM_THREADS entries to guarantee that the queue could be full, and have room for every possible thread to be writing (in parallel) a new item to be enqueued imminmently.

In practice, we will prefer our powers of two for buffer sizes, so when one asks for a log ring with L items, where L is a power of 2, we will actually reserve 2L  items in R (assuming L is greater than MAX_NUM_THREADS).

This two-tier construction, where the second tier is not strictly ordered, but has extra entries for pending operations, is the key insight making our construction possible. And it does so without requiring dynamic allocation, using space that's linear to the number of entries, with a small constant.

In this algorithm, enqueuers do the following:

1. Reserve a slot in L.
2. Copy data into L.
3. Enqueue a pointer to L into R.
4. Write into L the epoch R used to enqueue.
5. Update their slot in L to indicate they're done with their enqueue.

There will be no competition for the enqueuer's slot from other enqueuers.

However, a dequeuer can come in after step 3 completes and before step 4 completes.  The dequeuer could even finish the dequeue operation before the enqueuer finishes his operation.

That's not a problem for us-- the linearization point is the enqueue into R.  We just need to make sure that enqueuers can only claim a slot if BOTH enqueuers and dequeuers are done with their operation (and, if it's not in R, of course).

Dequeuers do the following:

1. Dequeue a value from R.
2. Attempt to mark the cell in L for read.
3. Perform the read.
4. Mark the cell in L to indicate the read is done.

Note that a slow dequeuer might find that by the time they attempt to flag the cell in L for read, someone has already claimed that cell for writing a newer log message.  In such cases, dequeuers just need to try again.

We could add a backoff mechanism to ensure wait-freedom, but if our buffer is big enough, this is going to be a non-issue. So for the moment, this algorithm, despite being built on a wait-free ring, is not itself strictly wait-free.[^4]

The remaining question is how to ensure that we know when an enqueuer can safely take a spot that's been enqueued, but not dequeued.

Note that, above, we write an epoch from R into the slot from L. That means, when an enqueuer starts an operation, it can look at the current state of R, and calculate an epoch that it knows is safe to overwrite.  However, if that enqueuer is very slow, then they will refresh that state whenever they see a slot in L that's enqueued but with no started dequeue operation associated with it (and only if their current state indicates they CAN'T reclaim the spot).

We could consider adding a 'soft' reservation in so that other writers don't waste too much time in competing over the slot, but I don't think that's going to be impactful enough in practice, so I don't do it.

Additionally, we may want to be able to support threads "reading" from the thing without dequeuing from the ring, and we might want people to be able to scan either forward or backward through the ring (knowing there may be dequeues and enqueues that impact us).


--
[^1]: S. Feldman, A. Barrington, D. Dechev. A scalable multi-producer multi-consumer wait-free ring buffer. In proceedings of ACM Symposium on Applied Computing, April 2015
[^2]: Since `l` in our definition is a constant value, it's not included in our bound. However, we generally would like a more precise storage bound of O(c1*n*l + c2) where c1 and c2 are *very* low constants.  Here, c1 may be a bit higher than 1, for instance, to allow us to keep per-item state.
[^3]: M. Herlihy, N. Shavit. On the Nature of Progress. Principles of Distributed Systems. LNCS Vol. 7109, 2011.
[^4]: This might be an excuse really-- I think I've just lost interest in perfecting every algorithm :)