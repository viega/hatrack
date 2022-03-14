# Hatstack: A fast wait-free stack

The classic illustration of 'lock free' algorithms involves a simple linked-list based stack, where nodes are pushed and popped via a compare-and-swap operation. While incredibly simple, the algorithm performs poorly under contention, particularly when operations are greatly unbalanced, with more threads either pushing, or popping.

The unbalance in such an algorithm motivates the need for wait-free algorithms, that can provide upper bounds to the amount of work any individual thread needs to do.

In the last fifteen years, there have been only two wait-free stack algorithms described in the literature ([^1] and [^2]). Generally, as mentioned in these papers, there is a wide spread belief that wait-free algorithms are vastly less performant in the average case, but both show how wait-free algorithms can often perform better than lock-free alternatives.

However, the existing algorithms are linked-list based (requiring many small allocations for busy lists), and have complicated helping mechanisms.

Hatstack is a fast stack with both lock-free and wait-free variants, backed by flat arrays, reducing allocation overhead and improving cache performance. The arrays in Hatstack are resizable, so still supports arbitrary stack growth (and contraction), but with few allocations.

## A simple, yet impractical wait-free stack

To motivate our implementation, let's consider a stack implementation where our store is of unbounded size, and we simply keep a 'head' pointer that only ever increments; when items get pushed onto the stack, they are given a location to write.

When a pop is requested, a search starts from the current head of the stack, scanning through the stack from the top down, for items that are not yet popped, swapping in the POPPED flag.

This scheme actually preserves stack semantics, with a clear approach to linearizing every operation, though this was unintuitive to me, at least.

For instance, let's consider an empty stack, where three threads T1, T2, and T3 all attempt to push values V1 through V3 at approximately the same time, but they receive an index to which to write in the order they're listed (i.e, T1 is first and gets assigned index 0 and T3 is last, getting index 2).

Any of those threads may get suspended before completing their write, and thus the actual wall-clock times where the cells get written out may be in any order.

If we add concurrent pops, we could easily imagine a situation where T1 and T3's values get written before those of T2, and we pop either or both of those values before other writes complete. While this is unintuitive, we certainly can provide a firm linearization of operations, algorithmically.

Particularly, let's order operations on a number line arbitrarily from O1 to On, where n is the total number of operations.  This number line will not necessarily match the slot indexing we give out.

In this scenario where we've launched four pop operations in parallel, let's imagine the first POP operation is launched by thread T4, and reads T2's item with vallue V2.  Note that from the perspective of both T2 and T4, the only information they have about the top of the stack is that, at the time T2's operation linearizes, V2 is at the top of the stack, and by the time T4 linearizes, V2 goes from being on top of the stack, to not being on the stack.  Any of the following states are valid from their perspective:

| Case | Stack before POP | Stack After POP |
| ---- | ---------------- | --------------- |
|  #1  | {V2}             | {⊥}             |
|  #2  | {V1, V2}         | {V1}            |
|  #3  | {V3, V2}         | {V3}            |
|  #4  | {V1, V3, V2}     | {V1, V3}        |
|  #5  | {V3, V1, V2}     | {V3, V1}        |


If another pop comes in at this point, depending on the speed of threads T1 and T3, might see an empty stack (case 1), might pop V1 (consistent with case 2 or case 5), or might pop V3 (consistent with cases 3 and 4).

Whatever the new thread finds, culls the state space, but it should be obvious that the popping thread will not see a state that's inconsistent with some valid linearization.

When we think about whether we have a valid linearization, we note that, in general, while a thread T is suspended, it will not have any way to 'disprove' whether any changes to the state of the stack map to a valid linearization due to its own evidence. At best, it could learn about previous states from other threads.

To model whether there is a plausable linearization, let's assume that threads have a way of sharing state information about the stack in such a way that the thread T will always know anything other threads know about the state S at the time it begins an operation. However, this does not need to be full state information, there can be 'unknowns' in the evidence, due to operations in progress that may have linearized, but not yet reported back on additional state information.

From that point, however, T is unaware of the changes to S, until the moment of time where its own operation linearizes, and then cannot be aware until their operation also returns, and somehow learns about parallel operations.

Let's assume, for the moment, that each value has some unique field to it, so that our threads are in the best position possible to detect linearization problems.

Since pushed items are unique, any thread that 'knows' two items any were dequeued in a LIFO order, instead of a FIFO order, discovers an invalid stack state, and thus there cannot be a proper linearization.

Let's now extend our example to assume that the start of our stack is actually some negative index, where, at some epoch in time E, index 0 represents the head of the stack, and everything with a negative index (to the left of the head), is properly ordered, based on its index in the array.

Now, let's imagine overlapping operations are happening in parallel, without making assumptions about the length of time of those overlapping operations. Operations may also begin and end at any point, inducing state changes. If a thread ends up popping something with a negative index, then it should come off in an order that is consistent with any pop of something with a smaller index.

That is, it should be clear that any POP operation using this algorithm that descends into negative indexes can be mapped to the timeline based on the order they actually appear in the array.

The only challenge is when we dequeue items in an order that is inconsistent with the array ordering.  This can occur when we have fast pushers and slow writers, per above.

The basic idea here is that for any ordered sequence of cells, if no pop operations observe out-of-order pushes, then we map the pushes to their order in the array.

But, if pop operations actually NOTICE that writes are happening out of order, then we will order based on the evidence provided by pop operations.  Let's return to the specific example above, where three threads are writing at nearly the same time, but we have concurrent pops.

Since pop operations set a POPPED flag, pushers who start a search from index I know that their operation will:

1) Linearize before any push to an index higher than I.
2) Linearize before any push in progress, if the POP operation 'passes' it.
3) Linearize after any pop operation it passes during its search.

Let's imagine the our stack, growing down, looks like this, at the moment where
a thread reads at index (which actually extends the above example):

| Index | Item |
| ----- | ---- |
|   0   |      |
|   1   |  V2  |
|   2   |      |
|   3   |  ⊥   |

In this case, four push slots have been given out via FAA (to a minimum of three separate threads).  Our current pop operation sees that the next push will write at index #4, so starts its scan at index #3, at which point it sees a ⊥ (the bottom symbol), which we use to denote that the cell has been popped.

Then, our thread walks down to the cell at index 2, and sees that it has not yet been written out, meaning that T1 knows that the eventual write to this cell will come AFTER the pop of the item above it.

When this thread finds V2 at index 1, it does NOT know that the item at index 1 was successfully pushed before the item at index 2. Our pop thread may have stalled before reading V2, giving time for V3 to get written.  And we learn nothing about the state of the push to index 0.

That is, the current thread's view of the state is as follows:

| Index | Item |
| ----- | ---- |
|   0   |  ?   |
|   1   |  ⊥   |
|   2   |  ?   |
|   3   |  ⊥   |


However, if any thread that successfully popped below index 2 passes that index again, and the item is still not written out, then we have proof that its writing will map on the timeline to after the point where we last read that cell.

Generalizing this, it should be easy to see that, given the linearization point of a push operation being the successful installation of the value into its slot, the linearization point will map in the following way:

1. After the most recent pop can be seen as passing this slot, and
2. Before any push operation ordered AFTER the above pop.
3. If there was no such pop operation, then we simply order our push to the next point after the push operation with the next lower index.

Note that any pop operation that sees an unwritten cell necessarily sees NO enqueued value above it, otherwise it would never have visited the cell in question.  Therefore, once a cell is properly written out, no future pops will linearize BEFORE the write, and we can easily linearize any writes to cells with a higher index AFTER this index.

To clarify, if writing to both index 0 and 1 goes slowly, both may get passed.  But, no matter which order these two items get popped, it's easy for all threads to see a consistent linearization of the operations.

And interestingly, the only significant contention with this algorithm is multiple current pop operations, which compare-and-swap when they see a written item, to replace it with the POPPED flag. If they succeed they return, but if they don't, they know someone else popped, and move on to the next bucket. Thus, the algorithm is pretty trivially wait-free.

The big problems with this algorithm relate to our underlying storage mechanism:

1. We do NOT have unbounded storage.
2. Individual pops may take time linear to the number of items that have EVER been in the stack, whereas a single-thread stack gives constant-time results.

For #2, the worst case scenario occurs when we do all our pushing first, and all our popping second. If we push N items, while each push will require constant time, the average number of cells a popper will scan is (n-1)/2, making pop a linear operation, when we want it to be a constant-time operation with a small constant.

For the moment, this document is going to outline approaches to addressing these two problems. As I started documenting the current Hatstack algorithm, it occured to me that there were some additional choices I could explore, so for the moment, I'm going to focus on the possibilities, and possibly implement some alternative approaches.

## Addressing the storage problem

The biggest issue is that our stack can easily end up with a lot of dead space. For instance, a stack that has had, in its lifetime, n pushed items, and n-3 popped items could easily look line this:

| Index | Item |
| ----- | ---- |
|   0   |  V0  |
|   1   |  V1  |
|   2   |  ⊥   |
|   3   |  ⊥   |
|  ...  |  ⊥   |
|  n-1  |  ⊥   |
|   n   |  Vn  |


Ideally, we will find ways to effectively compress the stack to remove dead space. One obvious way to do that would be with our migration technique.  For instance, if our array is of size n, we can trigger a resize every n pushes, marking buckets for momvement from bottom to top, to make sure we preserve our linearization.

We could trigger this migration when the cost of a pop gets above some constant threshold. However, directly copying an entire backing store is a lot of overhead in cases where there are either few empty spaces, or whether there are many empty spaces.

Ideally, we would find a way to compress the stack in-place.

One observation we can make to help is that, if a pop successfully completes, and they can be sure that no NEW push operations started after it (even though there may have been push operations in progress when we began), then we can move the head pointer to one of two places:

1. The index one above the highest index at which we saw a late writer, or
2. If we see no late writers, then the index from which we popped.

Clearly such a reset is safe if no push operations come in.  Even if we termine that it's safe to move the head to from slot n to slot n - 3, and by the time we are successful a competing pop operation removed the item at slot n - 3, further pushes and pops still will clearly have the correct semantics.

### The basic head reset
We simply need a way to ensure that such a head reset is safe.

We can do this with two primitives:

1. A nonce that counts up every time a store's head pointer moves backward.
2. A compare-and-swap operation.

Essentially, if our head state is atomically updatable, and consists not only of a head pointer, but also the nonce value, then our popping thread can 'expect' that the nonce will not have changed, nor will have the head pointer, since its operation began, and set its desired value such that the nonce is incremented by one, and the head pointer is contracted to the desired state.

If such an update fails, then either new items have been pushed, or some other popper was competing, and faster in setting the head state. Either way, the attempting thread can ignore the failure, and the only consequence of the failure is resulting 'dead space' on the stack.

The 'dead space' may go away with additional head resets, or it may not. We will consider other compression techniques, but a simple approach would be to use the simple head reset as much as possible, but resort to our store migration algorithm whenever a push operation is assigned a slot outside the bounds of an array.

Let's now consider threads that suspend for a singificant amount of time.

First, think about a very late push operation. For a cell at index I to be a late push, then the head pointer will always be higher than I, no matter how many pushes or pops succeed while we are waiting, because no pop operation who passes our cell will swing the index past I+1, due to the state of the cell being unpopped.

A very late pop operation is not a problem, as any time the head state moves forward or the epoch ticks up, the late popper will be unable to complete its compare-and-swap operation.


### Changing the linear mapping

We can enforce a constraint that no pop operation may ever see an out-of-order write in progress, when scanning.  We can do this by having pops attempt to compare-and-swap the POPPED flag into cells before reading (expecting an empty cell). If the proactive pop operation succeeds, then there was a slow pusher, and that pusher then has to re-start its operation.

This forces pushes to always write out in a way that their push order (as seen by pops) is consistent the index.  That is, no pop will ever cause the linear mapping to reorder pushes in such a way that a push with a lower index maps to a point on the timeline that's higher than a push with a lower index.

This approach definitely is not necessary, and introduces between the push and the pop, where pushes may need to retry if a pop operation invalidates them. Without some additional helping mechanism, this approach is obviously *not* wait-free.

The advantage to this would be that it allows us to swing the head pointer farther when there are suspended threads.  However, swinging the head below stalled pushes can create a situation where multiple threads can be attempting to push into the same cell, at the same time.

Meaning, we need to support a scenario where the stalled push will fail, and the most recent push will succeed.

We can do this simply by having all push and pop operations (including invalidation pops) write out the nonce they saw when reading the head state, when they successfully complete their operation.

Then, as push operations happen, they will check the stored epoch in the cell before they attempt to compare-and-swap their value into the cell; if they see either a higher epoch, or a successful push from an earlier epoch, then they need to retry and re-linearize.

However, this now introduces some contention not just between pushes and pops, but across pushes.  As a result, being able to swing the head pointer further may not be worth the additional contention.

My current algorithm *does* implement this approach. And, we add an additional helping mechanism to slow down pops when there is contention, using an exponential backoff scheme that makes the operation wait-free. However, I now suspect that the first presented solution will be better, even if changing the linear mapping can maximize the compression we get from a head reset.

### In-place compression

It would be straightforward to use our migration technique to 'compress' the stack (and the current implementation does that).

However, if a stack is sparsely populated, that could involve touching every cell, in order to move very few cells. Here too, we can use a nonce in order to compress stacks in-place.

First, let's consider how we'd do it by using a variant of our migration operation across the entire store. Then, once we understand this, it should be clear how we can do this on a smaller scale.

1. We would add state bits to cells, in order to help with the compression. This would include an integer field the measures the distance we are moving a cell, to help us with the synchronization.
2. When we decide, by whatever metric, to start a compression, we would read the nonce in the head state, then go through and mark all cells in the store withthe nonce (via a compare-and-swap operation).  As part of this operation, we explicitly set the field in the cell indicating the distance to move, to the value 0.  Note that the nonce will be checked every step of the way... if at any point we see a nonce that we believe to be a 'future' nonce, we will know that our thread got stalled, and that the compression operation finished.
3. Pushes and pops in progress who see a nonce value greater than or equal to the one they read from the head state in any cell will stop what they're doing, help with the compression, and then retry their operation after. Pushes and pops that finish before their cells are marked are okay, since all threads attmpting to compress mark the cells in order.
4. Once all cells in the store are marked, we go through from lowest to highest, to find the first empty space on the stack (meaning, spot where there is a pop).
5. We then scan forward, looking for the lowest indexed item that is higher than the first empty spot we found on the stack.
6. We calculate the difference between the two locations, and swap it into the cell currently containing the item, along with the `COMPRESS-MOVING` bit.
7. We then swap the item into the empty slot we identified, along with in the difference field (showing how far this cell has moved).
8. Now, we change the `COMPRESS-MOVING` bit in the cell we moved to `POPPED`, and remove the item. We do not, however, remove the nonce or the difference field, leaving them there for the benefit of other latecoming threads that might need to synchronize.
9. Once we have visited all cells, we attempt to increase the head state's nonce value by one, via a compare-and-swap operation.
10. If at any time our compression algorithm sees an inconsistent state, but the nonce is the same, it knows some other thread is ahead of us, and resynchronizes based on the difference field, to help complete the operation, if necessary.

The nonce, difference and COMPRESS-MOVING field together give every thread enough state to prevent accedental compare-and-swap operations, ensuring that each operation only happens a single time. In particular, the nonce value combined with state information ensures that slow operations never succeed in a future compression, and the additional state not only prevents accidental updates in the current compression, but also allows threads to reconstruct the correct state under the current nonce, even when a move is in progress.

The primary thing we would need to make this compression algorithm work on less than the whole store would be a way for all threads to agree on how deep down the stack to begin the compression operation. We could, for instance, coordinate across threads to track the lowest index without a pop, and trigger a compression from that location only when the head pointer strays too far from that spot.

Note that this compression algorithm can work with or without the forced linearization where we invalidate slow buckets. If used in conjunction, then we can share the nonce field.

Also note that this algorithm also introduces contention that makes the algorithm lock-free without helping mechanisms, but they are easily added. For instance, the need for pushes and pops to retry when they notice a compression in progress is easily handled by having them ask for 'help' after some fixed number of attempts, and when help is requested, we keep doubling our metric fro triggering the compression algorithm, until we see the help bit has been removed (at which point where can reset it).

The challenge with in-place compression is that it is clearly has to do more accounting and checking on a per-cell basis, than a basic algorithm. And, if compressions are frequent, the constant overhead could end up being high.

At some point I may implement this to see if we get any improvements, but I think it won't be necessary.