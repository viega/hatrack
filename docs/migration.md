# Hatrack Migration Algorithms

Most of the data structures in Hatrack are capable of growing based their initial allocated size (the exception being ring buffers, which are always fixed size).

Generally, hatrack data structures that resize will be backed by a store, implementated array that can resize. When we wish to put more items in the data structure than the current store can hold, then we need to safely migrate the data structure's store to a larger one.

By 'safe', we informally mean:

1. That all operations on the data structure have expected semantics, regardless of the store migration.

2. That all operations are consistent. Particularly, we are looking for all operations to be *linearizable*, meaning that, we can take all operations on a data structure, and map them to a moment of time on a timeline in such a way that would be consistent with the order of operations seen by every thread working on the data structure.

3. That we will never write to or read from memory addresses that have been reclaimed by the system.

If our algorithms are linearizable, then they can be mapped to a sequential exectuion. This is generally done by identifying the *linearization point* for operations, the instant where an operation will be seen as taking effect instantaneously.

Additionally, we would like for our store migration algorithms to not impact our progress guarantees. We want lock-free algorithms to remain lock-free, and wait-free algorithms to remain wait-free.

## Basic approach

Our basic approach to store migration is similar across all our algorithms, even though the exact semantics will change based on the semantics of the operations that the data structure supports.

Generally, when a size modification is required, our basic approach is as follows:

1. Whenever any thread attempting a mutation operation that has not passed its linearization point notices that a migration is necessary, it 'helps' perform the migration, then re-starts its operation, working out of the newest store.  Note that different threads may notice the need to migrate at different times.

2. Non-mutable operations (read operations) continue to work out of the current store, and completely ignore migrations. 

3. Migrating threads compete to mark all cells in the store as 'MOVING', signalling to other mutation threads that a migration is in progress.

4. Once all cells are marked, migrating threads agree on a new store of the apropriate size, which is generally kept in a store's "next" pointer. At this point, no further mutations are possible on the old store.

5. Migrating threads compete to set data in the new store, in a well-defined ordering, into the new store. Since the old store is now immutable, it is generally easy to identify such an ordering.  Note that we generally will not only migrate state from one cell to another, we may need to set up other data in the store.

6. Once new stores are appropriately set up, migrating threads then compete to "install" the store as the current store in the top-level data structure.

7. The thread that successfully managed to install the new store enqueues the old store for deferred reclaimation, where the reclaimation will not occur until we can guarantee that no other threads have a reference to that store. There are plenty of techniques to implement this requirement, including hazard pointers. All of Hatrack's algorithms use an epoch-based memory management system, which tends to be much faster and less error prone.

8. As an optional optimization, when cells are successfully moved (or if we see they DEFINITELY do not need to be moved after the MOVING bit is set), we may mark the cell in the source store as 'MOVED', telling other migrating threads that they do not need to perform work on that cell. They do need to update any accounting appropriately, however.

9. As a further optimization, we may choose to check the top-level data structure on entry (or at any point, really), to make sure that it is still current. If it is not, then we know we are a late migrator, and that the entire operation is already completed.

### Initial focus: Random-access data structures

For the sake of this discussion, we will limit consideration to data structures where mutations of cells are random-access (particularly, flex arrays and hash tables). That is because, for other algorithms, data store migrations will need to meet additional constraints on data that require enhancements of the above algorithm.

For instance, if we are keeping an ordered queue, we may not want to allow dequeing item X, if an item earlier than it in the list is definitely being migrated.  That would violate the semantics of our basic queue operations.

We can extend this algorithm to work in such situations. But, for this document, we limit ourselves to arrays where mutation operations happening on independent cells can do not have to maintain an overall entry / exit order across cells.  Specifically, let's say that we have an array of two items, where both items start out as having the value 0.  Now let's say that we start a store migration to double the store size, and at about the same time, we have threads seeking to update the two cells' values to 1 and 2 respectively, and cells looking to read values.

In a FIFO or LIFO, if the value 1 gets enqueued before the value 2, then it's incredibly important that reads are consistent with that ordering.  In a random-access array though, that ordering is only important for individual cells-- the order that the two parallel readers return results is utterly irrelevent, and we can choose to map those reads to our timeline in any order, as long as each individual thread sees a consistent linear view in the cells it queries.

### Linearization points

The linearization point for the migration operation is when the current store is installed in the top level data structure.

Note that, after the physical point in time where that operation succeeds, we may still have an arbitrary number of threads still operating with a reference to the old store.

That is okay!  If the operation is a 'read' operation, the operation gets linearized when the cell from the old store is read. We map that on the timeline to any point after the cell's migration bit was set, but before the migration to the new store took effect.

If the operation is a mutation operation, then we are also okay! By the time the new store is installed, all cells in the old store are marked as 'MOVING', which will prevent the operation from mutating the old store. This will lead to the late writer to eventually discover the new store, where it will re-try its operation.

Individual reads all linearize at the point where they atomically read from a data cell.  If a migration is in progress, then reads will all happen our of the 'old' store, because the migration operation ends at its linearization point, where the new store is installed. Any readers getting the new store are necessarily coming in after the migration, just as any readers getting the old store are coming in before it.

If a read operation is occuring in parallel with a migration, then there will still be a linear ordering with any other operations on the individual cell. However, note that once the 'MOVING' bit is set, the cell's value cannot change again until after the migration completes, so such reads can be said happen after all mutations that came before the migration.

Write operations linearize with the CAS operation that installs them, overwriting whatever was previously in the cell. Note that some write operations may happen after the move bit gets set in adjacent cells, but per the discussion in the previous section, it's irrelevent to the semantics of random access operations if those parallel mutations linearize before us or after us.

### Correctness

The algorithm above mandates that no items can be moved until ALL items are marked as MOVING. Therefore, if we're comfortable with the correctness of the individual write operations that can work on the cell when the MOVING bit is *not* set, we should not need to consider them once data copying begins, since the operation will be re-tried in full, once the migration is complete.

For read-only operations, we note that, once cells are marked for MOVING, the only possible change to the contents is the addition of a MOVED bit for purposes of optimization. The read should therefore be even easier to see as correct, since we know there will be no mutations to the semantic value of the memory cell.

Therefore, we need only concern ourselves with the correctness of the copying of data from one store to another, and the successful initialization of the new store.

We have expressed our migration in such a way that there's a simple, sequential mapping, that would be obviously correct if performed by a single thread. For it to be correct when multiple threads work together, we need to be sure of the following:

1. Each successful write in the algorithm happens in the same order in which it would happen, and if done by a single thread.
2. Each successful write happens with the same value in which it would get, if done by a single thread.
3. There is a one-to-one mapping from write operations in the sequential algorithm, to writes in the parallel algorithm.  Particularly, we will make sure that no data cell in the destination store is successfully written to multiple times during the migration (see below).

At the time copying begins, all threads will see the same values in the source store, in the same order, since no further mutation is possible in the old store, once the copying begins. Therefore, all threads can easily work through these items in an identical order. Similarly, it's easy for threads to agree on the values to be written.

All that remains is making sure that no data cell can be successfully written multiple times; we will leverage a standard compare-and-swap operation, with constraints around it, to meet this requirement.

This is all reasonably straightforward with our flex array.  If the source store has N buckets, and the destination store has M buckets, where M > N, then for number from cell indexed I from 0 to N - 1, we will move the contents in source store's cell I to the destination's store at cell I.

However, we need each thread to be able to derermine definitively when some other thread's operation completed, so that the same value is not erroneously set twice.

For instance, let's imagine that our array cells contain a 64-bit value and a 64-bit field for state keeping, that both start out zeroed with the underlying memory allocation.  Let's also imagine that, beyond migration bits, we use a bit of the state field to distinguish between 'USED' and 'UNUSED'.

The migrating threads can rely on a 128-bit compare-and-swap operation to change the value only when the destination cell is still unititialized. However, that will only work if it is otherwise impossible in the life of a store for a cell to look fully uninitialized.

Let's imagine, for the sake of example, that our state is zeroed out when we delelete an item from the array, but we do not bother to change the value field.

Now, let's consider the case where we migrate a cell with value V from store S1 to store S2.

Each thread copies the value and state at source cell I, removes the migration-specific bits from the copy, and tries to write to the destination cell S2[I] via compare-and-swap, where the 'expected' value that it compares against is a zeroed-out cell.

Let's say that one thread (call it T1) is fast, and the other (T2) is slow.  Both start on the cell S1[I], and read the same value and state, with only the MOVING bit set.  T1 succeeds, and finishes the migration, whereas T2 gets suspended, until after the migration is finished.

T2 will not immediately notice that the migration was finished, and it will also try to copy from S1[I] into S2[I].  It will always fail, unless there is some way for S2[I] to ever again equal an all-zero value.

Therefore, when deleting a value from the array, we CANNOT simply zero out the contents, otherwise a stale value from a migration might end up getting written out twice.

To that end, our algorithm require that all writes during a migration operation meet one of the following two considitions:

1) The writes occur to a store BEFORE migrating threads begin working on the store in parallel. For instance, we can safely write the number of items we expect to be in the new store, before we agree on the store.

2) The writes occur via a compare-and-swap operation, such that the value in the DESTINATION cell is an invalid value after the first time a successful write occurs.

We can meet this requirement by having a single bit in our state field that indicates whether the cell has ever been written to in its lifetime.

We can also write an articficial, monotonically increasing timestamp (which we generally refer to as an epoch; when an item is present in the array, we also use this epoch if we wish to recover the approximate insertion order), and then use a bit to signify whether the item has been deleted.  We use both of these approaches, depending on the algorithm.

### Correctness for hash table stores

The copying algorithm for our hash table algorithms needs to be more complicated than direct indexing, since indexing in hash tables is controlled both by the hash value of items, as well as by the probing strategy being used to manage collisions.

In all our hash tables, we use linear probing to manage collisions. Also, we add a restriction, that any time a hash key is added to a store, it cannot be removed. The most we can do, is flag that the value associated with the key has been deleted.

However, when we migrate stores, we will only copy out values that are linearly considered to be in the hash table, at the point where all cells in the source store have the MOVING bit set.

As a result of those considerations, the copying operation for our hash table implementations consists of the following:

1. Iterate through each cell of the source array S1 in array order, from I = 0 to I = N - 1.

2. For each cell in the source array, load the hash value associated with that cell, and the current value/state in that cell.  Note that, in most of our implementations, the hash value is a 128-bit value, and the value/state fields are combined into another 128-bit value.  The semantics of the value and state field differs depending on the algorithm, but the migration approach is the same.

3. If there is no hash value associated with the cell, or if the state of the cell is flagged as deleted, then the cell does not need migration.  Note that no hash value may repeat in S1, so if a hash value is skipped for not being present, it will not appear anywhere in the initial state of S2.

4. Otherwise, we use our hash function to determine the initial cell we'd like to hash into in S2, given the size of S2.  This may or may not be the same cell in S1. Note that, since all threads have the same information, all threads will compute the same result.

5. If the cell is currently occupied by a DIFFERENT hash value, we continue to linear probe, either until we find a cell in S2 that is uninitialized, or until we find one where our hash value has been successfully installed.

6. Once we find an appropriate cell, we first CAS in the hash value (if necessary), and then the value and state (identical to the state in S1, except without any MOVING bits set).

Note that, since all threads visit the source buckets in the same order, compute the exact same hash locations, and probe in the exact same order, all of these operations will try the same buckets in the same order. So any contention in CAS operations will either be:

a. From other threads attempting to migrate in parallel.
b. From other threads once the migration is complete.

Either way, a thread can safely ignore CAS operations that fail.

Similar plain to arrays, we still need to ensure that the state of cells at initialization time is not a valid state once some value gets written, which is easily done either via an "initialized" bit or a timestamp of any kind.

### Store agreement and installation

From the above description, it should be obvious how we can properly set up the state of the destination store. What may not be obvious is now to agree on a destination store.

Our approach is as follows:

1. As each thread is marking cells as 'MOVING', we count cells that need to be migrated, based on if they have a value to migrate at the time the bit is set. Threads count this whether or not they are successful at marking the moving bit.

2. After a thread knows that all cells are successfully marked, it allocates a new store of a size that's appropriate, based on whatever metrics we are using, using a call that will zero-allocate the memory (generally via calloc in the posix world).

3. Every thread will attempt to use the compare-and-swap operation to install the store it allocated into the "next" field of the OLD store.

4. All threads agree on the store that was successfully installed.

5. Threads that do not successfully install the store they alloced free the store they did create.

Note that this approach does not tend to over-consume memory, because most architecture implement calloc in such a way that it initially maps to a page of all-zeros, with copy-on-write semantics.  If we do not write to the memory, it does not take up significant additional space.

Therefore, if we do not write to stores before they get committed, we will not over-consume memory on most architectures.

### Progress guarantees

Since there is no point where any individual thread will block (or wait) for a result another thread, the algorithm is clearly lock-free.

The algorithm itself is bounded in the number of steps based on the number of cells in the source array and the amount of linear probing necessary. However, that is all clearly finite.

The above algorithm specifies no additional loops. In particular, none of the compare-and-swap operations ever need to be retried.

Note that this assumes we are setting the original 'MOVING' bit via an atomic "fetch and OR" operation. If we set it via a compare-and-swap operation, then we could wait arbitrarily on successful SET operations for any individual cell, which would make the migration algorithm merely lock-free (unless some additional helping mechanism were to be added).

Note that we are helped in our goal of wait freedom by having each thread attempt to perform every single step. Any individual thread can be suspended at essentially any time, including in the middle of a migration. In order to progress in the face of migrations, threads either need to be able to complete out of the old store (as we do with reads), or need to attempt to actively help, at least until they determine the migration is done.

### Computational complexity

In the above algorithm, each thread visits each cell in the source array at most one time.  For the hash table migration, each thread may visit buckets multiple times due to linear probing, yet still each individual insertion in a hash table with linear probing is itself an amortized O(1) operation.

In this algorithm, failed compare-and-swap operations need never be retried.

Therefore, migrations are linear. In terms of total CPU time, the complexity of O(n * t), where n is the number of cells in the source array, and t is the number of threads participating in the migration.

However, since the t threads are working in parallel, the wall-clock complexity is O(n).

### Performance Notes

As an optimization, whenever we load a value, we check to see if a thread can skip work. For instance, if we see the 'next store' is already created and installed, we can skip marking as well as store creation, and move to cell migration.

Additionally, we generally additionally write a 'MOVED' bit to cells in the source array, to signal to other threads that they do not need to do any work to migrate this cell.

We could also look to skip past marking to the store creation phase by checking the last cell to get marked; if it's already been marked, we can move to store creation. If it isn't, we go through the marking process, whether or not this thread successfully manages to mark anything.

In practice, when multiple threads migrate at the same time, there is a 'caravaning' effect. Threads significantly ahead of others will slow down as they do the bulk of the work, and other threads will quickly catch up. At that point, there will be modest contention, but primarily threads tend to leap-frog each other, and often finish nearly at the same time.

### The Impact of Store Size

Note that, while the algorithm is specified in such a way that there's a clear implicit assumption that the destination store is large enough to contain every item in the source store, the algorithm is otherwise effectively agnostic as to whether the store grows or shrinks.

For instance, if we wish to "shrink" an array of size N to N/2 cells, we would simply have threads performing our migration algorithm stop copying after N/2 cells.

However, we should note that, since mutation operations pause and retry in the face of a migration, the store sizing strategy can have an impact on whether the mutation operations are lock free or wait free.

For instance, let's imagine we have numerous threads doing contending mutations, resulting in the store frequently growing, then subsequently shrinking.

We can imagine a single operation to set the value of cell N that loops indefinititely, continually finds a store migration in progress, suspending as it starts its retry, and then waking up to see the next migration in progress.

Generally, if the store size increases exponentially each time, it will, in a small, finite number of resizes, be large enough to accomidate the activity of all threads, and can keep wait-free mutation operations wait-free.

However, if we want to support arbitrary sized resizes, many help mechanisms can make them wait-free without significant additional cost.

For instance, with our wait-free hash tables, we address this problem by mutation operations counting how many times they contend with a resize, and if it hits a fixed threshold, atomically increment a "help" counter local to the data structure, decrementing counter only when the operation completes successfully.

When any thread is migrating, they check the value of the counter, and if it's non-zero, they double the size of the backing store, no matter what.

In effect, we grow and shrink, but go back to the wait-free "double-only" strategy, whenever a thread needs help.

## Copying and viewing

We can use this algorithm to provide consistent views of a hash table or array, at the time of the linearization point of the migration.

To do this, we need to make a small addition to our algorithm:

1. The view operation attempts to mark the current store as 'reserved' via compare-and-swap, and then goes and helps with the migration.  Note that the value will initialize to false, and never go from false to true.

2. When a thread is successful in installing the new store to the top-level data structure, it checks the 'reserved' field, and only deal with memory reclaimation if the field is set to false, letting the view operation instead handle the reclaimation.

3. A view operation that successfully marked the store can return it after the igration is complete, and then use it however it desires.

4. View operations that fail must retry.

While this algorithm is lock-free, not wait-free, we generally do not expect to see many contending view operations in the real world. If such things need to be supported, we can easily make the following modification:

1. Views still kick off a migration, but they do not take 'ownership' of a migrating store. While they still set the reserved field, they instead make a full copy out of the old store for their own use, once the migration is successful.

2. The memory management strategy for dealing with the old store reverts to our standard approach, where the thread installing the store is responsible for reclaiming its memory, when no other threads are using it.

## Related work notes

The idea of setting bits in each data cell to communicate migration state is perhaps obvious, and it's currently unclear in my research the first to do it, but it certainly is a key feature of Cliff's lock-free hash table.

We also see a similar approach in Feldman et al.'s hash map (in which core operations use O(log n) time, since the algorithm keeps a tree of arrays internally).

However, those two algorithms have much more complicated semantics. For instance, both algorithms will have read operations check to see if a migration is in progress, and if so, look in the destination store.

Both also support mutation in the new store before a migration is completed. And, at least with Click's algorithm, it's also possible to have nested migrations, where so many new inserts get serviced while doing a resize, that a new resize can start before the old one is completed.

Our approach prioritizes read operations, meaning that it is easy for us to make read operations both fast and wait-free.

Additionally, the wall clock time of this algorithm having an O(n) upper bound, our emphasis on minimizing the accounting work that each thread needs to do to keep state on the migration should spend overall less time in migraiton than other algorithms.

However, the latency of individual mutation operations can be higher, since generally at least one write operation will be paused for the entire migration. However, the migration operation seems to be incredibly fast, to the point where that appears to be a non-issue.

For instance, on my M1 Mac Pro laptop, if I start with an empty hash table of the minimum size, as well as a single-threaded hash table, and a single-threaded migration, I can insert 2,500,000 keys into the table in about .3443 seconds. As the number of threads grows (even past the number of processors), the fastest thread keeps getting faster (indicating the table has grown to maximum size within in that time frame), with times as short as .0055 seconds.

Throwing more threads at the problem seems to scale the migration time well, and it's hard to imagine a real-world scenario where the latency of a migration would have any noticable impact to a real-world application.
