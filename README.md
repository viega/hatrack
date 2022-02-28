# Hatrack
## Fast, multi-reader, multi-writer, lockless data structures for parallel programming

Hatrack provides a number of data structures for parallel programming, currently including:
1. **Hash tables** (via the `hatrack_dict_*` interface, found in `include/dict.h`)
2. **Sets** (via `hatrack_set_*`; see `include/set.h`)
3. **Stacks** (via `hatstack_*`; see `include/stack.h`)
4. **FIFO queues** (via `hq_*` ; see `include/hq.h`)
5. **Ring buffers** (via `hatring_*` for pointer-size entries and `logbuffer_*` for abitrary entry sizes)
6. **Flex arrays** (i.e., resizable arrays, via `flexarray_*`; see `include/flexarray.h`)
7. **A "debug ring"**, a ring buffer that trades off correctness in favor of speed (see `include/debug.h`)

Additionally, before the 1.0 release, there will be:

1. A `vector`: a flex array, but with additional `push()` and `pop()` operations, so that you can switch between using a single data structure as a stack or as a random-access array, interchangably.

## Status

For the most part, these algorithms seem to be a huge improvement over the state of the art for lock-free parallel programming. I originally started this because it was impossible to find a good O(1) multi-producer, multi-consumer hash table, despite a bit of promising work that was unfortunately about 15 years old. I've since found it easy to improve greatly on the state of the art in almost all areas.

For instance, with ring buffers, (an admittedly cursory) search has only turned up single-producer / single-consumer buffers, and a recent buffer that isn't actually a ring-buffer by my definition-- it can get full, where the correct behavior should be to safely overwrite old entries. So to me, judging it by the interface, that's a fixed-size FIFO, not a ring buffer (I'm sure it keeps a ring internally as part of its implementation).

The algorithms that I've pushed have been pretty rigorously tested (with the exception of flex arrays, which I'll do when I am also ready to test my vector). There's a pretty thorough hash table benchmarking program if you run 'make check'.  For the many queues, the examples directory focuses on both correctness testing (by checking to see if the inputs match the outputs), along with some timing that tends to _underestimate_ the throughput of algorithms, because I do not yet pre-fill queues to avoid lots of dequeue misses (so they are overhead not counted as ops, but factor into the denominator when calculating ops/sec).

I'm currently targeting being feature complete for a 1.0 release by mid-april of 2022. Beyond the remaining algorithms above, I also intend to do the following:

1. Evolve the queue and array examples into more well-rounded performance testing programs, the way I've done for hash tables.
2. Do a significant amount of minor cleanup and normalization.
3. Do better cross-platform testing (right now, I'm developing mainly on a mac, and occasionally testing on a Linux box).
4. Improve the packaging (for instance, generate shared objects, not just a static lib)
5. Provide significant developer documentation.
6. Reorganize the source tree.

Currently, the source tree hints at the fact that this started out as a bunch of hash table algorithms-- several new approaches, and other reference/ pedagalogical examples, which currently all live in the `src` directory.  The other algorithms currently live in the `bonus` directory.  Soon each class of algorithm will get its own directory.

Note that, certainly for hash tables, but also for various kinds of queues, there are multiple algorithms implemented. Occasionally I've implemented someone else's algorithm (right now, I think it's actually just the basic linked list stack), but often I've come up with multiple approaches, and then done comparative testing. For instance, my first queue (currently in `bonus/queue.c`) does not perform as well as my second, at least in my environment (`hq`), but I leave it around for comparative testing, learning, etc.

## Performance

We do performance testing in ideal conditions, trying to remove any external variables to the degree possible. For instance, we precompute all possible hash values for the range of inputs we use when testing hash functions.

I also try to perform as many operations as possible as quickly as possible, and try to mimic typical workload performance where possible. For instance, with hash tables we'll test individual operations, but also test them in contention with each other.  A 'cache' use case would be very read-heavy, with few updates. But we also simulate situations where we get a rush of data very quickly, with relatively few reads.

To date, my testing has generally happened on a 2020 13" M1 MacBook Pro.  Across all of our "core" algorithms, here's what I've seen so far:

1) Read speeds (when there's little or no modification of the data structure) can easily get into the 100's of MOps per second (yes, I've sometimes seen in excess of 400 *million* operations per second in hash tables and flex array).

2) Mutation operations that change the data structure vary in performance, based on the data structure.  Hash tables tend to give better performance, because there's usually a lot less coordination / contention between threads for any data item (it can approach 0 pretty quickly).  Queues, even when they're wait-free, generally need all threads reading and writing from some of the same data items (like pointers to the head or tail of the queue).

3) Even so, our mutation operations still tend to (again, on my laptop), run really fast in the grand scheme of things. For instance, while I've not yet fully built out queue testing (so the performance should get even better), `hq`, `hatstack` and `hatring` still tend to be able to push between 10 and 40 MOps per second, depending on the configuration.

These algorithms are good for general-purpose use; they will all perform pretty admirably, even in single-threaded applications.


## Installation and Use

This project will install `libhatrack.a`. Examples of using data structures are in the `examples` directory.


If you downloaded a release distro, you can simply do:

```
./configure
make
sudo make install
```

If you cloned the repo, then you'll first need to create the configure
script and `Makefile.in`, using autotools:

```
aclocal
autoheader
autoconf
automake
```

If you want to run the test suite, first build it with:

```
make check
```

This will create an executable named `test` in the `tests` directory, which will run functionality and performance tests for all the different hash table implementations.

There are no library dependencies, beyond bits that are a part of the C11 standard (particularly stdatomic and pthreads). Parallel malloc implementations like `jemalloc` (http://jemalloc.net) or `hoard`  (http://hoard.org) can be added via `LD_PRELOAD`, which may boost performance when there's significant concurancy, depending on the system malloc you're using... particularly with `hatrack_set` objects, which require more dynamic memory allocation than `hatrack_dictionary` objects do (this is due to sets requiring fully consistent views; see the `README.md` file in the `src` directory for more information).

## Getting Started

Once you've built, you can just link against the library, and go. See the `examples` directory.

All of the algorithms provided support multiple concurrent readers and writers. All of the algorithms are lock-free; most of them are also wait-free.  See the section *Progress Guarantees* below for a brief explaination.

There are a bunch of 'off-by-default' algorithms, including lower-level hash tables.  They can be compiled in if desired, and currently live in the `src` directory.  By the 1.0 release, I may make some of them pluggable into the higher-level interface.

Note that you are responsible for all memory management of data items you put in; these algorithms only sort the memory management associated with any internal state.  Though, we do support registering callbacks to allow you to bump reference counts (or do other memory management) safely before returning.


## Progress Guarantees and Comparison to Locking Algorithms

Above, I said that our algorithms[^1]  are all lock-free, and most of them are wait free.  If you dig deeper into the documentation on hash tables, I explain in a lot more detail, but this section provides a basic explaination.

Wait-freedom gives you the best guarantees of progress. For instance, with locking algorithms, when the underlying scheduler suspends threads, they may easily block progress of other threads if they're holding a lock.

In lock-free algorithms, you might end up competing a lot with other threads operating on the same data structure in a way that meaningfully slows down progress (theoretically forever), although such conditions might be rare.

With wait freedom, we can put a firm bound on how much work it will take to complete any operation (no unbounded waiting).

Often, achieving wait-freedom will require a tiny bit of extra overhead in the average case, compared to lock freedom, but will prevent worst-case performance. Generally, I've defaulted to wait freedom, except in the stack, where the case where contention is a potential issue is not very real-world. However, I may go back and add the option there, before the 1.0 release.

While the conventional wisdom is that locking algorithms tend to be a little faster than lock-free algorithms, I'm finding that it all depends on your algorithm. For instance, My low-level hash table named `newshat` is a locking table very similar to a lock-free (but not wait-free) version called `hihat`; the wait-free version of the same thing is called `witchhat`.  Usually, they all have pretty similar performance numbers, but `hihat` (lock-free) will generally be a smidge faster than `witchhat` (wait-free), which will be a smidge faster than `newshat` (locking).

Specifically, on my M1 Macbook Pro laptop, an 8-thread concurrent read test has `hihat` able to perform 167.5M reads/sec, whereas `witchhat` does 126.8M reads/sec and `newshat` does 121.9M.  For 8-thread puts, `hihat` does 33.4M, `witchhat` does 34.0M, and `newshat` does 25.9M.  (Occasionally when contention situations arise as a matter of randomness, the wait-free version will perform a little better).

[^1]: Ignoring some reference implementations used for performance comparisons.