# Hatrack
## Hash tables for parallel programming.

This project consisists of several different hash tables suitable for
parallel programming, and one reference hash table that is only
suitable for single-threaded applications (refhat).

We've seen and heard a lot of misconceptions about parallelizable hash
tables. For instance:

- That highly parallel tables cannot shrink as items are deleted.

- That there is no way to get both lock-freedom and amortized O(1)
  operations, with a low constant.

- That it's not possible to get a view / an iterator on the table that
  gives a consistent, "moment in time" snapshot of the hash table
  (necessary, for instance, to implement set operations such as
  intersection and union).
  
- That, with multiple concurrent writers, it's not possible to
  maintain a linearized insertion ordering (e.g., the way that
  Python3's dictionaries order their items based on when they were
  inserted).

- That it's not possible to get a highly parallel table that is
  performant enough to use when apps are single-threaded.

This Hatrack has something for every situation!  We address all of
those problems, with highly performant hash tables.

## Overview

There are a few ways to divide up these hash tables:

1. By progress guarantee (deadlock-free, lock-free, and wait-free).

2. By their ability to parallelize operations (e.g. reads or writes),
as the number of cores available goes up.

3. By their ability to provide fully consistent views, and preserve
insertion-ordering.

We briefly describe the differences below. I'm currently working on a
document that goes into much more detail.

### Progress Guarantees

From weakest guarantees to the strongest:

1. **Deadlock Freedom**, means that the algorithms are designed to
avoid deadlock situations.  In practice, this means we use mutex locks
that may lead to threads blocking waiting on a lock, but there are no
cases where a lock in our code will be acquired without being
released. Of our algorithms, *duncecap, swimcap, newshat* and
*ballcap* are deadlock free (but, using mutexes, do not provide
stronger guarantees for most operations).  Most true hash tables we've
seen in the real world that accept multiple concurrent writers, use
locking and are deadlock free.

2. **Lock Freedom**.  With deadlock freedom alone, threads holding
locks could be suspended for a long time, stalling threads that are
running and could be doing work. With Lock Freedom, some thread is
always able to get work done. And if a thread finds itself not getting
work done, even though it's running, it's because other threads are
being productive.  Of our algorithms, *hihat, oldhat, lohat and
lohat-a* all provide lock freedom (and, in most cases, wait freedom).
We have found one other true hash table that is lock-free, written in
Java by Cliff Click. Our lock-free tables are far simpler, work with C
(Click doesn't have to worry about the hard task of parallel memory
management, as he can simply depend on Java's garbage collection), and
is more efficient.

3). **Wait Freedom**. With lock freedom, individual threads can have
operations that fail to make progress by "spinning" in the same
place. For example, their attempt to update a record might stall
indefinitely, if every time the thread tries an operation, it fails
due to some other thread's progress.  Indefinite loops of
compare-and-swap operations are a common example; they typically only
fail if some other thread was sucessful at the same operation, but
it's possible to have enough activity that individual threads get
starved.  Wait freedom removes that restriction; all threads are
guaranteed to make progress independent of the others.  Of our
algorithms, *witchhat8 and *woolhat* are fully wait free.  Actually,
all our hash tables except for duncecap have fully wait free read (get)
operations.  And our lock free variants are mostly wait-free, except
under exceptional conditions-- the work to convert them to full
wait-freedom is small, and has no practical performance impact.

### Parallelizability

All of our hash tables allow for multiple simultaneous readers, at all
times.  Most of our hash tables also allow for multiple simultaneous
writers, in most cases:

0) *Refhat*, our reference algorithm, is not designed for parallelism. It
assumes there is never more than a single thread accessing the hash
table at any time.

1) The algorithms *duncecap* and *swimcap* are multiple reader, but
single writer only. The first, *duncecap*, cannot start reads while
write operations in progress. However, a write operation can generally
begin when read operations are in progress.  *Swimcap* uses a global
write lock to ensure a single writer, but reads are wait-free, and can
always start and progress even if a writer is stalled.

2) *Newshat* and *ballcap* are multiple-reader, multiple-writer hash
tables. Read operations are fully wait free, but write operations are
not. And, if the table needs to be resized, write operations will
generally wait on a lock, while one thread performs the resizing.

3) All other hash tables allow for multiple concurrent readers and
writers, at all times, including the *hihats*, the *lohats*, *oldhat*,
*witchhat* and *woolhat*.

### Order Insertion Preservation and consistent views.

In every other hash table I've ever seen that allows multiple
concurrent writers, it's impossible to ensure a consistent view of the
hash table. This means you could get an iteration that maps to an
"impossible" state of the hash table (if such is semantically
meaningful to your program). Depending on the implementation, you
could even potentially have a key appear twice in the output (if it is
present in an early bucket when iteration begins, gets deleted and
then re-inserted into a later bucket, while the iteration continues).

We can solve the problem of consistent views, and we do it in a way
that perserves a valid insertion order. In such cases, much like
Python with its dictionaries, when any kind of view is requested on a
table (e.g., an iterator), we can provide the items in the original
insertion order!

Solving the consistent view problem also allows us to implement
meaningful set operations, as they require consistency.

Maintaining consistent views does have an performance penalty. Though,
it's an O(1) penalty, that comes mainly from worse cache performance
and more (relatively expensive) dynamic memory management
operations. In practice, these algorithms are still very fast, in the
grand scheme of things..

Note that, in our tables that do not provide consistent views, we can
still provide *approximate* insertion ordering.

1) Tables without consistent views (and thus would not be good for set
operations): *swimcap, newshat, hihat, oldhat, witchhat*.

2) Tables with consistent views: *ballcap, lohat, lohat-a, woolhat*.

3) Table with a compile-time option for consistent views: *duncecap*.

## Performance

In general, my initial testing of the options indicates that, if you
are using an architecture with a double-word compare-and-swap, there's
no good reason to use anything other than the wait-free versions.  The
only consideration should be whether fully consistent views and order
preservation are important.

Note that, unlike some other parallel data structures that have an
associative (dictionary-like) interface, most these hash tables O(1)
insertions, lookups and deletes.

## Getting started

Right now, there are no high-level interfaces yet. Each algorithm has
a low-level interface you can use. This means:

1) That the implementations don't memory-manage the ITEMS stored in
   the hash table.

2) That the implementations don't run a hash function for you-- you
   pass in a hash value that you're already calculated. We do supply
   XXHASH3-128, as a fast and appropriate hash (see hash.h for an API
   to it that we use in testing).

   This allows you to decide if you want to cache hashes with objects,
   use different kinds of functions for different kinds of data types,
   etc.

I'll produce some developer documentation on using the API soon. Right
now, the source code is documented; start by looking at refhat to
understand the basic API structure.

For general purpose use, witchhat is your best bet (at least when you
have a 128-bit CAS operation; I've not yet tested it on legacy
systems). It runs close enough in speed to refhat that I wouldn't
hesitate to use it as a general-purpose table, even in the case of a
single thread.

If you care about sequential consistency (for instance, for set
operations), woolhat is the best option.

Note that woolhat should still be more than fast enough for most
general-purpose use, but because it requires more dynamic memory
management (and will have worse cache performance as a result), it's
never going to be anywhere near as fast as witchhat.  In my early
testing, woolhat tends to incur a penalty performance of 80-120% most
of the time, though workloads with lots of writes in short succession
will see this go up.

### Logical order of the tables

If you're looking to understand the implementation details better,
then you can review the source code for the different tables, in
logical order (I'd built the hihats and the lohats first, and then the
locking tables to be able to directly compare performance; and woolhat
and witchhat are the culmination of our lock-free tables). Generally,
start with the .h file to get a sense of the data structures, then
scan the comments in the associated .c file.

Note that I've tried to keep tables reasonably independent, to make
them easier to pick out, and drop into some other program. That means
there's a lot of code duplication across files.

Because of that, I don't re-comment on everything in every source code
file. Tables that are incremental changes over other tables are
often VERY lightly commented.

But I should indicate everywhere (if you open the source code for any
table), what the appropriate table to look at would be, if the
comments in the current table wouldn't be enough to understand the
table independently.

Here is an overview of the tables in their 'logical' order:

1) **refhat**   A simple, fast, single-threaded reference hash table.

2) *duncecap**  A multiple-reader, single writer hash table, using locks,
                but with optimized locking for readers.
		
3) **swimcap**  Like duncecap, but readers do not use locking, and are
                wait-free. This implementation previews some of the
		memory management primitives we use in our lock-free
		implementations.
		
4) **newshat**  Also a lock-based implementation, but one that supports
                multiple readers and multiple writers, using a lock
                for each bucket. Note that, in this implementation,
                there is still a single write lock for all writers for
		when tables resize.
		
5) **hihat**    A mostly-wait free hash table, except when resizing, at
                which point most operations become lock-free instead.

                There's also hihat-a, which is identical to hihat,
                except that it uses a slightly different strategy for
                migrating stores.
		
6) **oldhat**   A mostly-wait free hash table that doesn't require a
                128-bit compare-and-swap operation. Note that, on
                architectures w/o this operation, hihat1 still works,
                but C implements the operation using fine-grained
                locking.
				
7) **lohat**    A lock-free hash table, that also has consistent,
                order-preserving views.
		
8) **lohat-a**  A variant of lohat that trades off space to improve the
                computational complexity of getting an
                order-preserving view.
		
9) **ballcap** Uses locks like newshat, but with consistent views,
               meaning that when you ask for a view (e.g., to
               iterate), you will get a moment-in-time snapshot with
               all of the items, that can be sorted reliably by their
               insertion order.  Really just meant for direct comparison
  	       to lohat.
		
10) **witchhat** A fully wait-free version of hihat.

11) **woolhat**  A fully wait-free version of lohat.

12) **tophat** A proof of concept illustrating how language
                implementations can maximize performance until a
                second thread starts, by waiting until that time to
                migrate the table to a different implementation (the
                given implementation lets you select from four,
                currently). Note, however, that, for general purpose
                use, witchhat and woolhat both perform admirably, even
                for single-threaded applications (especially witchhat,
                when multi-threaded order preservation and consistency
                are unimportant).

In general, looking at refhat will give you a good idea of the overall
structure of all these tables, including what the top-level API for
each table will look like.

Then, the earlier tables are generally going to have some redundant
documentation.  Hihat being our first lock-free table, that gets a lot
of exposition, as does Lohat, since it's the first one that adds in
full linearization of operations table-wide.

But, in terms of ones I'd actually pick up and use, I recommend
woolhat (if you need the linearization) and witchhat (if you don't,
and are looking to maximize efficiency).

## Status of this work

Right now, I'm making this available for early comment, but I still
have a lot of work remaining:

1) I'm still working on TopHat; I can definitely make it so that the
   non-threaded readers don't need to hold a lock. But it requires me
   to either change refhat or at least add a refhat-a. I'll make the
   mods in refhat-a soon, and if performance doesn't degrade, I'll
   replace refhat w/ refhat-a.

2) There's some minor work that's just undone. For instance, I need to
   add proper handling for signals, thread deaths, and thread
   exits. And I have a small memory leak in swimcap that I still need
   to squash...

3) I've been doing my primary development and testing on an OS X
   laptop. I have kept things compatable with Linux, but need to go
   back, ensure that all still works in both environments, and set up
   automated testing as I make future commits.

4) I need to keep going through the code base, doing clean-up and
   commenting. I'm also going to organize the directory structure,
   etc.

5) I'm going to write a long document describing the algorithms, that
   will be available in the doc/ directory.

   I have half a mind to turn that into a full-fledged doctoral
   dissertation-- there are certainly enough publishable results here
   (though I'm not sure I want to spend my next year plus tweaking and
   testing hash tables).
   
6) I'm going to provide two high-level interfaces Dict and Set, that
   are suitable for direct inclusion into projects, and allow you to
   dynamically select the properties you need in a hash table.

   Though, I probably will do that strictly as part of this project;
   this project is a bikeshed from a bikeshed project, which I call
   bikeshed... basically which is a library of C programming nicities.
   Witchhat and Woolhat will probably get copied into that project.

7) While I currently have a basic testbed and performance testing rig,
   I intend to do a lot to improve this (including a lot of
   functionality testing that hasn't happened yet). Though, I want to
   use the niceties I've been building myself in 'bikeshed', so these
   projects may get even more intertwined somehow.


Also, I seem to still messing around with some implementation details,
as I revisit my implementation decisions and explore the alternatives
I'd previously abandoned. So there are places where the algorithm
implementations will be slightly inconsistent.

For instance, many of the algorithms, when migrating to a new store,
set flags indicating a move is in progress on all the buckets quickly.
If the bucket is empty, most of those implementations will ALSO set a
flag indicating the migration is done, in an attempt to short-circuit
more work when threads go back to look at the bucket again.

Generally, on first glance that's seemed to have helped, but on one
algorithm, it was seeming to hurt. I haven't given it enough attention
yet, so different algorithms may or may not do this.

Finally, there are minor cosmetic differences that I might never
bother to resolve, often resulting from too LITTLE cut and paste
(perhaps). For instance, both implementations might have a test that
is identical, and currently algorithm A will do 'if (x)' where
algorithm b does 'if (!x)'.

Sorry about that. A part of me wants to keep polishing and tweaking
forever (or least before letting ANYONE see), but I also want to get
this out there for feedback from some friends!