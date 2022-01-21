# Hatrack
## Hash tables for parallel programming

This project provides several fast hash tables suitable for parallel
programming.

The primary interfaces consist of a high-level dictionary and set,
that are based on our most efficient lower-level algorithms. These
higher-level interfaces deal with a lot of practical challenges, some
of which have never been addressed.

Some key features of these interfaces include:

1. Full linearizability: you can do fully consistent set operations
   and iteratirs/views.
   
2. Good support for user-level memory management needs. Even if your
   contents are dynamically allocated, we make sure you have the
   opportunity to do things like freeing or reference counting,
   without fear of race conditions.

3. Performance and scalability-- these tables are not just fast and
   lock free, they are wait free, and do well in multi-processor
   environments. The core operations are O(1) operations with low
   fixed overhead.

4. The ability to keep tables "ordered" by insertion time.

5. Tables both grow and shrink, when necessary.

If you're interested in understanding the different algorithms, see
the `README.md` in the `src/` directory.

If you're just interested in using the most efficient algorithms in
your C applications, see below for instructions.

## Overview

This project will install `libhatrack.a`, which exports high-level
dictionary and set classes (`hatrack_dict` and `hatrack_set`), backed
by two appropriate lower-level hash tables. Examples of using these
classes are in the `examples` directory.

The lower-level hash tables can also be used directly; again, look at
the `src` directory.

The `hatrack_dict` and `hatrack_set` implementations both support
multiple simultaneous readers and multiple simultaneous writers.

You are responsible for all memory management for keys and items; our
hash tables only worries about memory management for state internal to
the table.

## Installing

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

This will create an executable named `test` in the `tests` directory,
which will run functionality and performance tests for all the
different hash table implementations.

There are no library dependencies, beyond bits that are a part of the
C11 standard (particularly stdatomic and pthreads). Parallel malloc
implementations like `jemalloc` (http://jemalloc.net) or `hoard`
(http://hoard.org) can be added via `LD_PRELOAD`, which may boost
performance when there's significant concurancy, depending on the
system malloc you're using... particularly with `hatrack_set` objects,
which require more dynamic memory allocation than `hatrack_dictionary`
objects do (this is due to sets requiring fully consistent views; see
the `README.md` file in the `src` directory for more information).

## Getting Started

Once you've built, you can just link against the library, and go. See
the `examples` directory.

