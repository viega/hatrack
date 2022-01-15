# Hatrack
## Hash tables for parallel programming

This project consisists of fast hash tables suitable for parallel
programming, including multiple lock-free, wait-free hashtables.

If you're interested in understanding the different algorithms, see
the README.md in the src/ directory.

If you're just interested in using the most efficient algorithms in
your C applications, see below for instructions.

## Overview

This project will install libhatrack.a, which exports high-level Dict
and Set classes, backed by two appropriate lower-level hash tables.

The lower-level hash tables can also be used directly; again, look at
the src directory.

The Dict and Set classes support multiple simultaneous readers and
multiple simultaneous writers.

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
script and Makefile.in, using autotools:

```
aclocal
autoconf
automake
```

If you want to run the test suite, first build it with:

```
make check
```

This will create an executable named `test` in this directory, which
will run functionality and performance tests for all the different
hash table implementations.

There are no library dependencies. Good parallel mallocs like jemalloc
or hoard can be added via LD_PRELOAD, which may boost performance when
there's significant concurancy, depending on the system malloc you're
using... particularly with Set objects, which require more dynamic
memory allocation than Dictionary objects do (this is due to sets
requiring fully consistent views; see the README in the src directory
for more information).

## Getting Started

