# The Hatrack test suite

Build this from the top level directory using:
```
make check
```

That will add an executable names `test` to this directory, which will
run some very basic functionality tests, and then run a ton of timing
tests.

At some point I may put a lot more work into this.

If you'd like to see counters for most of the lock-free
implementations, to see how often compare-and-swap applications fail,
then compile with `-DHATRACK_COUNTERS` on. This will slow down the
algorithms that use it, considerably.  See the `config-debug` script
in the `scripts` directory, which configures without optimization,
with counters, and with additional debugging turned on.

So far, I've run these tests in the following environments:

1) An M1 Mac laptop, using clang.

2) An x86 without a 128-bit compare-and-swap, using `clang`.

3) The same x86, using `gcc`.

I've tested with a few different memory managers as well.

In my initial testing, the lock-free algorithms usually performs about
as well-as their locking counterparts, or even better. That's true
even in my environments where there's no 128-bit compare-and-swap
operation.

I have not yet put together real scaling data to show how each
algorithm performs as the number of cores goes up, mainly because I've
not yet run on a machine with a high number of cores.

But I expect the lock-free algorithms to NOT be any sort of bottleneck
to scaling. The `malloc` implementation is more likely. In fact, I've
switched out `malloc`s using `LD_PRELOAD`, and on the Mac, the system
allocator definitely seems to be a bottleneck, as with `hoard`,
performance scales much more linearly.