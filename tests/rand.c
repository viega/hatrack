#include "testhat.h"

#include <fcntl.h>  // For open
#include <unistd.h> // For close and read

/*
 * To try to make algorithm comparisons as fair as possible, I try to
 * do everything I can to eliminate places where the OS might use a
 * mutex, where there might be contention among threads.
 *
 * Top of that list is malloc() -- where I recommend addressing by
 * linking in the hoard malloc implementation.
 *
 * Second on that list is the random number generator, since we use a
 * lot of random numbers in our testing, and would like to avoid
 * several things:
 *
 * 1) Calling into the kernel more than we need to (e.g., if we were
 *    to read from /dev/urandom).
 *
 * 2) Any locks around RNG APIs.  For instance, I'm pretty sure
 *    arc4random() has such a lock on my machine.
 *
 * 3) Holding on to too much memory.
 *
 * My basic approach is to implement ARC4 ourselves, and keep the
 * state on a per-thread basis, with the seed xor'd with the bottom
 * byte of the thread's pthread id (just to get some variance in the
 * number streams; multiple threads can definitely end up with
 * identical streams of numbers).  We read the seed once, at
 * initialization time, from /dev/urandom.
 *
 * Note that ARC4 isn't very good cryptographically, but we don't need
 * cryptographically strong random numbers for our purposes. This just
 * gets the job done with a quick algorithm, that can be done without
 * hitting the kernel, after initialization.
 */

static uint8_t seed_buf[HATRACK_RAND_SEED_SIZE];

typedef struct {
    uint32_t S[256];
    uint32_t x, y;
} arc4_ctx;

__thread arc4_ctx rng_ctx;
__thread bool     rand_inited = false;

void
test_init_rand(void)
{
    int rand_fd = open("/dev/urandom", O_RDONLY);

    read(rand_fd, seed_buf, HATRACK_RAND_SEED_SIZE);
    close(rand_fd);

    return;
}

void
test_thread_init_rand(void)
{
    uint64_t tid = (uint64_t)pthread_self();

    uint32_t a, i, j = 0, k = 0;

    rng_ctx.x = 1;
    rng_ctx.y = 0;

    for (i = 0; i < 256; i++) {
        rng_ctx.S[i] = i;
    }

    for (i = 0; i < 256; i++) {
        a            = rng_ctx.S[i];
        j            = (j + (seed_buf[k] ^ tid) + a) & 0xff;
        rng_ctx.S[i] = rng_ctx.S[j];
        rng_ctx.S[j] = a;
        ++k;
        if (k == 32)
            k = 0;
    }

    return;
}

uint32_t
test_rand(void)
{
    uint32_t out;
    uint8_t *p = (uint8_t *)&out;
    uint32_t a, b, ta, tb, ty, i;

    if (!rand_inited) {
        test_thread_init_rand();
        rand_inited = true;
    }

    a         = rng_ctx.S[rng_ctx.x];
    rng_ctx.y = (rng_ctx.y + a) & 0xff;
    b         = rng_ctx.S[rng_ctx.y];

    for (i = 0; i < 4; i++) {
        rng_ctx.S[rng_ctx.y] = a;
        a                    = (a + b) & 0xff;
        rng_ctx.S[rng_ctx.x] = b;
        rng_ctx.x            = (rng_ctx.x + 1) & 0xff;
        ta                   = rng_ctx.S[rng_ctx.x];
        ty                   = (rng_ctx.y + ta) & 0xff;
        tb                   = rng_ctx.S[ty];
        *p++                 = rng_ctx.S[a];
        rng_ctx.y            = ty;
        a                    = ta;
        b                    = tb;
    }

    return out;
}

