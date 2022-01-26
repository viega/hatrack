/* Copyright © 2021-2022 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           rand.c
 *
 *  Description:    A random number generation API that keeps RNGs
 *                  per-thread to minimize contention.
 *
 *                  Note that this interface isn't particularly high
 *                  level:
 *
 *                  1) You need to do the hashing yourself, and pass in
 *                     the value.
 *
 *                  2) You just pass in a pointer to an "item" that's
 *                     expected to represent the key/item pair.
 *
 *                  3) You need to do your own memory management for
 *                     the key / item pairs, if appropriate.
 *
 *                  Most of the implementation is inlined in the header
 *                  file, since it merely dispatches to individual
 *                  implementations.
 *
 *                  To try to make algorithm comparisons as fair as
 *                  possible, I try to do everything I can to
 *                  eliminate places where the OS might use a mutex,
 *                  where there might be contention among threads.
 *
 *                  Top of that list is malloc() -- where I recommend
 *                  addressing by linking in the hoard malloc
 *                  implementation.
 *
 *                  Second on that list is the random number
 *                  generator, since we use a lot of random numbers in
 *                  our testing, and would like to avoid several
 *                  things:
 *
 *                  1) Calling into the kernel more than we need to
 *                     (e.g., if we were to read from /dev/urandom).
 *
 *                  2) Any locks around RNG APIs.  For instance, I'm
 *                     pretty sure arc4random() has such a lock on my
 *                      machine.
 *
 *                  3) Holding on to too much memory.
 *
 *                  My basic approach is to implement ARC4 ourselves,
 *                  and keep the state on a per-thread basis, with the
 *                  seed xor'd with the bottom byte of the thread's
 *                  pthread id (just to get some variance in the
 *                  number streams; multiple threads can definitely
 *                  end up with identical streams of numbers).  We
 *                  read the seed once, at initialization time, from
 *                  /dev/urandom.
 *
 *                  Note that ARC4 is very broken, cryptographically
 *                  (it was once considered a cryptographic RNG, but
 *                  now certainly is not), but we don't need
 *                  cryptographically strong random numbers for our
 *                  purposes. This just gets the job done with a quick
 *                  algorithm, that can be done without hitting the
 *                  kernel, after initialization.
 *
 *  Author:         John Viega, john@zork.org
 */

#include "testhat.h"

#include <fcntl.h>  // For open
#include <unistd.h> // For close and read
#include <string.h> // For memcpy

static int rand_fd = 0;

typedef struct {
    uint32_t S[256];
    uint32_t x, y;
} arc4_ctx;

__thread arc4_ctx rng_ctx;
__thread bool     rand_inited = false;

static void
system_random(char *buf, size_t num)
{
    if (!rand_fd) {
        rand_fd = open("/dev/urandom", O_RDONLY);
    }

    read(rand_fd, buf, num);

    return;
}

/* Given a "seed", initialize the per-thread rng.  The seed is
 * effectively an RC4 key; we initialize the stream cipher, then use
 * the bytes as output.
 */
static void
test_thread_init_rand(char *seed_buf)
{
    uint32_t a, i, j = 0, k = 0;

    rng_ctx.x = 1;
    rng_ctx.y = 0;

    for (i = 0; i < 256; i++) {
        rng_ctx.S[i] = i;
    }

    for (i = 0; i < 256; i++) {
        a            = rng_ctx.S[i];
        j            = (j + (seed_buf[k]) + a) & 0xff;
        rng_ctx.S[i] = rng_ctx.S[j];
        rng_ctx.S[j] = a;
        ++k;
        if (k == HATRACK_RAND_SEED_SIZE) {
            k = 0;
        }
    }

    rand_inited = true;

    return;
}

/* Used for initializing the main thread's random number generator,
 * either via a 128-bit seed passed on the command line (for
 * repeatability purposes), or via HATRACK_RAND_SEED_SIZE random
 * bytes.
 *
 * Subsequent threads will ALWAYS get a new seed from the system.
 */
void
test_init_rand(__int128_t seed)
{
    char seed_buf[HATRACK_RAND_SEED_SIZE] = {
        0,
    };

    if (!seed) {
        system_random(seed_buf, HATRACK_SEED_SIZE);
    }
    else {
        memcpy(seed_buf, &seed, sizeof(seed));
    }

    test_thread_init_rand(seed_buf);

    return;
}

uint32_t
test_rand(void)
{
    uint32_t out;
    uint8_t *p = (uint8_t *)&out;
    uint32_t a, b, ta, tb, ty, i;

    if (!rand_inited) {
        char seed_buf[HATRACK_RAND_SEED_SIZE];

        system_random(seed_buf, HATRACK_SEED_SIZE);
        test_thread_init_rand(seed_buf);
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

void
test_shuffle_array(void *arr, uint32_t num_items, uint32_t item_size)
{
    uint8_t *first;
    uint8_t *p;
    uint8_t *swap_start;
    uint32_t n;
    uint32_t i;

    first = (uint8_t *)arr;
    p     = first + (num_items * item_size);

    while (num_items > 0) {
        p = p - item_size;
        n = test_rand() % num_items--;

        swap_start = first + (n * item_size);
        if (swap_start == p) {
            continue;
        }

        /* For now, swap item_size bytes, byte by byte.  Ideally, we'd
         * swap 128 bits at a time, until we have less than 128 bits
         * left.  But speed isn't critical in our instance, and I
         * don't want to over-complicate things right now.
         */
        for (i = 0; i < item_size; i++) {
            p[i] ^= swap_start[i];
            swap_start[i] ^= p[i];
            p[i] ^= swap_start[i];
        }
    }
}
