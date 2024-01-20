// Author: John Viega (just the wrapper)

#include <stdint.h>
#include <stdbool.h>

extern _Bool __atomic_compare_exchange_16(__int128_t *address,
                                       __int128_t *expected_value,
                                       __int128_t new_value);

__int128_t __atomic_load_16(__int128_t *address)
{
    // My understanding on the x86 is that we can do a direct load,
    // but we might need to deal w/ register pairs. Doesn't much
    // matter, this is the hammer; just do a CAS; the read value will
    // be in value.

    __int128_t value = 0;
    __atomic_compare_exchange_16(address, &value, 0);

    return value;
}

__int128_t __atomic_store_16(__int128_t *address, __int128_t new_value) {
    // This one, we're going to be conservative as well; unless the
    // target memory is 0'd, we'll end up doing at least 2 CAS
    // operations.

    __int128_t value = 0;

    while (!__atomic_compare_exchange_16(address, &value, new_value));
}

__int128_t __atomic_fetch_or_16(__int128_t *address, __int128_t operand) {
    // This one always requires at least 2 CAS ops, since we need to load
    // before we can do the | operation.

    __int128_t value = 0;
    __int128_t retval;
    __atomic_compare_exchange_16(address, &value, 0);

    do {
        retval = value | operand;
    } while(!__atomic_compare_exchange_16(address, &value, retval));

    return retval;
}
