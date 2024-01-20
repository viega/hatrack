// Author: Matt Messier (matt@crashoverride.com)
// bool
// atomic_compare_exchange_16(__int128_t *value,
//                            __int128_t *expected,
//                            __int128_t  new_value)
.text
.global atomic_compare_exchange_16
.type atomic_compare_exchange_16, @function
atomic_compare_exchange_16:
        ldp     x4, x5, [x1]            // [expected]
        ldp     x6, x7, [x1]            // [expected]
        caspal  x4, x5, x2, x3, [x0]    // [expected], new_value (x2, x3), [value]
        cmp     x4, x6
        b.ne    1f
        cmp     x5, x7
        b.ne    1f
        mov     x0, #1                  // return true
        b       2f
1:
        stp     x4, x5, [x1]            // [expected]
        mov     x0, #0                  // return false
2:
        ret
.size atomic_compare_exchange_16, . - atomic_compare_exchange_16
