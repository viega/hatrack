// Author: Matt Messier (matt@crashoverride.com)
// bool
// __atomic_compare_exchange_16(__int128_t *value,
//                              __int128_t *expected,
//                              __int128_t  new_value)
.text
.global __atomic_compare_exchange_16
.type __atomic_compare_exchange_16, @function
__atomic_compare_exchange_16:
        movq    %rbx, %r8       # preserve rbx as required by abi
        movq    %rdx, %rbx
        movq    8(%rsi), %rdx
        movq    0(%rsi), %rax
        lock cmpxchg16b (%rdi)
        jz      1f
        movq    %rdx, 8(%rsi)
        movq    %rax, 0(%rsi)
        movq    $0, %rax
        jmp     2f
1:
        movq    $1, %rax
2:
        movq    %r8, %rbx       # restore rbx
        ret
.size __atomic_compare_exchange_16, . - __atomic_compare_exchange_16
