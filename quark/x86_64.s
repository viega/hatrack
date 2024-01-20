// Author: Matt Messier (matt@crashoverride.com)
// bool
// atomic_compare_exchange_16(__int128_t *value,
//                            __int128_t *expected,
//                            __int128_t  new_value)
.text
.global atomic_compare_exchange_16
.type atomic_compare_exchange_16, @function
atomic_compare_exchange_16:
        pushq   %rbx
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
        movq   $1, %rax
2:
        popq   %rbx
        ret
.size atomic_compare_exchange_16, . - atomic_compare_exchange_16
