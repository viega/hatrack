#include <testhat.h>
#include <hatrack.h>
#include <stdio.h>
#include <hatrack/debug.h>

const uint64_t    num_ops       = 1 << 21;
const uint64_t    fail_multiple = 100;
_Atomic uint64_t  successful_pops;
__thread uint64_t cur_pops;
_Atomic uint64_t  write_total;
_Atomic uint64_t  read_total;
_Atomic uint64_t  failed_pops;
struct timespec   stop_times[HATRACK_THREADS_MAX];
static gate_t     starting_gate = ATOMIC_VAR_INIT(0);

typedef void (*push_func)(void *, uint64_t);
typedef uint64_t (*pop_func)(void *, bool *);
typedef void *(*new_func)(uint64_t);
typedef void (*del_func)(void *);

typedef struct {
    char *name;
    new_func new;
    push_func push;
    pop_func  pop;
    del_func  del;
    bool      can_prealloc;
} stack_impl_t;

typedef struct {
    bool          prealloc;
    uint64_t      num_ops;
    uint64_t      producers;
    uint64_t      consumers;
    stack_impl_t *implementation;
    double        elapsed;
} test_info_t;

// A wrapper for llstack_new to take an (ignored) prealloc argument.
llstack_t *
llstack_new_proxy(uint64_t ignore)
{
    // Silence that compiler warning the dumb way for now.
    if (!(ignore ^ ignore))
        return llstack_new();
    return NULL;
}

static stack_impl_t algorithms[] = {
    {.name         = "llstack",
     .new          = (new_func)llstack_new_proxy,
     .push         = (push_func)llstack_push,
     .pop          = (pop_func)llstack_pop,
     .del          = (del_func)llstack_delete,
     .can_prealloc = false},
    {.name         = "hatstack",
     .new          = (new_func)hatstack_new,
     .push         = (push_func)hatstack_push,
     .pop          = (pop_func)hatstack_pop,
     .del          = (del_func)hatstack_delete,
     .can_prealloc = true},
    {
        0,
    },
};

typedef struct {
    stack_impl_t *impl;
    void         *object;
    uint64_t      start;
    uint64_t      end; // Don't push the last item... array rules.
} thread_info_t;

typedef uint64_t thread_params_t[2];

thread_params_t thread_params[] = {{1, 1},
                                   {2, 2},
                                   {4, 4},
                                   {8, 8},
                                   {2, 1},
                                   {4, 1},
                                   {8, 1},
                                   {1, 2},
                                   {1, 4},
                                   {1, 8},
                                   {0, 0}};

static double
time_diff(struct timespec *end, struct timespec *start)
{
    return ((double)(end->tv_sec - start->tv_sec))
         + ((end->tv_nsec - start->tv_nsec) / 1000000000.0);
}

static void
state_reset(void)
{
    for (int i = 0; i < HATRACK_THREADS_MAX; i++) {
        stop_times[i].tv_sec  = 0;
        stop_times[i].tv_nsec = 0;
    }

    atomic_store(&read_total, 0);
    atomic_store(&write_total, 0);
    atomic_store(&failed_pops, 0);
    atomic_store(&successful_pops, 0);
    return;
}

void *
push_thread(void *info)
{
    uint64_t       my_total;
    uint64_t       i;
    uint64_t       end;
    thread_info_t *push_info;
    push_func      push;
    void          *stack;

    mmm_register_thread();

    push_info = (thread_info_t *)info;
    push      = push_info->impl->push;
    my_total  = 0;
    end       = push_info->end;
    stack     = push_info->object;

    starting_gate_thread_ready(&starting_gate);

    for (i = push_info->start; i < push_info->end; i++) {
        my_total += i;
        (*push)(stack, i);
    }

    atomic_fetch_add(&write_total, my_total);

    clock_gettime(CLOCK_MONOTONIC, &stop_times[mmm_mytid]);
    mmm_clean_up_before_exit();
    free(push_info);

    return NULL;
}

void *
pop_thread(void *info)
{
    uint64_t       consecutive_pops;
    uint64_t       my_total;
    uint64_t       n;
    uint64_t       target_ops;
    uint64_t       max_fails;
    thread_info_t *pop_info;
    bool           status;
    pop_func       pop;
    void          *stack;

    mmm_register_thread();

    pop_info         = (thread_info_t *)info;
    pop              = pop_info->impl->pop;
    consecutive_pops = 0;
    my_total         = 0;
    target_ops       = pop_info->end;
    max_fails        = target_ops * fail_multiple;
    stack            = pop_info->object;

    starting_gate_thread_ready(&starting_gate);

    while (atomic_read(&successful_pops) < target_ops) {
        while (true) {
            n = (uint64_t)pop(stack, &status);
            if (status) {
                consecutive_pops++;
                my_total += n;
            }
            else {
                break;
            }
        }

        atomic_fetch_add(&successful_pops, consecutive_pops);
        if (atomic_fetch_add(&failed_pops, 1) >= max_fails) {
            printf("Reached failure threshold :(\n");
            break;
        }

        consecutive_pops = 0;
    }

    atomic_fetch_add(&read_total, my_total);

    clock_gettime(CLOCK_MONOTONIC, &stop_times[mmm_mytid]);
    mmm_clean_up_before_exit();

    return NULL;
}

pthread_t push_threads[HATRACK_THREADS_MAX];
pthread_t pop_threads[HATRACK_THREADS_MAX];

bool
test_stack(test_info_t *test_info)
{
    uint64_t        i;
    uint64_t        prealloc_sz;
    uint64_t        ops_per_thread;
    uint64_t        num_ops;
    struct timespec start_time;
    double          cur, min, max;
    thread_info_t  *threadinfo;
    bool            err;
    void           *stack;

    err = false;

    fprintf(stdout,
            "%8s, prealloc = %c, # producers = %2llu, # consumers = %2llu: ",
            test_info->implementation->name,
            test_info->prealloc ? 'Y' : 'N',
            test_info->producers,
            test_info->consumers);
    fflush(stdout);
    state_reset();

    prealloc_sz    = test_info->prealloc ? test_info->num_ops : 0;
    stack          = (*test_info->implementation->new)(prealloc_sz);
    ops_per_thread = test_info->num_ops / test_info->producers;
    num_ops        = ops_per_thread * test_info->producers;

    starting_gate_init(&starting_gate);

    for (i = 0; i < test_info->producers; i++) {
        threadinfo         = (thread_info_t *)malloc(sizeof(thread_info_t));
        threadinfo->start  = (i * ops_per_thread) + 1;
        threadinfo->end    = ((i + 1) * ops_per_thread) + 1;
        threadinfo->object = stack;
        threadinfo->impl   = test_info->implementation;

        pthread_create(&push_threads[i], NULL, push_thread, (void *)threadinfo);
    }

    for (i = 0; i < test_info->consumers; i++) {
        threadinfo         = (thread_info_t *)malloc(sizeof(thread_info_t));
        threadinfo->end    = num_ops;
        threadinfo->object = stack;
        threadinfo->impl   = test_info->implementation;

        pthread_create(&pop_threads[i], NULL, pop_thread, (void *)threadinfo);
    }

    starting_gate_open_when_ready(&starting_gate,
                                  test_info->producers + test_info->consumers,
                                  &start_time);

    for (i = 0; i < test_info->producers; i++) {
        pthread_join(push_threads[i], NULL);
    }

    for (i = 0; i < test_info->consumers; i++) {
        pthread_join(pop_threads[i], NULL);
    }

    if (write_total != read_total) {
        fprintf(stdout,
                "\n  Error: push total (%llu) != pop total (%llu)\n",
                write_total,
                read_total);
        err = true;
    }

    if (num_ops != successful_pops) {
        fprintf(stdout,
                "  Error: # pushes (%llu) != # pops (%llu)\n",
                num_ops,
                successful_pops);
        err = true;
    }

    fprintf(stdout, "  nil pop()s: %-6llu ", failed_pops);

    min = 0;
    max = 0;

    for (i = 0; i < HATRACK_THREADS_MAX; i++) {
        if (stop_times[i].tv_sec || stop_times[i].tv_nsec) {
            cur = time_diff(&stop_times[i], &start_time);

            if (!min || cur < min) {
                min = cur;
            }
            if (!max || cur > max) {
                max = cur;
            }
        }
    }

    test_info->elapsed = max;
    test_info->num_ops = (num_ops * 2);

    fprintf(stdout, "\t%.4f sec\n", max);

    (*test_info->implementation->del)(stack);

    return err;
}

static const char LINE[]
    = "-----------------------------------------------------------\n";

void
format_results(test_info_t *tests, int num_tests, int row_size)
{
    int i;

    printf("Algorithm  | Prealloc? | Producers | Consumers | MOps/sec\n");

    for (i = 0; i < num_tests; i++) {
        if (!(i % row_size)) {
            printf(LINE);
        }
        printf("%-13s", tests[i].implementation->name);
        printf("%-12s", tests[i].prealloc ? "yes" : "no");
        printf("%-12llu", tests[i].producers);
        printf("%-12llu", tests[i].consumers);
        printf("%-.4f\n", (tests[i].num_ops / tests[i].elapsed) / 1000000);
    }
}

int
main(void)
{
    int          num_algos;
    int          num_params;
    int          num_tests;
    int          n;
    int          row_size;
    int          i, j;
    test_info_t *tests;

    num_algos  = 0;
    num_params = 0;
    num_tests  = 0;
    n          = 0;

    for (num_algos = 0; algorithms[num_algos].name; num_algos++) {
        n++;
        if (algorithms[num_algos].can_prealloc) {
            n++;
        }
    }

    row_size = n;

    for (num_params = 0; thread_params[num_params][0]; num_params++)
        ;

    num_tests = n * num_params;
    tests     = (test_info_t *)malloc(sizeof(test_info_t) * num_tests);
    n         = 0;

    for (i = 0; i < num_params; i++) {
        for (j = 0; j < num_algos; j++) {
            tests[n].prealloc       = false;
            tests[n].num_ops        = num_ops;
            tests[n].producers      = thread_params[i][0];
            tests[n].consumers      = thread_params[i][1];
            tests[n].implementation = &algorithms[j];
            n++;

            if (algorithms[j].can_prealloc) {
                tests[n].prealloc       = true;
                tests[n].num_ops        = num_ops;
                tests[n].producers      = thread_params[i][0];
                tests[n].consumers      = thread_params[i][1];
                tests[n].implementation = &algorithms[j];
                n++;
            }
        }
    }

    for (i = 0; i < n; i++) {
        test_stack(&tests[i]);
    }

    format_results(tests, n, row_size);

    return 0;
}
