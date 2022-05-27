#include <testhat.h>
#include <hatrack.h>
#include <stdio.h>
#include <hatrack/debug.h>

// clang-format off
const    uint64_t target_ops       = 1 << 26;
static   gate_t  *gate;

pthread_t threads[HATRACK_THREADS_MAX];

#ifndef ENQUEUE_INDEX
#define enqueue_value(x) 1
#else
#define enqueue_value(x) x
#endif

// clang-format off
typedef void     (*enqueue_func)(void *, uint64_t);
typedef uint64_t (*dequeue_func)(void *, bool *);
typedef void    *(*new_func)    (uint64_t);
typedef void     (*del_func)    (void *);

// clang-format off
typedef struct {
    char        *name;
    new_func     new;
    enqueue_func enqueue;
    dequeue_func dequeue;
    del_func     del;
    bool         can_prealloc;
} queue_impl_t;

typedef struct {
    bool          prealloc;
    uint64_t      num_ops;    
    uint64_t      enqueues_per_bundle;
    uint64_t      num_threads;
    queue_impl_t *implementation;
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

// Right now our queue takes a power-of-two instead of rounding.
// Until we change that, use this proxy that hardcodes the presize.
queue_t *
queue_new_proxy(uint64_t len) {
    if (len) {
	return queue_new_size(22);
    }
    return queue_new();
}

// clang-format off
static queue_impl_t algorithms[] = {
#ifdef HATRACK_TEST_LLSTACK    
    {
	.name         = "llstack",
	.new          = (new_func)llstack_new_proxy,
	.enqueue      = (enqueue_func)llstack_push,
	.dequeue      = (dequeue_func)llstack_pop,
	.del          = (del_func)llstack_delete,
	.can_prealloc = false
    },
#endif    
    {
	.name         = "hatstack",
	.new          = (new_func)hatstack_new,
	.enqueue      = (enqueue_func)hatstack_push,
	.dequeue      = (dequeue_func)hatstack_pop,
	.del          = (del_func)hatstack_delete,
	.can_prealloc = true
    },
    {
	.name         = "queue",
	.new          = (new_func)queue_new_proxy,
	.enqueue      = (enqueue_func)queue_enqueue,
	.dequeue      = (dequeue_func)queue_dequeue,
	.del          = (del_func)queue_delete,
	.can_prealloc = true
    },
    {
	.name         = "hq",
	.new          = (new_func)hq_new_size,
	.enqueue      = (enqueue_func)hq_enqueue,
	.dequeue      = (dequeue_func)hq_dequeue,
	.del          = (del_func)hq_delete,
	.can_prealloc = true
    },
    {
	.name         = "capq",
	.new          = (new_func)capq_new_size,
	.enqueue      = (enqueue_func)capq_enqueue,
	.dequeue      = (dequeue_func)capq_dequeue,
	.del          = (del_func)capq_delete,
	.can_prealloc = true
    },
    {
	.name         = "vector",
	.new          = (new_func)vector_new,
	.enqueue      = (enqueue_func)vector_push,
	.dequeue      = (dequeue_func)vector_pop,
	.del          = (del_func)vector_delete,
	.can_prealloc = true
    },
    {
        0,
    },
};

typedef struct {
    queue_impl_t *impl;
    void         *object;
    uint64_t      bundle_size;
    uint64_t      num_bundles;
} thread_info_t;

typedef uint64_t thread_params_t[2];

// clang-format off
thread_params_t thread_params[] = {
    {1, 1}, {1, 10}, {1, 100}, {1, 1000}, {1, 10000}, {1, 100000},    
    {2, 1}, {2, 10}, {2, 100}, {2, 1000}, {2, 10000}, {2, 100000},
    {4, 1}, {4, 10}, {4, 100}, {4, 1000}, {4, 10000}, {4, 100000},
    {8, 1}, {8, 10}, {8, 100}, {8, 1000}, {8, 10000}, {8, 100000},
    {16, 1}, {16, 10}, {16, 100}, {16, 1000}, {16, 10000}, {16, 100000},
    {32, 1}, {32, 10}, {32, 100}, {32, 1000}, {32, 10000}, {32, 100000},
    {64, 1}, {64, 10}, {64, 100}, {64, 1000}, {64, 10000}, {64, 100000},
    {128, 1}, {128, 10}, {128, 100}, {128, 1000}, {128, 10000}, {128, 100000},
    {0, 0}
};

//clang-format on

void *
worker_thread(void *info)
{
    uint64_t       i;
    uint64_t       j;
    uint64_t       num_bundles;
    uint64_t       bundle_size;
    thread_info_t *enqueue_info;
    enqueue_func   enqueue;
    dequeue_func   dequeue;
    void          *queue;
    bool           found;

    mmm_register_thread();

    enqueue_info = (thread_info_t *)info;
    enqueue      = enqueue_info->impl->enqueue;
    dequeue      = enqueue_info->impl->dequeue;
    num_bundles  = enqueue_info->num_bundles;
    bundle_size  = enqueue_info->bundle_size;
    queue        = enqueue_info->object;

    gate_thread_ready(gate);

    for (i = 0; i< num_bundles; i++) {

	for (j = 0; j < bundle_size; j++) {
	    (*enqueue)(queue, enqueue_value(i * bundle_size + j));
	}

	for (j = 0; j < bundle_size; j++) {
	    (*dequeue)(queue, &found);
	}
    }

    gate_thread_done(gate);
    mmm_clean_up_before_exit();
    free(enqueue_info);

    return NULL;
}

bool
test_queue(test_info_t *test_info)
{
    uint64_t        i;
    uint64_t        prealloc_sz;
    uint64_t        total_bundles;
    uint64_t        per_thread;
    uint64_t        actual_ops;
    double          max;
    thread_info_t  *threadinfo;
    bool            err;
    void           *queue;

    err = false;

    fprintf(stdout,
            "%8s, prealloc = %c, # threads = %2llu, bundle size = %2llu -> ",
            test_info->implementation->name,
            test_info->prealloc ? 'Y' : 'N',
            test_info->num_threads,
            test_info->enqueues_per_bundle);
    fflush(stdout);
    
    gate_init(gate, gate->max_threads);

    prealloc_sz    = test_info->prealloc ? target_ops >> 4 : 0;
    queue          = (*test_info->implementation->new)(prealloc_sz);
    per_thread     = ((target_ops >> 1) / test_info->enqueues_per_bundle) /
	             test_info->num_threads;
    total_bundles  = per_thread * test_info->num_threads;
    actual_ops     = (total_bundles * test_info->enqueues_per_bundle) << 1;

    DEBUG("Starting run.");
    for (i = 0; i < test_info->num_threads; i++) {
        threadinfo         = (thread_info_t *)malloc(sizeof(thread_info_t));
	
        threadinfo->object      = queue;
        threadinfo->impl        = test_info->implementation;
        threadinfo->bundle_size = test_info->enqueues_per_bundle;
	threadinfo->num_bundles = per_thread;


        pthread_create(&threads[i], NULL, worker_thread, (void *)threadinfo);
    }

    gate_open(gate, test_info->num_threads);

    for (i = 0; i < test_info->num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    max = gate_close(gate);
    
    test_info->elapsed = max;
    test_info->num_ops = actual_ops;

    fprintf(stdout, "%.3f sec\n", max);

    (*test_info->implementation->del)(queue);

    return err;
}

static const char HDR[]
    = "\nAlgorithm  | Prealloc? | # Threads | Op Batch  | MOps/sec\n";

static const char LINE[]
    = "-----------------------------------------------------------\n";

void
format_results(test_info_t *tests, int num_tests, int row_size)
{
    int i;

    printf(HDR);

    for (i = 0; i < num_tests; i++) {
        if (!(i % row_size)) {
            printf(LINE);
        }
        printf("%-13s", tests[i].implementation->name);
        printf("%-12s", tests[i].prealloc ? "yes" : "no");
        printf("%-12llu", tests[i].num_threads);
        printf("%-12llu", tests[i].enqueues_per_bundle);
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

    gate = gate_new();
	
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
            tests[n].prealloc            = false;
            tests[n].num_ops             = target_ops;
            tests[n].num_threads         = thread_params[i][0];
            tests[n].enqueues_per_bundle = thread_params[i][1];
            tests[n].implementation      = &algorithms[j];
            n++;

            if (algorithms[j].can_prealloc) {
                tests[n].prealloc            = true;
                tests[n].num_ops             = target_ops;
                tests[n].num_threads         = thread_params[i][0];
                tests[n].enqueues_per_bundle = thread_params[i][1];
                tests[n].implementation      = &algorithms[j];
                n++;
            }
        }
    }

    for (i = 0; i < n; i++) {
        test_queue(&tests[i]);
    }
    
    printf(LINE);

    format_results(tests, n, row_size);
    
    return 0;
}
