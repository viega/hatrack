#include <testhat.h>
#include <hatrack.h>
#include <stdio.h>
#include <hatrack/debug.h>

// clang-format off
const    uint64_t num_ops       = 1 << 21;
const    uint64_t fail_multiple = 1000;
_Atomic  uint64_t successful_dequeues;
__thread uint64_t cur_dequeues;
_Atomic  uint64_t write_total;
_Atomic  uint64_t read_total;
_Atomic  uint64_t failed_dequeues;
static   gate_t  *gate;

pthread_t enqueue_threads[HATRACK_THREADS_MAX];
pthread_t dequeue_threads[HATRACK_THREADS_MAX];

#ifdef __MACH__
// When -std=c11 is passed, these disappear from string.h but are still
// available at link time.
_Bool clock_service_inited = false;
clock_serv_t clock_service;
#endif

#ifdef ENQUEUE_ONES
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
    uint64_t      producers;
    uint64_t      consumers;
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

q64_t *
q64_new_proxy(uint64_t len) {
    if (len) {
	return q64_new_size(22);
    }
    return q64_new();
}

void
q64_int_enqueue(q64_t *self, uint64_t u32)
{
    q64_enqueue(self, (void *)(u32 << 32));
}

uint64_t
q64_int_dequeue(q64_t *self, bool *found)
{
    uint64_t res = (uint64_t)q64_dequeue(self, found);
    return res >> 32;
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
	.name         = "q64",
	.new          = (new_func)q64_new_proxy,
	.enqueue      = (enqueue_func)q64_int_enqueue,
	.dequeue      = (dequeue_func)q64_int_dequeue,
	.del          = (del_func)q64_delete,
	.can_prealloc = true
    },
/*    {
	.name         = "hq",
	.new          = (new_func)hq_new_size,
	.enqueue      = (enqueue_func)hq_enqueue,
	.dequeue      = (dequeue_func)hq_dequeue,
	.del          = (del_func)hq_delete,
	.can_prealloc = true
        },*/
    {
        0,
    },
};

typedef struct {
    queue_impl_t *impl;
    void         *object;
    uint64_t      start;
    uint64_t      end; // Don't enqueue the last item... array rules.
} h_threadinf_t;

typedef uint64_t thread_params_t[2];

// clang-format off
thread_params_t thread_params[] = {
    {1, 1}, {2, 2}, {4, 4}, {8, 8},
    {2, 1}, {4, 1}, {8, 1},
    {1, 2}, {1, 4}, {1, 8}, 
    {0, 0}
};
//clang-format on

static void
state_reset(void)
{
    gate_init   (gate, gate->max_threads);
    atomic_store(&read_total, 0);
    atomic_store(&write_total, 0);
    atomic_store(&failed_dequeues, 0);
    atomic_store(&successful_dequeues, 0);
    
    return;
}

void *
enqueue_thread(void *info)
{
    uint64_t       my_total;
    uint64_t       i;
    uint64_t       enqueue_value;
    uint64_t       end;
    h_threadinf_t *enqueue_info;
    enqueue_func   enqueue;
    void          *queue;

    mmm_register_thread();

    enqueue_info = (h_threadinf_t *)info;
    enqueue      = enqueue_info->impl->enqueue;
    my_total     = 0;
    end          = enqueue_info->end;
    queue        = enqueue_info->object;

    gate_thread_ready(gate);

    for (i = enqueue_info->start; i < end; i++) {
	enqueue_value = enqueue_value(i);
        my_total     += enqueue_value;
	
        (*enqueue)(queue, enqueue_value);
    }

    atomic_fetch_add(&write_total, my_total);

    gate_thread_done(gate);
    mmm_clean_up_before_exit();
    free(enqueue_info);

    return NULL;
}

void *
dequeue_thread(void *info)
{
    uint64_t       consecutive_dequeues;
    uint64_t       my_total;
    uint64_t       n;
    uint64_t       target_ops;
    uint64_t       max_fails;
    h_threadinf_t *dequeue_info;
    bool           status;
    dequeue_func   dequeue;
    void          *queue;

    mmm_register_thread();

    dequeue_info         = (h_threadinf_t *)info;
    dequeue              = dequeue_info->impl->dequeue;
    consecutive_dequeues = 0;
    my_total             = 0;
    target_ops           = dequeue_info->end;
    max_fails            = target_ops * fail_multiple;
    queue                = dequeue_info->object;

    gate_thread_ready(gate);

    while (atomic_read(&successful_dequeues) < target_ops) {
	
        while (true) {
            n = (uint64_t)dequeue(queue, &status);
            if (status) {
                consecutive_dequeues++;
                my_total += n;
            }
            else {
                break;
            }
        }

	atomic_fetch_add(&successful_dequeues, consecutive_dequeues);
	
        if (atomic_fetch_add(&failed_dequeues, 1) >= max_fails) {
            printf("Reached failure threshold :(\n");
            break;
        }

        consecutive_dequeues = 0;
    }

    atomic_fetch_add(&read_total, my_total);

    gate_thread_done(gate);
    
    mmm_clean_up_before_exit();
    free(dequeue_info);

    return NULL;
}

bool
test_queue(test_info_t *test_info)
{
    uint64_t        i;
    uint64_t        prealloc_sz;
    uint64_t        ops_per_thread;
    uint64_t        num_ops;
    double          max;
    h_threadinf_t  *threadinfo;
    bool            err;
    void           *queue;

    err = false;

    fprintf(stdout,
            "%8s, prealloc = %c, # enqueuers = %2llu, # dequeuers = %2llu -> ",
            test_info->implementation->name,
            test_info->prealloc ? 'Y' : 'N',
            test_info->producers,
            test_info->consumers);
    fflush(stdout);
    
    state_reset();

    prealloc_sz    = test_info->prealloc ? test_info->num_ops : 0;
    queue          = (*test_info->implementation->new)(prealloc_sz);
    ops_per_thread = test_info->num_ops / test_info->producers;
    num_ops        = ops_per_thread * test_info->producers;

    DEBUG("Starting run.");
    for (i = 0; i < test_info->producers; i++) {
        threadinfo         = (h_threadinf_t *)malloc(sizeof(h_threadinf_t));
        threadinfo->start  = (i * ops_per_thread) + 1;
        threadinfo->end    = ((i + 1) * ops_per_thread) + 1;
        threadinfo->object = queue;
        threadinfo->impl   = test_info->implementation;

        pthread_create(&enqueue_threads[i],
		       NULL,
		       enqueue_thread,
		       (void *)threadinfo);
    }

    for (i = 0; i < test_info->consumers; i++) {
        threadinfo         = (h_threadinf_t *)malloc(sizeof(h_threadinf_t));
        threadinfo->end    = num_ops;
        threadinfo->object = queue;
        threadinfo->impl   = test_info->implementation;

        pthread_create(&dequeue_threads[i], NULL, dequeue_thread, (void *)threadinfo);
    }

    gate_open(gate, test_info->producers + test_info->consumers);

    for (i = 0; i < test_info->producers; i++) {
        pthread_join(enqueue_threads[i], NULL);
    }

    for (i = 0; i < test_info->consumers; i++) {
        pthread_join(dequeue_threads[i], NULL);
    }

    max = gate_close(gate);
    
    if (write_total != read_total) {
        fprintf(stdout,
                "\n  Error: enqueue total (%llu) != dequeue total (%llu); "
		"diff = %llu\n",
                write_total,
                read_total,
		write_total > read_total ?
		write_total - read_total :
		read_total - write_total
		);
        err = true;
    }

    if (num_ops != successful_dequeues) {
        fprintf(stdout,
                "\n  Error: # enqueues (%llu) != # dequeues (%llu)\n",
                num_ops,
                successful_dequeues);
        err = true;
    }

    fprintf(stdout, "nil dequeue()s: %-9llu ", failed_dequeues);

    test_info->elapsed = max;
    test_info->num_ops = (num_ops * 2);

    fprintf(stdout, "%.3f sec\n", max);

    (*test_info->implementation->del)(queue);

    return err;
}

static const char HDR[]
    = "\nAlgorithm  | Prealloc? | Enqueuers | Dequeuers | MOps/sec\n";

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
    
    gate = gate_new();

#ifdef HATRACK_TEST_LLSTACK    
    printf("Warning: llstack can get VERY slow when there's lots of "
	   "enqueue contention.\n");
    printf("Give it some time.\n\n");
#endif    
	   
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
        test_queue(&tests[i]);
    }
    
    printf(LINE);
    
    format_results(tests, n, row_size);
    
    return 0;
}
