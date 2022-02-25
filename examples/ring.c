#include <hatrack.h>
#include <stdio.h>

#undef CONSISTENCY_CHECK

typedef uint64_t thread_params_t[2];

typedef struct {
    uint64_t start;
    uint64_t end;
} thread_info_t;

static gate_t        *gate;
static hatring_t     *ring;
static const uint64_t num_ops = 1 << 20;
_Atomic uint64_t      finished_enqueuers;
_Atomic uint64_t      failed_dequeues;
_Atomic uint64_t      successful_dequeues;
_Atomic int64_t       enqueue_result;
_Atomic int64_t       dequeue_result;
_Atomic int64_t       eject_result;
static pthread_t      enqueue_threads[HATRACK_THREADS_MAX];
static pthread_t      dequeue_threads[HATRACK_THREADS_MAX];

// clang-format off

static const int ring_test_sizes[] = {
    16, 128, 1024, 4096, 32768, 0
};

static const thread_params_t thread_params[] = {
    {1, 1}, {2, 2}, {4, 4}, {8, 8},
    {2, 1}, {4, 1}, {8, 1},
    {1, 2}, {1, 4}, {1, 8},
    {0, 0}
};

static void
state_reset(void)
{
    gate_init   (gate, gate->max_threads);
    atomic_store(&finished_enqueuers, 0);
    atomic_store(&failed_dequeues, 0);
    atomic_store(&successful_dequeues, 0);
    atomic_store(&enqueue_result, 0);
    atomic_store(&dequeue_result, 0);
    atomic_store(&eject_result, 0);
    
    return;
}

void
handle_eject(void *value)
{
    atomic_fetch_add(&eject_result, (uint64_t)value);
}

void *
enqueue_thread(void *info)
{
    thread_info_t *enqueue_info;
    uint64_t       end;
    uint64_t       i;
    uint64_t       sum = 0;
    
    mmm_register_thread();
    
    enqueue_info = (thread_info_t *)info;
    end          = enqueue_info->end;

    gate_thread_ready(gate);

    for (i = enqueue_info->start; i < end; i++) {
	hatring_enqueue(ring, (void *)(i));
	sum += i;
    }

    atomic_fetch_add(&finished_enqueuers, 1);
    atomic_fetch_add(&enqueue_result, sum);
    gate_thread_done(gate);

    free(enqueue_info);

    return NULL;
}

void *
dequeue_thread(void *info)
{
    bool     ending = false;
    bool     status;
    uint64_t enqueuers;
    uint64_t success;
    uint64_t fail;
    uint64_t sum;
    uint64_t n;


    enqueuers = (uint64_t)info;
    success   = 0;
    fail      = 0;
    sum       = 0;
    
    mmm_register_thread();

    gate_thread_ready(gate);

    while (true) {
	n = (uint64_t)hatring_dequeue(ring, &status);
	if (!status) {
	    if (ending) {
		break;
	    }
	    fail++;
	    if (atomic_read(&finished_enqueuers) >= enqueuers) {
		ending = true;
	    }
	    continue;
	}
	sum += n;
	success++;
    }

    gate_thread_done(gate);
    atomic_fetch_add(&failed_dequeues, fail);
    atomic_fetch_add(&successful_dequeues, success);
    atomic_fetch_add(&dequeue_result, sum);
    mmm_clean_up_before_exit();

    return NULL;
}

void
run_one_ring_test(uint64_t enqueuers, uint64_t dequeuers, uint64_t ring_size)
{
    uint64_t       i;
    uint64_t       ops_per_thread;
    uint64_t       total_ops;
    thread_info_t *info;
    double         max;

    fprintf(stdout,
	    "#e= %2llu, #d= %2llu, sz= %05llu -> ",
	    enqueuers,
	    dequeuers,
	    ring_size);
    fflush(stdout);
	    
    state_reset();

    ring           = hatring_new(ring_size);
    ops_per_thread = num_ops / enqueuers;
    total_ops      = num_ops * enqueuers;

#ifdef CONSISTENCY_CHECK
    hatring_set_drop_handler(ring, handle_eject);
#endif    
    
    for (i = 0; i < enqueuers; i++) {
	info           = (thread_info_t *)malloc(sizeof(thread_info_t));
	info->start    = (i * ops_per_thread) + 1;
	info->end      = ((i + 1) * ops_per_thread) + 1;
	    
	pthread_create(&enqueue_threads[i],
		       NULL,
		       enqueue_thread,
		       (void *)info);
    }

    for (i = 0; i < dequeuers; i++) {
	pthread_create(&dequeue_threads[i],
		       NULL,
		       dequeue_thread,
		       (void *)enqueuers);
    }

    gate_open(gate, enqueuers + dequeuers);

    for (i = 0; i < enqueuers; i++) {
	pthread_join(enqueue_threads[i], NULL);
    }

    for (i = 0; i < dequeuers; i++) {
	pthread_join(dequeue_threads[i], NULL);
    }

    max = gate_close(gate);
    hatring_delete(ring);

    fprintf(stdout, "Qs=%llu; ", num_ops);
    fprintf(stdout, "DQs=%llu; ", successful_dequeues);
    fprintf(stdout, "(⊥=%llu in ", failed_dequeues);
    fprintf(stdout, "%.3f sec ", max);
    fprintf(stdout, "(%.3f MOps / sec)\n",
	    (((double)(num_ops + successful_dequeues))/1000000.) / max);

#ifdef CONSISTENCY_CHECK
    fprintf(stdout,
	    "pushed value: %llu; dq + eject: %llu; "
	    "diff: %lld\n",
	    enqueue_result,
	    dequeue_result + eject_result,
	    enqueue_result - (dequeue_result + eject_result)
	    );
#endif    

    return;
    
}

static const char LINE[]
    = "-----------------------------------------------------------\n";
int
main(void)
{
    uint64_t               i, j;
    
    gate = gate_new();
    i    = 0;
    j    = 0;

    while (thread_params[i][0]) {
	j = 0;
	
	while (ring_test_sizes[j]) {
	    run_one_ring_test(thread_params[i][0],
			      thread_params[i][1],
			      ring_test_sizes[j++]);
	}
	printf(LINE);
	i++;
    }
    
    return 0;
}
