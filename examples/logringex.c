#include <hatrack.h>
#include <stdio.h>

logring_t *ring;

typedef struct {
    uint64_t tid;
    uint64_t mid;
    char     msg[112];
} log_msg_t;

void *
log_thread(void *item)
{
    log_msg_t log;
    uint64_t  id;

    strcpy(log.msg, "This is a log message!");
    log.tid = (uint64_t)item;

    for (id = 0; id < 512; id++) {
	log.mid = id;
	logring_enqueue(ring, &log, sizeof(log));
    }

    return NULL;
}

void
output_logs(void)
{
    log_msg_t log;
    uint64_t  len;
    
    while (logring_dequeue(ring, &log, &len)) {
	printf("tid=%llu; mid=%llu; msg=%s\n", log.tid, log.mid, log.msg);
    }

    return;
}

int
main(void)
{
    pthread_t threads[4];
    uint64_t  i;
    
    ring = logring_new(1024, sizeof(log_msg_t));

    for (i = 0; i < 4; i++) {
	pthread_create(&threads[i], NULL, log_thread, (void *)(i+1));
    }

    for (i = 0; i < 4; i++) {
	pthread_join(threads[i], NULL);
    }

    output_logs();

    return 0;
}
