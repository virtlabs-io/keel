/**
 * @file keel_snapshot_queue.c
 */
#include "keel_snapshot_queue.h"

#include "keel/mem/mem.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <string.h>
#include <time.h>

struct keel_snapshot_queue {
    keel_otlp_snapshot_t* slots;
    size_t                capacity;
    size_t                head;       /* next pop index */
    size_t                tail;       /* next push index */
    size_t                count;
    bool                  shutdown;
    _Atomic uint64_t      dropped_total;
    pthread_mutex_t       mu;
    pthread_cond_t        not_empty;
};

keel_snapshot_queue_t* keel_snapshot_queue_create(size_t capacity)
{
    if (capacity == 0)
        return NULL;
    keel_snapshot_queue_t* q = keel_calloc(1, sizeof(*q));
    if (!q)
        return NULL;
    q->slots = keel_calloc(capacity, sizeof(*q->slots));
    if (!q->slots) {
        keel_free(q);
        return NULL;
    }
    q->capacity = capacity;
    atomic_store(&q->dropped_total, 0);
    pthread_mutex_init(&q->mu, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    return q;
}

void keel_snapshot_queue_destroy(keel_snapshot_queue_t* q)
{
    if (!q)
        return;
    pthread_cond_destroy(&q->not_empty);
    pthread_mutex_destroy(&q->mu);
    keel_free(q->slots);
    keel_free(q);
}

int keel_snapshot_queue_push(keel_snapshot_queue_t* q, const keel_otlp_snapshot_t* snap)
{
    if (!q || !snap)
        return 0;
    int dropped = 0;
    pthread_mutex_lock(&q->mu);
    if (q->count == q->capacity) {
        /* Drop oldest: advance head, decrement count, then proceed to push. */
        q->head = (q->head + 1) % q->capacity;
        q->count--;
        atomic_fetch_add(&q->dropped_total, 1);
        dropped = 1;
    }
    memcpy(&q->slots[q->tail], snap, sizeof(*snap));
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mu);
    return dropped;
}

int keel_snapshot_queue_try_pop(keel_snapshot_queue_t* q, keel_otlp_snapshot_t* out)
{
    if (!q || !out)
        return 0;
    int got = 0;
    pthread_mutex_lock(&q->mu);
    if (q->count > 0) {
        memcpy(out, &q->slots[q->head], sizeof(*out));
        q->head = (q->head + 1) % q->capacity;
        q->count--;
        got = 1;
    }
    pthread_mutex_unlock(&q->mu);
    return got;
}

static void abs_deadline_from_ms(uint32_t timeout_ms, struct timespec* out)
{
    clock_gettime(CLOCK_REALTIME, out);
    uint64_t add_ns = (uint64_t)timeout_ms * 1000000ULL;
    uint64_t ns     = (uint64_t)out->tv_nsec + add_ns;
    out->tv_sec  += (time_t)(ns / 1000000000ULL);
    out->tv_nsec  = (long)(ns % 1000000000ULL);
}

int keel_snapshot_queue_pop(keel_snapshot_queue_t* q, keel_otlp_snapshot_t* out, uint32_t timeout_ms)
{
    if (!q || !out)
        return 0;
    struct timespec deadline;
    abs_deadline_from_ms(timeout_ms, &deadline);
    int rc = 0;
    pthread_mutex_lock(&q->mu);
    while (q->count == 0 && !q->shutdown) {
        int w = pthread_cond_timedwait(&q->not_empty, &q->mu, &deadline);
        if (w == ETIMEDOUT)
            break;
    }
    if (q->count > 0) {
        memcpy(out, &q->slots[q->head], sizeof(*out));
        q->head = (q->head + 1) % q->capacity;
        q->count--;
        rc = 1;
    } else if (q->shutdown) {
        rc = -1;
    }
    pthread_mutex_unlock(&q->mu);
    return rc;
}

void keel_snapshot_queue_shutdown(keel_snapshot_queue_t* q)
{
    if (!q)
        return;
    pthread_mutex_lock(&q->mu);
    q->shutdown = true;
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->mu);
}

size_t keel_snapshot_queue_depth(const keel_snapshot_queue_t* q)
{
    if (!q)
        return 0;
    keel_snapshot_queue_t* mq = (keel_snapshot_queue_t*)q;
    pthread_mutex_lock(&mq->mu);
    size_t n = q->count;
    pthread_mutex_unlock(&mq->mu);
    return n;
}

size_t keel_snapshot_queue_capacity(const keel_snapshot_queue_t* q)
{
    return q ? q->capacity : 0;
}

uint64_t keel_snapshot_queue_dropped_total(const keel_snapshot_queue_t* q)
{
    if (!q)
        return 0;
    return atomic_load(&q->dropped_total);
}
