/**
 * @file worker_catchup.c
 * @brief Reactor-owned replica catch-up wait list (Phase 2a scaffolding).
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * See `include/keel/engine/catchup.h` for the contract.
 *
 * # Implementation notes — Phase 2a (this file)
 *
 * This patch ships the data structures, public API, and timer integration
 * scaffolding. The two pieces it intentionally does NOT include:
 *
 *   - The probe state machines (Patches 2b/2c add PostgreSQL `Q
 *     SELECT pg_last_wal_replay_lsn() >= …` and MySQL `SHOW REPLICA STATUS`
 *     respectively).
 *   - The router → engine wiring (Patch 2d).
 *
 * As a result the tick loop today only:
 *   - expires waiters whose deadline has passed (→ KEEL_CATCHUP_TIMEOUT);
 *   - cleans up cache entries past their TTL;
 *   - decays probe-socket backoff windows.
 *
 * Because `keel_router_create()` still downgrades `KEEL_STALE_READ_WAIT`
 * to `KEEL_STALE_READ_ROUTE_PRIMARY` until Patch 2d, no production code
 * path enqueues a waiter yet. The scaffolding can therefore land safely
 * with unit tests only — operator behavior is unchanged.
 */

#include "keel/engine/catchup.h"
#include "worker_catchup_internal.h"

#include "keel/engine/backend_pool.h"
#include "keel/engine/worker.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"
#include "keel/util/util.h"

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>            /* close() */

/* Type aliases for the still-internal cache_entry name used below. */
typedef keel_catchup_cache_entry_t cache_entry_t;

/* ============================================================================
 * Small helpers
 * ============================================================================ */

static inline uint64_t now_ns_or(uint64_t fallback)
{
    /* keel_time_now() returns nanoseconds; falls back to a caller-supplied
     * "frozen" time when called from a unit test that drives the tick
     * explicitly. */
    uint64_t t = (uint64_t)keel_time_now();
    return t ? t : fallback;
}

static inline bool tokens_equal(const keel_consistency_token_t* a,
                                const keel_consistency_token_t* b)
{
    if (a->timeline_id != b->timeline_id) return false;
    return strncmp(a->value, b->value, KEEL_CONSISTENCY_TOKEN_MAX) == 0;
}

static void list_unlink(keel_catchup_manager_t* m, keel_catchup_waiter_t* w)
{
    if (w->prev) w->prev->next = w->next; else m->head = w->next;
    if (w->next) w->next->prev = w->prev; else m->tail = w->prev;
    w->prev = w->next = NULL;
    if (m->waiters_active > 0) m->waiters_active--;
}

static void list_append(keel_catchup_manager_t* m, keel_catchup_waiter_t* w)
{
    w->prev = m->tail;
    w->next = NULL;
    if (m->tail) m->tail->next = w; else m->head = w;
    m->tail = w;
    m->waiters_active++;
    if (m->waiters_active > m->waiters_high_water)
        m->waiters_high_water = m->waiters_active;
}

/** Single release point. Fires the resume callback exactly once. */
static void release_waiter(keel_catchup_manager_t* m,
                           keel_catchup_waiter_t* w,
                           keel_catchup_outcome_t outcome)
{
    if (w->released) return;
    w->released = true;

    list_unlink(m, w);

    switch (outcome) {
    case KEEL_CATCHUP_REACHED:    m->fulfilled_total++; break;
    case KEEL_CATCHUP_TIMEOUT:    m->timeout_total++;   break;
    case KEEL_CATCHUP_PROBE_FAILED: m->timeout_total++; break;  /* counted with timeouts for ops */
    case KEEL_CATCHUP_CANCELLED:  m->cancelled_total++; break;
    }

    keel_catchup_resume_cb cb = w->resume;
    void* ud = w->userdata;
    struct keel_session* s = w->session;

    /* Free before invoking the callback: the callback may immediately
     * re-enqueue a follow-up waiter, and we want the slot accounted. */
    keel_free(w);

    if (cb) cb(s, outcome, ud);
}

/* ============================================================================
 * Probe-result cache
 * ============================================================================ */

/** Look up a fresh, matching cache entry; returns NULL on miss or stale. */
static const cache_entry_t* cache_lookup(
    const keel_catchup_manager_t* m,
    size_t server_index,
    const keel_consistency_token_t* token,
    uint64_t now_ns)
{
    for (size_t i = 0; i < KEEL_CATCHUP_CACHE_SLOTS; i++) {
        const cache_entry_t* e = &m->cache[i];
        if (e->server_index != server_index) continue;
        if (e->expires_ns <= now_ns) continue;
        if (!tokens_equal(&e->token, token)) continue;
        return e;
    }
    return NULL;
}

/* Reserved for Patches 2b/2c. Marked static-but-unused so the scaffolding
 * compiles clean today; the probe callback will start populating the cache. */
static void cache_insert(keel_catchup_manager_t* m,
                         size_t server_index,
                         const keel_consistency_token_t* token,
                         bool reached,
                         uint64_t now_ns)
{
    /* Try to overwrite a stale or matching slot first. */
    cache_entry_t* victim = NULL;
    for (size_t i = 0; i < KEEL_CATCHUP_CACHE_SLOTS; i++) {
        cache_entry_t* e = &m->cache[i];
        if (e->server_index == SIZE_MAX ||
            e->expires_ns <= now_ns ||
            (e->server_index == server_index && tokens_equal(&e->token, token))) {
            victim = e;
            break;
        }
    }
    if (!victim) {
        /* All slots fresh and distinct — round-robin replace. */
        victim = &m->cache[m->cache_next_evict];
        m->cache_next_evict = (m->cache_next_evict + 1) % KEEL_CATCHUP_CACHE_SLOTS;
    }
    victim->server_index = server_index;
    victim->token = *token;
    victim->reached = reached;
    victim->expires_ns = now_ns + (uint64_t)m->cfg.cache_ttl_ms * 1000000ULL;
}

/* ============================================================================
 * Helpers exported to per-protocol probe state machines (internal header)
 * ============================================================================ */

void keel_catchup_cache_put(struct keel_catchup_manager* m,
                            size_t server_index,
                            const keel_consistency_token_t* token,
                            bool reached,
                            uint64_t now_ns)
{
    if (!m || !token || server_index >= KEEL_MAX_SERVERS) return;
    cache_insert(m, server_index, token, reached, now_ns);
}

struct keel_catchup_waiter* keel_catchup_first_waiter_for(
    struct keel_catchup_manager* m, size_t server_index)
{
    if (!m) return NULL;
    for (keel_catchup_waiter_t* w = m->head; w; w = w->next) {
        if (w->server_index == server_index) return w;
    }
    return NULL;
}

bool keel_catchup_pick_probe_token(
    struct keel_catchup_manager* m,
    size_t server_index,
    int (*compare)(const keel_consistency_token_t*,
                   const keel_consistency_token_t*),
    keel_consistency_token_t* out_token)
{
    if (!m || !compare || !out_token) return false;
    const keel_consistency_token_t* best = NULL;
    for (keel_catchup_waiter_t* w = m->head; w; w = w->next) {
        if (w->server_index != server_index) continue;
        if (!best || compare(&w->token, best) > 0) {
            best = &w->token;
        }
    }
    if (!best) return false;
    *out_token = *best;
    return true;
}

size_t keel_catchup_release_satisfied(
    struct keel_catchup_manager* m,
    size_t server_index,
    const keel_consistency_token_t* reached_token,
    uint64_t now_ns,
    bool (*is_satisfied_by)(const keel_consistency_token_t*,
                            const keel_consistency_token_t*))
{
    (void)now_ns;
    if (!m || !reached_token || !is_satisfied_by) return 0;
    size_t released = 0;
    keel_catchup_waiter_t* w = m->head;
    while (w) {
        keel_catchup_waiter_t* next = w->next;
        if (w->server_index == server_index &&
            is_satisfied_by(&w->token, reached_token)) {
            release_waiter(m, w, KEEL_CATCHUP_REACHED);
            released++;
        }
        w = next;
    }
    return released;
}

void keel_catchup_apply_backoff(struct keel_catchup_manager* m,
                                size_t server_index,
                                uint64_t now_ns)
{
    if (!m || server_index >= KEEL_MAX_SERVERS) return;
    keel_catchup_probe_socket_t* s = &m->sockets[server_index];
    uint32_t next_ms = s->backoff_current_ms
        ? s->backoff_current_ms * 2u
        : (m->cfg.probe_backoff_initial_ms ? m->cfg.probe_backoff_initial_ms : 50u);
    uint32_t cap = m->cfg.probe_backoff_max_ms ? m->cfg.probe_backoff_max_ms : 30000u;
    if (next_ms > cap) next_ms = cap;
    s->backoff_current_ms = next_ms;
    s->backoff_until_ns   = now_ns + (uint64_t)next_ms * 1000000ULL;
    m->probes_failed_total++;
}

/* ============================================================================
 * Public API — lifecycle
 * ============================================================================ */

keel_catchup_manager_t* keel_catchup_manager_create(
    struct keel_worker* worker,
    const keel_catchup_config_t* config)
{
    keel_catchup_manager_t* m = keel_calloc(1, sizeof *m);
    if (!m) return NULL;

    m->worker = worker;
    m->cfg = config ? *config : (keel_catchup_config_t)KEEL_CATCHUP_CONFIG_DEFAULT;
    /* Defensive clamps: a zero tick interval would spin the timer wheel. */
    if (m->cfg.tick_interval_ms == 0) m->cfg.tick_interval_ms = 5;
    if (m->cfg.probe_timeout_ms == 0) m->cfg.probe_timeout_ms = 1000;

    for (size_t i = 0; i < KEEL_MAX_SERVERS; i++) {
        m->sockets[i].fd = -1;
    }
    for (size_t i = 0; i < KEEL_CATCHUP_CACHE_SLOTS; i++) {
        m->cache[i].server_index = SIZE_MAX;
    }
    return m;
}

void keel_catchup_manager_destroy(keel_catchup_manager_t* m)
{
    if (!m) return;

    /* Cancel every parked waiter so the engine can release session state. */
    while (m->head) {
        release_waiter(m, m->head, KEEL_CATCHUP_CANCELLED);
    }

    /* Tear down per-server PG probe state (closes fd, destroys synthetic
     * pool, frees probe ctx). Safe to call when nothing is allocated. */
    for (size_t i = 0; i < KEEL_MAX_SERVERS; i++) {
        keel_catchup_pg_close(m, i);
        if (m->sockets[i].probe_state && m->sockets[i].probe_state_free) {
            m->sockets[i].probe_state_free(m->sockets[i].probe_state);
            m->sockets[i].probe_state = NULL;
        }
        if (m->sockets[i].fd >= 0) {
            close(m->sockets[i].fd);  /* NOLINT(keel-syscall) */
            m->sockets[i].fd = -1;
        }
    }

    keel_free(m);
}

/* ============================================================================
 * Public API — wait-list operations
 * ============================================================================ */

keel_catchup_waiter_t* keel_catchup_enqueue(
    keel_catchup_manager_t* m,
    struct keel_session* session,
    size_t server_index,
    const keel_consistency_token_t* token,
    uint32_t max_wait_ms,
    keel_catchup_resume_cb resume,
    void* userdata)
{
    if (!m || !session || !token || !resume) return NULL;
    if (server_index >= KEEL_MAX_SERVERS) return NULL;
    if (m->cfg.max_waiters && m->waiters_active >= m->cfg.max_waiters) {
        return NULL;
    }

    uint64_t now = now_ns_or(0);

    /* Fast path: probe-result cache says this replica already reached the
     * token. Fire the resume callback inline without ever enqueueing.
     * This is the common case under a steady write load: K parallel
     * waiters for the same LSN cost one probe. */
    const cache_entry_t* hit = cache_lookup(m, server_index, token, now);
    if (hit) {
        m->cache_hits_total++;
        if (hit->reached) {
            /* No allocation, no list mutation — just call back. */
            resume(session, KEEL_CATCHUP_REACHED, userdata);
            return NULL;
        }
        /* Cache says not-yet-reached. Still need to park so the next
         * probe tick can resolve us; fall through. */
    }

    keel_catchup_waiter_t* w = keel_calloc(1, sizeof *w);
    if (!w) return NULL;

    w->session      = session;
    w->server_index = server_index;
    w->token        = *token;
    w->enqueued_ns  = now;
    w->deadline_ns  = now + (uint64_t)max_wait_ms * 1000000ULL;
    w->resume       = resume;
    w->userdata     = userdata;

    list_append(m, w);
    m->enqueued_total++;
    return w;
}

void keel_catchup_cancel(keel_catchup_manager_t* m,
                         keel_catchup_waiter_t* w)
{
    if (!m || !w || w->released) return;
    release_waiter(m, w, KEEL_CATCHUP_CANCELLED);
}

/* ============================================================================
 * Public API — tick
 * ============================================================================ */

void keel_catchup_manager_tick(keel_catchup_manager_t* m, uint64_t now_ns)
{
    if (!m) return;
    if (now_ns == 0) now_ns = now_ns_or(0);

    /* Expire deadline-reached waiters. We snapshot `next` before
     * release_waiter() unlinks `w` from the list. */
    keel_catchup_waiter_t* w = m->head;
    while (w) {
        keel_catchup_waiter_t* next = w->next;
        if (now_ns >= w->deadline_ns) {
            release_waiter(m, w, KEEL_CATCHUP_TIMEOUT);
        }
        w = next;
    }

    /* Decay backoff windows: a window in the past becomes "no backoff". */
    for (size_t i = 0; i < KEEL_MAX_SERVERS; i++) {
        if (m->sockets[i].backoff_until_ns &&
            m->sockets[i].backoff_until_ns <= now_ns) {
            m->sockets[i].backoff_until_ns = 0;
            m->sockets[i].backoff_current_ms = 0;
        }
    }

    /* Drive the per-server probe state machine for every server that has
     * at least one parked waiter. We iterate the wait list once, marking
     * which server indices have been visited so the same server is not
     * driven twice in one tick. Dispatch is keyed on the backend pool's
     * `protocol` field so a mixed-protocol worker drives PG and MySQL
     * pools through their respective state machines. */
    bool driven[KEEL_MAX_SERVERS] = {0};
    for (keel_catchup_waiter_t* w = m->head; w; w = w->next) {
        if (w->server_index >= KEEL_MAX_SERVERS) continue;
        if (driven[w->server_index]) continue;
        if (m->sockets[w->server_index].backoff_until_ns > now_ns) continue;
        driven[w->server_index] = true;

        const char* proto = NULL;
        if (m->worker && m->worker->server_pools &&
            w->server_index < m->worker->server_pool_count &&
            m->worker->server_pools[w->server_index])
        {
            proto = m->worker->server_pools[w->server_index]->config.protocol;
        }
        if (proto && strcmp(proto, "mysql") == 0) {
            keel_catchup_my_drive(m, w->server_index, now_ns);
        } else {
            /* Default to PostgreSQL when protocol is NULL or unknown:
             * the only existing test/production drivers are PG and
             * MySQL, and PG is the historical default. */
            keel_catchup_pg_drive(m, w->server_index, now_ns);
        }
    }
}

/* ============================================================================
 * Introspection
 * ============================================================================ */

void keel_catchup_manager_snapshot(const keel_catchup_manager_t* m,
                                   keel_catchup_stats_snapshot_t* out)
{
    if (!out) return;
    memset(out, 0, sizeof *out);
    if (!m) return;

    out->waiters_active           = m->waiters_active;
    out->waiters_high_water       = m->waiters_high_water;
    out->waiters_enqueued_total   = m->enqueued_total;
    out->waiters_fulfilled_total  = m->fulfilled_total;
    out->waiters_timeout_total    = m->timeout_total;
    out->waiters_cancelled_total  = m->cancelled_total;
    out->probes_issued_total      = m->probes_issued_total;
    out->probes_succeeded_total   = m->probes_succeeded_total;
    out->probes_failed_total      = m->probes_failed_total;
    out->cache_hits_total         = m->cache_hits_total;

    size_t open = 0;
    for (size_t i = 0; i < KEEL_MAX_SERVERS; i++) {
        if (m->sockets[i].fd >= 0) open++;
    }
    out->probe_sockets_open = open;
}
