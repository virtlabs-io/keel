/**
 * @file migration.c
 * @brief Inter-worker session migration helpers.
 *
 * Migration lets KEEL move an idle frontend session from one worker to another
 * without forcing the client to reconnect. The implementation pairs two data
 * paths:
 *
 * - a Unix-domain socket carrying the live client file descriptor via
 *   `SCM_RIGHTS`;
 * - an SPSC ring buffer carrying the serialized session metadata the
 *   destination worker needs to reconstruct the session shell.
 *
 * That split keeps FD ownership transfer in the kernel while letting the
 * higher-level state move through a cheap lock-free queue.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/engine/migration.h"
#include "keel/engine/worker.h"
#include "keel/engine/engine.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"
#include "keel/core/stats.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>

#ifdef __linux__
#include <sys/eventfd.h>
#endif

/* ============================================================================
 * Internal helpers — FD passing via SCM_RIGHTS
 * ============================================================================ */

/**
 * @brief Send a file descriptor to the destination worker's migration socket.
 *
 * Uses sendmsg(2) with a SCM_RIGHTS ancillary datagram.  The data payload
 * is a single byte so the message is never empty (required by POSIX for
 * SCM_RIGHTS to be delivered reliably on Linux and macOS).
 *
 * @return 0 on success, -1 on error.
 */
static int migration_send_fd(int dst_sock, int fd)
{
    char dummy = 0;
    struct iovec iov = { .iov_base = &dummy, .iov_len = 1 };

    union {
        struct cmsghdr cmsg;
        char           buf[CMSG_SPACE(sizeof(int))];
    } ctrl;

    memset(&ctrl, 0, sizeof(ctrl));
    ctrl.cmsg.cmsg_level = SOL_SOCKET;
    ctrl.cmsg.cmsg_type  = SCM_RIGHTS;
    ctrl.cmsg.cmsg_len   = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(&ctrl.cmsg), &fd, sizeof(int));

    struct msghdr msg = {
        .msg_iov        = &iov,
        .msg_iovlen     = 1,
        .msg_control    = ctrl.buf,
        .msg_controllen = sizeof(ctrl.buf),
    };

    ssize_t rc = sendmsg(dst_sock, &msg, MSG_NOSIGNAL | MSG_DONTWAIT);
    if (rc < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "migration: sendmsg FD failed: %s", strerror(errno));
        return -1;
    }
    return 0;
}

/**
 * @brief Receive a file descriptor from the migration socket (non-blocking).
 *
 * @param[out] fd_out  Received file descriptor.
 * @return 0 on success, -1 on error, -EAGAIN if no message ready.
 */
static int migration_recv_fd(int src_sock, int* fd_out)
{
    char dummy;
    struct iovec iov = { .iov_base = &dummy, .iov_len = 1 };

    union {
        struct cmsghdr cmsg;
        char           buf[CMSG_SPACE(sizeof(int))];
    } ctrl;

    memset(&ctrl, 0, sizeof(ctrl));

    struct msghdr msg = {
        .msg_iov        = &iov,
        .msg_iovlen     = 1,
        .msg_control    = ctrl.buf,
        .msg_controllen = sizeof(ctrl.buf),
    };

    ssize_t rc = recvmsg(src_sock, &msg, MSG_DONTWAIT);
    if (rc < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return -EAGAIN;
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "migration: recvmsg FD failed: %s", strerror(errno));
        return -1;
    }

    /* Extract the received FD from ancillary data */
    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    if (!cmsg ||
        cmsg->cmsg_level != SOL_SOCKET ||
        cmsg->cmsg_type  != SCM_RIGHTS) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "migration: recvmsg: no SCM_RIGHTS ancillary data");
        return -1;
    }

    memcpy(fd_out, CMSG_DATA(cmsg), sizeof(int));
    return 0;
}

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Initialize one worker's migration endpoints and inbox.
 *
 * @param mig Migration structure to initialize.
 * @param worker_id Owning worker identifier.
 * @param eventfd Eventfd used to wake the destination worker.
 * @return `0` on success, `-1` on failure.
 */
int keel_migration_init(keel_worker_migration_t* mig,
                        uint32_t worker_id,
                        int eventfd)
{
    memset(mig, 0, sizeof(*mig));
    mig->worker_id = worker_id;
    mig->eventfd   = eventfd;
    mig->sock_recv = -1;
    mig->sock_send = -1;

    /* Create Unix socketpair for FD passing */
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                   0, sv) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "migration_init(worker %u): socketpair failed: %s",
            worker_id, strerror(errno));
        return -1;
    }
    mig->sock_recv = sv[0];   /* owner reads here */
    mig->sock_send = sv[1];   /* senders write here */

    /* Create SPSC inbox ring buffer */
    mig->inbox = keel_spsc_ringbuf_create(KEEL_MIGRATION_INBOX_CAPACITY,
                                          sizeof(keel_migration_msg_t));
    if (!mig->inbox) {
        close(mig->sock_recv);
        close(mig->sock_send);
        mig->sock_recv = -1;
        mig->sock_send = -1;
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "migration_init(worker %u): ring buffer alloc failed", worker_id);
        return -1;
    }

    return 0;
}

void keel_migration_destroy(keel_worker_migration_t* mig)
{
    if (!mig) return;

    if (mig->sock_recv >= 0) { close(mig->sock_recv); mig->sock_recv = -1; }
    if (mig->sock_send >= 0) { close(mig->sock_send); mig->sock_send = -1; }

    if (mig->inbox) {
        keel_spsc_ringbuf_destroy(mig->inbox);
        mig->inbox = NULL;
    }
}

/* ============================================================================
 * Eligibility check
 * ============================================================================ */

/**
 * @brief Check whether a session is in a safe state for worker migration.
 *
 * Migration is intentionally restricted to sessions that are fully idle, not
 * backend-pinned, free of residual input, and not carrying protocol/plugin
 * state that the destination cannot yet serialize transparently.
 *
 * @param session Session to inspect.
 * @return true when migration is considered safe.
 */
bool keel_migration_can_migrate(const keel_session_t* session)
{
    if (!session) return false;

    /* Must be idle — between queries, not inside a transaction */
    if (session->state != KEEL_SESSION_READY)    return false;
    if (session->in_transaction)                  return false;

    /* Must not be pinned to a backend in any way */
    if (session->pin_reason != 0)                 return false;
    if (session->hard_pinned)                     return false;

    /* Backend connection must already be returned to the pool */
    if (session->backend_conn)                    return false;

    /* Must not have pending partial data on the FE side */
    if (!keel_residual_empty(&session->client_residual)) return false;

    /* Client FD must be valid */
    if (session->client_fd < 0)                   return false;

    /* plugin_state must be NULL — we only migrate sessions that have
     * fully returned to the "clean" state after protocol init.
     * Sessions with active plugin state (prepared statements etc.) are
     * unsafe to migrate until a proper serialisation layer exists. */
    if (session->plugin_state)                    return false;

    return true;
}

/* ============================================================================
 * Send-Side
 * ============================================================================ */

/**
 * @brief Transfer one eligible session to another worker.
 *
 * The FD is sent first and the metadata message second so the destination can
 * safely assume that processing a queued migration message will eventually let
 * it `recvmsg()` the matching client FD.
 *
 * @param session Session to migrate.
 * @param dst Destination worker migration channel.
 * @return `0` on success, `-1` when migration could not be queued.
 */
int keel_migration_send(keel_session_t*          session,
                        keel_worker_migration_t* dst)
{
    if (!session || !dst || !dst->inbox) return -1;

    /* Refuse if destination inbox is full — caller should keep session */
    if (keel_spsc_ringbuf_is_full(dst->inbox)) {
        dst->rejected++;
        KEEL_LOG_WARN(KEEL_LOG_CAT_CONN,
            "migration: dst worker %u inbox full — rejecting session %lu",
            dst->worker_id, (unsigned long)session->id);
        return -1;
    }

    /* Serialise session state into the migration message */
    keel_migration_msg_t msg;
    memset(&msg, 0, sizeof(msg));

    msg.session_id     = session->id;
    msg.state          = KEEL_SESSION_READY;
    msg.flags          = session->flags;
    msg.created_at     = session->created_at;
    msg.last_activity  = session->last_activity;
    msg.query_count    = session->query_count;
    msg.state_hash     = session->state_hash;
    msg.src_worker_id  = session->worker ? session->worker->id : UINT32_MAX;

    /* Copy identity strings */
    strncpy(msg.username,        session->username,        sizeof(msg.username)        - 1);
    strncpy(msg.database,        session->database,        sizeof(msg.database)        - 1);
    strncpy(msg.client_password, session->client_password, sizeof(msg.client_password) - 1);

    /* Carry any inline residual data (must be empty at this point, but
     * include as a safety net) */
    size_t rlen = keel_residual_len(&session->client_residual);
    if (rlen > KEEL_MIGRATION_MAX_RESIDUAL) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "migration: session %lu residual too large (%zu bytes) — aborting",
            (unsigned long)session->id, rlen);
        return -1;
    }
    if (rlen > 0) {
        msg.residual_len = (size_t)keel_residual_linearize(
            &session->client_residual, msg.residual, KEEL_MIGRATION_MAX_RESIDUAL);
    }

    /* --- Critical section — no lock needed because:
     *   (a) Only one worker ever calls keel_migration_send() at a time
     *       for a given destination (workers don't share client FDs).
     *   (b) The ring buffer is SPSC and caller serialises pushes.
     *
     * Step 1: Transfer FD via SCM_RIGHTS.  The FD copy arrives in the
     *         destination process' FD table immediately; the destination
     *         worker can call recvmsg() any time after this point. */
    if (migration_send_fd(dst->sock_send, session->client_fd) < 0) {
        return -1;
    }

    /* Step 2: Push message into destination's ring buffer.
     *         This must happen AFTER the FD transfer so the consumer
     *         can safely call recvmsg() when it processes the message. */
    if (!keel_spsc_ringbuf_try_push(dst->inbox, &msg)) {
        /* Ring buffer push failed — the FD was already sent via SCM_RIGHTS,
         * so we need to drain it from the destination's socket to avoid
         * leaking it.  A MSG_DONTWAIT recvmsg will consume the datagram and
         * close the received copy automatically (CMSG_DATA fd gets closed
         * by the kernel when the socket drains). */
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "migration: ring push failed after FD transfer — session %lu",
            (unsigned long)session->id);
        /* Best-effort drain of the socket */
        int leaked_fd = -1;
        (void)migration_recv_fd(dst->sock_recv, &leaked_fd);
        if (leaked_fd >= 0) close(leaked_fd);
        dst->rejected++;
        return -1;
    }

    /* Step 3: Wake the destination worker via its eventfd.
     * 8-byte eventfd add never blocks (kernel always accepts it). */
#ifdef __linux__
    if (dst->eventfd >= 0) {
        uint64_t val = 1;
        ssize_t r = write(dst->eventfd, &val, sizeof(val)); /* NOLINT(keel-blocking) */
        (void)r;  /* Best-effort; worker will drain on next reactor_wait() */
    }
#endif

    dst->sent++;
    return 0;
}

/* ============================================================================
 * Receive-Side
 * ============================================================================ */

/**
 * @brief Forward declaration: worker.c exposes on_accept_complete logic
 *        via keel_worker_on_accept().
 */
void keel_worker_on_accept(keel_worker_t* worker, int client_fd);

/**
 * @brief Drain all queued inbound migrations for one worker loop iteration.
 *
 * @param mig Migration channel to drain.
 * @param worker Destination worker.
 * @return Number of sessions successfully adopted, or `-1` on invalid input.
 */
int keel_migration_drain(keel_worker_migration_t* mig,
                         struct keel_worker*      worker)
{
    if (!mig || !worker) return -1;

    int adopted = 0;

    /* Process all pending migration messages */
    keel_migration_msg_t msg;
    while (keel_spsc_ringbuf_try_pop(mig->inbox, &msg)) {
        /* Receive the corresponding FD from the socketpair */
        int client_fd = -1;
        int rc = migration_recv_fd(mig->sock_recv, &client_fd);
        if (rc == -EAGAIN) {
            /* Ring message arrived before the SCM_RIGHTS datagram — retry next
             * iteration.  This is very unlikely with a sane kernel but handle
             * it defensively by re-inserting the message.  Since we've already
             * popped we can't push back to the SPSC; just log and abandon. */
            KEEL_LOG_WARN(KEEL_LOG_CAT_CONN,
                "migration: FD not yet available for session %lu — abandoning",
                (unsigned long)msg.session_id);
            mig->rejected++;
            continue;
        }
        if (rc < 0 || client_fd < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                "migration: failed to receive FD for session %lu",
                (unsigned long)msg.session_id);
            mig->rejected++;
            continue;
        }

        /* Hand the FD to the standard on_accept path.
         *
         * keel_worker_on_accept() allocates a session slot, initialises the
         * flow, and arms a reactor recv — exactly what happens for a freshly
         * accepted connection.  The session will re-authenticate or
         * (preferably) be pre-authenticated depending on the protocol plugin.
         *
         * For a fully transparent migration we would need to restore the
         * serialised session state BEFORE arming the recv.  That is done
         * below: we use the standard accept path to create the boilerplate,
         * then overwrite identity fields from the migration message. */
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_CONN,
            "W%u: migration: accepting FD %d for session %lu from worker %u",
            worker->id, client_fd,
            (unsigned long)msg.session_id, msg.src_worker_id);

        keel_worker_on_accept(worker, client_fd);

        /* Update session metadata on the newly created slot.
         *
         * on_accept creates the session with a fresh ID; we preserve the
         * original session_id, credentials, and timing from the migration
         * message so stats and authentication remain coherent.
         *
         * We find the session by matching client_fd (just assigned by
         * on_accept, before any recv completes). */
        for (size_t i = 0; i < worker->sessions.capacity; i++) {
            keel_session_t* s = &worker->sessions.sessions[i];
            if (s->client_fd == client_fd &&
                s->state == KEEL_SESSION_INIT) {
                /* Restore identity and timing */
                s->id           = msg.session_id;
                s->flags        = msg.flags;
                s->created_at   = msg.created_at;
                s->last_activity = msg.last_activity;
                s->query_count  = msg.query_count;
                s->state_hash   = msg.state_hash;

                strncpy(s->username,        msg.username,
                        sizeof(s->username)        - 1);
                strncpy(s->database,        msg.database,
                        sizeof(s->database)        - 1);
                strncpy(s->client_password, msg.client_password,
                        sizeof(s->client_password) - 1);

                /* Restore residual if any */
                if (msg.residual_len > 0) {
                    keel_residual_append(&s->client_residual,
                                        msg.residual, msg.residual_len);
                }
                break;
            }
        }

        mig->received++;
        if (worker->stats_ctx)
            KEEL_STAT_INC(worker->stats_ctx, migrations_received);
        adopted++;
    }

    return adopted;
}

/* ============================================================================
 * Load Balancing Hint
 * ============================================================================ */

/**
 * @brief Pick the least-loaded running worker as a migration target.
 *
 * The current heuristic is intentionally simple: choose the running worker with
 * the fewest allocated sessions. That is cheap to compute and good enough for a
 * first-pass balancing hint.
 *
 * @param engine Engine owning the worker pool.
 * @param src_id Current worker identifier, which is excluded from selection.
 * @return Target worker id, or `UINT32_MAX` when no target is suitable.
 */
uint32_t keel_migration_find_target(struct keel_engine* engine,
                                    uint32_t src_id)
{
    if (!engine) return UINT32_MAX;

    uint32_t num_workers = keel_engine_get_num_workers(engine);
    if (num_workers <= 1) return UINT32_MAX;

    uint32_t best_idx   = UINT32_MAX;
    size_t   best_count = SIZE_MAX;

    for (uint32_t i = 0; i < num_workers; i++) {
        if (i == src_id) continue;

        const keel_worker_t* w = keel_engine_get_worker(engine, i);
        if (!w || w->state != KEEL_WORKER_RUNNING) continue;

        size_t active = w->sessions.allocated;
        if (active < best_count) {
            best_count = active;
            best_idx   = i;
        }
    }

    return best_idx;
}
