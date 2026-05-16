/**
 * @file test_migration.c
 * @brief White-box tests for inter-worker session migration primitives.
 *
 * Migration is one of the more delicate worker-runtime features because it must
 * move three things together without tearing the session apart: metadata in the
 * inbox ring, the client socket via `SCM_RIGHTS`, and the scheduler's view of
 * which worker now owns the session. These tests isolate those pieces so each
 * contract can be verified without spinning up a full worker pool.
 *
 * The suite therefore focuses on transport-level guarantees and eligibility
 * rules rather than end-to-end query forwarding. If these primitives fail, the
 * larger integration tests would be much harder to diagnose.
 */

#include "test_utils.h"

#include "keel/engine/migration.h"
#include "keel/engine/backend_pool.h"
#include "keel/session/session.h"
#include "keel/mem/ringbuf.h"

#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <errno.h>

#ifdef __linux__
#include <sys/eventfd.h>
#endif

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Create an event notification fd compatible with migration tests.
 * @return Valid fd or `-1` on failure.
 *
 * Linux builds use `eventfd()` because the production subsystem does. Other
 * platforms fall back to a socketpair-derived fd so the tests can still verify
 * lifecycle and bookkeeping behavior without depending on a Linux-only API.
 */
static int make_eventfd(void)
{
#ifdef __linux__
    return eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
#else
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sv) < 0)
        return -1;
    close(sv[1]);
    return sv[0];
#endif
}

/**
 * @brief Populate a session fixture with the exact properties required for a
 *        legal migration candidate.
 * @param s [out] Session to initialize.
 * @param client_fd Connected client fd to transfer.
 * @return
 *
 * The initialized fields are intentionally explicit because migration safety is
 * defined by a conjunction of negatives: not in a transaction, not pinned, no
 * backend ownership, no plugin-local state, and a valid client socket.
 */
static void make_ready_session(keel_session_t* s, int client_fd)
{
    memset(s, 0, sizeof(*s));
    s->state          = KEEL_SESSION_READY;
    s->in_transaction = false;
    s->pin_reason     = 0;
    s->hard_pinned    = false;
    s->backend_conn   = NULL;
    s->plugin_state   = NULL;
    s->client_fd      = client_fd;
    keel_residual_init(&s->client_residual);
    keel_residual_init(&s->server_residual);
    strncpy(s->username,        "alice",    sizeof(s->username)        - 1);
    strncpy(s->database,        "testdb",   sizeof(s->database)        - 1);
    strncpy(s->client_password, "s3cr3t",   sizeof(s->client_password) - 1);
    s->id           = 42;
    s->query_count  = 7;
    s->created_at   = 1000;
    s->last_activity = 2000;
    s->state_hash   = 0xDEADBEEF;
    s->flags        = KEEL_SESSION_FLAG_AUTHENTICATED;
}

/* ============================================================================
 * Test: lifecycle
 * ============================================================================ */

static void test_migration_init_destroy(void)
{
    TEST_BEGIN("migration channel init/destroy");

    int efd = make_eventfd();
    TEST_ASSERT(efd >= 0);

    keel_worker_migration_t mig;
    int rc = keel_migration_init(&mig, /*worker_id=*/0, efd);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(mig.sock_recv >= 0);
    TEST_ASSERT(mig.sock_send >= 0);
    TEST_ASSERT_NOT_NULL(mig.inbox);
    TEST_ASSERT_EQ(mig.worker_id, 0u);
    TEST_ASSERT_EQ(mig.eventfd, efd);

    keel_migration_destroy(&mig);
    TEST_ASSERT_EQ(mig.sock_recv, -1);
    TEST_ASSERT_EQ(mig.sock_send, -1);
    TEST_ASSERT_NULL(mig.inbox);

    /* Double-destroy must be safe */
    keel_migration_destroy(&mig);

    close(efd);
    TEST_END();
}

/* ============================================================================
 * Test: eligibility checks
 * ============================================================================ */

static void test_migration_can_migrate(void)
{
    TEST_BEGIN("migration eligibility checks");

    /* Create a dummy socket pair so client_fd is a real fd */
    int sv[2];
    TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

    keel_session_t s;
    make_ready_session(&s, sv[0]);

    /* Base case: all conditions met */
    TEST_ASSERT(keel_migration_can_migrate(&s));

    /* NULL pointer */
    TEST_ASSERT(!keel_migration_can_migrate(NULL));

    /* Wrong state */
    s.state = KEEL_SESSION_QUERY;
    TEST_ASSERT(!keel_migration_can_migrate(&s));
    s.state = KEEL_SESSION_READY;

    /* In transaction */
    s.in_transaction = true;
    TEST_ASSERT(!keel_migration_can_migrate(&s));
    s.in_transaction = false;

    /* Pinned (by reason) */
    s.pin_reason = 1;
    TEST_ASSERT(!keel_migration_can_migrate(&s));
    s.pin_reason = 0;

    /* Hard-pinned */
    s.hard_pinned = true;
    TEST_ASSERT(!keel_migration_can_migrate(&s));
    s.hard_pinned = false;

    /* Has backend connection */
    backend_conn_t fake_be;
    s.backend_conn = &fake_be;
    TEST_ASSERT(!keel_migration_can_migrate(&s));
    s.backend_conn = NULL;

    /* Has plugin state */
    int dummy = 1;
    s.plugin_state = &dummy;
    TEST_ASSERT(!keel_migration_can_migrate(&s));
    s.plugin_state = NULL;

    /* Invalid client fd */
    s.client_fd = -1;
    TEST_ASSERT(!keel_migration_can_migrate(&s));
    s.client_fd = sv[0];

    /* Confirm still OK */
    TEST_ASSERT(keel_migration_can_migrate(&s));

    close(sv[0]);
    close(sv[1]);
    TEST_END();
}

/* ============================================================================
 * Test: FD transfer + ring buffer (keel_migration_send / drain)
 * ============================================================================ */

static void test_migration_send_drain(void)
{
    TEST_BEGIN("migration send / message serialisation");

    /* Two migration channels: src -> dst */
    int efd_src = make_eventfd();
    int efd_dst = make_eventfd();
    TEST_ASSERT(efd_src >= 0);
    TEST_ASSERT(efd_dst >= 0);

    keel_worker_migration_t src_mig, dst_mig;
    TEST_ASSERT_EQ(keel_migration_init(&src_mig, 0, efd_src), 0);
    TEST_ASSERT_EQ(keel_migration_init(&dst_mig, 1, efd_dst), 0);

    /* Create a real fd pair to act as the client socket */
    int cli[2];
    TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, cli), 0);

    keel_session_t s;
    make_ready_session(&s, cli[0]);

    /* --- Send → dst --- */
    int rc = keel_migration_send(&s, &dst_mig);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ((int)dst_mig.sent, 1);

    /* Ring must have exactly one pending message */
    TEST_ASSERT(!keel_spsc_ringbuf_is_empty(dst_mig.inbox));

    /* Pop the message manually and verify fields */
    keel_migration_msg_t msg;
    bool popped = keel_spsc_ringbuf_try_pop(dst_mig.inbox, &msg);
    TEST_ASSERT(popped);
    TEST_ASSERT_EQ(msg.session_id, 42u);
    TEST_ASSERT_EQ(msg.query_count, 7u);
    TEST_ASSERT_EQ(msg.state_hash, (uint64_t)0xDEADBEEF);
    TEST_ASSERT_EQ(msg.flags, (uint32_t)KEEL_SESSION_FLAG_AUTHENTICATED);
    TEST_ASSERT_STR_EQ(msg.username, "alice");
    TEST_ASSERT_STR_EQ(msg.database, "testdb");
    TEST_ASSERT_STR_EQ(msg.client_password, "s3cr3t");
    TEST_ASSERT_EQ(msg.src_worker_id, (uint32_t)UINT32_MAX);  /* no worker set */
    TEST_ASSERT_EQ(msg.residual_len, 0u);

    /* The FD was transferred via SCM_RIGHTS — receive and close it */
    int recv_fd = -1;
    /* We need to peek at the socketpair for the ancillary datagram.
     * Re-use the internal recvmsg path by reading from dst_mig.sock_recv. */
    {
        char dummy;
        struct iovec iov = { .iov_base = &dummy, .iov_len = 1 };
        union {
            struct cmsghdr cmsg;
            char buf[CMSG_SPACE(sizeof(int))];
        } ctrl;
        memset(&ctrl, 0, sizeof(ctrl));
        struct msghdr m = {
            .msg_iov        = &iov,
            .msg_iovlen     = 1,
            .msg_control    = ctrl.buf,
            .msg_controllen = sizeof(ctrl.buf),
        };
        ssize_t n = recvmsg(dst_mig.sock_recv, &m, MSG_DONTWAIT);
        TEST_ASSERT(n >= 0);
        struct cmsghdr* cm = CMSG_FIRSTHDR(&m);
        TEST_ASSERT_NOT_NULL(cm);
        if (cm) {
            memcpy(&recv_fd, CMSG_DATA(cm), sizeof(int));
            TEST_ASSERT(recv_fd >= 0);
            close(recv_fd);
        }
    }

    /* Ring must be empty now */
    TEST_ASSERT(keel_spsc_ringbuf_is_empty(dst_mig.inbox));

    keel_migration_destroy(&src_mig);
    keel_migration_destroy(&dst_mig);
    close(efd_src);
    close(efd_dst);
    /* cli[0] was the migrated fd; cli[1] is the remote end */
    close(cli[1]);
    /* cli[0] was passed via SCM_RIGHTS and closed as recv_fd above;
     * close our original copy too (source side cleanup) */
    close(cli[0]);

    TEST_END();
}

/* ============================================================================
 * Test: inbox-full rejection
 * ============================================================================ */

static void test_migration_inbox_full(void)
{
    TEST_BEGIN("migration inbox-full rejection");

    int efd = make_eventfd();
    TEST_ASSERT(efd >= 0);

    keel_worker_migration_t dst_mig;
    TEST_ASSERT_EQ(keel_migration_init(&dst_mig, 0, efd), 0);

    /* Create KEEL_MIGRATION_INBOX_CAPACITY + 1 fake fds and fill the ring */
    int overflow_pair[2];
    TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, overflow_pair), 0);

    keel_session_t s;
    make_ready_session(&s, overflow_pair[0]);

    /* Force the ring to be full.
     * The SPSC ring with capacity N holds N-1 elements (one slot reserved
     * as the full sentinel: full when (head+1) & mask == tail). */
    keel_migration_msg_t dummy_msg;
    memset(&dummy_msg, 0, sizeof(dummy_msg));
    size_t max_items = KEEL_MIGRATION_INBOX_CAPACITY - 1;
    for (size_t i = 0; i < max_items; i++) {
        bool pushed = keel_spsc_ringbuf_try_push(dst_mig.inbox, &dummy_msg);
        TEST_ASSERT(pushed);
    }
    TEST_ASSERT(keel_spsc_ringbuf_is_full(dst_mig.inbox));

    /* Now try to migrate — should be rejected */
    int rc = keel_migration_send(&s, &dst_mig);
    TEST_ASSERT_EQ(rc, -1);
    TEST_ASSERT_EQ((int)dst_mig.rejected, 1);

    keel_migration_destroy(&dst_mig);
    close(efd);
    close(overflow_pair[0]);
    close(overflow_pair[1]);

    TEST_END();
}

/* ============================================================================
 * Test: residual data blocks migration
 * ============================================================================ */

static void test_migration_blocks_on_residual(void)
{
    TEST_BEGIN("migration blocked by residual data");

    int sv[2];
    TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

    keel_session_t s;
    make_ready_session(&s, sv[0]);

    /* Append some residual */
    const uint8_t residual_data[] = "SELECT 1";
    keel_residual_append(&s.client_residual, residual_data, sizeof(residual_data) - 1);
    TEST_ASSERT(!keel_residual_empty(&s.client_residual));

    /* Should not be migratable */
    TEST_ASSERT(!keel_migration_can_migrate(&s));

    close(sv[0]);
    close(sv[1]);
    TEST_END();
}

/* ============================================================================
 * Entry point
 * ============================================================================ */

int main(void)
{
    test_migration_init_destroy();
    test_migration_can_migrate();
    test_migration_send_drain();
    test_migration_inbox_full();
    test_migration_blocks_on_residual();

    return test_summary();
}
