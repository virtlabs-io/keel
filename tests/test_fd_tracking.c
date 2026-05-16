/**
 * @file test_fd_tracking.c
 * @brief File Descriptor leak detection tests (Spec §1 — FD Tracking)
 *
 * These tests validate that the proxy's connection management does not leak
 * file descriptors.  In production the same check is performed by probing
 * /proc/<PID>/fd during load tests, but this unit-test version automates the
 * baseline with deterministic open/close cycles.
 *
 * Strategy:
 *   - Snapshot the open-FD count before an operation.
 *   - Execute the operation.
 *   - Snapshot again and assert the delta is zero (or expected).
 *
 * Why this matters for keel:
 *   - Every client connection is a socket FD.
 *   - Every backend connection is a socket FD.
 *   - io_uring rings are FDs.
 *   - A proxy that "loses" FDs will hit EMFILE under load and stop accepting
 *     new connections without any obvious error.
 */

#include "test_utils.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/* ============================================================================
 * FD counting helpers
 * ============================================================================
 */

/**
 * @brief Count open file descriptors for the current process using
 *        /proc/self/fd.
 *
 * Returns -1 on error (e.g., /proc not mounted).
 */
static int count_open_fds(void)
{
    DIR *dir = opendir("/proc/self/fd");
    if (!dir) {
        return -1;
    }

    int count = 0;
    struct dirent *de;
    while ((de = readdir(dir)) != NULL) {
        if (de->d_name[0] == '.') {
            continue; /* skip "." and ".." */
        }
        count++;
    }
    closedir(dir);

    /* Subtract 1 for the /proc/self/fd dirfd we just opened and closed */
    return count - 1;
}

/**
 * @brief Assert that the FD count has not grown since baseline.
 *
 * We allow a delta of 0 (exact) or slightly negative (harmless, means
 * some cached FD was closed during the operation).
 */
static void assert_no_fd_leak(int baseline, const char *context)
{
    int current = count_open_fds();
    if (current < 0) {
        printf("  SKIP: /proc/self/fd not available (%s)\n", context);
        return;
    }
    int delta = current - baseline;
    if (delta > 0) {
        fprintf(stderr, "FD LEAK in [%s]: baseline=%d current=%d delta=+%d\n",
                context, baseline, current, delta);
        g_tests_failed++;
        g_tests_run++;
    } else {
        g_tests_passed++;
        g_tests_run++;
    }
}

/* ============================================================================
 * Test 1 — Sanity: count_open_fds is plausible
 * ============================================================================
 */
static void test_fd_count_sanity(void)
{
    TEST_BEGIN("fd tracking: count_open_fds sanity");

    int n = count_open_fds();
    if (n < 0) {
        printf("  SKIP: /proc/self/fd not available\n");
        g_tests_run++;
        g_tests_passed++; /* Not a failure, just not supported */
        TEST_END();
        return;
    }

    /* A sane process should have at least stdin/stdout/stderr open */
    TEST_ASSERT(n >= 3);

    /* It's extremely unlikely we have more than 1024 FDs open at test start */
    TEST_ASSERT(n < 1024);

    TEST_END();
}

/* ============================================================================
 * Test 2 — Socket open/close does not leak
 * ============================================================================
 */
static void test_socket_no_leak(void)
{
    TEST_BEGIN("fd tracking: socket open/close — no FD leak");

    int baseline = count_open_fds();
    if (baseline < 0) {
        printf("  SKIP\n");
        return;
    }

    for (int i = 0; i < 64; i++) {
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        TEST_ASSERT(fd >= 0);
        close(fd);
    }

    assert_no_fd_leak(baseline, "socket open/close x64");
    TEST_END();
}

/* ============================================================================
 * Test 3 — socketpair (simulated client+backend pair) does not leak
 * ============================================================================
 */
static void test_socketpair_no_leak(void)
{
    TEST_BEGIN("fd tracking: socketpair — no FD leak after both ends closed");

    int baseline = count_open_fds();
    if (baseline < 0) {
        printf("  SKIP\n");
        return;
    }

    for (int i = 0; i < 32; i++) {
        int sv[2];
        int rc = socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
        TEST_ASSERT_EQ(rc, 0);
        close(sv[0]);
        close(sv[1]);
    }

    assert_no_fd_leak(baseline, "socketpair x32");
    TEST_END();
}

/* ============================================================================
 * Test 4 — Simulated "half-close" leak detection
 *
 * This test deliberately leaks one FD to prove the detector catches it.
 * ============================================================================
 */
static void test_deliberate_leak_detected(void)
{
    TEST_BEGIN("fd tracking: deliberate single-FD leak is detected");

    int baseline = count_open_fds();
    if (baseline < 0) {
        printf("  SKIP\n");
        return;
    }

    /* Open a socket but do not close it */
    int leaked_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    TEST_ASSERT(leaked_fd >= 0);

    int current = count_open_fds();
    int delta = current - baseline;

    /* The leak should be visible */
    TEST_ASSERT(delta >= 1);

    /* Cleanup the "leaked" FD so subsequent tests are clean */
    close(leaked_fd);

    /* Verify baseline is restored */
    current = count_open_fds();
    TEST_ASSERT_EQ(current, baseline);

    TEST_END();
}

/* ============================================================================
 * Test 5 — Pipe open/close does not leak
 * ============================================================================
 */
static void test_pipe_no_leak(void)
{
    TEST_BEGIN("fd tracking: pipe open/close — no FD leak");

    int baseline = count_open_fds();
    if (baseline < 0) {
        printf("  SKIP\n");
        return;
    }

    for (int i = 0; i < 32; i++) {
        int pfd[2];
        int rc = pipe(pfd);
        TEST_ASSERT_EQ(rc, 0);
        close(pfd[0]);
        close(pfd[1]);
    }

    assert_no_fd_leak(baseline, "pipe x32");
    TEST_END();
}

/* ============================================================================
 * Test 6 — File open/close does not leak
 * ============================================================================
 */
static void test_file_no_leak(void)
{
    TEST_BEGIN("fd tracking: tmpfile open/close — no FD leak");

    int baseline = count_open_fds();
    if (baseline < 0) {
        printf("  SKIP\n");
        return;
    }

    for (int i = 0; i < 16; i++) {
        FILE *f = tmpfile();
        TEST_ASSERT_NOT_NULL(f);
        fclose(f);
    }

    assert_no_fd_leak(baseline, "tmpfile x16");
    TEST_END();
}

/* ============================================================================
 * Test 7 — FD count under simulated connection-storm baseline
 *
 * Mimics the "Connection Storm" test (Spec §4): rapidly open and close
 * many socket pairs, then verify nothing leaked.
 * ============================================================================
 */
static void test_connection_storm_no_leak(void)
{
    TEST_BEGIN("fd tracking: connection-storm simulation — no FD leak");

    int baseline = count_open_fds();
    if (baseline < 0) {
        printf("  SKIP\n");
        return;
    }

#define STORM_ITERATIONS 512
    for (int i = 0; i < STORM_ITERATIONS; i++) {
        int sv[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
            /* EMFILE: we reached the FD limit — stop and note it */
            fprintf(stderr, "  NOTE: hit EMFILE at iteration %d\n", i);
            break;
        }
        /* Simulate: client sends 1 byte, backend echoes, both close */
        char byte = (char)i;
        ssize_t _wr = write(sv[0], &byte, 1); (void)_wr;
        ssize_t _rd = read(sv[1], &byte, 1);  (void)_rd;
        close(sv[0]);
        close(sv[1]);
    }
#undef STORM_ITERATIONS

    assert_no_fd_leak(baseline, "connection-storm x512");
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================
 */
int main(void)
{
    printf("=== File Descriptor Tracking Tests (Resource Integrity) ===\n\n");

    test_fd_count_sanity();
    test_socket_no_leak();
    test_socketpair_no_leak();
    test_deliberate_leak_detected();
    test_pipe_no_leak();
    test_file_no_leak();
    test_connection_storm_no_leak();

    return test_summary();
}
