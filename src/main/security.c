/**
 * @file security.c
 * @brief Process security hardening: privilege dropping and seccomp BPF sandboxing.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Extracted from src/main/main.c — no logic changes, only relocation.
 */

#include "keel/main/security.h"

#include "keel/log/log.h"
#include "keel/core/ini.h"

#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <pwd.h>
#include <grp.h>
#include <sys/prctl.h>
#include <stdint.h>

#ifdef __linux__
#include <linux/seccomp.h>
#include <linux/filter.h>
#include <linux/audit.h>
#include <sys/syscall.h>
#include <stddef.h>  /* offsetof */

#if defined(__x86_64__)
#define KEEL_AUDIT_ARCH_NATIVE AUDIT_ARCH_X86_64
#elif defined(__aarch64__)
#define KEEL_AUDIT_ARCH_NATIVE AUDIT_ARCH_AARCH64
#elif defined(__i386__)
#define KEEL_AUDIT_ARCH_NATIVE AUDIT_ARCH_I386
#else
#define KEEL_AUDIT_ARCH_NATIVE 0u
#endif
#endif /* __linux__ */

/* ============================================================================
 * Global Instance
 * ============================================================================ */

keel_security_config_t g_security_cfg = {
    .privilege_drop         = false,
    .run_user               = "nobody",
    .run_group              = "nogroup",
    .require_privilege_drop = false,
    .seccomp_mode           = KEEL_SECCOMP_BASELINE,
    .require_seccomp        = false,
    .no_new_privs           = true,
    .strict_auth            = false,
};

/* ============================================================================
 * Internal Helpers
 * ============================================================================ */

/**
 * @brief Decode the configured seccomp policy mode.
 *
 * Unrecognized values intentionally collapse to `OFF` rather than attempting a
 * guess, because applying the wrong sandbox is far worse than leaving the process
 * unsandboxed and logging the operator error elsewhere.
 *
 * @param s Raw configuration string.
 * @return Parsed seccomp mode.
 */
static keel_seccomp_mode_t parse_seccomp_mode(const char* s) {
    if (!s || !*s) return KEEL_SECCOMP_OFF;
    if (strcasecmp(s, "off") == 0 || strcmp(s, "0") == 0)
        return KEEL_SECCOMP_OFF;
    if (strcasecmp(s, "baseline") == 0)
        return KEEL_SECCOMP_BASELINE;
    if (strcasecmp(s, "strict") == 0)
        return KEEL_SECCOMP_STRICT;
    return KEEL_SECCOMP_OFF;
}

/**
 * @brief Parse a decimal UID or GID string.
 *
 * @param s Candidate decimal string.
 * @param[out] out Parsed numeric identifier on success.
 * @return `true` when parsing succeeded, otherwise `false`.
 */
static bool parse_numeric_id(const char* s, unsigned long* out) {
    if (!s || !*s) return false;
    char* end = NULL;
    errno = 0;
    unsigned long v = strtoul(s, &end, 10);
    if (errno != 0 || !end || *end != '\0') return false;
    *out = v;
    return true;
}

/**
 * @brief Resolve the configured runtime user and group into numeric IDs.
 *
 * @param[out] out_uid Resolved target UID.
 * @param[out] out_gid Resolved target GID.
 * @return 0 on success, or -1 if either account lookup fails.
 */
static int resolve_security_ids(uid_t* out_uid, gid_t* out_gid) {
    uid_t target_uid = (uid_t)-1;
    gid_t target_gid = (gid_t)-1;

    unsigned long idv = 0;
    if (parse_numeric_id(g_security_cfg.run_user, &idv)) {
        target_uid = (uid_t)idv;
    } else {
        struct passwd* pw = getpwnam(g_security_cfg.run_user);
        if (!pw) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                           "security: unknown run_user '%s'",
                           g_security_cfg.run_user ? g_security_cfg.run_user : "(null)");
            return -1;
        }
        target_uid = pw->pw_uid;
        if (!g_security_cfg.run_group || !*g_security_cfg.run_group) {
            target_gid = pw->pw_gid;
        }
    }

    if (target_gid == (gid_t)-1) {
        if (parse_numeric_id(g_security_cfg.run_group, &idv)) {
            target_gid = (gid_t)idv;
        } else {
            struct group* gr = getgrnam(g_security_cfg.run_group);
            if (!gr) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                               "security: unknown run_group '%s'",
                               g_security_cfg.run_group ? g_security_cfg.run_group : "(null)");
                return -1;
            }
            target_gid = gr->gr_gid;
        }
    }

    *out_uid = target_uid;
    *out_gid = target_gid;
    return 0;
}

/**
 * @brief Drop root privileges after privileged startup work is complete.
 *
 * @return 0 on success, 0 when privilege dropping was optional and not possible,
 *         or -1 when the configured policy requires a drop that cannot be safely
 *         completed.
 */
static int apply_privilege_drop(void) {
    if (!g_security_cfg.privilege_drop)
        return 0;

    if (geteuid() != 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                      "security: privilege_drop requested but process is not root (euid=%u)",
                      (unsigned)geteuid());
        return g_security_cfg.require_privilege_drop ? -1 : 0;
    }

    uid_t target_uid;
    gid_t target_gid;
    if (resolve_security_ids(&target_uid, &target_gid) < 0)
        return -1;

    if (setgroups(0, NULL) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "security: setgroups(0) failed: %s", strerror(errno));
        return -1;
    }

    if (setgid(target_gid) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "security: setgid(%u) failed: %s",
                       (unsigned)target_gid, strerror(errno));
        return -1;
    }

    if (setuid(target_uid) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "security: setuid(%u) failed: %s",
                       (unsigned)target_uid, strerror(errno));
        return -1;
    }

    if (setuid(0) == 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "security: privilege drop is reversible (unexpected)");
        return -1;
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                  "security: privileges dropped to uid=%u gid=%u",
                  (unsigned)geteuid(), (unsigned)getegid());
    return 0;
}

#ifdef __linux__
/**
 * @brief Install a baseline seccomp filter that blocks obviously dangerous syscalls.
 *
 * @return 0 on success, or -1 if installation fails.
 */
static int apply_seccomp_filter_baseline(void) {
    struct sock_filter filter[] = {
#if KEEL_AUDIT_ARCH_NATIVE != 0u
        BPF_STMT(BPF_LD  | BPF_W | BPF_ABS, (uint32_t)offsetof(struct seccomp_data, arch)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, KEEL_AUDIT_ARCH_NATIVE, 1, 0),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
#endif

        BPF_STMT(BPF_LD  | BPF_W | BPF_ABS, (uint32_t)offsetof(struct seccomp_data, nr)),

        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_execve, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#ifdef __NR_execveat
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_execveat, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#endif
#ifdef __NR_ptrace
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_ptrace, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#endif
#ifdef __NR_kexec_load
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_kexec_load, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#endif
#ifdef __NR_init_module
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_init_module, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#endif
#ifdef __NR_finit_module
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_finit_module, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#endif

        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
    };

    struct sock_fprog prog = {
        .len    = (unsigned short)(sizeof(filter) / sizeof(filter[0])),
        .filter = filter,
    };

    if (g_security_cfg.no_new_privs) {
        if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                           "security: PR_SET_NO_NEW_PRIVS failed: %s", strerror(errno));
            return -1;
        }
    }

    if (prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &prog) != 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "security: baseline seccomp install failed: %s", strerror(errno));
        return -1;
    }

    return 0;
}

/**
 * @brief Install a tighter allowlist seccomp policy tailored to KEEL's runtime syscall surface.
 *
 * @return 0 on success, or -1 if installation fails.
 */
static int apply_seccomp_filter_strict(void) {
    struct sock_filter filter[] = {
#if KEEL_AUDIT_ARCH_NATIVE != 0u
        BPF_STMT(BPF_LD  | BPF_W | BPF_ABS, (uint32_t)offsetof(struct seccomp_data, arch)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, KEEL_AUDIT_ARCH_NATIVE, 1, 0),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
#endif

        BPF_STMT(BPF_LD  | BPF_W | BPF_ABS, (uint32_t)offsetof(struct seccomp_data, nr)),

#define KEEL_SC_ALLOW(nr) \
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, nr, 0, 1), \
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW)

        KEEL_SC_ALLOW(__NR_read),
        KEEL_SC_ALLOW(__NR_write),
    #ifdef __NR_readv
        KEEL_SC_ALLOW(__NR_readv),
    #endif
    #ifdef __NR_writev
        KEEL_SC_ALLOW(__NR_writev),
    #endif
        KEEL_SC_ALLOW(__NR_close),
        KEEL_SC_ALLOW(__NR_recvfrom),
        KEEL_SC_ALLOW(__NR_sendto),
        KEEL_SC_ALLOW(__NR_recvmsg),
        KEEL_SC_ALLOW(__NR_sendmsg),
#ifdef __NR_recvmmsg
        KEEL_SC_ALLOW(__NR_recvmmsg),
#endif
#ifdef __NR_sendmmsg
        KEEL_SC_ALLOW(__NR_sendmmsg),
#endif
        KEEL_SC_ALLOW(__NR_socket),
    #ifdef __NR_socketpair
        KEEL_SC_ALLOW(__NR_socketpair),
    #endif
        KEEL_SC_ALLOW(__NR_bind),
        KEEL_SC_ALLOW(__NR_listen),
        KEEL_SC_ALLOW(__NR_accept),
#ifdef __NR_accept4
        KEEL_SC_ALLOW(__NR_accept4),
#endif
        KEEL_SC_ALLOW(__NR_connect),
        KEEL_SC_ALLOW(__NR_shutdown),
        KEEL_SC_ALLOW(__NR_getsockname),
        KEEL_SC_ALLOW(__NR_getpeername),
        KEEL_SC_ALLOW(__NR_setsockopt),
        KEEL_SC_ALLOW(__NR_getsockopt),

        KEEL_SC_ALLOW(__NR_epoll_create1),
        KEEL_SC_ALLOW(__NR_epoll_ctl),
    #ifdef __NR_epoll_wait
        KEEL_SC_ALLOW(__NR_epoll_wait),
    #endif
#ifdef __NR_epoll_pwait
        KEEL_SC_ALLOW(__NR_epoll_pwait),
#endif

#ifdef __NR_io_uring_setup
        KEEL_SC_ALLOW(__NR_io_uring_setup),
#endif
#ifdef __NR_io_uring_enter
        KEEL_SC_ALLOW(__NR_io_uring_enter),
#endif
#ifdef __NR_io_uring_register
        KEEL_SC_ALLOW(__NR_io_uring_register),
#endif

        KEEL_SC_ALLOW(__NR_futex),
        KEEL_SC_ALLOW(__NR_mmap),
        KEEL_SC_ALLOW(__NR_munmap),
#ifdef __NR_mremap
        KEEL_SC_ALLOW(__NR_mremap),
#endif
        KEEL_SC_ALLOW(__NR_mprotect),
#ifdef __NR_madvise
        KEEL_SC_ALLOW(__NR_madvise),
#endif
        KEEL_SC_ALLOW(__NR_brk),

        KEEL_SC_ALLOW(__NR_rt_sigaction),
        KEEL_SC_ALLOW(__NR_rt_sigprocmask),
        KEEL_SC_ALLOW(__NR_rt_sigreturn),
        KEEL_SC_ALLOW(__NR_sigaltstack),
#ifdef __NR_rt_sigtimedwait
        KEEL_SC_ALLOW(__NR_rt_sigtimedwait),
#endif
#ifdef __NR_rt_sigpending
        KEEL_SC_ALLOW(__NR_rt_sigpending),
#endif
        KEEL_SC_ALLOW(__NR_clock_gettime),
        KEEL_SC_ALLOW(__NR_nanosleep),
#ifdef __NR_clock_nanosleep
        KEEL_SC_ALLOW(__NR_clock_nanosleep),
#endif

        KEEL_SC_ALLOW(__NR_getpid),
        KEEL_SC_ALLOW(__NR_gettid),
#ifdef __NR_tgkill
        KEEL_SC_ALLOW(__NR_tgkill),
#endif
        KEEL_SC_ALLOW(__NR_exit),
        KEEL_SC_ALLOW(__NR_exit_group),

        KEEL_SC_ALLOW(__NR_openat),
        KEEL_SC_ALLOW(__NR_newfstatat),
        KEEL_SC_ALLOW(__NR_fstat),
    #ifdef __NR_readlink
        KEEL_SC_ALLOW(__NR_readlink),
    #endif
        KEEL_SC_ALLOW(__NR_lseek),
        KEEL_SC_ALLOW(__NR_fcntl),
        KEEL_SC_ALLOW(__NR_ioctl),

#ifdef __NR_dup
        KEEL_SC_ALLOW(__NR_dup),
#endif
    #ifdef __NR_dup2
        KEEL_SC_ALLOW(__NR_dup2),
    #endif
#ifdef __NR_dup3
        KEEL_SC_ALLOW(__NR_dup3),
#endif
        KEEL_SC_ALLOW(__NR_pipe2),
#ifdef __NR_eventfd2
        KEEL_SC_ALLOW(__NR_eventfd2),
#endif

#ifdef __NR_timerfd_create
        KEEL_SC_ALLOW(__NR_timerfd_create),
#endif
#ifdef __NR_timerfd_settime
        KEEL_SC_ALLOW(__NR_timerfd_settime),
#endif
#ifdef __NR_timerfd_gettime
        KEEL_SC_ALLOW(__NR_timerfd_gettime),
#endif

        KEEL_SC_ALLOW(__NR_set_tid_address),
        KEEL_SC_ALLOW(__NR_set_robust_list),
    #ifdef __NR_rseq
        KEEL_SC_ALLOW(__NR_rseq),
    #endif
        KEEL_SC_ALLOW(__NR_clone),
#ifdef __NR_clone3
        KEEL_SC_ALLOW(__NR_clone3),
#endif
        KEEL_SC_ALLOW(__NR_sched_yield),
#ifdef __NR_sched_getaffinity
        KEEL_SC_ALLOW(__NR_sched_getaffinity),
#endif
#ifdef __NR_sched_setaffinity
        KEEL_SC_ALLOW(__NR_sched_setaffinity),
#endif
        KEEL_SC_ALLOW(__NR_uname),
#ifdef __NR_getrandom
        KEEL_SC_ALLOW(__NR_getrandom),
#endif
        KEEL_SC_ALLOW(__NR_prlimit64),
        KEEL_SC_ALLOW(__NR_getrlimit),
        KEEL_SC_ALLOW(__NR_setrlimit),

        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#undef KEEL_SC_ALLOW
    };

    struct sock_fprog prog = {
        .len    = (unsigned short)(sizeof(filter) / sizeof(filter[0])),
        .filter = filter,
    };

    if (g_security_cfg.no_new_privs) {
        if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                           "security: PR_SET_NO_NEW_PRIVS failed: %s", strerror(errno));
            return -1;
        }
    }

    if (prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &prog) != 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "security: strict seccomp install failed: %s", strerror(errno));
        return -1;
    }

    return 0;
}
#endif /* __linux__ */

static int apply_seccomp_policy(void) {
    if (g_security_cfg.seccomp_mode == KEEL_SECCOMP_OFF)
        return 0;

#ifdef __linux__
    int rc = (g_security_cfg.seccomp_mode == KEEL_SECCOMP_STRICT)
             ? apply_seccomp_filter_strict()
             : apply_seccomp_filter_baseline();
    if (rc == 0) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                      "security: seccomp policy installed (%s)",
                      g_security_cfg.seccomp_mode == KEEL_SECCOMP_STRICT ? "strict" : "baseline");
    }
    return rc;
#else
    KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                  "security: seccomp requested but unsupported on this platform");
    return g_security_cfg.require_seccomp ? -1 : 0;
#endif
}

/* ============================================================================
 * Public API
 * ============================================================================ */

void security_config_from_ini(const keel_config_t* cfg)
{
    if (!cfg || !keel_config_has_section(cfg, "security"))
        return;

    const char *v;

    g_security_cfg.privilege_drop = keel_config_get_bool(
        cfg, "security", "privilege_drop", g_security_cfg.privilege_drop);
    g_security_cfg.require_privilege_drop = keel_config_get_bool(
        cfg, "security", "require_privilege_drop", g_security_cfg.require_privilege_drop);

    v = keel_config_get_string(cfg, "security", "run_user", NULL);
    if (v) g_security_cfg.run_user = v;

    v = keel_config_get_string(cfg, "security", "run_group", NULL);
    if (v) g_security_cfg.run_group = v;

    v = keel_config_get_string(cfg, "security", "seccomp", NULL);
    if (v) g_security_cfg.seccomp_mode = parse_seccomp_mode(v);

    g_security_cfg.require_seccomp = keel_config_get_bool(
        cfg, "security", "require_seccomp", g_security_cfg.require_seccomp);
    g_security_cfg.no_new_privs = keel_config_get_bool(
        cfg, "security", "no_new_privs", g_security_cfg.no_new_privs);
}

int apply_runtime_security_policy(void) {
    if (apply_privilege_drop() < 0)
        return -1;
    if (apply_seccomp_policy() < 0)
        return -1;
    return 0;
}
