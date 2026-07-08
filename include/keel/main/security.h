/**
 * @file security.h
 * @brief Process security hardening: privilege dropping and seccomp BPF sandboxing.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <stdbool.h>
#include "keel/core/ini.h"

/* ============================================================================
 * Security Configuration
 * ============================================================================ */

typedef enum keel_seccomp_mode {
    KEEL_SECCOMP_OFF = 0,
    KEEL_SECCOMP_BASELINE,
    KEEL_SECCOMP_STRICT,
} keel_seccomp_mode_t;

typedef struct keel_security_config {
    bool                privilege_drop;
    const char*         run_user;
    const char*         run_group;
    bool                require_privilege_drop;

    keel_seccomp_mode_t seccomp_mode;
    bool                require_seccomp;
    bool                no_new_privs;

    bool                strict_auth;  /**< Reject deprecated auth methods (md5, trust) at startup */
} keel_security_config_t;

/** Global security configuration instance. Definition lives in security.c. */
extern keel_security_config_t g_security_cfg;

/**
 * @brief Populate g_security_cfg from a [security] INI/YAML section.
 *
 * No-op when @p cfg is NULL or has no [security] section.
 *
 * @param cfg Loaded configuration tree.
 */
void security_config_from_ini(const keel_config_t* cfg);

/**
 * @brief Apply privilege dropping and seccomp policy in the required order.
 *
 * Must be called after bind() but before worker threads start.
 *
 * @return 0 on success, -1 on mandatory hardening failure.
 */
int apply_runtime_security_policy(void);
