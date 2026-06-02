/*
 * diag_ps.h — env-gated diagnostic logging for the PS-virtualize path.
 *
 * Enabled at runtime when the environment variable KEEL_DIAG_PS is set to a
 * non-empty value other than "0".  The check is cached in a thread-local on
 * first use so the steady-state cost is a single atomic load + branch.
 *
 * This is intentionally a separate macro from KEEL_LOG_* so the diagnostic
 * sites can be enabled/disabled without touching the global log level.
 */
#ifndef KEEL_UTIL_DIAG_PS_H
#define KEEL_UTIL_DIAG_PS_H

#include <stdbool.h>
#include <stdlib.h>
#include "keel/log/log.h"

static inline bool keel_diag_ps_enabled(void) {
    static int cached = -1;
    if (__builtin_expect(cached == -1, 0)) {
        const char* e = getenv("KEEL_DIAG_PS");
        cached = (e && *e && !(e[0] == '0' && e[1] == '\0')) ? 1 : 0;
    }
    return cached == 1;
}

#define KEEL_DIAG_PS(...) \
    do { \
        if (keel_diag_ps_enabled()) { \
            KEEL_LOG_WARN(KEEL_LOG_CAT_PROTO, __VA_ARGS__); \
        } \
    } while (0)

#endif /* KEEL_UTIL_DIAG_PS_H */
