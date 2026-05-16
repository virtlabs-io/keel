/**
 * @file runtime_mode.h
 * @brief Public API for runtime-tier feature gating in the engine hot path.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * KEEL supports four runtime tiers that control which hot-path features
 * are active.  Lower tiers disable expensive features so that users who
 * only need basic pooling pay near-zero overhead for capabilities they
 * never use.
 *
 *   Tier       Config value   Purpose
 *   ─────────  ─────────────  ─────────────────────────────────
 *   PROXY      "proxy"        Minimal pass-through.  No pooling,
 *                              no parsing beyond framing.  Session
 *                              is hard-pinned to one backend.
 *
 *   POOL       "pool"         Connection pooling + PS replay.
 *                              No R/W routing, no hooks, no query
 *                              logging, no state sync.
 *
 *   SMART      "smart"        Pooling + intelligent R/W routing +
 *                              sticky-primary + state sync + query
 *                              logging.  No hooks, no XID probe,
 *                              no LSN capture.
 *
 *   FULL       "full"         Everything enabled: hooks, txn
 *                              tracking, LSN capture, full stats.
 *
 * Feature matrix:
 *
 *   Feature                     PROXY  POOL  SMART  FULL
 *   ────────────────────────    ─────  ────  ─────  ────
 *   Frame extraction + fwd      ✓      ✓     ✓      ✓
 *   Connection pooling          ✗      ✓     ✓      ✓
 *   PS replay (virtualize)      ✗      ✓     ✓      ✓
 *   R/W routing + sticky-pri    ✗      ✗     ✓      ✓
 *   Query logging + SQL anal    ✗      ✗     ✓      ✓
 *   State sync                  ✗      ✗     ✓      ✓
 *   Hook dispatch (4 points)    ✗      ✗     ✗      ✓
 *   Transaction tracking        ✗      ✗     ✗      ✓
 *   LSN / GTID capture          ✗      ✗     ✗      ✓
 *   Full statistics             ✗      basic ✓      ✓
 *
 * The tier is stored as a uint8_t on keel_session_flow_t for branch-free
 * comparison in the hot path (sf->tier >= KEEL_TIER_X).
 */

#ifndef KEEL_ENGINE_RUNTIME_MODE_H
#define KEEL_ENGINE_RUNTIME_MODE_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Runtime Tier Enum — ordered by ascending feature set
 * ============================================================================ */

typedef enum keel_tier {
    KEEL_TIER_PROXY = 0,   /**< Minimal pass-through, session-pinned */
    KEEL_TIER_POOL  = 1,   /**< Connection pooling + PS replay */
    KEEL_TIER_SMART = 2,   /**< + R/W routing, query log, state sync */
    KEEL_TIER_FULL  = 3,   /**< + hooks, txn tracking, LSN capture */

    KEEL_TIER_COUNT = 4,
} keel_tier_t;

/* ============================================================================
 * Hot-Path Feature Gate Macros
 *
 * Single integer comparison per check — compiles to a single cmp+jcc.
 * ============================================================================ */

/** True if connection pooling is active (tier >= POOL). */
#define KEEL_TIER_HAS_POOLING(t)     ((t) >= KEEL_TIER_POOL)

/** True if intelligent R/W routing is active (tier >= SMART). */
#define KEEL_TIER_HAS_ROUTING(t)     ((t) >= KEEL_TIER_SMART)

/** True if query logging + SQL analysis is active (tier >= SMART). */
#define KEEL_TIER_HAS_QUERY_LOG(t)   ((t) >= KEEL_TIER_SMART)

/** True if backend state sync is active (tier >= SMART). */
#define KEEL_TIER_HAS_STATE_SYNC(t)  ((t) >= KEEL_TIER_SMART)

/** True if hook dispatch is active (tier >= FULL). */
#define KEEL_TIER_HAS_HOOKS(t)       ((t) >= KEEL_TIER_FULL)

/** True if transaction tracking (XID probe) is active (tier >= FULL). */
#define KEEL_TIER_HAS_TXN_TRACK(t)   ((t) >= KEEL_TIER_FULL)

/** True if WAL LSN / GTID capture is active (tier >= FULL). */
#define KEEL_TIER_HAS_LSN_CAPTURE(t) ((t) >= KEEL_TIER_FULL)

/** True if full statistics collection is active (tier >= SMART).
 *  POOL tier collects only basic counters (queries_total, pool_borrows). */
#define KEEL_TIER_HAS_FULL_STATS(t)  ((t) >= KEEL_TIER_SMART)

/* ============================================================================
 * Parse / Name Helpers
 * ============================================================================ */

/**
 * @brief Parse a tier string from config ("proxy", "pool", "smart", "full").
 *
 * The parser uses a fast first-character dispatch instead of `strcasecmp()` to
 * keep startup parsing simple and dependency-free. Unknown strings fall back to
 * `FULL`, which is the safest operational default because it preserves all
 * features rather than silently disabling routing or consistency logic.
 *
 * @param str  NUL-terminated string (case-insensitive).  NULL → FULL.
 * @return The matching tier, or KEEL_TIER_FULL if unrecognised.
 */
static inline keel_tier_t keel_tier_parse(const char* str) {
    if (!str || !str[0]) return KEEL_TIER_FULL;
    /* Fast first-char dispatch */
    switch (str[0] | 0x20) {  /* lowercase */
    case 'p':
        if ((str[1] | 0x20) == 'r') return KEEL_TIER_PROXY;
        if ((str[1] | 0x20) == 'o') return KEEL_TIER_POOL;
        break;
    case 's': return KEEL_TIER_SMART;
    case 'f': return KEEL_TIER_FULL;
    }
    return KEEL_TIER_FULL;  /* unknown → safest default */
}

/**
 * @brief Return a stable string name for a runtime tier.
 *
 * @param t Tier enum value.
 * @return Static string literal naming the tier, or `"unknown"` for invalid values.
 */
static inline const char* keel_tier_name(keel_tier_t t) {
    switch (t) {
    case KEEL_TIER_PROXY: return "proxy";
    case KEEL_TIER_POOL:  return "pool";
    case KEEL_TIER_SMART: return "smart";
    case KEEL_TIER_FULL:  return "full";
    default:              return "unknown";
    }
}

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ENGINE_RUNTIME_MODE_H */
