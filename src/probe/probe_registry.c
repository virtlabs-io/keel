/**
 * @file probe_registry.c
 * @brief Static registry for probe implementations and small probe utilities.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The registry deliberately stays simple: a bounded static table filled during
 * startup. That keeps probe lookup deterministic and allocation-free during the
 * manager lifecycle. The cost is a small fixed maximum probe count and the rule
 * that all registrations must finish before probing begins.
 */

#include "keel/probe/probe.h"
#include "keel/log/log.h"

#include <string.h>
#include <stdio.h>

/* ============================================================================
 * Registry (module-level storage)
 * ============================================================================ */

/** Registry entry: maps a name string to a probe vtable. */
static struct {
    const char*             name;   /**< Probe type name (e.g. "postgres") */
    const keel_probe_ops_t*  ops;    /**< Vtable for create/check/destroy */
} g_registry[KEEL_MAX_PROBE_TYPES];

/** Number of registered probe types. */
static size_t g_registry_count = 0;

/**
 * @brief Register a probe plugin by name.
 *
 * Must be called before keel_probe_manager_create().  Thread-unsafe;
 * intended for single-threaded startup only.
 *
 * @param name  Unique probe type name (pointer must remain valid)
 * @param ops   Vtable with create/check/destroy callbacks
 * @return KEEL_OK, KEEL_ERR_INVALID_ARG, KEEL_ERR_NOMEM, or KEEL_ERR_ALREADY_EXISTS
 */
keel_error_t keel_probe_register(const char* name, const keel_probe_ops_t* ops)
{
    if (!name || !ops) return KEEL_ERR_INVALID_ARG;
    if (g_registry_count >= KEEL_MAX_PROBE_TYPES) return KEEL_ERR_NOMEM;

    /* Reject duplicates */
    for (size_t i = 0; i < g_registry_count; i++) {
        if (strcmp(g_registry[i].name, name) == 0)
            return KEEL_ERR_ALREADY_EXISTS;
    }

    g_registry[g_registry_count].name = name;
    g_registry[g_registry_count].ops  = ops;
    g_registry_count++;

    KEEL_LOG_DEBUG(KEEL_LOG_CAT_PROBE, "registered probe type '%s'", name);
    return KEEL_OK;
}

/**
 * @brief Look up a registered probe plugin by name.
 *
 * @param name  Probe type name to search for
 * @return Pointer to vtable, or NULL if not found
 */
const keel_probe_ops_t* keel_probe_lookup(const char* name)
{
    if (!name) return NULL;
    for (size_t i = 0; i < g_registry_count; i++) {
        if (strcmp(g_registry[i].name, name) == 0)
            return g_registry[i].ops;
    }
    return NULL;
}

/**
 * @brief Register all built-in probe types.
 *
 * Called once at startup before config parsing.  Registers:
 *   - "postgres" — PostgreSQL SQL probe (pg_is_in_recovery)
 *   - "patroni"  — Patroni REST API probe (/primary, /replica)
 */
void keel_probe_register_builtins(void)
{
    keel_probe_register("postgres", &keel_probe_postgres_ops);
    keel_probe_register("patroni",  &keel_probe_patroni_ops);
    keel_probe_register("mysql",    &keel_probe_mysql_ops);
    keel_probe_register("mariadb",  &keel_probe_mysql_ops);  /* alias */
}

/* ============================================================================
 * Utility
 * ============================================================================ */

/**
 * @brief Convert a health status enum to a human-readable string.
 *
 * @param s  Health status value
 * @return Static string: "unknown", "up", "down", "degraded", or "?"
 */
const char* keel_health_status_str(keel_health_status_t s)
{
    switch (s) {
    case KEEL_HEALTH_UNKNOWN:  return "unknown";
    case KEEL_HEALTH_UP:       return "up";
    case KEEL_HEALTH_DOWN:     return "down";
    case KEEL_HEALTH_DEGRADED: return "degraded";
    default:                  return "?";
    }
}
