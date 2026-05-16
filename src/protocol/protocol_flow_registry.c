/**
 * @file protocol_flow_registry.c
 * @brief Static registry for protocol-flow implementations.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The registry is intentionally tiny and startup-oriented. Protocol flows are not
 * expected to be hot-plugged at runtime, so a bounded array with read-mostly access
 * semantics keeps registration and lookup simple while still giving the engine a
 * protocol-neutral discovery point.
 */

#include "keel/protocol/protocol_flow.h"
#include <string.h>
#include <stdatomic.h>

/* ---------- storage ---------- */
#define MAX_FLOWS 8

typedef struct {
    const keel_proto_flow_vtable_t* vtable;
} flow_entry_t;

static flow_entry_t g_flows[MAX_FLOWS];
static atomic_int    g_flow_count = 0;

/* ---------- API ---------- */

/**
 * @brief Register one protocol-flow vtable.
 *
 * @param vtable Flow implementation descriptor.
 * @return 0 on success, or -1 for invalid input or capacity exhaustion.
 */
int keel_proto_flow_register(const keel_proto_flow_vtable_t* vtable)
{
    if (!vtable || !vtable->name) return -1;

    int n = atomic_load_explicit(&g_flow_count, memory_order_acquire);
    if (n >= MAX_FLOWS) return -1;

    /* Duplicate check */
    for (int i = 0; i < n; i++) {
        if (strcmp(g_flows[i].vtable->name, vtable->name) == 0) {
            return 0; /* already registered */
        }
    }

    g_flows[n].vtable = vtable;
    atomic_store_explicit(&g_flow_count, n + 1, memory_order_release);
    return 0;
}

/**
 * @brief Look up a protocol-flow vtable by name.
 *
 * @param name Protocol name, such as `postgres` or `mysql`.
 * @return Matching vtable, or `NULL` if no implementation is known.
 */
const keel_proto_flow_vtable_t* keel_proto_flow_get(const char* name)
{
    if (!name) return NULL;

    int n = atomic_load_explicit(&g_flow_count, memory_order_acquire);
    for (int i = 0; i < n; i++) {
        if (strcmp(g_flows[i].vtable->name, name) == 0) {
            return g_flows[i].vtable;
        }
    }

    /* Fallback: check built-in externs by name */
    if (strcmp(name, "postgres") == 0) return &keel_proto_flow_postgres;
    if (strcmp(name, "mysql") == 0)    return &keel_proto_flow_mysql;

    return NULL;
}
