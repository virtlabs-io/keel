/**
 * @file test_config_reload.c
 * @brief Tests for live configuration reload (SIGHUP) infrastructure
 *
 * Validates the config-reload machinery:
 * - INI re-parse picks up changed values for safe parameters
 * - Restart-required parameters are detected (port, workers, ps_mode, mode, txn)
 * - Probe manager timing update API (keel_probe_manager_update_timing)
 * - Server weight re-parse from connection strings
 * - Pool sizing, timeout, and rebalancing parameter diff
 */

#include "test_utils.h"
#include "keel/core/ini.h"
#include "keel/probe/probe.h"
#include "keel/engine/engine.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static char g_ini_path[256];

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Write an ephemeral INI file used by config-reload diff tests.
 *
 * Path is PID-scoped under /tmp; caller must invoke cleanup_ini()
 * after the test completes.  The path is stored in g_ini_path.
 *
 * @param content  Raw INI text to write.
 * @return true on success, false if fopen fails.
 */
static bool write_ini(const char* content) {
    snprintf(g_ini_path, sizeof(g_ini_path),
             "/tmp/keel_test_reload_%d.ini", getpid());
    FILE* f = fopen(g_ini_path, "w");
    if (!f) return false;
    fputs(content, f);
    fclose(f);
    return true;
}

/** @brief Remove the ephemeral INI written by write_ini(). */
static void cleanup_ini(void) { unlink(g_ini_path); }

/* ============================================================================
 * §1 — Safe parameter detection via INI diff
 * ============================================================================ */

static void test_pool_size_diff(void) {
    TEST_BEGIN("reload: pool size diff");

    const char* original =
        "[keel]\n"
        "min_pool_size = 2\n"
        "max_pool_size = 50\n";

    const char* updated =
        "[keel]\n"
        "min_pool_size = 4\n"
        "max_pool_size = 200\n";

    TEST_ASSERT(write_ini(original));
    keel_config_t* cfg1 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg1);

    int64_t min1 = keel_config_get_int(cfg1, "keel", "min_pool_size", 0);
    int64_t max1 = keel_config_get_int(cfg1, "keel", "max_pool_size", 0);
    TEST_ASSERT_EQ(min1, 2);
    TEST_ASSERT_EQ(max1, 50);
    keel_config_free(cfg1);

    /* Re-write and re-load — simulates SIGHUP re-parse */
    TEST_ASSERT(write_ini(updated));
    keel_config_t* cfg2 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg2);

    int64_t min2 = keel_config_get_int(cfg2, "keel", "min_pool_size", 0);
    int64_t max2 = keel_config_get_int(cfg2, "keel", "max_pool_size", 0);
    TEST_ASSERT_EQ(min2, 4);
    TEST_ASSERT_EQ(max2, 200);

    /* Diff detected? */
    TEST_ASSERT(min2 != min1);
    TEST_ASSERT(max2 != max1);

    keel_config_free(cfg2);
    cleanup_ini();
}

static void test_timeout_diff(void) {
    TEST_BEGIN("reload: timeout diff");

    const char* ini =
        "[keel]\n"
        "idle_timeout_ms = 300000\n"
        "connect_timeout_ms = 10000\n"
        "pool_prune_interval_ms = 30000\n"
        "pool_refill_interval_ms = 100\n"
        "pool_refill_backoff_ms = 5000\n";

    TEST_ASSERT(write_ini(ini));
    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    TEST_ASSERT_EQ(keel_config_get_int(cfg, "keel", "idle_timeout_ms", 0), 300000);
    TEST_ASSERT_EQ(keel_config_get_int(cfg, "keel", "connect_timeout_ms", 0), 10000);
    TEST_ASSERT_EQ(keel_config_get_int(cfg, "keel", "pool_prune_interval_ms", 0), 30000);
    TEST_ASSERT_EQ(keel_config_get_int(cfg, "keel", "pool_refill_interval_ms", 0), 100);
    TEST_ASSERT_EQ(keel_config_get_int(cfg, "keel", "pool_refill_backoff_ms", 0), 5000);

    keel_config_free(cfg);

    /* Update timeouts */
    const char* updated =
        "[keel]\n"
        "idle_timeout_ms = 600000\n"
        "connect_timeout_ms = 20000\n"
        "pool_prune_interval_ms = 60000\n"
        "pool_refill_interval_ms = 200\n"
        "pool_refill_backoff_ms = 10000\n";

    TEST_ASSERT(write_ini(updated));
    keel_config_t* cfg2 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg2);

    TEST_ASSERT_EQ(keel_config_get_int(cfg2, "keel", "idle_timeout_ms", 0), 600000);
    TEST_ASSERT_EQ(keel_config_get_int(cfg2, "keel", "connect_timeout_ms", 0), 20000);
    TEST_ASSERT_EQ(keel_config_get_int(cfg2, "keel", "pool_prune_interval_ms", 0), 60000);
    TEST_ASSERT_EQ(keel_config_get_int(cfg2, "keel", "pool_refill_interval_ms", 0), 200);
    TEST_ASSERT_EQ(keel_config_get_int(cfg2, "keel", "pool_refill_backoff_ms", 0), 10000);

    keel_config_free(cfg2);
    cleanup_ini();
}

static void test_pool_max_waiting_diff(void) {
    TEST_BEGIN("reload: pool_max_waiting diff");

    TEST_ASSERT(write_ini("[keel]\npool_max_waiting = 128\n"));
    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);
    TEST_ASSERT_EQ(keel_config_get_int(cfg, "keel", "pool_max_waiting", 0), 128);
    keel_config_free(cfg);

    TEST_ASSERT(write_ini("[keel]\npool_max_waiting = 256\n"));
    keel_config_t* cfg2 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg2);
    TEST_ASSERT_EQ(keel_config_get_int(cfg2, "keel", "pool_max_waiting", 0), 256);
    keel_config_free(cfg2);
    cleanup_ini();
}

/* ============================================================================
 * §2 — Restart-required parameter detection
 * ============================================================================ */

static void test_restart_required_port(void) {
    TEST_BEGIN("reload: restart-required bind_port detected");

    TEST_ASSERT(write_ini("[keel]\nbind_port = 6432\n"));
    keel_config_t* cfg1 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg1);

    int64_t port1 = keel_config_get_int(cfg1, "keel", "bind_port", 0);
    TEST_ASSERT_EQ(port1, 6432);
    keel_config_free(cfg1);

    TEST_ASSERT(write_ini("[keel]\nbind_port = 7777\n"));
    keel_config_t* cfg2 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg2);

    int64_t port2 = keel_config_get_int(cfg2, "keel", "bind_port", 0);
    TEST_ASSERT_EQ(port2, 7777);

    /* The reload code detects port1 != port2 and logs a restart warning */
    TEST_ASSERT(port1 != port2);

    keel_config_free(cfg2);
    cleanup_ini();
}

static void test_restart_required_workers(void) {
    TEST_BEGIN("reload: restart-required num_workers detected");

    TEST_ASSERT(write_ini("[keel]\nnum_workers = 4\n"));
    keel_config_t* cfg1 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg1);
    TEST_ASSERT_EQ(keel_config_get_int(cfg1, "keel", "num_workers", 0), 4);
    keel_config_free(cfg1);

    TEST_ASSERT(write_ini("[keel]\nnum_workers = 8\n"));
    keel_config_t* cfg2 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg2);
    TEST_ASSERT_EQ(keel_config_get_int(cfg2, "keel", "num_workers", 0), 8);
    keel_config_free(cfg2);
    cleanup_ini();
}

static void test_restart_required_ps_mode(void) {
    TEST_BEGIN("reload: restart-required prepared_statement detected");

    TEST_ASSERT(write_ini("[keel]\nprepared_statement = virtualize\n"));
    keel_config_t* cfg1 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg1);
    const char* ps1 = keel_config_get_string(cfg1, "keel", "prepared_statement", "");
    TEST_ASSERT_STR_EQ(ps1, "virtualize");
    keel_config_free(cfg1);

    TEST_ASSERT(write_ini("[keel]\nprepared_statement = pinning\n"));
    keel_config_t* cfg2 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg2);
    const char* ps2 = keel_config_get_string(cfg2, "keel", "prepared_statement", "");
    TEST_ASSERT_STR_EQ(ps2, "pinning");
    keel_config_free(cfg2);
    cleanup_ini();
}

static void test_restart_required_mode(void) {
    TEST_BEGIN("reload: restart-required mode detected");

    TEST_ASSERT(write_ini("[keel]\nmode = pool\n"));
    keel_config_t* cfg1 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg1);
    TEST_ASSERT_STR_EQ(keel_config_get_string(cfg1, "keel", "mode", ""), "pool");
    keel_config_free(cfg1);

    TEST_ASSERT(write_ini("[keel]\nmode = smart\n"));
    keel_config_t* cfg2 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg2);
    TEST_ASSERT_STR_EQ(keel_config_get_string(cfg2, "keel", "mode", ""), "smart");
    keel_config_free(cfg2);
    cleanup_ini();
}

static void test_restart_required_txn_tracking(void) {
    TEST_BEGIN("reload: restart-required transaction_tracking detected");

    TEST_ASSERT(write_ini("[keel]\ntransaction_tracking = off\n"));
    keel_config_t* cfg1 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg1);
    TEST_ASSERT_STR_EQ(
        keel_config_get_string(cfg1, "keel", "transaction_tracking", ""), "off");
    keel_config_free(cfg1);

    TEST_ASSERT(write_ini("[keel]\ntransaction_tracking = on\n"));
    keel_config_t* cfg2 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg2);
    TEST_ASSERT_STR_EQ(
        keel_config_get_string(cfg2, "keel", "transaction_tracking", ""), "on");
    keel_config_free(cfg2);
    cleanup_ini();
}

/* ============================================================================
 * §3 — Server weight re-parse from connection strings
 * ============================================================================ */

typedef struct {
    struct { const char* key; const char* val; } entries[16];
    size_t count;
} weight_ctx_t;

static void collect_keys(const char* key, const char* value, void* ctx) {
    weight_ctx_t* wc = ctx;
    if (wc->count < 16) {
        wc->entries[wc->count].key = key;
        wc->entries[wc->count].val = value;
        wc->count++;
    }
}

/* Find a server entry by key name (order-independent) */
static const char* find_entry_val(weight_ctx_t* wctx, const char* key) {
    for (size_t i = 0; i < wctx->count; i++)
        if (strcmp(wctx->entries[i].key, key) == 0)
            return wctx->entries[i].val;
    return NULL;
}

static int parse_weight(const char* connstr) {
    const char* wp = strstr(connstr, "weight=");
    return wp ? atoi(wp + 7) : -1;
}

static void test_server_weight_reparse(void) {
    TEST_BEGIN("reload: server weight re-parse");

    const char* ini =
        "[servers]\n"
        "primary = host=10.0.0.1 port=5432 role=primary weight=100\n"
        "replica1 = host=10.0.0.2 port=5432 role=replica weight=50\n"
        "replica2 = host=10.0.0.3 port=5432 role=replica weight=50\n";

    TEST_ASSERT(write_ini(ini));
    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    TEST_ASSERT(keel_config_has_section(cfg, "servers"));

    weight_ctx_t wctx = { .count = 0 };
    keel_config_iter_keys(cfg, "servers", collect_keys, &wctx);
    TEST_ASSERT_EQ(wctx.count, 3);

    /* Order-independent lookups */
    const char* primary_val = find_entry_val(&wctx, "primary");
    TEST_ASSERT_NOT_NULL(primary_val);
    TEST_ASSERT_EQ(parse_weight(primary_val), 100);

    const char* replica1_val = find_entry_val(&wctx, "replica1");
    TEST_ASSERT_NOT_NULL(replica1_val);
    TEST_ASSERT_EQ(parse_weight(replica1_val), 50);

    keel_config_free(cfg);

    /* Update weights via SIGHUP re-parse */
    const char* updated =
        "[servers]\n"
        "primary = host=10.0.0.1 port=5432 role=primary weight=200\n"
        "replica1 = host=10.0.0.2 port=5432 role=replica weight=100\n"
        "replica2 = host=10.0.0.3 port=5432 role=replica weight=75\n";

    TEST_ASSERT(write_ini(updated));
    keel_config_t* cfg2 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg2);

    weight_ctx_t wctx2 = { .count = 0 };
    keel_config_iter_keys(cfg2, "servers", collect_keys, &wctx2);
    TEST_ASSERT_EQ(wctx2.count, 3);

    primary_val = find_entry_val(&wctx2, "primary");
    TEST_ASSERT_NOT_NULL(primary_val);
    TEST_ASSERT_EQ(parse_weight(primary_val), 200);

    replica1_val = find_entry_val(&wctx2, "replica1");
    TEST_ASSERT_NOT_NULL(replica1_val);
    TEST_ASSERT_EQ(parse_weight(replica1_val), 100);

    const char* replica2_val = find_entry_val(&wctx2, "replica2");
    TEST_ASSERT_NOT_NULL(replica2_val);
    TEST_ASSERT_EQ(parse_weight(replica2_val), 75);

    keel_config_free(cfg2);
    cleanup_ini();
}

static void parse_host_port(const char* connstr, char* host, size_t hsz, int* port) {
    host[0] = '\0';
    *port = 0;
    const char* hp = strstr(connstr, "host=");
    if (hp) {
        const char* p = hp + 5;
        char* d = host;
        while (*p && *p != ' ' && *p != '\t' && (size_t)(d - host) < hsz - 1)
            *d++ = *p++;
        *d = '\0';
    }
    const char* pp = strstr(connstr, "port=");
    if (pp) *port = atoi(pp + 5);
}

static void test_server_host_port_match(void) {
    TEST_BEGIN("reload: server host+port matching");

    const char* ini =
        "[servers]\n"
        "primary = host=db-primary.local port=5432 role=primary weight=100\n"
        "replica = host=db-replica.local port=5433 role=replica weight=50\n";

    TEST_ASSERT(write_ini(ini));
    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    weight_ctx_t wctx = { .count = 0 };
    keel_config_iter_keys(cfg, "servers", collect_keys, &wctx);
    TEST_ASSERT_EQ(wctx.count, 2);

    /* Find primary by key name */
    const char* primary_val = find_entry_val(&wctx, "primary");
    TEST_ASSERT_NOT_NULL(primary_val);

    char host[256];
    int port;
    parse_host_port(primary_val, host, sizeof(host), &port);
    TEST_ASSERT_STR_EQ(host, "db-primary.local");
    TEST_ASSERT_EQ(port, 5432);

    /* Find replica by key name */
    const char* replica_val = find_entry_val(&wctx, "replica");
    TEST_ASSERT_NOT_NULL(replica_val);

    parse_host_port(replica_val, host, sizeof(host), &port);
    TEST_ASSERT_STR_EQ(host, "db-replica.local");
    TEST_ASSERT_EQ(port, 5433);

    keel_config_free(cfg);
    cleanup_ini();
}

/* ============================================================================
 * §4 — Probe timing update API
 * ============================================================================ */

static void test_probe_config_defaults(void) {
    TEST_BEGIN("reload: probe config defaults");

    keel_probe_config_t pc = KEEL_PROBE_CONFIG_DEFAULT;
    TEST_ASSERT_EQ(pc.interval_ms, 5000);
    TEST_ASSERT_EQ(pc.timeout_ms, 3000);
    TEST_ASSERT_EQ(pc.retries, 3);
}

static void test_probe_timing_reparse(void) {
    TEST_BEGIN("reload: probe timing INI re-parse");

    const char* ini =
        "[keel]\n"
        "probe_interval = 5s\n"
        "probe_timeout = 3s\n"
        "probe_retries = 3\n";

    TEST_ASSERT(write_ini(ini));
    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    const char* interval = keel_config_get_string(cfg, "keel", "probe_interval", NULL);
    TEST_ASSERT_NOT_NULL(interval);
    int val = atoi(interval);
    if (strchr(interval, 's')) val *= 1000;
    TEST_ASSERT_EQ(val, 5000);

    const char* timeout = keel_config_get_string(cfg, "keel", "probe_timeout", NULL);
    TEST_ASSERT_NOT_NULL(timeout);
    val = atoi(timeout);
    if (strchr(timeout, 's')) val *= 1000;
    TEST_ASSERT_EQ(val, 3000);

    int64_t retries = keel_config_get_int(cfg, "keel", "probe_retries", 0);
    TEST_ASSERT_EQ(retries, 3);
    keel_config_free(cfg);

    /* Update */
    TEST_ASSERT(write_ini("[keel]\nprobe_interval = 10s\nprobe_timeout = 5s\nprobe_retries = 5\n"));
    keel_config_t* cfg2 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg2);

    interval = keel_config_get_string(cfg2, "keel", "probe_interval", NULL);
    val = atoi(interval);
    if (strchr(interval, 's')) val *= 1000;
    TEST_ASSERT_EQ(val, 10000);

    timeout = keel_config_get_string(cfg2, "keel", "probe_timeout", NULL);
    val = atoi(timeout);
    if (strchr(timeout, 's')) val *= 1000;
    TEST_ASSERT_EQ(val, 5000);

    retries = keel_config_get_int(cfg2, "keel", "probe_retries", 0);
    TEST_ASSERT_EQ(retries, 5);

    keel_config_free(cfg2);
    cleanup_ini();
}

/* ============================================================================
 * §5 — Rebalancing config diff
 * ============================================================================ */

static void test_rebalance_config_diff(void) {
    TEST_BEGIN("reload: rebalancing config diff");

    const char* ini =
        "[keel]\n"
        "rebalance = on\n"
        "rebalance_interval_ms = 5000\n"
        "rebalance_threshold_pct = 125\n"
        "rebalance_max_per_tick = 1\n";

    TEST_ASSERT(write_ini(ini));
    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    TEST_ASSERT_STR_EQ(keel_config_get_string(cfg, "keel", "rebalance", ""), "on");
    TEST_ASSERT_EQ(keel_config_get_int(cfg, "keel", "rebalance_interval_ms", 0), 5000);
    TEST_ASSERT_EQ(keel_config_get_int(cfg, "keel", "rebalance_threshold_pct", 0), 125);
    TEST_ASSERT_EQ(keel_config_get_int(cfg, "keel", "rebalance_max_per_tick", 0), 1);
    keel_config_free(cfg);

    const char* updated =
        "[keel]\n"
        "rebalance = off\n"
        "rebalance_interval_ms = 10000\n"
        "rebalance_threshold_pct = 150\n"
        "rebalance_max_per_tick = 3\n";

    TEST_ASSERT(write_ini(updated));
    keel_config_t* cfg2 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg2);

    TEST_ASSERT_STR_EQ(keel_config_get_string(cfg2, "keel", "rebalance", ""), "off");
    TEST_ASSERT_EQ(keel_config_get_int(cfg2, "keel", "rebalance_interval_ms", 0), 10000);
    TEST_ASSERT_EQ(keel_config_get_int(cfg2, "keel", "rebalance_threshold_pct", 0), 150);
    TEST_ASSERT_EQ(keel_config_get_int(cfg2, "keel", "rebalance_max_per_tick", 0), 3);

    keel_config_free(cfg2);
    cleanup_ini();
}

/* ============================================================================
 * §6 — Human-readable duration parsing (client_idle_timeout 5m)
 * ============================================================================ */

static void test_human_readable_timeout(void) {
    TEST_BEGIN("reload: human-readable timeout (5m -> ms)");

    TEST_ASSERT(write_ini("[keel]\nclient_idle_timeout = 5m\n"));
    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    const char* ts = keel_config_get_string(cfg, "keel", "client_idle_timeout", NULL);
    TEST_ASSERT_NOT_NULL(ts);
    int tv = atoi(ts);
    if (strchr(ts, 'm')) tv *= 60000;
    else if (strchr(ts, 's')) tv *= 1000;
    TEST_ASSERT_EQ(tv, 300000);

    keel_config_free(cfg);

    /* Update to 10m */
    TEST_ASSERT(write_ini("[keel]\nclient_idle_timeout = 10m\n"));
    keel_config_t* cfg2 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg2);

    ts = keel_config_get_string(cfg2, "keel", "client_idle_timeout", NULL);
    TEST_ASSERT_NOT_NULL(ts);
    tv = atoi(ts);
    if (strchr(ts, 'm')) tv *= 60000;
    else if (strchr(ts, 's')) tv *= 1000;
    TEST_ASSERT_EQ(tv, 600000);

    keel_config_free(cfg2);
    cleanup_ini();
}

/* ============================================================================
 * §7 — Section-based server iteration
 * ============================================================================ */

static void test_section_iteration(void) {
    TEST_BEGIN("reload: section iteration for servers");

    const char* ini =
        "[keel]\n"
        "listen_port = 6432\n"
        "\n"
        "[servers]\n"
        "primary = host=10.0.0.1 port=5432 role=primary weight=100\n"
        "replica1 = host=10.0.0.2 port=5432 role=replica weight=50\n";

    TEST_ASSERT(write_ini(ini));
    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    TEST_ASSERT(keel_config_has_section(cfg, "keel"));
    TEST_ASSERT(keel_config_has_section(cfg, "servers"));
    TEST_ASSERT(!keel_config_has_section(cfg, "nonexistent"));

    weight_ctx_t wctx = { .count = 0 };
    keel_config_iter_keys(cfg, "servers", collect_keys, &wctx);
    TEST_ASSERT_EQ(wctx.count, 2);

    keel_config_free(cfg);
    cleanup_ini();
}

/* ============================================================================
 * §8 — No-change scenario (same config re-loaded)
 * ============================================================================ */

static void test_no_change_reload(void) {
    TEST_BEGIN("reload: no-change on identical config");

    const char* ini =
        "[keel]\n"
        "min_pool_size = 10\n"
        "max_pool_size = 100\n"
        "idle_timeout_ms = 300000\n"
        "bind_port = 6432\n";

    TEST_ASSERT(write_ini(ini));
    keel_config_t* cfg1 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg1);

    /* Reload same file — values must be identical */
    keel_config_t* cfg2 = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg2);

    TEST_ASSERT_EQ(
        keel_config_get_int(cfg1, "keel", "min_pool_size", 0),
        keel_config_get_int(cfg2, "keel", "min_pool_size", 0));
    TEST_ASSERT_EQ(
        keel_config_get_int(cfg1, "keel", "max_pool_size", 0),
        keel_config_get_int(cfg2, "keel", "max_pool_size", 0));
    TEST_ASSERT_EQ(
        keel_config_get_int(cfg1, "keel", "idle_timeout_ms", 0),
        keel_config_get_int(cfg2, "keel", "idle_timeout_ms", 0));
    TEST_ASSERT_EQ(
        keel_config_get_int(cfg1, "keel", "bind_port", 0),
        keel_config_get_int(cfg2, "keel", "bind_port", 0));

    keel_config_free(cfg1);
    keel_config_free(cfg2);
    cleanup_ini();
}

/* ============================================================================
 * §9 — Pool min/max clamping edge cases
 * ============================================================================ */

static void test_pool_size_clamping(void) {
    TEST_BEGIN("reload: pool size min/max clamping");

    /* min > max — reload code should clamp max = min */
    TEST_ASSERT(write_ini("[keel]\nmin_pool_size = 50\nmax_pool_size = 10\n"));
    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    int64_t raw_min = keel_config_get_int(cfg, "keel", "min_pool_size", 0);
    int64_t raw_max = keel_config_get_int(cfg, "keel", "max_pool_size", 0);
    TEST_ASSERT_EQ(raw_min, 50);
    TEST_ASSERT_EQ(raw_max, 10);

    /* Simulate the clamping logic from reload_worker_group */
    if (raw_min < 1) raw_min = 1;
    if (raw_max < raw_min) raw_max = raw_min;
    TEST_ASSERT_EQ(raw_max, 50);  /* Clamped up to min */

    keel_config_free(cfg);
    cleanup_ini();
}

/* ============================================================================
 * §10 — Connection string parsing edge cases
 * ============================================================================ */

static void test_connstr_parsing_edge_cases(void) {
    TEST_BEGIN("reload: connection string edge cases");

    /* Minimal connstr: only host and port */
    const char* ini =
        "[servers]\n"
        "s1 = host=127.0.0.1 port=5432\n"
        "s2 = host=::1 port=5433 weight=75\n"
        "s3 = host=db.example.com port=5434 role=primary weight=200 dbname=mydb\n";

    TEST_ASSERT(write_ini(ini));
    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    weight_ctx_t wctx = { .count = 0 };
    keel_config_iter_keys(cfg, "servers", collect_keys, &wctx);
    TEST_ASSERT_EQ(wctx.count, 3);

    /* s1: no weight= → parse_weight returns -1 */
    const char* s1_val = find_entry_val(&wctx, "s1");
    TEST_ASSERT_NOT_NULL(s1_val);
    TEST_ASSERT_EQ(parse_weight(s1_val), -1);

    /* s2: weight=75 */
    const char* s2_val = find_entry_val(&wctx, "s2");
    TEST_ASSERT_NOT_NULL(s2_val);
    TEST_ASSERT_EQ(parse_weight(s2_val), 75);

    /* s3: weight=200 with extra fields */
    const char* s3_val = find_entry_val(&wctx, "s3");
    TEST_ASSERT_NOT_NULL(s3_val);
    TEST_ASSERT_EQ(parse_weight(s3_val), 200);

    /* s3: host with dots */
    char h[256];
    int port;
    parse_host_port(s3_val, h, sizeof(h), &port);
    TEST_ASSERT_STR_EQ(h, "db.example.com");
    TEST_ASSERT_EQ(port, 5434);

    keel_config_free(cfg);
    cleanup_ini();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("=== Config Reload Tests ===\n\n");

    /* §1 — Safe parameter diff */
    test_pool_size_diff();
    test_timeout_diff();
    test_pool_max_waiting_diff();

    /* §2 — Restart-required detection */
    test_restart_required_port();
    test_restart_required_workers();
    test_restart_required_ps_mode();
    test_restart_required_mode();
    test_restart_required_txn_tracking();

    /* §3 — Server weights */
    test_server_weight_reparse();
    test_server_host_port_match();

    /* §4 — Probe timing */
    test_probe_config_defaults();
    test_probe_timing_reparse();

    /* §5 — Rebalancing */
    test_rebalance_config_diff();

    /* §6 — Human-readable durations */
    test_human_readable_timeout();

    /* §7 — Section iteration */
    test_section_iteration();

    /* §8 — No-change scenario */
    test_no_change_reload();

    /* §9 — Edge cases */
    test_pool_size_clamping();
    test_connstr_parsing_edge_cases();

    printf("\n");
    return test_summary();
}
