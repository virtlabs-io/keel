/**
 * @file config_migrate.c
 * @brief INI configuration migrator: schema v1 -> v2 (and idempotent v2 -> v2).
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Migration rules (v1 -> v2):
 *
 *   1. Keys carrying explicit unit suffixes in their names
 *      (`*_ms`, `*_bytes`, and the legacy `scatter_merge_max_mem_mb`)
 *      are renamed to drop the suffix. The unit moves into the value
 *      domain, which is then parsed by `keel_config_get_duration` or
 *      `keel_config_get_bytes` (a bare integer means milliseconds for
 *      durations and bytes for byte counts).
 *
 *   2. `config_version = 2` is injected as the first non-blank line of
 *      the `[keel]` section. If the input file has no `[keel]`
 *      section, a synthetic one is prepended to the output.
 *
 * The migrator is line-based on purpose: it preserves comments,
 * blank lines, and section ordering so the diff between input and
 * output is limited to the key renames plus the version marker.
 *
 * The migrator is idempotent. Running it on a v2 input yields the
 * same content (modulo possible reordering of `config_version` to the
 * top of `[keel]` if it was placed lower).
 */

#include "keel/core/config_migrate.h"
#include "keel/mem/mem.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---------------------------------------------------------------------
 * Rename table
 *
 * One entry per INI key whose v1 spelling carries a unit suffix in
 * the name. Order does not matter; lookup is linear (table is small).
 *
 * IMPORTANT: keys are matched case-insensitively to align with the
 * existing INI parser's `strcasecmp` semantics.
 * --------------------------------------------------------------------- */

typedef struct {
    const char* v1;
    const char* v2;
} v2_rename_t;

static const v2_rename_t k_rename_table[] = {
    /* Durations (suffix _ms) */
    { "shutdown_timeout_ms",              "shutdown_timeout"              },
    { "log_interval_ms",                  "log_interval"                  },
    { "otlp_timeout_ms",                  "otlp_timeout"                  },
    { "otlp_interval_ms",                 "otlp_interval"                 },
    { "heartbeat_interval_ms",            "heartbeat_interval"            },
    { "heartbeat_timeout_ms",             "heartbeat_timeout"             },
    { "flush_interval_ms",                "flush_interval"                },
    { "export_timeout_ms",                "export_timeout"                },
    { "idle_timeout_ms",                  "idle_timeout"                  },
    { "connect_timeout_ms",               "connect_timeout"               },
    { "pool_prune_interval_ms",           "pool_prune_interval"           },
    { "pool_refill_interval_ms",          "pool_refill_interval"          },
    { "pool_refill_backoff_ms",           "pool_refill_backoff"           },
    { "pool_wait_timeout_ms",             "pool_wait_timeout"             },
    { "rebalance_interval_ms",            "rebalance_interval"            },
    { "sqpoll_idle_ms",                   "sqpoll_idle"                   },
    { "sticky_primary_ttl_ms",            "sticky_primary_ttl"            },
    { "tls_handshake_timeout_ms",         "tls_handshake_timeout"         },
    { "tls_read_timeout_ms",              "tls_read_timeout"              },
    { "backend_tls_handshake_timeout_ms", "backend_tls_handshake_timeout" },
    { "backend_tls_read_timeout_ms",      "backend_tls_read_timeout"      },
    { "max_connection_age_ms",            "max_connection_age"            },
    /* Byte counts (suffix _bytes) */
    { "session_max_buffered_bytes",       "session_max_buffered"          },
    { "backend_max_replay_bytes",         "backend_max_replay"            },
    { "otlp_encode_buf_bytes",            "otlp_encode_buf"               },
    { "compress_threshold_bytes",         "compress_threshold"            },
    /* Legacy MiB-multiplier key (becomes bytes-typed). */
    { "scatter_merge_max_mem_mb",         "scatter_merge_max_mem"         },
};

static const size_t k_rename_count = sizeof(k_rename_table) / sizeof(k_rename_table[0]);

static const char* lookup_v2(const char* v1_key, size_t v1_len) {
    for (size_t i = 0; i < k_rename_count; i++) {
        if (strlen(k_rename_table[i].v1) == v1_len &&
            strncasecmp(k_rename_table[i].v1, v1_key, v1_len) == 0) {
            return k_rename_table[i].v2;
        }
    }
    return NULL;
}

/* ---------------------------------------------------------------------
 * Line classification
 * --------------------------------------------------------------------- */

/* Returns pointer to the first non-whitespace character in `line`. */
static const char* skip_ws(const char* p) {
    while (*p == ' ' || *p == '\t') p++;
    return p;
}

/* Detects a section header line of the form `   [name]   # comment`.
 * On match, writes the lowercase-trimmed section name into `out` (size
 * `out_sz`) and returns 1. Returns 0 otherwise. */
static int parse_section(const char* line, char* out, size_t out_sz) {
    const char* p = skip_ws(line);
    if (*p != '[') return 0;
    p++;
    const char* start = p;
    while (*p && *p != ']' && *p != '\n') p++;
    if (*p != ']') return 0;
    size_t n = (size_t)(p - start);
    /* Trim trailing whitespace inside the brackets. */
    while (n > 0 && (start[n - 1] == ' ' || start[n - 1] == '\t')) n--;
    if (n == 0 || n >= out_sz) return 0;
    for (size_t i = 0; i < n; i++) {
        out[i] = (char)tolower((unsigned char)start[i]);
    }
    out[n] = '\0';
    return 1;
}

/* Detects a `key = value` line. On match, sets `*key_off` to the offset
 * of the key's first character, `*key_len` to its length, and returns 1. */
static int parse_kv(const char* line, size_t* key_off, size_t* key_len) {
    const char* start = line;
    const char* p = skip_ws(start);
    if (*p == '\0' || *p == '\n' || *p == '#' || *p == ';' || *p == '[') {
        return 0;
    }
    const char* key_start = p;
    while (*p && *p != '=' && *p != ' ' && *p != '\t' && *p != '\n') p++;
    const char* key_end = p;
    /* Must reach an '=' (after optional whitespace) to be a kv line. */
    const char* q = skip_ws(p);
    if (*q != '=') return 0;
    if (key_end == key_start) return 0;
    *key_off = (size_t)(key_start - start);
    *key_len = (size_t)(key_end - key_start);
    return 1;
}

/* ---------------------------------------------------------------------
 * Migrator
 *
 * Single-pass over the input stream:
 *
 *   - tracks the current section name;
 *   - rewrites any key listed in `k_rename_table`;
 *   - injects `config_version = 2` immediately after the first
 *     `[keel]` section header, unless an existing `config_version` line
 *     is encountered later in that section (in which case it is
 *     normalized to `2`).
 *
 * If no `[keel]` section is seen by EOF, a synthetic `[keel]` block
 * is prepended to the output via a buffered approach: the simplest
 * correct implementation is two-pass. We use that to keep the code
 * readable.
 * --------------------------------------------------------------------- */

#define MAX_LINE_LEN 4096

typedef struct {
    char*  data;
    size_t len;
    size_t cap;
} strbuf_t;

static int strbuf_reserve(strbuf_t* b, size_t need) {
    if (b->cap >= need) return 0;
    size_t cap = b->cap ? b->cap : 1024;
    while (cap < need) cap *= 2;
    char* nd = (char*)keel_realloc(b->data, cap);
    if (!nd) return -1;
    b->data = nd;
    b->cap  = cap;
    return 0;
}

static int strbuf_append(strbuf_t* b, const char* s, size_t n) {
    if (strbuf_reserve(b, b->len + n + 1) != 0) return -1;
    memcpy(b->data + b->len, s, n);
    b->len += n;
    b->data[b->len] = '\0';
    return 0;
}

keel_error_t keel_config_migrate(FILE* in, FILE* out) {
    if (!in || !out) return KEEL_ERR_NULL_PTR;

    strbuf_t buf = { 0 };
    char line[MAX_LINE_LEN];
    char current_section[256] = { 0 };
    int  in_keel_section      = 0;
    int  keel_section_seen    = 0;
    int  version_injected     = 0;
    int  version_normalized   = 0;

    while (fgets(line, sizeof(line), in)) {
        char section[256];
        if (parse_section(line, section, sizeof(section))) {
            /* On entering a new section, if we were in [keel] and
             * never emitted `config_version = 2`, inject it before
             * leaving. */
            if (in_keel_section && !version_injected && !version_normalized) {
                if (strbuf_append(&buf, "config_version = 2\n", 19) != 0) {
                    goto oom;
                }
                version_injected = 1;
            }
            strncpy(current_section, section, sizeof(current_section) - 1);
            current_section[sizeof(current_section) - 1] = '\0';
            in_keel_section = (strcmp(current_section, "keel") == 0);
            if (in_keel_section) {
                keel_section_seen = 1;
            }
            if (strbuf_append(&buf, line, strlen(line)) != 0) goto oom;
            continue;
        }

        size_t key_off, key_len;
        int    is_kv = parse_kv(line, &key_off, &key_len);

        /* Handle existing config_version in [keel] BEFORE the "first real
         * line" injection so we don't end up with two version lines when the
         * input already declared one. */
        if (is_kv && in_keel_section &&
            key_len == strlen("config_version") &&
            strncasecmp(line + key_off, "config_version", key_len) == 0) {
            if (strbuf_append(&buf, line, key_off) != 0) goto oom;
            if (strbuf_append(&buf, "config_version = 2", 18) != 0) goto oom;
            const char* hash = strchr(line + key_off + key_len, '#');
            if (hash) {
                if (strbuf_append(&buf, "  ", 2) != 0) goto oom;
                if (strbuf_append(&buf, hash, strlen(hash)) != 0) goto oom;
            } else {
                if (strbuf_append(&buf, "\n", 1) != 0) goto oom;
            }
            version_normalized = 1;
            continue;
        }

        /* Inside [keel]: inject the version marker as the first
         * non-blank, non-comment line if we haven't yet. */
        if (in_keel_section && !version_injected && !version_normalized) {
            const char* p = skip_ws(line);
            if (*p && *p != '\n' && *p != '#' && *p != ';') {
                if (strbuf_append(&buf, "config_version = 2\n", 19) != 0) {
                    goto oom;
                }
                version_injected = 1;
            }
        }

        if (is_kv) {
            const char* key = line + key_off;
            const char* v2  = lookup_v2(key, key_len);
            if (v2) {
                if (strbuf_append(&buf, line, key_off) != 0) goto oom;
                if (strbuf_append(&buf, v2, strlen(v2)) != 0) goto oom;
                const char* tail = line + key_off + key_len;
                if (strbuf_append(&buf, tail, strlen(tail)) != 0) goto oom;
                continue;
            }
        }

        if (strbuf_append(&buf, line, strlen(line)) != 0) goto oom;
    }

    /* End-of-file fixups. */
    if (in_keel_section && !version_injected && !version_normalized) {
        if (strbuf_append(&buf, "config_version = 2\n", 19) != 0) goto oom;
    }
    if (!keel_section_seen) {
        /* No [keel] section in the input -- prepend a synthetic one. */
        const char* prelude = "[keel]\nconfig_version = 2\n\n";
        size_t prelude_len  = strlen(prelude);
        strbuf_t out_buf = { 0 };
        if (strbuf_reserve(&out_buf, prelude_len + buf.len + 1) != 0) {
            keel_free(buf.data);
            return KEEL_ERR_NOMEM;
        }
        memcpy(out_buf.data, prelude, prelude_len);
        memcpy(out_buf.data + prelude_len, buf.data, buf.len);
        out_buf.len = prelude_len + buf.len;
        out_buf.data[out_buf.len] = '\0';
        keel_free(buf.data);
        buf = out_buf;
    }

    if (buf.len > 0) {
        if (fwrite(buf.data, 1, buf.len, out) != buf.len) {
            keel_free(buf.data);
            return KEEL_ERR_IO;
        }
    }
    keel_free(buf.data);
    return KEEL_OK;

oom:
    keel_free(buf.data);
    return KEEL_ERR_NOMEM;
}

keel_error_t keel_config_migrate_file(const char* in_path,
                                      const char* out_path) {
    if (!in_path) return KEEL_ERR_NULL_PTR;

    FILE* in = fopen(in_path, "r");
    if (!in) {
        fprintf(stderr,
                "keel: --migrate-config: cannot open input '%s': %s\n",
                in_path, strerror(errno));
        return KEEL_ERR_IO;
    }

    int        close_out = 0;
    FILE*      out       = stdout;
    if (out_path && strcmp(out_path, "-") != 0) {
        out = fopen(out_path, "w");
        if (!out) {
            fprintf(stderr,
                    "keel: --migrate-config: cannot open output '%s': %s\n",
                    out_path, strerror(errno));
            fclose(in);
            return KEEL_ERR_IO;
        }
        close_out = 1;
    }

    keel_error_t rc = keel_config_migrate(in, out);
    fclose(in);
    if (close_out) fclose(out);
    return rc;
}
