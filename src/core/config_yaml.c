/**
 * @file config_yaml.c
 * @brief YAML configuration loader + INI<->YAML converters (config v2).
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * The YAML schema mirrors the INI shape one-to-one so that a single
 * `keel_config_t` populated from either side feeds the same downstream
 * `keel_config_get_*` accessors:
 *
 *   - top-level scalar keys live in the synthetic `[keel]` section
 *     (so `config_version: 2` at the document root === `[keel]
 *     config_version = 2`).
 *
 *   - top-level mappings other than `worker_groups:` are emitted verbatim as
 *     INI sections. Scalar children become `key = value` entries; nested
 *     mappings are flattened by joining keys with `_` (so
 *     `tls: { handshake_timeout: 5s }` -> `tls_handshake_timeout = 5s`).
 *
 *   - `worker_groups:` is a sequence. Each mapping in the sequence must have
 *     a `name:` scalar; that name forms the section `[worker_group.<name>]`.
 *     Sub-mappings flatten as above. The `servers:` sub-sequence is special:
 *     each server must carry a `name:` scalar; the remaining fields are
 *     packed into a single value string (`host=... port=... role=... ...`)
 *     keyed by the server name under `[worker_group.<name>.servers]`. This
 *     lets the existing INI-style router code consume both formats uniformly.
 *
 *   - `${VAR}` references inside scalar values are expanded from the
 *     environment. `$$` escapes a literal `$`. Unset references expand to an
 *     empty string and emit a WARN log so misconfigured deployments surface
 *     early rather than silently mis-routing.
 *
 * The converter functions are pure file-to-file: INI -> YAML re-emits the
 * structural form; YAML -> INI walks the loaded `keel_config_t` and
 * serializes it back as INI. Both are intended to be lossless for the
 * supported schema shape.
 */

#include "keel/core/config_yaml.h"
#include "keel/core/ini.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"

#include "config_internal.h"

#include <ctype.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <yaml.h>

/* ============================================================================
 * Small dynamic string buffer (keel-allocated).
 * ============================================================================ */

typedef struct {
    char*  data;
    size_t len;
    size_t cap;
} ystr_t;

static int ystr_reserve(ystr_t* s, size_t need) {
    if (s->cap >= need) return 0;
    size_t cap = s->cap ? s->cap : 64;
    while (cap < need) cap *= 2;
    char* nd = (char*)keel_realloc(s->data, cap);
    if (!nd) return -1;
    s->data = nd;
    s->cap  = cap;
    return 0;
}

static int ystr_append(ystr_t* s, const char* p, size_t n) {
    if (ystr_reserve(s, s->len + n + 1) != 0) return -1;
    memcpy(s->data + s->len, p, n);
    s->len += n;
    s->data[s->len] = '\0';
    return 0;
}

static int ystr_appendz(ystr_t* s, const char* p) {
    return ystr_append(s, p, strlen(p));
}

static int ystr_appendc(ystr_t* s, char c) {
    return ystr_append(s, &c, 1);
}

static void ystr_free(ystr_t* s) {
    keel_free(s->data);
    s->data = NULL;
    s->len = s->cap = 0;
}

/* ============================================================================
 * ${VAR} environment interpolation
 *
 * Rules:
 *   ${NAME}   -> getenv("NAME") (warn + empty if unset)
 *   $$        -> literal '$'
 *   anything else -> copied through unchanged
 *
 * The result is written into @p out (keel-allocated, NUL-terminated).
 * Returns 0 on success.
 * ============================================================================ */

static int yaml_interp_env(const char* src, ystr_t* out) {
    if (!src) return 0;
    const char* p = src;
    while (*p) {
        if (p[0] == '$' && p[1] == '$') {
            if (ystr_appendc(out, '$') != 0) return -1;
            p += 2;
            continue;
        }
        if (p[0] == '$' && p[1] == '{') {
            const char* close = strchr(p + 2, '}');
            if (!close) {
                /* Malformed reference: emit the rest verbatim, keep parsing
                 * the remainder so users see a clear error in the log
                 * downstream rather than getting silent truncation. */
                if (ystr_appendz(out, p) != 0) return -1;
                return 0;
            }
            size_t name_len = (size_t)(close - (p + 2));
            char name[256];
            if (name_len >= sizeof(name)) name_len = sizeof(name) - 1;
            memcpy(name, p + 2, name_len);
            name[name_len] = '\0';
            const char* val = getenv(name);
            if (val) {
                if (ystr_appendz(out, val) != 0) return -1;
            } else {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG,
                              "YAML config: ${%s} unset, expanding to empty string",
                              name);
            }
            p = close + 1;
            continue;
        }
        if (ystr_appendc(out, *p) != 0) return -1;
        p++;
    }
    return 0;
}

/* ============================================================================
 * libyaml -> in-memory tree walker
 *
 * libyaml's document API gives us a node tree (`yaml_document_t`) which is
 * the easiest model to walk recursively. The tree is freed after we're done
 * populating the `keel_config_t`.
 * ============================================================================ */

static int node_is_scalar(yaml_document_t* doc, int node_idx) {
    yaml_node_t* n = yaml_document_get_node(doc, node_idx);
    return n && n->type == YAML_SCALAR_NODE;
}

static const char* node_scalar(yaml_document_t* doc, int node_idx) {
    yaml_node_t* n = yaml_document_get_node(doc, node_idx);
    if (!n || n->type != YAML_SCALAR_NODE) return NULL;
    return (const char*)n->data.scalar.value;
}

/**
 * @brief Render a node's scalar value with ${ENV} expansion into @p out.
 *
 * For mappings/sequences this is meaningless; the caller must check
 * `node_is_scalar` first.
 */
static int render_scalar(yaml_document_t* doc, int node_idx, ystr_t* out) {
    const char* raw = node_scalar(doc, node_idx);
    if (!raw) return -1;
    out->len = 0;
    if (out->data) out->data[0] = '\0';
    return yaml_interp_env(raw, out);
}

/**
 * @brief Walk a mapping node, emitting every scalar (or nested-scalar) leaf
 *        as a (`section`, `joined_key`, `value`) triple into the config
 *        store. Nested mappings flatten by joining keys with `_`.
 *
 * @param key_prefix May be NULL or "" at the top of a section; non-empty when
 *                   recursing into a sub-mapping.
 */
static int load_mapping_into_section(keel_config_t* cfg,
                                     yaml_document_t* doc,
                                     yaml_node_t* node,
                                     const char* section,
                                     const char* key_prefix);

/**
 * @brief Pack a server mapping into the `host=H port=P ...` INI value form.
 *
 * The `name` field, if present, is consumed by the caller (it's the entry
 * key). Every other scalar field becomes a `name=value` token separated by
 * single spaces. Values containing whitespace are skipped with a warning
 * because the legacy INI form has no quoting and the router parser is
 * whitespace-delimited.
 */
static int pack_server_value(yaml_document_t* doc, yaml_node_t* server_map,
                             ystr_t* out) {
    out->len = 0;
    if (out->data) out->data[0] = '\0';
    int first = 1;
    for (yaml_node_pair_t* p = server_map->data.mapping.pairs.start;
         p < server_map->data.mapping.pairs.top; ++p) {
        const char* k = node_scalar(doc, p->key);
        if (!k || strcasecmp(k, "name") == 0) continue;
        if (!node_is_scalar(doc, p->value)) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG,
                          "YAML server entry: field '%s' is not a scalar, skipping", k);
            continue;
        }
        ystr_t v = {0};
        if (render_scalar(doc, p->value, &v) != 0) {
            ystr_free(&v);
            return -1;
        }
        if (v.data && strpbrk(v.data, " \t") != NULL) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG,
                          "YAML server entry: field '%s' contains whitespace, skipping", k);
            ystr_free(&v);
            continue;
        }
        if (!first && ystr_appendc(out, ' ') != 0) { ystr_free(&v); return -1; }
        if (ystr_appendz(out, k) != 0) { ystr_free(&v); return -1; }
        if (ystr_appendc(out, '=') != 0) { ystr_free(&v); return -1; }
        if (v.data && ystr_appendz(out, v.data) != 0) { ystr_free(&v); return -1; }
        ystr_free(&v);
        first = 0;
    }
    return 0;
}

/**
 * @brief Find a mapping pair by case-insensitive key name. Returns NULL when
 *        absent.
 */
static yaml_node_pair_t* mapping_find(yaml_document_t* doc, yaml_node_t* map,
                                      const char* key) {
    for (yaml_node_pair_t* p = map->data.mapping.pairs.start;
         p < map->data.mapping.pairs.top; ++p) {
        const char* k = node_scalar(doc, p->key);
        if (k && strcasecmp(k, key) == 0) return p;
    }
    return NULL;
}

/**
 * @brief Load one entry of `worker_groups:` into its synthetic sections.
 *
 * On entry @p group_node is the mapping. It MUST contain a scalar `name:`
 * key. The group's `servers:` sequence (if present) is recorded under
 * `[worker_group.<name>.servers]`; all other scalar/mapping children populate
 * `[worker_group.<name>]`.
 */
static int load_worker_group(keel_config_t* cfg, yaml_document_t* doc,
                             yaml_node_t* group_node) {
    if (group_node->type != YAML_MAPPING_NODE) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                       "YAML: worker_groups[] entry is not a mapping");
        return -1;
    }
    yaml_node_pair_t* name_pair = mapping_find(doc, group_node, "name");
    if (!name_pair || !node_is_scalar(doc, name_pair->value)) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                       "YAML: worker_groups[] entry missing required 'name:'");
        return -1;
    }
    const char* group_name = node_scalar(doc, name_pair->value);
    char section[256];
    snprintf(section, sizeof(section), "worker_group.%s", group_name);

    /* Emit the canonical `name = <group_name>` so consumers that read it
     * back from the config object still see it. */
    if (keel_config_set(cfg, section, "name", group_name) != 0) return -1;

    char servers_section[320];
    snprintf(servers_section, sizeof(servers_section),
             "worker_group.%s.servers", group_name);

    for (yaml_node_pair_t* p = group_node->data.mapping.pairs.start;
         p < group_node->data.mapping.pairs.top; ++p) {
        const char* k = node_scalar(doc, p->key);
        if (!k) continue;
        if (strcasecmp(k, "name") == 0) continue;
        yaml_node_t* v = yaml_document_get_node(doc, p->value);
        if (!v) continue;

        if (strcasecmp(k, "servers") == 0) {
            if (v->type != YAML_SEQUENCE_NODE) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                               "YAML: worker_groups[%s].servers must be a sequence",
                               group_name);
                return -1;
            }
            for (yaml_node_item_t* it = v->data.sequence.items.start;
                 it < v->data.sequence.items.top; ++it) {
                yaml_node_t* srv = yaml_document_get_node(doc, *it);
                if (!srv || srv->type != YAML_MAPPING_NODE) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                                   "YAML: worker_groups[%s].servers[] entry is not a mapping",
                                   group_name);
                    return -1;
                }
                yaml_node_pair_t* sname = mapping_find(doc, srv, "name");
                if (!sname || !node_is_scalar(doc, sname->value)) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                                   "YAML: worker_groups[%s].servers[] entry missing 'name:'",
                                   group_name);
                    return -1;
                }
                const char* srv_name = node_scalar(doc, sname->value);
                ystr_t packed = {0};
                if (pack_server_value(doc, srv, &packed) != 0) {
                    ystr_free(&packed);
                    return -1;
                }
                if (keel_config_set(cfg, servers_section, srv_name,
                                    packed.data ? packed.data : "") != 0) {
                    ystr_free(&packed);
                    return -1;
                }
                ystr_free(&packed);
            }
            continue;
        }

        if (v->type == YAML_SCALAR_NODE) {
            ystr_t val = {0};
            if (render_scalar(doc, p->value, &val) != 0) {
                ystr_free(&val);
                return -1;
            }
            if (keel_config_set(cfg, section, k, val.data ? val.data : "") != 0) {
                ystr_free(&val);
                return -1;
            }
            ystr_free(&val);
        } else if (v->type == YAML_MAPPING_NODE) {
            if (load_mapping_into_section(cfg, doc, v, section, k) != 0)
                return -1;
        } else {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG,
                          "YAML: worker_groups[%s].%s: sequences unsupported here, skipping",
                          group_name, k);
        }
    }
    return 0;
}

static int load_mapping_into_section(keel_config_t* cfg,
                                     yaml_document_t* doc,
                                     yaml_node_t* node,
                                     const char* section,
                                     const char* key_prefix) {
    if (!node || node->type != YAML_MAPPING_NODE) return -1;
    for (yaml_node_pair_t* p = node->data.mapping.pairs.start;
         p < node->data.mapping.pairs.top; ++p) {
        const char* k = node_scalar(doc, p->key);
        if (!k) continue;
        yaml_node_t* v = yaml_document_get_node(doc, p->value);
        if (!v) continue;

        char joined[256];
        if (key_prefix && *key_prefix) {
            snprintf(joined, sizeof(joined), "%s_%s", key_prefix, k);
        } else {
            snprintf(joined, sizeof(joined), "%s", k);
        }

        if (v->type == YAML_SCALAR_NODE) {
            ystr_t val = {0};
            if (render_scalar(doc, p->value, &val) != 0) {
                ystr_free(&val);
                return -1;
            }
            if (keel_config_set(cfg, section, joined,
                                val.data ? val.data : "") != 0) {
                ystr_free(&val);
                return -1;
            }
            ystr_free(&val);
        } else if (v->type == YAML_MAPPING_NODE) {
            if (load_mapping_into_section(cfg, doc, v, section, joined) != 0)
                return -1;
        } else {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG,
                          "YAML: [%s] %s: sequences not supported here, skipping",
                          section, joined);
        }
    }
    return 0;
}

/* ============================================================================
 * Public loader
 * ============================================================================ */

keel_config_t* keel_config_load_yaml(const char* path) {
    if (!path) return NULL;

    FILE* fp = fopen(path, "r");
    if (!fp) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                       "Failed to open YAML config file: %s (%s)",
                       path, strerror(errno));
        return NULL;
    }

    yaml_parser_t parser;
    yaml_document_t doc;
    if (!yaml_parser_initialize(&parser)) {
        fclose(fp);
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG, "yaml_parser_initialize failed");
        return NULL;
    }
    yaml_parser_set_input_file(&parser, fp);

    if (!yaml_parser_load(&parser, &doc)) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                       "YAML parse error in %s at line %zu col %zu: %s",
                       path,
                       (size_t)parser.problem_mark.line + 1,
                       (size_t)parser.problem_mark.column + 1,
                       parser.problem ? parser.problem : "(unknown)");
        yaml_parser_delete(&parser);
        fclose(fp);
        return NULL;
    }

    yaml_node_t* root = yaml_document_get_root_node(&doc);
    if (!root) {
        /* Empty document is valid YAML and produces an empty config. */
        keel_config_t* empty = keel_config_create_empty(path);
        yaml_document_delete(&doc);
        yaml_parser_delete(&parser);
        fclose(fp);
        if (empty) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CONFIG,
                          "Loaded configuration from %s (empty YAML)", path);
        }
        return empty;
    }
    if (root->type != YAML_MAPPING_NODE) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                       "YAML root must be a mapping in %s", path);
        yaml_document_delete(&doc);
        yaml_parser_delete(&parser);
        fclose(fp);
        return NULL;
    }

    keel_config_t* cfg = keel_config_create_empty(path);
    if (!cfg) {
        yaml_document_delete(&doc);
        yaml_parser_delete(&parser);
        fclose(fp);
        return NULL;
    }

    int rc = 0;
    for (yaml_node_pair_t* p = root->data.mapping.pairs.start;
         p < root->data.mapping.pairs.top; ++p) {
        const char* k = node_scalar(&doc, p->key);
        if (!k) continue;
        yaml_node_t* v = yaml_document_get_node(&doc, p->value);
        if (!v) continue;

        if (v->type == YAML_SCALAR_NODE) {
            /* Bare top-level scalars (e.g. `config_version: 2`) go into
             * `[keel]` so old INI consumers find them in the canonical
             * place. */
            ystr_t val = {0};
            if (render_scalar(&doc, p->value, &val) != 0) {
                ystr_free(&val); rc = -1; break;
            }
            if (keel_config_set(cfg, "keel", k,
                                val.data ? val.data : "") != 0) {
                ystr_free(&val); rc = -1; break;
            }
            ystr_free(&val);
            continue;
        }

        if (strcasecmp(k, "worker_groups") == 0) {
            if (v->type != YAML_SEQUENCE_NODE) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                               "YAML: worker_groups must be a sequence");
                rc = -1; break;
            }
            for (yaml_node_item_t* it = v->data.sequence.items.start;
                 it < v->data.sequence.items.top; ++it) {
                yaml_node_t* g = yaml_document_get_node(&doc, *it);
                if (!g) continue;
                if (load_worker_group(cfg, &doc, g) != 0) {
                    rc = -1; break;
                }
            }
            if (rc != 0) break;
            continue;
        }

        if (v->type == YAML_MAPPING_NODE) {
            /* A regular section: walk it. */
            if (load_mapping_into_section(cfg, &doc, v, k, NULL) != 0) {
                rc = -1; break;
            }
            continue;
        }

        KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG,
                      "YAML: top-level '%s' is neither scalar nor mapping nor "
                      "the special 'worker_groups' sequence, skipping", k);
    }

    yaml_document_delete(&doc);
    yaml_parser_delete(&parser);
    fclose(fp);

    if (rc != 0) {
        keel_config_free(cfg);
        return NULL;
    }
    KEEL_LOG_INFO(KEEL_LOG_CAT_CONFIG, "Loaded configuration from %s", path);
    return cfg;
}

/* ============================================================================
 * Format detection + auto-load
 * ============================================================================ */

keel_config_format_t keel_config_detect_format(const char* path) {
    if (!path) return KEEL_CONFIG_FORMAT_INI;
    const char* dot = strrchr(path, '.');
    if (!dot) return KEEL_CONFIG_FORMAT_INI;
    if (strcasecmp(dot, ".yaml") == 0 || strcasecmp(dot, ".yml") == 0)
        return KEEL_CONFIG_FORMAT_YAML;
    return KEEL_CONFIG_FORMAT_INI;
}

keel_config_t* keel_config_load_auto(const char* path) {
    if (keel_config_detect_format(path) == KEEL_CONFIG_FORMAT_YAML)
        return keel_config_load_yaml(path);
    return keel_config_load(path);
}

/* ============================================================================
 * Emitter: keel_config_t -> YAML on disk
 *
 * Walks the config sections in order, recognizing the synthetic
 * `worker_group.NAME` / `worker_group.NAME.servers` shape and unpacking
 * `host=... port=... ...` value strings back into structured server entries.
 * ============================================================================ */

/* For YAML output we never quote scalars: libyaml's emitter handles quoting
 * automatically. The custom emitter here uses raw fprintf with manual escape
 * only for newlines/colons embedded in values (rare in practice for config). */
static void emit_yaml_scalar(FILE* out, const char* s) {
    if (!s) { fputs("\"\"", out); return; }
    /* Quote when the string contains characters that would change YAML's
     * scalar interpretation (leading whitespace, leading `-`, embedded `:`,
     * `#`, `&`, `*`, etc.). */
    int need_quote = 0;
    if (*s == '\0' || *s == ' ' || *s == '\t' || *s == '-' || *s == '?' ||
        *s == '[' || *s == ']' || *s == '{' || *s == '}' || *s == ',' ||
        *s == '&' || *s == '*' || *s == '!' || *s == '|' || *s == '>' ||
        *s == '\'' || *s == '"' || *s == '%' || *s == '@' || *s == '`') {
        need_quote = 1;
    } else {
        for (const char* p = s; *p; ++p) {
            if (*p == ':' || *p == '#' || *p == '\n' || *p == '\\' ||
                (unsigned char)*p < 0x20) {
                need_quote = 1;
                break;
            }
        }
    }
    if (!need_quote) {
        fputs(s, out);
        return;
    }
    fputc('"', out);
    for (const char* p = s; *p; ++p) {
        if (*p == '\\' || *p == '"') fputc('\\', out);
        if (*p == '\n') { fputs("\\n", out); continue; }
        fputc(*p, out);
    }
    fputc('"', out);
}

/**
 * @brief Re-emit a packed server value (`host=H port=P role=R ...`) as a
 *        block-mapping under the current sequence item's indentation.
 *
 * @param indent Number of spaces preceding each emitted line (the items's
 *               `-` lives at indent-2; the children at indent+2).
 */
static void emit_server_packed(FILE* out, int indent, const char* packed) {
    if (!packed || !*packed) return;
    const char* p = packed;
    while (*p) {
        while (*p == ' ' || *p == '\t') p++;
        if (!*p) break;
        const char* eq = strchr(p, '=');
        if (!eq) break;
        size_t klen = (size_t)(eq - p);
        const char* vstart = eq + 1;
        const char* vend = vstart;
        while (*vend && *vend != ' ' && *vend != '\t') vend++;
        size_t vlen = (size_t)(vend - vstart);
        char key[64];
        if (klen >= sizeof(key)) klen = sizeof(key) - 1;
        memcpy(key, p, klen); key[klen] = '\0';
        char val[1024];
        if (vlen >= sizeof(val)) vlen = sizeof(val) - 1;
        memcpy(val, vstart, vlen); val[vlen] = '\0';
        fprintf(out, "%*s%s: ", indent, "", key);
        emit_yaml_scalar(out, val);
        fputc('\n', out);
        p = vend;
    }
}

/* Iteration helpers: walk a config_section_t via the public iter API. */
struct section_collector { char** names; size_t count; size_t cap; };

static void collect_section_cb(const char* name, void* udata) {
    struct section_collector* c = (struct section_collector*)udata;
    if (c->count == c->cap) {
        size_t nc = c->cap ? c->cap * 2 : 16;
        char** nn = (char**)keel_realloc(c->names, nc * sizeof(char*));
        if (!nn) return;
        c->names = nn; c->cap = nc;
    }
    c->names[c->count++] = keel_strdup(name);
}

struct kv_collector { char** keys; char** vals; size_t count; size_t cap; };

static void collect_kv_cb(const char* key, const char* val, void* udata) {
    struct kv_collector* c = (struct kv_collector*)udata;
    if (c->count == c->cap) {
        size_t nc = c->cap ? c->cap * 2 : 16;
        char** nk = (char**)keel_realloc(c->keys, nc * sizeof(char*));
        char** nv = (char**)keel_realloc(c->vals, nc * sizeof(char*));
        if (!nk || !nv) return;
        c->keys = nk; c->vals = nv; c->cap = nc;
    }
    c->keys[c->count] = keel_strdup(key);
    c->vals[c->count] = keel_strdup(val);
    c->count++;
}

static void kv_collector_free(struct kv_collector* c) {
    for (size_t i = 0; i < c->count; ++i) {
        keel_free(c->keys[i]);
        keel_free(c->vals[i]);
    }
    keel_free(c->keys);
    keel_free(c->vals);
}

static void section_collector_free(struct section_collector* c) {
    for (size_t i = 0; i < c->count; ++i) keel_free(c->names[i]);
    keel_free(c->names);
}

/**
 * @brief Write a `keel_config_t` as a YAML document to @p out.
 *
 * Sections are emitted in iteration order. The synthetic worker_group sections
 * are gathered into a single top-level `worker_groups:` sequence.
 */
static int emit_config_as_yaml(const keel_config_t* cfg, FILE* out) {
    fputs("# Generated by keel --convert-config. See docs/CONFIGURATION.md.\n",
          out);

    struct section_collector sc = {0};
    keel_config_iter_sections(cfg, collect_section_cb, &sc);

    /* Pass 1: emit plain (non-worker_group) sections. */
    for (size_t i = 0; i < sc.count; ++i) {
        const char* sname = sc.names[i];
        if (strncasecmp(sname, "worker_group.", 13) == 0) continue;

        fprintf(out, "%s:\n", sname);
        struct kv_collector kv = {0};
        keel_config_iter_keys(cfg, sname, collect_kv_cb, &kv);
        for (size_t j = 0; j < kv.count; ++j) {
            fprintf(out, "  %s: ", kv.keys[j]);
            emit_yaml_scalar(out, kv.vals[j]);
            fputc('\n', out);
        }
        kv_collector_free(&kv);
        fputc('\n', out);
    }

    /* Pass 2: emit worker_groups[]. We iterate sections again and pick up
     * `worker_group.NAME` blocks; for each, look for the matching
     * `worker_group.NAME.servers` block and emit the structured form. */
    int wg_emitted = 0;
    for (size_t i = 0; i < sc.count; ++i) {
        const char* sname = sc.names[i];
        if (strncasecmp(sname, "worker_group.", 13) != 0) continue;
        /* Skip the `.servers` companion; we render it together with its parent. */
        if (strstr(sname + 13, ".servers")) continue;
        const char* gname = sname + 13;

        if (!wg_emitted) { fputs("worker_groups:\n", out); wg_emitted = 1; }

        fprintf(out, "  - name: %s\n", gname);
        struct kv_collector kv = {0};
        keel_config_iter_keys(cfg, sname, collect_kv_cb, &kv);
        for (size_t j = 0; j < kv.count; ++j) {
            if (strcasecmp(kv.keys[j], "name") == 0) continue;
            fprintf(out, "    %s: ", kv.keys[j]);
            emit_yaml_scalar(out, kv.vals[j]);
            fputc('\n', out);
        }
        kv_collector_free(&kv);

        char servers_section[320];
        snprintf(servers_section, sizeof(servers_section),
                 "worker_group.%s.servers", gname);
        if (keel_config_has_section(cfg, servers_section)) {
            struct kv_collector sv = {0};
            keel_config_iter_keys(cfg, servers_section, collect_kv_cb, &sv);
            if (sv.count > 0) {
                fputs("    servers:\n", out);
                for (size_t j = 0; j < sv.count; ++j) {
                    fprintf(out, "      - name: %s\n", sv.keys[j]);
                    emit_server_packed(out, 8, sv.vals[j]);
                }
            }
            kv_collector_free(&sv);
        }
        fputc('\n', out);
    }
    section_collector_free(&sc);
    return 0;
}

/**
 * @brief Write a `keel_config_t` as an INI document to @p out.
 */
static int emit_config_as_ini(const keel_config_t* cfg, FILE* out) {
    fputs("# Generated by keel --convert-config. See docs/CONFIGURATION.md.\n",
          out);
    struct section_collector sc = {0};
    keel_config_iter_sections(cfg, collect_section_cb, &sc);
    for (size_t i = 0; i < sc.count; ++i) {
        const char* sname = sc.names[i];
        fprintf(out, "\n[%s]\n", sname);
        struct kv_collector kv = {0};
        keel_config_iter_keys(cfg, sname, collect_kv_cb, &kv);
        for (size_t j = 0; j < kv.count; ++j) {
            fprintf(out, "%s = %s\n", kv.keys[j], kv.vals[j]);
        }
        kv_collector_free(&kv);
    }
    section_collector_free(&sc);
    return 0;
}

keel_error_t keel_config_convert_ini_to_yaml(const char* in_path,
                                             const char* out_path) {
    if (!in_path || !out_path) return KEEL_ERR_NULL_PTR;
    keel_config_t* cfg = keel_config_load(in_path);
    if (!cfg) return KEEL_ERR_IO;
    FILE* fp = fopen(out_path, "w");
    if (!fp) {
        keel_config_free(cfg);
        return KEEL_ERR_IO;
    }
    int rc = emit_config_as_yaml(cfg, fp);
    fclose(fp);
    keel_config_free(cfg);
    return rc == 0 ? KEEL_OK : KEEL_ERR_IO;
}

keel_error_t keel_config_convert_yaml_to_ini(const char* in_path,
                                             const char* out_path) {
    if (!in_path || !out_path) return KEEL_ERR_NULL_PTR;
    keel_config_t* cfg = keel_config_load_yaml(in_path);
    if (!cfg) return KEEL_ERR_IO;
    FILE* fp = fopen(out_path, "w");
    if (!fp) {
        keel_config_free(cfg);
        return KEEL_ERR_IO;
    }
    int rc = emit_config_as_ini(cfg, fp);
    fclose(fp);
    keel_config_free(cfg);
    return rc == 0 ? KEEL_OK : KEEL_ERR_IO;
}
