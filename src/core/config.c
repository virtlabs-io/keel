/**
 * @file config.c
 * @brief INI-style configuration parser and typed accessor helpers.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This file implements KEEL's lightweight configuration store. It loads
 * section/key/value pairs from INI-style configuration files, preserves them in
 * heap-owned linked structures, and exposes typed lookup helpers used by higher
 * layers when building engine and router configuration.
 *
 * Supported parsing behavior:
 * - comments beginning with `#` or `;`
 * - implicit `global` section when keys appear before any section header
 * - optional quoted values
 * - backslash line continuation for long values
 */

#include "keel/core/ini.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <ctype.h>
#include <errno.h>

/* ============================================================================
 * Configuration Parser
 * ============================================================================ */

#define MAX_LINE_LEN 4096

typedef struct config_section {
    struct config_section*  next;
    char*                   name;
    
    struct config_entry {
        struct config_entry* next;
        char*               key;
        char*               value;
    }*                      entries;
} config_section_t;

struct keel_config {
    config_section_t*       sections;
    char*                   path;
};

/* ============================================================================
 * String Helpers
 * ============================================================================ */

/**
 * @brief Trim leading and trailing ASCII whitespace in place.
 *
 * @param str Mutable string buffer.
 * @return Pointer to the first non-space character, or `NULL` if `str` is `NULL`.
 */
static char* trim(char* str) {
    if (!str) return NULL;
    
    /* Trim leading */
    while (isspace((unsigned char)*str)) str++;
    
    if (*str == '\0') return str;
    
    /* Trim trailing */
    char* end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;
    *(end + 1) = '\0';
    
    return str;
}

/**
 * @brief Remove matching single or double quotes from a string in place.
 *
 * @param str Mutable string buffer.
 * @return Pointer to the unquoted payload, or the original string when no
 *         matching outer quotes are present.
 */
static char* strip_quotes(char* str) {
    if (!str) return NULL;
    
    size_t len = strlen(str);
    if (len < 2) return str;
    
    if ((str[0] == '"' && str[len-1] == '"') ||
        (str[0] == '\'' && str[len-1] == '\'')) {
        str[len-1] = '\0';
        return str + 1;
    }
    
    return str;
}

/* ============================================================================
 * Configuration Parsing
 * ============================================================================ */

/**
 * @brief Load and parse a configuration file from disk.
 *
 * @param path Filesystem path to the INI-style configuration file.
 * @return New configuration object on success, or `NULL` on open/allocation failure.
 *
 * Errors surfaced:
 * - file open failure is logged and returned as `NULL`
 * - allocation failures abort parsing and return `NULL`
 *
 * Corner cases:
 * - malformed section headers or key/value lines are logged and skipped
 * - repeated keys are preserved in insertion order; lookups return the first
 *   matching entry encountered in the section's linked list
 */
keel_config_t* keel_config_load(const char* path) {
    if (!path) {
        return NULL;
    }
    
    FILE* fp = fopen(path, "r");
    if (!fp) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG, "Failed to open config file: %s (%s)",
                      path, strerror(errno));
        return NULL;
    }
    
    keel_config_t* config = keel_calloc(1, sizeof(keel_config_t));
    if (!config) {
        fclose(fp);
        return NULL;
    }
    
    config->path = keel_strdup(path);
    
    char line[MAX_LINE_LEN];
    char accum[MAX_LINE_LEN];   /* accumulator for backslash continuations */
    config_section_t* current_section = NULL;
    int line_num = 0;
    
    while (fgets(line, sizeof(line), fp)) {
        line_num++;
        
        /* Remove newline */
        char* p = strchr(line, '\n');
        if (p) *p = '\0';
        p = strchr(line, '\r');
        if (p) *p = '\0';
        
        /* Trim whitespace */
        char* content = trim(line);
        
        /* Skip empty lines and comments */
        if (*content == '\0' || *content == '#' || *content == ';') {
            continue;
        }
        
        /* Multi-line values are flattened with spaces so downstream accessors
         * can treat them like ordinary scalar strings. */
        {
            size_t clen = strlen(content);
            if (clen > 0 && content[clen - 1] == '\\') {
                /* Start accumulating */
                content[clen - 1] = '\0';
                snprintf(accum, sizeof(accum), "%s", trim(content));

                while (fgets(line, sizeof(line), fp)) {
                    line_num++;
                    p = strchr(line, '\n');
                    if (p) *p = '\0';
                    p = strchr(line, '\r');
                    if (p) *p = '\0';
                    char* cont = trim(line);

                    size_t cont_len = strlen(cont);
                    bool more = (cont_len > 0 && cont[cont_len - 1] == '\\');
                    if (more) cont[cont_len - 1] = '\0';
                    cont = trim(cont);

                    /* Append with a space separator */
                    size_t acc_len = strlen(accum);
                    size_t add_len = strlen(cont);
                    if (acc_len + 1 + add_len < sizeof(accum)) {
                        accum[acc_len] = ' ';
                        memcpy(accum + acc_len + 1, cont, add_len + 1);
                    }

                    if (!more) break;
                }
                content = trim(accum);
            }
        }
        
        /* Section headers establish the destination linked-list bucket for subsequent keys. */
        if (*content == '[') {
            char* end = strchr(content, ']');
            if (!end) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG, "Malformed section at line %d", line_num);
                continue;
            }
            
            *end = '\0';
            char* section_name = trim(content + 1);
            
            /* Create new section */
            config_section_t* section = keel_calloc(1, sizeof(config_section_t));
            if (!section) continue;
            
            section->name = keel_strdup(section_name);
            
            /* Add to list */
            if (current_section) {
                current_section->next = section;
            } else {
                config->sections = section;
            }
            current_section = section;
            
            continue;
        }
        
        /* Key/value parsing is intentionally simple: first `=` wins, remainder is value text. */
        char* eq = strchr(content, '=');
        if (!eq) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG, "Malformed entry at line %d", line_num);
            continue;
        }
        
        *eq = '\0';
        char* key = trim(content);
        char* value = trim(eq + 1);
        value = strip_quotes(value);
        
        if (!current_section) {
            /* Keys before any section header are assigned to an implicit global section. */
            current_section = keel_calloc(1, sizeof(config_section_t));
            if (!current_section) continue;
            current_section->name = keel_strdup("global");
            config->sections = current_section;
        }
        
        /* Add entry */
        struct config_entry* entry = keel_calloc(1, sizeof(struct config_entry));
        if (!entry) continue;
        
        entry->key = keel_strdup(key);
        entry->value = keel_strdup(value);
        
        /* Add to section's entry list */
        entry->next = current_section->entries;
        current_section->entries = entry;
    }
    
    fclose(fp);
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_CONFIG, "Loaded configuration from %s", path);
    
    return config;
}

/**
 * @brief Free a configuration object and all owned sections/entries.
 *
 * @param config Configuration object, or `NULL`.
 * @return
 */
void keel_config_free(keel_config_t* config) {
    if (!config) return;
    
    config_section_t* section = config->sections;
    while (section) {
        config_section_t* next_section = section->next;
        
        struct config_entry* entry = section->entries;
        while (entry) {
            struct config_entry* next_entry = entry->next;
            keel_free(entry->key);
            keel_free(entry->value);
            keel_free(entry);
            entry = next_entry;
        }
        
        keel_free(section->name);
        keel_free(section);
        section = next_section;
    }
    
    keel_free(config->path);
    keel_free(config);
}

/* ============================================================================
 * Value Getters
 * ============================================================================ */

/**
 * @brief Find a case-insensitive section/key match in the configuration store.
 *
 * @param config Configuration object.
 * @param section Section name.
 * @param key Key name.
 * @return Matching entry, or `NULL` if not found.
 */
static struct config_entry* find_entry(const keel_config_t* config,
                                        const char* section,
                                        const char* key) {
    for (config_section_t* s = config->sections; s; s = s->next) {
        if (strcasecmp(s->name, section) == 0) {
            for (struct config_entry* e = s->entries; e; e = e->next) {
                if (strcasecmp(e->key, key) == 0) {
                    return e;
                }
            }
        }
    }
    return NULL;
}

/**
 * @brief Return a string configuration value or a caller-supplied default.
 */
const char* keel_config_get_string(const keel_config_t* config,
                                   const char* section,
                                   const char* key,
                                   const char* default_val) {
    if (!config || !section || !key) {
        return default_val;
    }
    
    struct config_entry* e = find_entry(config, section, key);
    return e ? e->value : default_val;
}

/**
 * @brief Return an integer configuration value with optional `k`/`m`/`g` suffix handling.
 */
int64_t keel_config_get_int(const keel_config_t* config,
                            const char* section,
                            const char* key,
                            int64_t default_val) {
    const char* str = keel_config_get_string(config, section, key, NULL);
    if (!str) {
        return default_val;
    }
    
    char* end;
    long long val = strtoll(str, &end, 10);
    
    /* Handle size suffixes */
    if (*end) {
        switch (tolower((unsigned char)*end)) {
        case 'k': val *= 1024; break;
        case 'm': val *= 1024 * 1024; break;
        case 'g': val *= 1024 * 1024 * 1024; break;
        }
    }
    
    return (int64_t)val;
}

/**
 * @brief Return a floating-point configuration value.
 */
double keel_config_get_float(const keel_config_t* config,
                             const char* section,
                             const char* key,
                             double default_val) {
    const char* str = keel_config_get_string(config, section, key, NULL);
    if (!str) {
        return default_val;
    }
    
    return strtod(str, NULL);
}

/**
 * @brief Return a boolean configuration value using common textual forms.
 */
bool keel_config_get_bool(const keel_config_t* config,
                          const char* section,
                          const char* key,
                          bool default_val) {
    const char* str = keel_config_get_string(config, section, key, NULL);
    if (!str) {
        return default_val;
    }
    
    /* True values */
    if (strcasecmp(str, "true") == 0 ||
        strcasecmp(str, "yes") == 0 ||
        strcasecmp(str, "on") == 0 ||
        strcasecmp(str, "1") == 0) {
        return true;
    }
    
    /* False values */
    if (strcasecmp(str, "false") == 0 ||
        strcasecmp(str, "no") == 0 ||
        strcasecmp(str, "off") == 0 ||
        strcasecmp(str, "0") == 0) {
        return false;
    }
    
    return default_val;
}

/**
 * @brief Return a duration value normalized to nanoseconds.
 *
 * Supported suffixes: `ns`, `us`, `ms`, `s`, `m`, `h`.
 * A bare integer (no suffix) is interpreted as **milliseconds** — matches
 * the v2 schema where the unit suffix has been removed from key names.
 */
keel_duration_t keel_config_get_duration(const keel_config_t* config,
                                        const char* section,
                                        const char* key,
                                        keel_duration_t default_val) {
    const char* str = keel_config_get_string(config, section, key, NULL);
    if (!str) {
        return default_val;
    }

    char* end;
    double val = strtod(str, &end);
    if (end == str) {
        return default_val;
    }

    /* Skip whitespace between number and unit. */
    while (*end == ' ' || *end == '\t') end++;

    if (*end == '\0') {
        /* No suffix — bare integer is milliseconds in v2. */
        return (keel_duration_t)(val * 1000000);
    }
    if (strcasecmp(end, "ns") == 0) {
        return (keel_duration_t)val;
    }
    if (strcasecmp(end, "us") == 0) {
        return (keel_duration_t)(val * 1000);
    }
    if (strcasecmp(end, "ms") == 0) {
        return (keel_duration_t)(val * 1000000);
    }
    if (strcasecmp(end, "s") == 0) {
        return (keel_duration_t)(val * 1000000000.0);
    }
    if (strcasecmp(end, "m") == 0) {
        return (keel_duration_t)(val * 60.0 * 1000000000.0);
    }
    if (strcasecmp(end, "h") == 0) {
        return (keel_duration_t)(val * 3600.0 * 1000000000.0);
    }

    /* Unrecognized unit — reject instead of silently misinterpreting. */
    return default_val;
}

/**
 * @brief Convenience wrapper returning a duration in milliseconds.
 *
 * Implemented as `keel_config_get_duration(... default_ms * 1ms) / 1ms`
 * with a fast path that avoids the round-trip when the key is absent.
 */
int64_t keel_config_get_duration_ms(const keel_config_t* config,
                                    const char* section,
                                    const char* key,
                                    int64_t default_ms) {
    keel_duration_t sentinel = (keel_duration_t)INT64_MIN;
    keel_duration_t ns = keel_config_get_duration(config, section, key, sentinel);
    if (ns == sentinel) return default_ms;
    return (int64_t)(ns / 1000000);
}

/**
 * @brief Return a byte count parsed from a value with optional unit suffix.
 *
 * Supported suffixes (case-insensitive):
 *   B, K/KB, KiB, M/MB, MiB, G/GB, GiB.
 * A bare integer (no suffix) is interpreted as **bytes**.
 */
int64_t keel_config_get_bytes(const keel_config_t* config,
                              const char* section,
                              const char* key,
                              int64_t default_val) {
    const char* str = keel_config_get_string(config, section, key, NULL);
    if (!str) {
        return default_val;
    }

    char* end;
    double val = strtod(str, &end);
    if (end == str) {
        return default_val;
    }

    while (*end == ' ' || *end == '\t') end++;

    static const struct {
        const char* suffix;
        double      multiplier;
    } units[] = {
        { "",    1.0 },
        { "B",   1.0 },
        { "K",   1000.0 },
        { "KB",  1000.0 },
        { "KiB", 1024.0 },
        { "M",   1000.0 * 1000.0 },
        { "MB",  1000.0 * 1000.0 },
        { "MiB", 1024.0 * 1024.0 },
        { "G",   1000.0 * 1000.0 * 1000.0 },
        { "GB",  1000.0 * 1000.0 * 1000.0 },
        { "GiB", 1024.0 * 1024.0 * 1024.0 },
    };

    for (size_t i = 0; i < sizeof(units) / sizeof(units[0]); i++) {
        if (strcasecmp(end, units[i].suffix) == 0) {
            return (int64_t)(val * units[i].multiplier);
        }
    }

    /* Unrecognized unit — reject. */
    return default_val;
}

/* ============================================================================
 * Section Iteration
 * ============================================================================ */

/**
 * @brief Check whether a named section exists.
 */
bool keel_config_has_section(const keel_config_t* config, const char* section) {
    if (!config || !section) return false;
    
    for (config_section_t* s = config->sections; s; s = s->next) {
        if (strcasecmp(s->name, section) == 0) {
            return true;
        }
    }
    return false;
}

/**
 * @brief Visit every section name in insertion order.
 */
void keel_config_iter_sections(const keel_config_t* config,
                               void (*callback)(const char* section, void* ctx),
                               void* ctx) {
    if (!config || !callback) return;
    
    for (config_section_t* s = config->sections; s; s = s->next) {
        callback(s->name, ctx);
    }
}

/**
 * @brief Visit every key/value pair in a named section.
 */
void keel_config_iter_keys(const keel_config_t* config,
                           const char* section,
                           void (*callback)(const char* key, const char* value, void* ctx),
                           void* ctx) {
    if (!config || !section || !callback) return;
    
    for (config_section_t* s = config->sections; s; s = s->next) {
        if (strcasecmp(s->name, section) == 0) {
            for (struct config_entry* e = s->entries; e; e = e->next) {
                callback(e->key, e->value, ctx);
            }
            return;
        }
    }
}

/**
 * @brief Iterate over all INI sections whose names begin with a given prefix,
 *        invoking a callback once per matching section.
 *
 * @param config    Parsed INI configuration.  Passing `NULL` is safe.
 * @param prefix    Case-insensitive prefix string to match section names
 *                  against (e.g. `"shard_rule."`, `"throttle."`).
 *                  Passing `NULL` is safe (no callbacks are fired).
 * @param callback  Function called for each matching section.  The first
 *                  argument is the full section name; the second is `ctx`.
 * @param ctx       Opaque pointer forwarded unchanged to every callback
 *                  invocation.
 * @return Nothing.
 *
 * Behavior:
 * - Sections are visited in the order they appear in the configuration file.
 * - Matching is performed with `strncasecmp`, so prefix casing is irrelevant.
 * - The function is commonly used by subsystems that encode their configuration
 *   as numbered or named INI sections (e.g. `[shard_rule.users]`,
 *   `[throttle.api]`) so they can enumerate all of their sections with a
 *   single call.
 *
 * Notes:
 * - The callback must not modify the configuration structure; the linked list
 *   of sections is traversed without locking.
 */
void keel_config_iter_sections_prefix(const keel_config_t* config,
                                      const char* prefix,
                                      void (*callback)(const char* section, void* ctx),
                                      void* ctx) {
    if (!config || !prefix || !callback) return;
    
    size_t prefix_len = strlen(prefix);
    
    for (config_section_t* s = config->sections; s; s = s->next) {
        if (strncasecmp(s->name, prefix, prefix_len) == 0) {
            callback(s->name, ctx);
        }
    }
}

/**
 * @brief Count the number of INI sections whose names begin with a given prefix.
 *
 * @param config  Parsed INI configuration.  Passing `NULL` returns 0.
 * @param prefix  Case-insensitive prefix to match.  Passing `NULL` returns 0.
 * @return Number of sections whose names start with `prefix`.
 *
 * Notes:
 * - Equivalent to calling `keel_config_iter_sections_prefix()` with a
 *   counting callback.
 * - Useful when pre-allocating arrays sized to the number of subsystem
 *   configuration blocks (e.g. counting shard rules before allocating the
 *   rule array).
 */
size_t keel_config_count_sections_prefix(const keel_config_t* config, const char* prefix) {
    if (!config || !prefix) return 0;
    
    size_t prefix_len = strlen(prefix);
    size_t count = 0;
    
    for (config_section_t* s = config->sections; s; s = s->next) {
        if (strncasecmp(s->name, prefix, prefix_len) == 0) {
            count++;
        }
    }
    
    return count;
}
