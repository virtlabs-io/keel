/**
 * @file config_migrate.h
 * @brief INI configuration migration between schema versions.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * KEEL's configuration schema is versioned via the `config_version` key in
 * the `[keel]` section. v2 removes unit suffixes from key names
 * (`idle_timeout_ms` -> `idle_timeout`) and pushes the unit into the value
 * (`idle_timeout = 5m`, or bare integer = milliseconds for durations,
 * bytes for byte counts).
 *
 * This header exposes a programmatic migrator used by the
 * `keel --migrate-config` CLI subcommand and by the configuration test
 * suite. The migration is purely textual: it preserves comments, blank
 * lines, and section ordering so the diff between input and output is
 * limited to renamed keys plus the injected `config_version = 2` marker.
 */

#ifndef KEEL_CONFIG_MIGRATE_H
#define KEEL_CONFIG_MIGRATE_H

#include <stdio.h>

#include "keel_error.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Current INI/YAML schema version expected by the KEEL runtime. */
#define KEEL_CONFIG_SCHEMA_VERSION 2

/**
 * @brief Transform an INI configuration stream from any prior schema
 *        version to the current `KEEL_CONFIG_SCHEMA_VERSION`.
 *
 * The migration is line-based and lossless with respect to non-key
 * content: comments (`# ...` / `; ...`), blank lines, and section
 * ordering are preserved. Renamed keys are rewritten in place while
 * keeping the surrounding whitespace and any trailing inline comment.
 *
 * `config_version = 2` is injected as the first non-blank line of the
 * `[keel]` section. If the input has no `[keel]` section, one is
 * prepended to the output.
 *
 * The migrator is idempotent: running it on a v2 input produces an
 * equivalent v2 output (no double-renames, no duplicate version key).
 *
 * @param in  Input INI stream. Must be open for reading. Not closed by
 *            this function.
 * @param out Output stream for the transformed INI. Must be open for
 *            writing. Not closed by this function.
 * @return KEEL_OK on success; a `keel_error_t` value otherwise.
 */
keel_error_t keel_config_migrate(FILE* in, FILE* out);

/**
 * @brief Convenience wrapper that opens `in_path` and `out_path` and
 *        calls `keel_config_migrate`.
 *
 * @param in_path  Path to a readable INI configuration file.
 * @param out_path Path to write the migrated INI to. Pass `NULL` or
 *                 `"-"` to write to `stdout`.
 * @return KEEL_OK on success; a `keel_error_t` value otherwise.
 */
keel_error_t keel_config_migrate_file(const char* in_path,
                                      const char* out_path);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_CONFIG_MIGRATE_H */
