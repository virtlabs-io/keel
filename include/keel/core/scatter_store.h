/**
 * @file scatter_store.h
 * @brief Scatter-merge result store: in-memory row collection with transparent
 *        spill-to-disk when the configured memory limit is exceeded.
 *
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 *
 * ---
 *
 * ## Overview
 *
 * The scatter store collects result rows from multiple shards so that the
 * proxy can merge them (ORDER BY, aggregate reduction, GROUP BY) before
 * returning a unified response to the client.
 *
 * Two storage backends are managed transparently:
 *
 *   - **Memory path** — rows land in compact slab arenas.  Fast for small
 *     result sets.
 *   - **Spill path** — when accumulated bytes exceed `mem_limit_bytes`, all
 *     in-memory rows are written to a temporary file, the slab is released,
 *     and subsequent rows are written directly to disk.  The caller sees the
 *     same API in both cases.
 *
 * ## Disk format
 *
 * Spill files use a compact block-oriented format designed for sequential I/O:
 *
 *   File header (12 bytes fixed, then per-column metadata):
 *     [magic: uint32_t]                      -- KEEL_SPILL_MAGIC
 *     [version: uint8_t]                     -- KEEL_SPILL_VERSION
 *     [pad: uint8_t[3]]
 *     [ncols: uint16_t]
 *     [pad: uint16_t]
 *     [col_types: uint32_t[ncols]]
 *     [col_formats: uint16_t[ncols]]         -- 0=text, 1=binary
 *
 *   Rows (tightly packed, no alignment padding between rows):
 *     [total_bytes: uint32_t]                -- includes this field; 0 = EOF
 *     [col 0: len: int32_t, data: len bytes if len >= 0]
 *     [col 1: len: int32_t, data: len bytes if len >= 0]
 *     ...
 *
 * A sentinel row with total_bytes == 0 marks the end of data.  The write
 * buffer is flushed before iteration begins, so a clean sentinel is always
 * present.
 *
 * Writes are buffered at KEEL_SPILL_BLOCK_SIZE (64 KiB) to amortise syscall
 * overhead.  Reads during iteration use an equal-sized buffer so each pass
 * over the file makes a minimal number of read(2) calls.
 *
 * ## Column type comparator
 *
 * `keel_scatter_col_cmp()` dispatches over the generic `keel_col_type_t`
 * families that cover all common SQL types (integers, floats, text,
 * date/time, UUID, bool) and handles both text and binary wire formats.
 * Unknown types fall back to byte-lexicographic comparison.
 *
 * Database-specific type identifiers (PostgreSQL OIDs, MySQL type bytes)
 * must be mapped to `keel_col_type_t` by the protocol layer before being
 * stored in a `keel_scatter_col_desc_t`.  See:
 *   - include/keel/protocol/postgres/pg_scatter.h  (keel_pg_oid_to_col_type)
 *   - include/keel/protocol/mysql_scatter.h        (keel_mysql_type_to_col_type)
 */

#ifndef KEEL_CORE_SCATTER_STORE_H
#define KEEL_CORE_SCATTER_STORE_H

#include "keel_error.h"
#include "keel_types.h"
#include "keel/mem/mem.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Constants
 * ============================================================================ */

/** I/O block size for spill-file reads and writes (64 KiB). */
#define KEEL_SPILL_BLOCK_SIZE        (64U * 1024U)

/** Magic number in the spill-file header ('KEES'). */
#define KEEL_SPILL_MAGIC             0x4B454553U

/** Spill-file format version. */
#define KEEL_SPILL_VERSION           1U

/** Default in-memory limit per result set: 32 MiB. */
#define KEEL_SCATTER_DEFAULT_MEM_LIMIT_BYTES   (32UL * 1024UL * 1024UL)

/** Minimum allowed memory limit: 1 MiB. */
#define KEEL_SCATTER_MIN_MEM_LIMIT_BYTES       (1UL * 1024UL * 1024UL)

/** Default directory for temporary spill files. */
#define KEEL_SCATTER_DEFAULT_SPILL_DIR         "/tmp"

/** Maximum column name length stored in keel_scatter_col_desc_t. */
#define KEEL_SCATTER_COL_NAME_MAX    64

/* ============================================================================
 * Generic SQL column type families
 *
 * The scatter store uses this DB-agnostic enum for comparison and merge
 * dispatch.  Protocol layers (PostgreSQL, MySQL) map their native type
 * identifiers to these families via keel_pg_oid_to_col_type() /
 * keel_mysql_type_to_col_type() respectively.
 * ============================================================================ */

/**
 * @brief Generic SQL column type family used by the scatter-merge engine.
 *
 * Covers the common SQL type families.  Values intentionally do not match
 * any database's internal type numbering.
 */
typedef enum keel_col_type {
    KEEL_COL_TYPE_UNKNOWN   =  0, /**< Unrecognised — fallback byte comparison */
    KEEL_COL_TYPE_BOOL      =  1, /**< Boolean                               */
    KEEL_COL_TYPE_INT16     =  2, /**< 16-bit integer (smallint)             */
    KEEL_COL_TYPE_INT32     =  3, /**< 32-bit integer (int, oid)             */
    KEEL_COL_TYPE_INT64     =  4, /**< 64-bit integer (bigint)               */
    KEEL_COL_TYPE_FLOAT32   =  5, /**< 32-bit float (real)                   */
    KEEL_COL_TYPE_FLOAT64   =  6, /**< 64-bit float / numeric                */
    KEEL_COL_TYPE_TEXT      =  7, /**< Text (char, varchar, text, name, …)   */
    KEEL_COL_TYPE_DATE      =  8, /**< Calendar date                         */
    KEEL_COL_TYPE_TIME      =  9, /**< Time of day                           */
    KEEL_COL_TYPE_TIMESTAMP = 10, /**< Date + time (with or without TZ)      */
    KEEL_COL_TYPE_UUID      = 11, /**< UUID                                  */
    KEEL_COL_TYPE_BYTES     = 12, /**< Raw binary / bytea                    */
    KEEL_COL_TYPE_TEXT_ARRAY = 13, /**< Text array (e.g., TEXT[], VARCHAR[])  */
    KEEL_COL_TYPE_JSONB      = 14, /**< JSONB                                 */
} keel_col_type_t;

/**
 * @brief Wire encoding format for a column value.
 *
 * TEXT   — value bytes are a human-readable string (e.g. "42", "true").
 * BINARY — value bytes use the database's native binary encoding.
 */
typedef enum keel_wire_format {
    KEEL_WIRE_TEXT   = 0, /**< Text encoding   */
    KEEL_WIRE_BINARY = 1, /**< Binary encoding */
} keel_wire_format_t;

/* ============================================================================
 * Column metadata
 * ============================================================================ */

/**
 * @brief Column metadata for one column in a scatter result set.
 *
 * Populated once per result set when the first row description is received
 * from any shard backend.  The @c name field is null-terminated and truncated
 * to @c KEEL_SCATTER_COL_NAME_MAX-1 characters.
 *
 * @c type holds the generic SQL type family; protocol layers must map their
 * native type identifiers to @ref keel_col_type_t before storing here.
 * @c format indicates whether column bytes use text or binary encoding.
 * The remaining fields (@c table_id, @c col_num, @c type_len, @c type_mod)
 * carry protocol-specific hints; set to 0 / -1 when not applicable.
 */
typedef struct keel_scatter_col_desc {
    char               name[KEEL_SCATTER_COL_NAME_MAX]; /**< Column name (null-terminated) */
    keel_col_type_t    type;       /**< Generic SQL type family                           */
    keel_wire_format_t format;     /**< Wire encoding: TEXT or BINARY                    */
    uint32_t           table_id;   /**< Source table identifier (0 = computed/unknown)   */
    int32_t            type_mod;   /**< Type modifier / precision (-1 = unspecified)      */
    int16_t            col_num;    /**< Column position in source table (0 = expression)  */
    int16_t            type_len;   /**< Byte length hint (-1 = variable, -2 = cstring)    */
    int16_t            _pad;
} keel_scatter_col_desc_t;

/* ============================================================================
 * Row value representation
 * ============================================================================ */

/**
 * @brief A single decoded column value.
 *
 * When @c len == -1 the column is SQL NULL and @c data is undefined.
 * Otherwise @c data points to exactly @c len raw bytes in the wire format
 * (text or binary) specified by the column's @ref keel_scatter_col_desc_t.format.
 *
 * Lifetime note: the @c data pointer is valid only as long as the owning
 * context (slab or read buffer) is alive.  Callers that need longer-lived
 * values must copy the bytes before the next @ref keel_scatter_result_iter_next
 * call or @ref keel_scatter_result_destroy.
 */
typedef struct keel_scatter_col_val {
    int32_t     len;   /**< Byte length; -1 = SQL NULL */
    const char* data;  /**< Raw column bytes (not null-terminated) */
} keel_scatter_col_val_t;

/**
 * @brief A single materialized row stored in-slab.
 *
 * The struct is allocated in contiguous slab memory as:
 * @code
 *   [keel_scatter_row_t header (ncols field + padding)]
 *   [keel_scatter_col_val_t cols[ncols]]
 *   [raw data bytes for all columns]
 * @endcode
 *
 * The @c cols[i].data pointers reference the raw-data region within the same
 * contiguous allocation and are therefore valid for the lifetime of the
 * owning slab.
 */
typedef struct keel_scatter_row {
    uint16_t               ncols;
    uint16_t               _pad[3];
    keel_scatter_col_val_t cols[]; /**< Flexible array — ncols entries */
} keel_scatter_row_t;

/* ============================================================================
 * Result store
 * ============================================================================ */

/**
 * @brief Materialized result set for scatter-merge operations.
 *
 * Use @ref keel_scatter_result_create to allocate, @ref keel_scatter_result_append to
 * add rows, @ref keel_scatter_result_iter_init + @ref keel_scatter_result_iter_next to
 * scan, and @ref keel_scatter_result_destroy to release all resources including
 * the spill file.
 *
 * Thread safety: not thread-safe.  One instance should be used by a single
 * scatter-merge context at a time.
 */
typedef struct keel_scatter_result {
    /* Column metadata (allocated via keel_malloc, owned by this struct) */
    uint16_t                  ncols;
    keel_scatter_col_desc_t*  cols;            /**< [ncols] column descriptors */

    /* Memory limit */
    size_t                    mem_limit_bytes; /**< Spill threshold from config */

    /* Memory-resident rows */
    keel_arena_t*             slab;            /**< Bump allocator for row data */
    keel_scatter_row_t**      rows;            /**< Realloc'd pointer array into slab */
    size_t                    rows_cap;        /**< Allocated capacity of rows[] */

    /* Aggregate counters */
    size_t                    row_count;       /**< Total rows (memory + spill) */
    size_t                    mem_bytes;       /**< Bytes currently in memory */

    /* Spill state */
    bool                      spilled;         /**< True once spill threshold was crossed */
    int                       spill_fd;        /**< Spill file descriptor; -1 = none */
    char                      spill_path[256]; /**< Path to temp file (for unlink on destroy) */
    size_t                    spill_row_count; /**< Rows written to disk */
    size_t                    spill_bytes;     /**< Total payload bytes in spill file */

    /* Spill write buffer (KEEL_SPILL_BLOCK_SIZE bytes, keel_malloc'd on first spill) */
    char*                     write_buf;
    size_t                    write_buf_pos;

    /* Spill directory (pointer into engine config — not owned) */
    const char*               spill_dir;
} keel_scatter_result_t;

/* ============================================================================
 * Iterator
 * ============================================================================ */

/**
 * @brief Forward-only row iterator over a @ref keel_scatter_result_t.
 *
 * Allocate on the stack and initialise with @ref keel_scatter_result_iter_init.
 * Call @ref keel_scatter_result_iter_next in a loop; the returned @c vals pointer
 * is valid until the subsequent @ref keel_scatter_result_iter_next call.
 * Release resources with @ref keel_scatter_result_iter_close.
 *
 * @code
 * keel_scatter_result_iter_t it;
 * if (keel_scatter_result_iter_init(&it, result) == KEEL_OK) {
 *     const keel_scatter_col_val_t* vals;
 *     while (keel_scatter_result_iter_next(&it, &vals)) {
 *         // use vals[0..ncols-1]
 *     }
 *     keel_scatter_result_iter_close(&it);
 * }
 * @endcode
 */
typedef struct keel_scatter_result_iter {
    keel_scatter_result_t*  result;          /**< Back-pointer to the owning result */
    size_t                  rows_returned;   /**< Running count of rows delivered */

    /* Spill-path read buffer (NULL for memory-only results) */
    char*                   read_buf;        /**< keel_malloc'd; grows for large columns */
    size_t                  read_buf_cap;    /**< Allocated capacity of read_buf */
    size_t                  read_buf_valid;  /**< Valid bytes in read_buf */
    size_t                  read_buf_pos;    /**< Current parse position */
    size_t                  row_start_pos;   /**< Buffer offset at start of current row */

    /* Decoded column values for the current row.  For the memory path these
     * are a shallow copy from the row struct.  For the spill path data
     * pointers reference read_buf (valid until next call).  keel_malloc'd. */
    keel_scatter_col_val_t* vals;
} keel_scatter_result_iter_t;

/* ============================================================================
 * Result lifecycle
 * ============================================================================ */

/**
 * @brief Allocate and initialise a result store.
 *
 * @param ncols           Number of columns in each row.
 * @param cols            Column descriptors; copied internally.
 * @param mem_limit_bytes Maximum bytes held in memory before spilling.
 *                        Pass 0 to use KEEL_SCATTER_DEFAULT_MEM_LIMIT_BYTES.
 *                        Values below KEEL_SCATTER_MIN_MEM_LIMIT_BYTES are
 *                        clamped up silently.
 * @param spill_dir       Directory for temporary spill files.
 *                        Pass NULL to use KEEL_SCATTER_DEFAULT_SPILL_DIR.
 *                        The pointer is stored (not copied) — must remain
 *                        valid for the lifetime of the result.
 * @return Heap-allocated result, or NULL on allocation failure.
 */
KEEL_NODISCARD
keel_scatter_result_t* keel_scatter_result_create(
    uint16_t                        ncols,
    const keel_scatter_col_desc_t*  cols,
    size_t                          mem_limit_bytes,
    const char*                     spill_dir);

/**
 * @brief Release all resources owned by a result store.
 *
 * Destroys the slab arena, frees the row-pointer array, closes the spill
 * file descriptor, and unlinks the spill file from disk.  After this call
 * the pointer is invalid.
 *
 * @param r Result to destroy.  NULL is a safe no-op.
 */
void keel_scatter_result_destroy(keel_scatter_result_t* r);

/* ============================================================================
 * Row insertion
 * ============================================================================ */

/**
 * @brief Append one row to the result store.
 *
 * Copies the column values (including data bytes) into the store.  If the
 * accumulated in-memory bytes would exceed @c mem_limit_bytes, all current
 * in-memory rows are flushed to the spill file before the new row is
 * written to disk.
 *
 * @param r    Result store.
 * @param vals Column values for the new row.  @c vals[i].data must point to
 *             at least @c vals[i].len bytes; NULL data is only valid when
 *             @c len == -1.
 * @return KEEL_OK on success.
 *         KEEL_ERR_IO if a spill-file write fails.
 *         KEEL_ERR_NOMEM if a slab allocation fails.
 */
keel_error_t keel_scatter_result_append(keel_scatter_result_t*        r,
                                    const keel_scatter_col_val_t* vals);

/* ============================================================================
 * Iterator lifecycle
 * ============================================================================ */

/**
 * @brief Initialise an iterator over all rows in the result.
 *
 * For spill-path results: flushes any pending write-buffer data and rewinds
 * the file to the first row before returning.
 *
 * @param it  Iterator to initialise (caller-allocated, any storage class).
 * @param r   Result to iterate.
 * @return KEEL_OK on success.
 *         KEEL_ERR_IO if the spill-file flush or seek fails.
 *         KEEL_ERR_NOMEM if internal buffers cannot be allocated.
 */
keel_error_t keel_scatter_result_iter_init(keel_scatter_result_iter_t* it,
                                       keel_scatter_result_t*      r);

/**
 * @brief Advance the iterator and return the next row.
 *
 * @param it       Iterator context.
 * @param vals_out Set to the column values of the current row on success.
 *                 Valid until the next call to this function or
 *                 @ref keel_scatter_result_iter_close.
 * @return true while rows remain; false when iteration is complete or on I/O
 *         error.  Check @ref keel_scatter_result_iter_has_error for the
 *         distinction if needed.
 */
bool keel_scatter_result_iter_next(keel_scatter_result_iter_t*    it,
                               const keel_scatter_col_val_t** vals_out);

/**
 * @brief Release resources held by the iterator.
 *
 * Does not affect the underlying result store.  Safe to call on an iterator
 * that was never initialised or has already been closed.
 *
 * @param it Iterator to close.
 */
void keel_scatter_result_iter_close(keel_scatter_result_iter_t* it);

/* ============================================================================
 * Introspection
 * ============================================================================ */

/**
 * @brief Total number of rows appended (memory + spill).
 */
size_t keel_scatter_result_row_count(const keel_scatter_result_t* r);

/**
 * @brief Number of column descriptors.
 */
uint16_t keel_scatter_result_ncols(const keel_scatter_result_t* r);

/**
 * @brief True if the result has spilled at least once to disk.
 */
bool keel_scatter_result_spilled(const keel_scatter_result_t* r);

/**
 * @brief Current in-memory byte usage (slab + row-pointer array).
 */
size_t keel_scatter_result_mem_bytes(const keel_scatter_result_t* r);

/**
 * @brief Total bytes written to the spill file (0 if not spilled).
 */
size_t keel_scatter_result_spill_bytes(const keel_scatter_result_t* r);

/* ============================================================================
 * Generic column comparator
 * ============================================================================ */

/**
 * @brief Compare two column values according to the generic SQL type family.
 *
 * Dispatches over @p type (a @ref keel_col_type_t value) and handles both
 * text (@c KEEL_WIRE_TEXT) and binary (@c KEEL_WIRE_BINARY) wire encodings.
 * SQL NULLs sort before non-NULL values (NULLS FIRST).
 *
 * | keel_col_type_t    | Text encoding             | Binary encoding           |
 * |--------------------|---------------------------|---------------------------|
 * | BOOL               | "t"/"f" → true>false      | single byte               |
 * | INT16              | strtol                    | big-endian int16          |
 * | INT32              | strtol                    | big-endian int32          |
 * | INT64              | strtoll                   | big-endian int64          |
 * | FLOAT32            | strtof                    | big-endian float32        |
 * | FLOAT64            | strtod                    | big-endian float64        |
 * | TEXT               | memcmp then by length     | memcmp then by length     |
 * | DATE / TIME        | lexicographic (ISO)       | big-endian int32/int64    |
 * | TIMESTAMP          | lexicographic (ISO)       | big-endian int64 (µs)     |
 * | UUID               | lexicographic             | 16-byte memcmp            |
 * | UNKNOWN / BYTES    | memcmp then by length     | memcmp then by length     |
 *
 * @param type    Generic SQL type family.
 * @param format  Wire encoding format (TEXT or BINARY).
 * @param a       First value.
 * @param b       Second value.
 * @return Negative if a < b, zero if a == b, positive if a > b.
 */
int keel_scatter_col_cmp(
    keel_col_type_t               type,
    keel_wire_format_t            format,
    const keel_scatter_col_val_t* a,
    const keel_scatter_col_val_t* b);

/* ============================================================================
 * ORDER BY / LIMIT merge support (Phase C)
 * ============================================================================ */

/** Maximum number of ORDER BY keys supported in a scatter merge. */
#define KEEL_SCATTER_MAX_ORDER_KEYS  8

/**
 * @brief Sort direction for a single ORDER BY key.
 *
 * Deliberately distinct from the SQL AST enum so that scatter_store.h
 * does not depend on sql_ast.h.
 */
typedef enum keel_sort_dir {
    KEEL_SORT_ASC  = 0, /**< Ascending  */
    KEEL_SORT_DESC = 1, /**< Descending */
} keel_sort_dir_t;

/**
 * @brief NULL ordering for a single ORDER BY key.
 */
typedef enum keel_sort_nulls {
    KEEL_SORT_NULLS_DEFAULT = 0, /**< Default: NULLS LAST for ASC, NULLS FIRST for DESC */
    KEEL_SORT_NULLS_FIRST   = 1,
    KEEL_SORT_NULLS_LAST    = 2,
} keel_sort_nulls_t;

/**
 * @brief One column specification within an ORDER BY key sequence.
 *
 * @c col_index is the 0-based column position in the result's
 * @ref keel_scatter_col_desc_t array.  It must be set to a valid index before
 * passing to @ref keel_scatter_result_sort; a value of @c KEEL_SORT_COL_UNRESOLVED
 * means the key should be skipped.
 */
#define KEEL_SORT_COL_UNRESOLVED  (-1)

typedef struct keel_sort_key {
    int16_t           col_index; /**< 0-based column index, or KEEL_SORT_COL_UNRESOLVED */
    keel_sort_dir_t   dir;       /**< ASC / DESC */
    keel_sort_nulls_t nulls;     /**< NULLS FIRST / LAST / DEFAULT */
} keel_sort_key_t;

/**
 * @brief Sort the rows in a result store in-place by the given key sequence.
 *
 * For memory-resident results the existing @c rows[] pointer array is sorted
 * with @c qsort_r.  For spilled results every row is re-read into memory,
 * sorted, and written back — this is an O(N log N) + 2×disk-pass operation
 * and is intentionally avoided at the query-execution layer by choosing an
 * appropriately large @c mem_limit_bytes.
 *
 * Only keys with @c col_index >= 0 are used; keys with
 * @c KEEL_SORT_COL_UNRESOLVED are silently skipped.
 *
 * A null or zero-length key list is a no-op and returns @c KEEL_OK.
 *
 * @param r     Result store to sort.
 * @param keys  Array of sort keys (primary, secondary, …).
 * @param nkeys Number of entries in @p keys.
 * @return @c KEEL_OK on success; @c KEEL_ERR_NOMEM if re-materialisation
 *         fails; @c KEEL_ERR_IO if the spill file cannot be rewritten.
 */
KEEL_NODISCARD
keel_error_t keel_scatter_result_sort(keel_scatter_result_t*      r,
                                  const keel_sort_key_t* keys,
                                  uint16_t               nkeys);

/**
 * @brief Truncate the result to @p limit rows beginning at @p offset.
 *
 * Intended to be called after @ref keel_scatter_result_sort so that global ORDER
 * BY + LIMIT semantics are preserved across shards.
 *
 * - @p offset rows at the head are discarded.
 * - At most @p limit rows remain after the offset is applied.
 * - Passing @c limit == 0 means "no limit" (only the offset is applied).
 * - Passing @c offset == 0 means "start from the first row".
 *
 * For memory-resident results the @c rows[] array and @c row_count are
 * updated in O(1).  For spilled results the file is truncated to the
 * relevant slice.
 *
 * @param r      Result store.
 * @param limit  Maximum rows to keep after offset; 0 = unlimited.
 * @param offset Number of leading rows to skip.
 */
void keel_scatter_result_apply_limit(keel_scatter_result_t* r,
                                 size_t            limit,
                                 size_t            offset);

/* ============================================================================
 * Scalar aggregate merge support (Phase D)
 * ============================================================================ */

/** Maximum number of aggregate column specs in a scatter merge spec. */
#define KEEL_SCATTER_MAX_AGG_COLS  16

/**
 * @brief Aggregate function to apply when merging partial shard results.
 *
 * Each shard returns one row containing a partial aggregate value.
 * @ref keel_scatter_result_merge_aggs collapses the N-shard rows into a single
 * merged row by combining the partial values according to this enum.
 *
 * @note @c KEEL_AGG_AVG requires a query rewrite (AVG → SUM + COUNT) at the
 *       dispatch layer before results are collected.  Passing
 *       @c KEEL_AGG_AVG to @ref keel_scatter_result_merge_aggs returns
 *       @c KEEL_ERR_NOT_SUPPORTED.
 */
typedef enum keel_agg_func {
    KEEL_AGG_NONE  = 0, /**< Not an aggregate — passthrough: use first shard's value */
    KEEL_AGG_COUNT = 1, /**< COUNT(*)  / COUNT(expr): merge = integer sum */
    KEEL_AGG_SUM   = 2, /**< SUM(expr):               merge = numeric sum */
    KEEL_AGG_MIN   = 3, /**< MIN(expr):               merge = minimum value */
    KEEL_AGG_MAX   = 4, /**< MAX(expr):               merge = maximum value */
    KEEL_AGG_AVG   = 5, /**< AVG(expr): needs rewrite; merge returns NOT_SUPPORTED */
} keel_agg_func_t;

/**
 * @brief Per-column aggregate specification for scatter merge.
 */
typedef struct keel_agg_col_spec {
    int16_t         col_index; /**< 0-based column index in the result */
    keel_agg_func_t func;      /**< Aggregate function to apply */
} keel_agg_col_spec_t;

/**
 * @brief Collapse N shard rows into a single merged aggregate row.
 *
 * The result @p r is expected to contain exactly one row per contributing
 * shard, each row holding the shard's partial aggregate values.  After the
 * call the result contains a single merged row.
 *
 * Merge semantics per function:
 *  - @c KEEL_AGG_COUNT / @c KEEL_AGG_SUM — numeric sum of partial values;
 *    SQL NULLs are treated as 0 for COUNT, and propagate as NULL for SUM only
 *    when *all* partial values are NULL.
 *  - @c KEEL_AGG_MIN — minimum over all non-NULL partial values; result is
 *    NULL if all partials are NULL.
 *  - @c KEEL_AGG_MAX — maximum over all non-NULL partial values; result is
 *    NULL if all partials are NULL.
 *  - @c KEEL_AGG_NONE — value taken from the first shard's row (passthrough).
 *  - @c KEEL_AGG_AVG — @c KEEL_ERR_NOT_SUPPORTED (requires query rewrite).
 *
 * Only text-format (format == 0) columns are supported; binary-format
 * aggregate columns return @c KEEL_ERR_NOT_SUPPORTED.
 *
 * @param r      Result store (modified in place).
 * @param specs  Per-column aggregate specifications.
 * @param nspecs Number of entries in @p specs.
 * @return @c KEEL_OK on success.
 *         @c KEEL_ERR_NOT_SUPPORTED if any spec is @c KEEL_AGG_AVG or a
 *         column is in binary format.
 *         @c KEEL_ERR_NOMEM if temporary allocation fails.
 *         @c KEEL_ERR_IO if the spill file cannot be rewritten.
 */
KEEL_NODISCARD
keel_error_t keel_scatter_result_merge_aggs(keel_scatter_result_t*          r,
                                        const keel_agg_col_spec_t* specs,
                                        uint16_t                   nspecs);

/* ============================================================================
 * GROUP BY + partial aggregate hash-merge support (Phase E)
 * ============================================================================ */

/** Maximum number of GROUP BY key columns supported in a scatter merge spec. */
#define KEEL_SCATTER_MAX_GROUP_COLS  8

/**
 * @brief A single GROUP BY key column specification.
 */
typedef struct keel_group_col_spec {
    int16_t col_index; /**< 0-based column index in the result */
} keel_group_col_spec_t;

/**
 * @brief Hash-merge N shard rows (grouped by @p group_keys) into one row
 *        per distinct group, applying @p agg_specs to non-key columns.
 *
 * Each shard contributes one partially-aggregated row per group.  This
 * function combines all shard rows with matching GROUP BY key values into a
 * single output row by:
 *  - Treating @p group_keys columns as the merge key (equal byte sequences,
 *    including NULLs, land in the same bucket).
 *  - Applying aggregate functions (COUNT/SUM/MIN/MAX) to non-key columns as
 *    specified in @p agg_specs.
 *  - Preserving the first shard's value for columns with @c KEEL_AGG_NONE.
 *
 * The output order is unspecified (hash-table iteration order).  Callers that
 * require ORDER BY should call @ref keel_scatter_result_sort after this function.
 *
 * Limitations:
 *  - @c KEEL_AGG_AVG returns @c KEEL_ERR_NOT_SUPPORTED.
 *  - Binary-format aggregate columns return @c KEEL_ERR_NOT_SUPPORTED.
 *  - At most @c KEEL_SCATTER_MAX_GROUP_COLS group keys are honoured.
 *
 * @param r           Result store (modified in place).
 * @param group_keys  GROUP BY column specifications.
 * @param ngroup_keys Number of group key specs.
 * @param agg_specs   Per-column aggregate specs for non-key columns.
 * @param nagg_specs  Number of aggregate specs.
 * @return @c KEEL_OK on success.
 *         @c KEEL_ERR_NOT_SUPPORTED for AVG or binary agg columns.
 *         @c KEEL_ERR_NOMEM on allocation failure.
 *         @c KEEL_ERR_IO if the spill file cannot be rewritten.
 */
KEEL_NODISCARD
keel_error_t keel_scatter_result_group_aggs(keel_scatter_result_t*            r,
                                        const keel_group_col_spec_t* group_keys,
                                        uint16_t                     ngroup_keys,
                                        const keel_agg_col_spec_t*   agg_specs,
                                        uint16_t                     nagg_specs);

/* ============================================================================
 * HAVING post-filter support (Phase H)
 * ============================================================================ */

/** Maximum number of HAVING predicates supported in a scatter merge spec. */
#define KEEL_SCATTER_MAX_HAVING_PREDS  8

/**
 * @brief Comparison operator for a HAVING predicate.
 */
typedef enum keel_cmp_op {
    KEEL_CMP_EQ = 0, /**< =  */
    KEEL_CMP_NE,     /**< <> */
    KEEL_CMP_LT,     /**< <  */
    KEEL_CMP_LE,     /**< <= */
    KEEL_CMP_GT,     /**< >  */
    KEEL_CMP_GE,     /**< >= */
} keel_cmp_op_t;

/**
 * @brief A single HAVING predicate: col OP literal.
 *
 * Applied after @ref keel_scatter_result_group_aggs (or @ref keel_scatter_result_merge_aggs)
 * to discard rows that do not satisfy the condition.  Both the column value
 * and the literal are compared in text wire format using the column's generic type.
 */
typedef struct keel_having_pred {
    int16_t       col_index;      /**< 0-based column index to test */
    keel_cmp_op_t op;             /**< Comparison operator */
    char          literal[32];    /**< RHS literal value in text format */
    int32_t       literal_len;    /**< Byte length of literal; -1 = NULL */
} keel_having_pred_t;

/**
 * @brief Remove rows from the result that do not satisfy all HAVING predicates.
 *
 * Applies a conjunction (AND) of predicates: a row is kept only when every
 * predicate evaluates to true.  NULL column values are treated as not
 * matching any comparison (the row is dropped).
 *
 * Typically called immediately after @ref keel_scatter_result_group_aggs to
 * implement the HAVING filter over merged aggregate results.
 *
 * @param r      Result store (modified in place).
 * @param preds  Array of predicates.
 * @param npreds Number of predicates.
 * @return @c KEEL_OK on success.
 *         @c KEEL_ERR_NOMEM if a spill-path rewrite cannot be allocated.
 *         @c KEEL_ERR_IO   if a spill-file rewrite fails.
 */
KEEL_NODISCARD
keel_error_t keel_scatter_result_apply_having(keel_scatter_result_t*         r,
                                          const keel_having_pred_t* preds,
                                          uint16_t                  npreds);

/* ============================================================================
 * Window function recomputation support (Phase F)
 * ============================================================================ */

/** Maximum number of window function column specs per scatter merge. */
#define KEEL_SCATTER_MAX_WINDOW_COLS  4

/**
 * @brief Window function to apply when recomputing per-column values globally.
 *
 * Values 0–5 are Tier 2 stateless ranking functions that can be recomputed
 * from a globally sorted result set.  Values 6–10 are Tier 3 value-access
 * functions that require partition-aware, frame-bounded row access.
 * Values 11–15 are Tier 4 aggregate window functions (SUM/COUNT/MIN/MAX/AVG
 * applied within a frame, computed in-memory after scatter).
 */
typedef enum keel_window_func {
    /* Tier 2: stateless ranking */
    KEEL_WFUNC_ROW_NUMBER   = 0,  /**< ROW_NUMBER(): unique 1-based sequential    */
    KEEL_WFUNC_RANK         = 1,  /**< RANK(): ties share rank, next skips        */
    KEEL_WFUNC_DENSE_RANK   = 2,  /**< DENSE_RANK(): ties share rank, no skip     */
    KEEL_WFUNC_NTILE        = 3,  /**< NTILE(n): divide rows into n buckets       */
    KEEL_WFUNC_PERCENT_RANK = 4,  /**< PERCENT_RANK(): (rank-1)/(total-1)         */
    KEEL_WFUNC_CUME_DIST    = 5,  /**< CUME_DIST(): rows_lte_current/total        */
    /* Tier 3: value-access (partition + frame aware) */
    KEEL_WFUNC_LAG          = 6,  /**< LAG(col, offset, default)                  */
    KEEL_WFUNC_LEAD         = 7,  /**< LEAD(col, offset, default)                 */
    KEEL_WFUNC_FIRST_VALUE  = 8,  /**< FIRST_VALUE(col) within frame              */
    KEEL_WFUNC_LAST_VALUE   = 9,  /**< LAST_VALUE(col) within frame               */
    KEEL_WFUNC_NTH_VALUE    = 10, /**< NTH_VALUE(col, n) within frame             */
    /* Tier 4: aggregate window functions (partition + frame accumulation) */
    KEEL_WFUNC_AGG_SUM      = 11, /**< SUM(col) OVER (...)                        */
    KEEL_WFUNC_AGG_COUNT    = 12, /**< COUNT(*) OVER (...)                        */
    KEEL_WFUNC_AGG_MIN      = 13, /**< MIN(col) OVER (...)                        */
    KEEL_WFUNC_AGG_MAX      = 14, /**< MAX(col) OVER (...)                        */
    KEEL_WFUNC_AGG_AVG      = 15, /**< AVG(col) OVER (...)                        */
} keel_window_func_t;

/* ============================================================================
 * Tier 3: Window frame specification (for FIRST_VALUE, LAST_VALUE, NTH_VALUE)
 * ============================================================================ */

/**
 * @brief Window frame mode (ROWS / RANGE / GROUPS).
 *
 * Only @c KEEL_FRAME_ROWS is fully supported by Phase F; RANGE and GROUPS
 * are accepted from the SQL AST but treated as ROWS for positional access.
 */
typedef enum keel_frame_mode {
    KEEL_FRAME_ROWS   = 0, /**< ROWS: frame in physical row offsets    */
    KEEL_FRAME_RANGE  = 1, /**< RANGE: frame by value range (→ ROWS)   */
    KEEL_FRAME_GROUPS = 2, /**< GROUPS: frame by peer groups (→ ROWS)  */
} keel_frame_mode_t;

/**
 * @brief Frame boundary type for one side of a window frame.
 */
typedef enum keel_frame_bound_type {
    KEEL_FRAME_UNBOUNDED_PRECEDING = 0, /**< UNBOUNDED PRECEDING              */
    KEEL_FRAME_N_PRECEDING         = 1, /**< N PRECEDING (n stored in .n)     */
    KEEL_FRAME_CURRENT_ROW         = 2, /**< CURRENT ROW                      */
    KEEL_FRAME_N_FOLLOWING         = 3, /**< N FOLLOWING (n stored in .n)     */
    KEEL_FRAME_UNBOUNDED_FOLLOWING = 4, /**< UNBOUNDED FOLLOWING              */
} keel_frame_bound_type_t;

/**
 * @brief One bound of a window frame (start or end).
 */
typedef struct keel_frame_bound {
    keel_frame_bound_type_t type; /**< Bound variant                           */
    int64_t                 n;    /**< Row count for N PRECEDING / N FOLLOWING */
} keel_frame_bound_t;

/**
 * @brief Specification for globally recomputing one window function column.
 *
 * Covers both Tier 2 (stateless ranking) and Tier 3 (value-access) functions.
 * Tier 2 functions use only col_index, func, ntile_n, order_keys, norder_keys.
 * Tier 3 functions additionally use source_col, val_offset, partition_keys,
 * npartition_keys, frame_mode, frame_start, frame_end, default_val.
 */
typedef struct keel_window_col_spec {
    int16_t            col_index;    /**< 0-based output column (overwritten)   */
    keel_window_func_t func;         /**< Window function to apply              */

    /* Tier 2: ranking */
    int64_t            ntile_n;      /**< NTILE(n) bucket count                 */

    /* Tier 2 + Tier 3: window ORDER BY keys */
    keel_sort_key_t    order_keys[KEEL_SCATTER_MAX_ORDER_KEYS];
    uint16_t           norder_keys;

    /* Tier 3: source column and offset */
    int16_t            source_col;      /**< Column to read value from          */
    int64_t            val_offset;      /**< LAG/LEAD offset (≥1); NTH_VALUE n  */

    /* Tier 3: PARTITION BY sort keys (grouping within Phase F) */
    keel_sort_key_t    partition_keys[KEEL_SCATTER_MAX_ORDER_KEYS];
    uint16_t           npartition_keys;

    /* Tier 3: window frame spec for FIRST_VALUE / LAST_VALUE / NTH_VALUE */
    keel_frame_mode_t  frame_mode;   /**< ROWS / RANGE / GROUPS                 */
    keel_frame_bound_t frame_start;  /**< Start bound of the frame              */
    keel_frame_bound_t frame_end;    /**< End bound of the frame                */

    /* Tier 3: default value for LAG / LEAD when offset is outside partition */
    char               default_val[64]; /**< Text-format default; "" if none    */
    int32_t            default_val_len; /**< Length; -1 = SQL NULL default      */
} keel_window_col_spec_t;

/**
 * @brief Recompute window function values globally after scatter collection.
 *
 * Handles two tiers of window function support:
 *
 * **Tier 2 — Stateless ranking** (ROW_NUMBER, RANK, DENSE_RANK, NTILE,
 * PERCENT_RANK, CUME_DIST):  No PARTITION BY.  Sorts by @c spec->order_keys,
 * walks sorted rows, and overwrites @c col_index with globally correct values.
 *
 * **Tier 3 — Value-access** (LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE):
 * Sorts by the combined @c partition_keys + @c order_keys sequence so that
 * rows in the same partition are contiguous.  Detects partition boundaries in
 * one pass, then for each row computes the frame start/end and copies the
 * value from @c source_col at the target row.
 *
 * For each spec the result is sorted before applying the function.  Multiple
 * specs are applied left-to-right; when two specs share identical sort keys
 * the second sort is O(N) (already-sorted data).
 *
 * Both memory and spill-to-disk paths are supported.
 *
 * @param r      Result store (modified in place; row order may change).
 * @param specs  Array of window column specs (Tier 2 or Tier 3).
 * @param nspecs Number of entries in @p specs.
 * @return @c KEEL_OK on success.
 *         @c KEEL_ERR_NOMEM if a buffer allocation fails.
 *         @c KEEL_ERR_IO    if a spill-file rewrite fails.
 */
KEEL_NODISCARD
keel_error_t keel_scatter_result_window_compute(keel_scatter_result_t*             r,
                                            const keel_window_col_spec_t* specs,
                                            uint16_t                      nspecs);

/* ============================================================================
 * AVG finalize support (Phase H)
 * ============================================================================ */

/** Maximum number of AVG finalize specs per scatter merge. */
#define KEEL_SCATTER_MAX_AVG_SPECS  8

/**
 * @brief Mapping used to compute a final AVG from SUM and COUNT columns.
 *
 * When a query containing @c AVG(x) is rewritten to @c SUM(x), COUNT(x)
 * before being sent to shards, the merged result contains separate SUM and
 * COUNT columns.  After merging, call @ref keel_scatter_result_finalize_avg with
 * one spec per original AVG expression to replace @c out_col with
 * @c sum_col / count_col.
 */
typedef struct keel_avg_finalize_spec {
    int16_t sum_col;   /**< Column index holding the merged SUM  */
    int16_t count_col; /**< Column index holding the merged COUNT */
    int16_t out_col;   /**< Column index to overwrite with the final AVG */
} keel_avg_finalize_spec_t;

/**
 * @brief Compute final AVG values from pre-merged SUM / COUNT columns.
 *
 * For each spec: reads @c sum_col and @c count_col (text-format numbers),
 * divides them to obtain the average, and writes the result back into
 * @c out_col.  If COUNT is 0 or either input is NULL the output column is
 * set to SQL NULL.
 *
 * Must be called after the SUM and COUNT columns have been fully merged
 * (via @ref keel_scatter_result_merge_aggs or @ref keel_scatter_result_group_aggs).
 * Only text-format columns are supported.
 *
 * @param r      Result store (modified in place).
 * @param specs  Array of AVG finalize specs.
 * @param nspecs Number of specs.
 * @return @c KEEL_OK on success.
 *         @c KEEL_ERR_NOMEM if a buffer allocation fails.
 */
KEEL_NODISCARD
keel_error_t keel_scatter_result_finalize_avg(keel_scatter_result_t*               r,
                                          const keel_avg_finalize_spec_t* specs,
                                          uint16_t                        nspecs);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_CORE_SCATTER_STORE_H */
