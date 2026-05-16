/**
 * @file scatter_store.c
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
 * ## Memory path
 *
 * Rows are stored in a keel_arena_t slab.  Each row is laid out as a single
 * contiguous arena allocation:
 *
 *   [keel_scatter_row_t header (ncols + padding)]
 *   [keel_scatter_col_val_t cols[ncols]]          ← pointers into the raw region
 *   [raw column data bytes]                  ← destination of cols[i].data
 *
 * A parallel keel_scatter_row_t** array (grown with keel_realloc) keeps row
 * addresses so the caller can do random access or sorted passes.
 *
 * Memory accounting tracks slab bytes consumed plus the row-pointer array.
 * When the total exceeds mem_limit_bytes the in-memory content is flushed to
 * the spill file.
 *
 * ## Spill path
 *
 * Opening the spill file: an unlinked file is created via mkstemp(3) in
 * spill_dir so it is removed automatically even if the process crashes.
 *
 * Row encoding:
 *
 *   [total_bytes: uint32_t]  -- full record size including this field; 0=EOF
 *   [col 0: len: int32_t, then len bytes if len >= 0]
 *   [col 1: …]
 *   ...
 *
 * A 64 KiB write buffer amortises write(2) calls.  Rows that exceed the
 * buffer capacity are written directly.  An explicit EOF sentinel (4 zero
 * bytes) is appended on close/flush so readers always see a clean end.
 *
 * Read buffer: the iterator maintains a KEEL_SPILL_BLOCK_SIZE read buffer.
 * Leftover bytes from the previous read are memmoved to the front when the
 * buffer is refilled, providing zero-copy row decoding for the common case.
 */


#include "keel/core/scatter_store.h"
#include "keel/mem/mem.h"
#include "keel_error.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>  /* PRId64, PRIu64 */
#include <stddef.h>
#include <stdbool.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <math.h>        /* strtof, strtod */
#include <sys/stat.h>

/* ============================================================================
 * Internal helpers — byte-order (big-endian reads for binary wire format)
 * ============================================================================ */

static inline int16_t read_be16(const char* p)
{
    uint8_t a = (uint8_t)p[0], b = (uint8_t)p[1];
    return (int16_t)((uint16_t)a << 8 | (uint16_t)b);
}

static inline int32_t read_be32(const char* p)
{
    uint8_t a = (uint8_t)p[0], b = (uint8_t)p[1];
    uint8_t c = (uint8_t)p[2], d = (uint8_t)p[3];
    return (int32_t)(  (uint32_t)a << 24 | (uint32_t)b << 16
                     | (uint32_t)c << 8  | (uint32_t)d);
}

static inline int64_t read_be64(const char* p)
{
    uint64_t hi = (uint64_t)(uint32_t)read_be32(p);
    uint64_t lo = (uint64_t)(uint32_t)read_be32(p + 4);
    return (int64_t)((hi << 32) | lo);
}

static inline float read_be_float(const char* p)
{
    uint32_t bits = (uint32_t)read_be32(p);
    float    v;
    memcpy(&v, &bits, sizeof v);
    return v;
}

static inline double read_be_double(const char* p)
{
    uint64_t bits = (uint64_t)read_be64(p);
    double   v;
    memcpy(&v, &bits, sizeof v);
    return v;
}

/* ============================================================================
 * OID-dispatch comparator
 * ============================================================================ */

/**
 * @brief Byte-lexicographic fallback comparison (text or unknown binary).
 */
static int cmp_bytes(const keel_scatter_col_val_t* a, const keel_scatter_col_val_t* b)
{
    /* Both NULL → equal; NULL < non-NULL (NULLS FIRST) */
    if (a->len < 0 && b->len < 0) return 0;
    if (a->len < 0)               return -1;
    if (b->len < 0)               return  1;

    size_t  la = (size_t)a->len;
    size_t  lb = (size_t)b->len;
    int rc = memcmp(a->data, b->data, la < lb ? la : lb);
    if (rc != 0) return rc;
    if (la < lb) return -1;
    if (la > lb) return  1;
    return 0;
}

int keel_scatter_col_cmp(
    keel_col_type_t               type,
    keel_wire_format_t            format,
    const keel_scatter_col_val_t* a,
    const keel_scatter_col_val_t* b)
{
    /* NULL handling: NULLS FIRST */
    if (a->len < 0 && b->len < 0) return 0;
    if (a->len < 0)               return -1;
    if (b->len < 0)               return  1;

    if (format == KEEL_WIRE_BINARY) {
        /* ------------------------------------------------------------------ */
        /* Binary wire format                                                  */
        /* ------------------------------------------------------------------ */
        switch (type) {

        case KEEL_COL_TYPE_BOOL: {
            uint8_t va = (a->len >= 1) ? (uint8_t)a->data[0] : 0;
            uint8_t vb = (b->len >= 1) ? (uint8_t)b->data[0] : 0;
            return (int)va - (int)vb;
        }

        case KEEL_COL_TYPE_INT16: {
            if (a->len < 2 || b->len < 2) return cmp_bytes(a, b);
            int16_t va = read_be16(a->data);
            int16_t vb = read_be16(b->data);
            return (va > vb) - (va < vb);
        }

        case KEEL_COL_TYPE_INT32:
        case KEEL_COL_TYPE_DATE: {
            if (a->len < 4 || b->len < 4) return cmp_bytes(a, b);
            int32_t va = read_be32(a->data);
            int32_t vb = read_be32(b->data);
            return (va > vb) - (va < vb);
        }

        case KEEL_COL_TYPE_INT64:
        case KEEL_COL_TYPE_TIME:
        case KEEL_COL_TYPE_TIMESTAMP: {
            if (a->len < 8 || b->len < 8) return cmp_bytes(a, b);
            int64_t va = read_be64(a->data);
            int64_t vb = read_be64(b->data);
            return (va > vb) - (va < vb);
        }

        case KEEL_COL_TYPE_FLOAT32: {
            if (a->len < 4 || b->len < 4) return cmp_bytes(a, b);
            float va = read_be_float(a->data);
            float vb = read_be_float(b->data);
            return (va > vb) - (va < vb);
        }

        case KEEL_COL_TYPE_FLOAT64: {
            if (a->len < 8 || b->len < 8) return cmp_bytes(a, b);
            double va = read_be_double(a->data);
            double vb = read_be_double(b->data);
            return (va > vb) - (va < vb);
        }

        case KEEL_COL_TYPE_UUID:
            /* UUID binary is 16 bytes; byte-compare is correct for RFC 4122 */
            /* FALLTHROUGH */
        default:
            return cmp_bytes(a, b);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* Text wire format                                                        */
    /* ---------------------------------------------------------------------- */
    switch (type) {

    case KEEL_COL_TYPE_BOOL: {
        /* Standard SQL sends "t"/"true"/"1" for true, "f"/"false"/"0" for false */
        char va = (a->len >= 1) ? a->data[0] : 'f';
        char vb = (b->len >= 1) ? b->data[0] : 'f';
        int  ba = (va == 't' || va == '1') ? 1 : 0;
        int  bb = (vb == 't' || vb == '1') ? 1 : 0;
        return ba - bb;
    }

    case KEEL_COL_TYPE_INT16:
    case KEEL_COL_TYPE_INT32: {
        char bufa[32], bufb[32];
        size_t la = (size_t)a->len < sizeof bufa - 1 ? (size_t)a->len : sizeof bufa - 1;
        size_t lb = (size_t)b->len < sizeof bufb - 1 ? (size_t)b->len : sizeof bufb - 1;
        memcpy(bufa, a->data, la); bufa[la] = '\0';
        memcpy(bufb, b->data, lb); bufb[lb] = '\0';
        long va = strtol(bufa, NULL, 10);
        long vb = strtol(bufb, NULL, 10);
        return (va > vb) - (va < vb);
    }

    case KEEL_COL_TYPE_INT64: {
        char bufa[32], bufb[32];
        size_t la = (size_t)a->len < sizeof bufa - 1 ? (size_t)a->len : sizeof bufa - 1;
        size_t lb = (size_t)b->len < sizeof bufb - 1 ? (size_t)b->len : sizeof bufb - 1;
        memcpy(bufa, a->data, la); bufa[la] = '\0';
        memcpy(bufb, b->data, lb); bufb[lb] = '\0';
        long long va = strtoll(bufa, NULL, 10);
        long long vb = strtoll(bufb, NULL, 10);
        return (va > vb) - (va < vb);
    }

    case KEEL_COL_TYPE_FLOAT32: {
        char bufa[64], bufb[64];
        size_t la = (size_t)a->len < sizeof bufa - 1 ? (size_t)a->len : sizeof bufa - 1;
        size_t lb = (size_t)b->len < sizeof bufb - 1 ? (size_t)b->len : sizeof bufb - 1;
        memcpy(bufa, a->data, la); bufa[la] = '\0';
        memcpy(bufb, b->data, lb); bufb[lb] = '\0';
        float va = strtof(bufa, NULL);
        float vb = strtof(bufb, NULL);
        return (va > vb) - (va < vb);
    }

    case KEEL_COL_TYPE_FLOAT64: {
        char bufa[64], bufb[64];
        size_t la = (size_t)a->len < sizeof bufa - 1 ? (size_t)a->len : sizeof bufa - 1;
        size_t lb = (size_t)b->len < sizeof bufb - 1 ? (size_t)b->len : sizeof bufb - 1;
        memcpy(bufa, a->data, la); bufa[la] = '\0';
        memcpy(bufb, b->data, lb); bufb[lb] = '\0';
        double va = strtod(bufa, NULL);
        double vb = strtod(bufb, NULL);
        return (va > vb) - (va < vb);
    }

    /* Text and text-like types: lexicographic with length tiebreak */
    case KEEL_COL_TYPE_TEXT:
    case KEEL_COL_TYPE_BYTES:
        return cmp_bytes(a, b);

    /* Date/time in ISO text format sorts correctly as plain strings */
    case KEEL_COL_TYPE_DATE:
    case KEEL_COL_TYPE_TIME:
    case KEEL_COL_TYPE_TIMESTAMP:
        return cmp_bytes(a, b);

    /* UUID in canonical form (8-4-4-4-12 hexadecimal) sorts correctly */
    case KEEL_COL_TYPE_UUID:
        return cmp_bytes(a, b);

    default:
        return cmp_bytes(a, b);
    }
}

/* ============================================================================
 * Spill-file helpers
 * ============================================================================ */

/* Fixed-length file header (12 bytes) */
#define SPILL_HEADER_FIXED_SIZE 12U

/* Per-column info in the file header: col_type (4) + format (2) */
#define SPILL_HEADER_COL_SIZE   6U

/** Compute full file-header size for ncols columns. */
static inline size_t spill_header_size(uint16_t ncols)
{
    return SPILL_HEADER_FIXED_SIZE + (size_t)ncols * SPILL_HEADER_COL_SIZE;
}

/**
 * @brief Flush accumulated bytes in write_buf to the spill file.
 *
 * Does not append the EOF sentinel — that is written only at close/iter_init.
 */
static keel_error_t spill_flush_write_buf(keel_scatter_result_t* r)
{
    if (r->write_buf_pos == 0) return KEEL_OK;

    size_t  remaining = r->write_buf_pos;
    char*   ptr       = r->write_buf;

    while (remaining > 0) {
        ssize_t n = write(r->spill_fd, ptr, remaining);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return KEEL_ERR_IO;
        }
        ptr       += (size_t)n;
        remaining -= (size_t)n;
    }

    r->write_buf_pos = 0;
    return KEEL_OK;
}

/**
 * @brief Write the EOF sentinel (4 zero bytes) to the spill file.
 *
 * Flushes the write buffer first, then appends the sentinel.
 */
static keel_error_t spill_write_eof(keel_scatter_result_t* r)
{
    keel_error_t err = spill_flush_write_buf(r);
    if (err != KEEL_OK) return err;

    static const char zero[4] = {0, 0, 0, 0};
    ssize_t n;
    do { n = write(r->spill_fd, zero, 4); } while (n < 0 && errno == EINTR);
    return (n == 4) ? KEEL_OK : KEEL_ERR_IO;
}

/**
 * @brief Write file header containing magic, version, ncols, and per-column
 *        col_type + format.  Seeks to offset 0 first.
 */
static keel_error_t spill_write_header(keel_scatter_result_t* r)
{
    /* Build the fixed header */
    uint8_t hdr[SPILL_HEADER_FIXED_SIZE];
    uint32_t magic   = KEEL_SPILL_MAGIC;
    uint8_t  version = KEEL_SPILL_VERSION;
    uint16_t ncols   = r->ncols;

    memcpy(hdr,      &magic,   4);
    hdr[4] = version;
    hdr[5] = 0; hdr[6] = 0; hdr[7] = 0;   /* padding */
    memcpy(hdr + 8,  &ncols,   2);
    hdr[10] = 0; hdr[11] = 0;              /* padding */

    if (lseek(r->spill_fd, 0, SEEK_SET) < 0) return KEEL_ERR_IO;

    ssize_t n;
    do { n = write(r->spill_fd, hdr, sizeof hdr); } while (n < 0 && errno == EINTR);
    if (n != (ssize_t)sizeof hdr) return KEEL_ERR_IO;

    /* Per-column info */
    for (uint16_t i = 0; i < ncols; i++) {
        uint32_t col_type = (uint32_t)r->cols[i].type;
        int16_t  fmt      = (int16_t)r->cols[i].format;
        uint8_t  col_info[SPILL_HEADER_COL_SIZE];
        memcpy(col_info,     &col_type, 4);
        memcpy(col_info + 4, &fmt, 2);
        do { n = write(r->spill_fd, col_info, sizeof col_info); }
        while (n < 0 && errno == EINTR);
        if (n != (ssize_t)sizeof col_info) return KEEL_ERR_IO;
    }

    return KEEL_OK;
}

/**
 * @brief Open the spill file and write the header.
 *
 * Uses mkstemp(3) for atomic creation.  The write buffer is allocated here.
 */
static keel_error_t spill_open(keel_scatter_result_t* r)
{
    /* Build the template path: "<spill_dir>/keel_scatter_XXXXXX" */
    const char* dir = r->spill_dir ? r->spill_dir : KEEL_SCATTER_DEFAULT_SPILL_DIR;
    int len = snprintf(r->spill_path, sizeof r->spill_path,
                       "%s/keel_scatter_XXXXXX", dir);
    if (len < 0 || (size_t)len >= sizeof r->spill_path) return KEEL_ERR_INVALID_ARG;

    r->spill_fd = mkstemp(r->spill_path);
    if (r->spill_fd < 0) return KEEL_ERR_IO;

    /* Set close-on-exec so the fd is not inherited by child processes */
    (void)fcntl(r->spill_fd, F_SETFD, FD_CLOEXEC);

    /* Allocate write buffer */
    r->write_buf = (char*)keel_malloc(KEEL_SPILL_BLOCK_SIZE);
    if (!r->write_buf) {
        close(r->spill_fd);
        r->spill_fd = -1;
        unlink(r->spill_path);
        return KEEL_ERR_NOMEM;
    }
    r->write_buf_pos = 0;

    /* Write the file header */
    keel_error_t err = spill_write_header(r);
    if (err != KEEL_OK) {
        keel_free(r->write_buf);
        r->write_buf = NULL;
        close(r->spill_fd);
        r->spill_fd = -1;
        unlink(r->spill_path);
        return err;
    }

    return KEEL_OK;
}

/**
 * @brief Encode one row into the spill write buffer (and flush if needed).
 *
 * Row encoding:
 *   [total_bytes: uint32_t]  includes this field
 *   [col i: len: int32_t, then len bytes if len >= 0] × ncols
 */
static keel_error_t spill_append_row(keel_scatter_result_t*        r,
                                      const keel_scatter_col_val_t* vals)
{
    /* Calculate total record size */
    uint32_t total = 4; /* total_bytes field itself */
    for (uint16_t i = 0; i < r->ncols; i++) {
        total += 4; /* len field */
        if (vals[i].len > 0) total += (uint32_t)vals[i].len;
    }

    /* If the row is larger than the write buffer, flush and write directly */
    if (total > KEEL_SPILL_BLOCK_SIZE) {
        keel_error_t err = spill_flush_write_buf(r);
        if (err != KEEL_OK) return err;

        /* Write total_bytes */
        ssize_t n;
        do { n = write(r->spill_fd, &total, 4); } while (n < 0 && errno == EINTR);
        if (n != 4) return KEEL_ERR_IO;

        /* Write each column */
        for (uint16_t i = 0; i < r->ncols; i++) {
            int32_t clen = vals[i].len;
            do { n = write(r->spill_fd, &clen, 4); } while (n < 0 && errno == EINTR);
            if (n != 4) return KEEL_ERR_IO;
            if (clen > 0) {
                size_t remaining = (size_t)clen;
                const char* ptr  = vals[i].data;
                while (remaining > 0) {
                    n = write(r->spill_fd, ptr, remaining);
                    if (n <= 0) {
                        if (n < 0 && errno == EINTR) continue;
                        return KEEL_ERR_IO;
                    }
                    ptr       += (size_t)n;
                    remaining -= (size_t)n;
                }
            }
        }
        r->spill_bytes += total;
        r->spill_row_count++;
        r->row_count++;
        return KEEL_OK;
    }

    /* Row fits in the write buffer — flush if not enough space */
    if (r->write_buf_pos + total > KEEL_SPILL_BLOCK_SIZE) {
        keel_error_t err = spill_flush_write_buf(r);
        if (err != KEEL_OK) return err;
    }

    /* Encode into write buffer */
    char* p = r->write_buf + r->write_buf_pos;
    memcpy(p, &total, 4);
    p += 4;
    for (uint16_t i = 0; i < r->ncols; i++) {
        int32_t clen = vals[i].len;
        memcpy(p, &clen, 4);
        p += 4;
        if (clen > 0) {
            memcpy(p, vals[i].data, (size_t)clen);
            p += (size_t)clen;
        }
    }
    r->write_buf_pos += total;
    r->spill_bytes   += total;
    r->spill_row_count++;
    r->row_count++;
    return KEEL_OK;
}

/**
 * @brief Flush all in-memory rows to the spill file, then release slab memory.
 *
 * After this call: spilled==true, slab==NULL, rows==NULL, rows_cap==0,
 * mem_bytes is reset to 0.  row_count is unchanged (all rows are now on disk).
 */
static keel_error_t spill_flush_memory(keel_scatter_result_t* r)
{
    keel_error_t err = spill_open(r);
    if (err != KEEL_OK) return err;

    r->spilled = true;

    /* Copy all in-memory rows to disk */
    size_t saved_row_count = r->row_count;
    r->row_count = 0;      /* spill_append_row increments this */

    for (size_t i = 0; i < saved_row_count; i++) {
        keel_scatter_row_t* row = r->rows[i];
        err = spill_append_row(r, row->cols);
        if (err != KEEL_OK) return err;
    }

    /* Release memory-resident data */
    keel_arena_destroy(r->slab);
    r->slab = NULL;

    keel_free(r->rows);
    r->rows     = NULL;
    r->rows_cap = 0;
    r->mem_bytes = 0;

    return KEEL_OK;
}

/* ============================================================================
 * Public API — lifecycle
 * ============================================================================ */

keel_scatter_result_t* keel_scatter_result_create(uint16_t                   ncols,
                                         const keel_scatter_col_desc_t*  cols,
                                         size_t                     mem_limit_bytes,
                                         const char*                spill_dir)
{
    keel_scatter_result_t* r = (keel_scatter_result_t*)keel_calloc(1, sizeof *r);
    if (!r) return NULL;

    r->ncols = ncols;

    if (ncols > 0 && cols) {
        r->cols = (keel_scatter_col_desc_t*)keel_malloc(ncols * sizeof *r->cols);
        if (!r->cols) { keel_free(r); return NULL; }
        memcpy(r->cols, cols, ncols * sizeof *r->cols);
    }

    /* Apply memory limit with floor */
    if (mem_limit_bytes < KEEL_SCATTER_MIN_MEM_LIMIT_BYTES)
        mem_limit_bytes = (mem_limit_bytes == 0)
                          ? KEEL_SCATTER_DEFAULT_MEM_LIMIT_BYTES
                          : KEEL_SCATTER_MIN_MEM_LIMIT_BYTES;
    r->mem_limit_bytes = mem_limit_bytes;
    r->spill_dir       = spill_dir;
    r->spill_fd        = -1;

    /* Initial slab: min(256 KiB, mem_limit / 4) to avoid immediate spill */
    size_t initial_slab = 256U * 1024U;
    if (initial_slab > r->mem_limit_bytes / 4)
        initial_slab = r->mem_limit_bytes / 4;
    if (initial_slab < 4096) initial_slab = 4096;

    r->slab = keel_arena_create(initial_slab);
    if (!r->slab) {
        keel_free(r->cols);
        keel_free(r);
        return NULL;
    }

    /* Initial row-pointer array (64 slots) */
    r->rows_cap = 64;
    r->rows = (keel_scatter_row_t**)keel_malloc(r->rows_cap * sizeof *r->rows);
    if (!r->rows) {
        keel_arena_destroy(r->slab);
        keel_free(r->cols);
        keel_free(r);
        return NULL;
    }

    return r;
}

void keel_scatter_result_destroy(keel_scatter_result_t* r)
{
    if (!r) return;

    if (r->slab)  keel_arena_destroy(r->slab);
    if (r->rows)  keel_free(r->rows);
    if (r->cols)  keel_free(r->cols);

    if (r->spill_fd >= 0) {
        /* Write the EOF sentinel so a concurrent reader (if any) sees a clean
         * end, then close and unlink. */
        (void)spill_write_eof(r);
        close(r->spill_fd);
        r->spill_fd = -1;
    }
    if (r->write_buf) keel_free(r->write_buf);

    /* Unlink the spill file if it was created */
    if (r->spill_path[0] != '\0') unlink(r->spill_path);

    keel_free(r);
}

/* ============================================================================
 * Public API — row insertion
 * ============================================================================ */

keel_error_t keel_scatter_result_append(keel_scatter_result_t*        r,
                                    const keel_scatter_col_val_t* vals)
{
    /* --- Spill path --- */
    if (r->spilled) return spill_append_row(r, vals);

    /* Calculate bytes needed for this row in the slab:
     *   struct header (ncols + padding) + col val array + raw data
     */
    size_t data_bytes = 0;
    for (uint16_t i = 0; i < r->ncols; i++) {
        if (vals[i].len > 0) data_bytes += (size_t)vals[i].len;
    }
    size_t row_struct = offsetof(keel_scatter_row_t, cols)
                        + (size_t)r->ncols * sizeof(keel_scatter_col_val_t);
    size_t row_bytes  = row_struct + data_bytes;

    /* Check if we need to trigger a spill BEFORE this row */
    size_t ptr_bytes  = (r->row_count + 1) * sizeof(keel_scatter_row_t*);
    if (r->mem_bytes + row_bytes + ptr_bytes > r->mem_limit_bytes) {
        keel_error_t err = spill_flush_memory(r);
        if (err != KEEL_OK) return err;
        return spill_append_row(r, vals);
    }

    /* --- Memory path --- */

    /* Grow the row-pointer array if needed */
    if (r->row_count >= r->rows_cap) {
        size_t new_cap = r->rows_cap * 2;
        keel_scatter_row_t** new_rows =
            (keel_scatter_row_t**)keel_realloc(r->rows, new_cap * sizeof *r->rows);
        if (!new_rows) return KEEL_ERR_NOMEM;
        r->rows     = new_rows;
        r->rows_cap = new_cap;
    }

    /* Allocate the row in the slab */
    keel_scatter_row_t* row = (keel_scatter_row_t*)keel_arena_alloc(r->slab, row_bytes);
    if (!row) {
        /* Arena is full; allocate a fresh larger one */
        size_t new_slab_size = row_bytes * 2;
        if (new_slab_size < 256U * 1024U) new_slab_size = 256U * 1024U;

        /* We can't free the old arena (rows point into it), so we track
         * only the arena we're currently filling.  Old arenas are kept alive
         * via the slab chain pattern: save old pointer, create new slab, but
         * since keel_arena_t is opaque we can't chain them.  Instead, grow
         * the current arena by destroying it only when we spill.  For now,
         * if the arena is full and we're still below the memory limit, we
         * simply spill to disk. */
        keel_error_t err = spill_flush_memory(r);
        if (err != KEEL_OK) return err;
        return spill_append_row(r, vals);
    }

    /* Populate the row struct */
    row->ncols = r->ncols;

    /* The raw data region starts right after the col val array */
    char* raw = (char*)row + row_struct;

    for (uint16_t i = 0; i < r->ncols; i++) {
        row->cols[i].len  = vals[i].len;
        if (vals[i].len > 0) {
            row->cols[i].data = raw;
            memcpy(raw, vals[i].data, (size_t)vals[i].len);
            raw += (size_t)vals[i].len;
        } else {
            row->cols[i].data = NULL;
        }
    }

    r->rows[r->row_count++] = row;
    r->mem_bytes += row_bytes + sizeof(keel_scatter_row_t*);

    return KEEL_OK;
}

/* ============================================================================
 * Public API — iterator
 * ============================================================================ */

keel_error_t keel_scatter_result_iter_init(keel_scatter_result_iter_t* it,
                                       keel_scatter_result_t*      r)
{
    memset(it, 0, sizeof *it);
    it->result = r;

    /* Allocate the decoded-values array (used by both paths) */
    if (r->ncols > 0) {
        it->vals = (keel_scatter_col_val_t*)keel_malloc(
                        (size_t)r->ncols * sizeof *it->vals);
        if (!it->vals) return KEEL_ERR_NOMEM;
    }

    if (!r->spilled) return KEEL_OK;

    /* Spill path: flush write buffer + write EOF + rewind */
    keel_error_t err = spill_write_eof(r);
    if (err != KEEL_OK) {
        keel_free(it->vals);
        it->vals = NULL;
        return err;
    }

    /* Seek to the first row (immediately after the file header) */
    off_t header_end = (off_t)spill_header_size(r->ncols);
    if (lseek(r->spill_fd, header_end, SEEK_SET) < 0) {
        keel_free(it->vals);
        it->vals = NULL;
        return KEEL_ERR_IO;
    }

    /* Allocate read buffer */
    it->read_buf = (char*)keel_malloc(KEEL_SPILL_BLOCK_SIZE);
    if (!it->read_buf) {
        keel_free(it->vals);
        it->vals = NULL;
        return KEEL_ERR_NOMEM;
    }
    it->read_buf_cap   = KEEL_SPILL_BLOCK_SIZE;
    it->read_buf_valid = 0;
    it->read_buf_pos   = 0;

    return KEEL_OK;
}

/**
 * @brief Ensure at least @p need bytes are available in the read buffer.
 *
 * Moves unconsumed bytes to the front of the buffer, then fills from the fd.
 * Returns false if EOF is reached or an I/O error occurs.
 */
static bool iter_ensure_bytes(keel_scatter_result_iter_t* it, size_t need)
{
    size_t avail = it->read_buf_valid - it->read_buf_pos;
    if (avail >= need) return true;

    /* Shift from row_start_pos so that any column data already parsed for
     * the current row (in [row_start_pos, read_buf_pos)) is preserved at the
     * front of the buffer and the vals[i].data pointers remain valid. */
    size_t shift = it->row_start_pos;
    size_t keep  = it->read_buf_valid - shift; /* existing current-row data + tail */
    if (shift > 0) {
        memmove(it->read_buf, it->read_buf + shift, keep);
        it->read_buf_valid = keep;
        it->read_buf_pos  -= shift;
        it->row_start_pos  = 0;
        /* Adjust vals[i].data pointers that were set for this row */
        if (it->vals) {
            uint16_t ncols = it->result ? it->result->ncols : 0;
            for (uint16_t i = 0; i < ncols; i++) {
                if (it->vals[i].data)
                    it->vals[i].data -= shift;
            }
        }
    } else {
        /* No already-read row data; fall back to old behaviour */
        if (avail > 0 && it->read_buf_pos > 0)
            memmove(it->read_buf, it->read_buf + it->read_buf_pos, avail);
        it->read_buf_valid = avail;
        it->read_buf_pos   = 0;
    }

    /* Grow the read buffer if the required total size exceeds current capacity.
     * Round up to the next KEEL_SPILL_BLOCK_SIZE multiple to amortise
     * reallocations when reading many large columns. */
    if (it->read_buf_pos + need > it->read_buf_cap) {
        size_t new_cap = ((it->read_buf_pos + need + KEEL_SPILL_BLOCK_SIZE - 1)
                          / KEEL_SPILL_BLOCK_SIZE) * KEEL_SPILL_BLOCK_SIZE;
        char* nb = (char*)keel_realloc(it->read_buf, new_cap);
        if (!nb) return false;
        it->read_buf     = nb;
        it->read_buf_cap = new_cap;
        /* After realloc, vals[i].data still point into the NEW buffer (same
         * relative offset) because keel_realloc preserves content. */
    }

    /* Fill the buffer */
    while (it->read_buf_valid < it->read_buf_pos + need) {
        size_t  space = it->read_buf_cap - it->read_buf_valid;
        ssize_t n = read(it->result->spill_fd,
                         it->read_buf + it->read_buf_valid,
                         space);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false; /* EOF or error */
        it->read_buf_valid += (size_t)n;
    }
    return true;
}

bool keel_scatter_result_iter_next(keel_scatter_result_iter_t*    it,
                               const keel_scatter_col_val_t** vals_out)
{
    keel_scatter_result_t* r = it->result;

    /* --- Memory path --- */
    if (!r->spilled) {
        if (it->rows_returned >= r->row_count) return false;
        keel_scatter_row_t* row = r->rows[it->rows_returned++];
        /* Shallow-copy the col val structs into it->vals */
        for (uint16_t i = 0; i < r->ncols; i++)
            it->vals[i] = row->cols[i];
        *vals_out = it->vals;
        return true;
    }

    /* --- Spill path --- */

    /* Record the start of this row in the read buffer so that
     * iter_ensure_bytes can preserve already-decoded column data if it needs
     * to shift the buffer.  Also clear all vals[i].data so the shift-fixup
     * loop in iter_ensure_bytes skips slots not yet set. */
    it->row_start_pos = it->read_buf_pos;
    for (uint16_t i = 0; i < r->ncols; i++) {
        it->vals[i].data = NULL;
        it->vals[i].len  = -1;
    }

    /* Read total_bytes (4 bytes) */
    if (!iter_ensure_bytes(it, 4)) return false;
    uint32_t total_bytes;
    memcpy(&total_bytes, it->read_buf + it->read_buf_pos, 4);
    if (total_bytes == 0) return false; /* EOF sentinel */

    it->read_buf_pos += 4;

    /* Decode each column */
    for (uint16_t i = 0; i < r->ncols; i++) {
        /* Read col length (4 bytes) */
        if (!iter_ensure_bytes(it, 4)) return false;
        int32_t clen;
        memcpy(&clen, it->read_buf + it->read_buf_pos, 4);
        it->read_buf_pos += 4;

        it->vals[i].len  = clen;
        it->vals[i].data = NULL;

        if (clen > 0) {
            /* Read col data */
            if (!iter_ensure_bytes(it, (size_t)clen)) return false;
            it->vals[i].data = it->read_buf + it->read_buf_pos;
            it->read_buf_pos += (size_t)clen;
        }
    }

    it->rows_returned++;
    *vals_out = it->vals;
    return true;
}

void keel_scatter_result_iter_close(keel_scatter_result_iter_t* it)
{
    if (!it) return;
    keel_free(it->read_buf);
    keel_free(it->vals);
    it->read_buf = NULL;
    it->vals     = NULL;
}

/* ============================================================================
 * Public API — introspection
 * ============================================================================ */

size_t keel_scatter_result_row_count(const keel_scatter_result_t* r)
{
    return r ? r->row_count : 0;
}

uint16_t keel_scatter_result_ncols(const keel_scatter_result_t* r)
{
    return r ? r->ncols : 0;
}

bool keel_scatter_result_spilled(const keel_scatter_result_t* r)
{
    return r && r->spilled;
}

size_t keel_scatter_result_mem_bytes(const keel_scatter_result_t* r)
{
    return r ? r->mem_bytes : 0;
}

size_t keel_scatter_result_spill_bytes(const keel_scatter_result_t* r)
{
    return r ? r->spill_bytes : 0;
}

/* ============================================================================
 * Phase C — ORDER BY sort and LIMIT/OFFSET trimming
 * ============================================================================ */

/* Context passed to the qsort_r comparator. */
typedef struct {
    const keel_scatter_result_t* r;
    const keel_sort_key_t*  keys;
    uint16_t                nkeys;
} sort_ctx_t;

/**
 * @brief Determine whether NULL sorts before or after a non-NULL value for
 *        the given key (honoring NULLS FIRST / LAST / DEFAULT semantics).
 *
 * PostgreSQL defaults: NULLS LAST for ASC, NULLS FIRST for DESC.
 * Returns -1 if NULL-comes-first, +1 if NULL-comes-last.
 */
static int null_sort_sign(const keel_sort_key_t* k)
{
    switch (k->nulls) {
    case KEEL_SORT_NULLS_FIRST:   return -1;
    case KEEL_SORT_NULLS_LAST:    return  1;
    case KEEL_SORT_NULLS_DEFAULT:
    default:
        /* PostgreSQL default: NULLS LAST for ASC, NULLS FIRST for DESC */
        return (k->dir == KEEL_SORT_ASC) ? 1 : -1;
    }
}

static int row_cmp(const void* va, const void* vb, void* vctx)
{
    const keel_scatter_row_t* const* pa  = (const keel_scatter_row_t* const*)va;
    const keel_scatter_row_t* const* pb  = (const keel_scatter_row_t* const*)vb;
    const keel_scatter_row_t* a   = *pa;
    const keel_scatter_row_t* b   = *pb;
    const sort_ctx_t*    ctx = (const sort_ctx_t*)vctx;

    for (uint16_t ki = 0; ki < ctx->nkeys; ki++) {
        const keel_sort_key_t* k = &ctx->keys[ki];
        if (k->col_index < 0 || (uint16_t)k->col_index >= a->ncols) continue;

        uint16_t ci = (uint16_t)k->col_index;
        const keel_scatter_col_val_t* ca = &a->cols[ci];
        const keel_scatter_col_val_t* cb = &b->cols[ci];

        /* Handle NULLs before calling keel_scatter_col_cmp (which also handles
         * them, but doesn't know about the per-key nulls directive). */
        if (ca->len < 0 && cb->len < 0) continue;   /* both NULL → tie */
        if (ca->len < 0) { int s = null_sort_sign(k); if (s) return s; continue; }
        if (cb->len < 0) { int s = null_sort_sign(k); if (s) return -s; continue; }

        uint32_t ci_safe = (ci < ctx->r->ncols) ? ci : 0;
        keel_col_type_t   col_type = (ci < ctx->r->ncols) ? ctx->r->cols[ci_safe].type   : KEEL_COL_TYPE_TEXT;
        keel_wire_format_t fmt     = (ci < ctx->r->ncols) ? ctx->r->cols[ci_safe].format  : KEEL_WIRE_TEXT;

        int cmp = keel_scatter_col_cmp(col_type, fmt, ca, cb);
        if (cmp == 0) continue;
        return (k->dir == KEEL_SORT_DESC) ? -cmp : cmp;
    }
    return 0;
}

keel_error_t keel_scatter_result_sort(keel_scatter_result_t*      r,
                                  const keel_sort_key_t* keys,
                                  uint16_t               nkeys)
{
    if (!r || !keys || nkeys == 0 || r->row_count == 0) return KEEL_OK;

    /* Clamp nkeys to the supported maximum */
    if (nkeys > KEEL_SCATTER_MAX_ORDER_KEYS)
        nkeys = KEEL_SCATTER_MAX_ORDER_KEYS;

    if (!r->spilled) {
        /* ------------------------------------------------------------------ */
        /* Memory path: sort the rows[] pointer array in-place.               */
        /* ------------------------------------------------------------------ */
        sort_ctx_t ctx = { .r = r, .keys = keys, .nkeys = nkeys };
        qsort_r(r->rows, r->row_count, sizeof(keel_scatter_row_t*), row_cmp, &ctx);
        return KEEL_OK;
    }

    /* ------------------------------------------------------------------ */
    /* Spill path: re-materialise all rows into a temporary arena, sort,   */
    /* then rewrite the spill file.                                         */
    /* ------------------------------------------------------------------ */

    /* Temporary arena and pointer array for all rows */
    keel_arena_t* tmp_slab = keel_arena_create(256U * 1024U);
    if (!tmp_slab) return KEEL_ERR_NOMEM;

    keel_scatter_row_t** tmp_rows =
        (keel_scatter_row_t**)keel_malloc(r->row_count * sizeof(keel_scatter_row_t*));
    if (!tmp_rows) { keel_arena_destroy(tmp_slab); return KEEL_ERR_NOMEM; }

    /* Read every row from the spill file */
    keel_scatter_result_iter_t it;
    keel_error_t err = keel_scatter_result_iter_init(&it, r);
    if (err != KEEL_OK) {
        keel_free(tmp_rows);
        keel_arena_destroy(tmp_slab);
        return err;
    }

    size_t n = 0;
    const keel_scatter_col_val_t* vals;
    while (keel_scatter_result_iter_next(&it, &vals)) {
        /* Compute row allocation size */
        size_t data_bytes = 0;
        for (uint16_t i = 0; i < r->ncols; i++)
            if (vals[i].len > 0) data_bytes += (size_t)vals[i].len;
        size_t row_sz = offsetof(keel_scatter_row_t, cols)
                        + (size_t)r->ncols * sizeof(keel_scatter_col_val_t)
                        + data_bytes;

        keel_scatter_row_t* row = (keel_scatter_row_t*)keel_arena_alloc(tmp_slab, row_sz);
        if (!row) {
            keel_scatter_result_iter_close(&it);
            keel_free(tmp_rows);
            keel_arena_destroy(tmp_slab);
            return KEEL_ERR_NOMEM;
        }
        row->ncols = r->ncols;
        char* raw  = (char*)row + offsetof(keel_scatter_row_t, cols)
                     + (size_t)r->ncols * sizeof(keel_scatter_col_val_t);
        for (uint16_t i = 0; i < r->ncols; i++) {
            row->cols[i].len  = vals[i].len;
            if (vals[i].len > 0) {
                row->cols[i].data = raw;
                memcpy(raw, vals[i].data, (size_t)vals[i].len);
                raw += (size_t)vals[i].len;
            } else {
                row->cols[i].data = NULL;
            }
        }
        tmp_rows[n++] = row;
    }
    keel_scatter_result_iter_close(&it);

    /* Sort the pointer array */
    sort_ctx_t ctx = { .r = r, .keys = keys, .nkeys = nkeys };
    qsort_r(tmp_rows, n, sizeof(keel_scatter_row_t*), row_cmp, &ctx);

    /* Rewrite the spill file with sorted rows.
     * Truncate the file to just the header, then append in order. */
    off_t data_start = (off_t)spill_header_size(r->ncols);
    if (ftruncate(r->spill_fd, data_start) < 0) {
        keel_free(tmp_rows);
        keel_arena_destroy(tmp_slab);
        return KEEL_ERR_IO;
    }
    if (lseek(r->spill_fd, data_start, SEEK_SET) < 0) {
        keel_free(tmp_rows);
        keel_arena_destroy(tmp_slab);
        return KEEL_ERR_IO;
    }

    /* Reset write-buffer state for re-use */
    r->write_buf_pos   = 0;
    r->spill_row_count = 0;
    r->spill_bytes     = 0;
    r->row_count       = 0;

    for (size_t i = 0; i < n; i++) {
        err = spill_append_row(r, tmp_rows[i]->cols);
        if (err != KEEL_OK) {
            keel_free(tmp_rows);
            keel_arena_destroy(tmp_slab);
            return err;
        }
    }
    err = spill_write_eof(r);

    keel_free(tmp_rows);
    keel_arena_destroy(tmp_slab);
    return err;
}

void keel_scatter_result_apply_limit(keel_scatter_result_t* r,
                                 size_t            limit,
                                 size_t            offset)
{
    if (!r || r->row_count == 0) return;

    if (!r->spilled) {
        /* ------------------------------------------------------------------ */
        /* Memory path: memmove the rows[] pointer array + adjust row_count.  */
        /* ------------------------------------------------------------------ */
        if (offset >= r->row_count) {
            r->row_count = 0;
            return;
        }
        size_t available = r->row_count - offset;
        size_t keep      = (limit == 0 || limit > available) ? available : limit;

        if (offset > 0)
            memmove(r->rows, r->rows + offset, keep * sizeof(keel_scatter_row_t*));

        r->row_count = keep;
        return;
    }

    /* ------------------------------------------------------------------ */
    /* Spill path: rewrite the file with only the selected slice.          */
    /* ------------------------------------------------------------------ */
    if (offset >= r->row_count) {
        /* Truncate to an empty result: keep header only + EOF sentinel */
        off_t data_start = (off_t)spill_header_size(r->ncols);
        if (ftruncate(r->spill_fd, data_start) < 0) return; /* best-effort */
        if (lseek(r->spill_fd, data_start, SEEK_SET) < 0) return;
        static const char zero[4] = {0, 0, 0, 0};
        ssize_t n;
        do { n = write(r->spill_fd, zero, 4); } while (n < 0 && errno == EINTR);
        r->row_count       = 0;
        r->spill_row_count = 0;
        r->spill_bytes     = 0;
        r->write_buf_pos   = 0;
        return;
    }

    size_t available = r->row_count - offset;
    size_t keep      = (limit == 0 || limit > available) ? available : limit;

    /* Read the rows in the keep range into a temp arena */
    keel_arena_t* tmp_slab = keel_arena_create(256U * 1024U);
    if (!tmp_slab) return; /* best-effort: leave result unchanged */

    keel_scatter_row_t** tmp_rows =
        (keel_scatter_row_t**)keel_malloc(keep * sizeof(keel_scatter_row_t*));
    if (!tmp_rows) { keel_arena_destroy(tmp_slab); return; }

    keel_scatter_result_iter_t it;
    if (keel_scatter_result_iter_init(&it, r) != KEEL_OK) {
        keel_free(tmp_rows);
        keel_arena_destroy(tmp_slab);
        return;
    }

    size_t read_n = 0;
    size_t collected = 0;
    const keel_scatter_col_val_t* vals;
    while (keel_scatter_result_iter_next(&it, &vals) && collected < keep) {
        if (read_n < offset) { read_n++; continue; }

        size_t data_bytes = 0;
        for (uint16_t i = 0; i < r->ncols; i++)
            if (vals[i].len > 0) data_bytes += (size_t)vals[i].len;
        size_t row_sz = offsetof(keel_scatter_row_t, cols)
                        + (size_t)r->ncols * sizeof(keel_scatter_col_val_t)
                        + data_bytes;

        keel_scatter_row_t* row = (keel_scatter_row_t*)keel_arena_alloc(tmp_slab, row_sz);
        if (!row) break; /* partial result — best effort */

        row->ncols = r->ncols;
        char* raw  = (char*)row + offsetof(keel_scatter_row_t, cols)
                     + (size_t)r->ncols * sizeof(keel_scatter_col_val_t);
        for (uint16_t i = 0; i < r->ncols; i++) {
            row->cols[i].len  = vals[i].len;
            if (vals[i].len > 0) {
                row->cols[i].data = raw;
                memcpy(raw, vals[i].data, (size_t)vals[i].len);
                raw += (size_t)vals[i].len;
            } else {
                row->cols[i].data = NULL;
            }
        }
        tmp_rows[collected++] = row;
        read_n++;
    }
    keel_scatter_result_iter_close(&it);

    /* Rewrite spill file */
    off_t data_start = (off_t)spill_header_size(r->ncols);
    if (ftruncate(r->spill_fd, data_start) < 0 ||
        lseek(r->spill_fd, data_start, SEEK_SET) < 0) {
        keel_free(tmp_rows);
        keel_arena_destroy(tmp_slab);
        return;
    }
    r->write_buf_pos   = 0;
    r->spill_row_count = 0;
    r->spill_bytes     = 0;
    r->row_count       = 0;

    for (size_t i = 0; i < collected; i++)
        spill_append_row(r, tmp_rows[i]->cols);
    spill_write_eof(r);

    keel_free(tmp_rows);
    keel_arena_destroy(tmp_slab);
}

/* ============================================================================
 * Phase D — Scalar aggregate merge
 * ============================================================================ */

/** Returns true when @p type is a floating-point or numeric type. */
static bool col_type_is_float(keel_col_type_t type)
{
    return type == KEEL_COL_TYPE_FLOAT32
        || type == KEEL_COL_TYPE_FLOAT64;
}

/** Return the agg func for @p col_index, or KEEL_AGG_NONE. */
static keel_agg_func_t agg_func_for_col(int16_t                    col_index,
                                         const keel_agg_col_spec_t* specs,
                                         uint16_t                   nspecs)
{
    for (uint16_t i = 0; i < nspecs; i++)
        if (specs[i].col_index == col_index) return specs[i].func;
    return KEEL_AGG_NONE;
}

/** Per-column running accumulator. */
typedef struct {
    keel_agg_func_t func;
    bool            is_float;   /* use dval (double) for SUM, else ival (int64) */
    bool            has_value;  /* any non-NULL value seen */
    int64_t         ival;       /* COUNT / integer SUM */
    double          dval;       /* float / numeric SUM */
    /* MIN / MAX winner + passthrough first-row copy: heap buffer */
    char*           mm_data;
    int32_t         mm_len;
} col_acc_t;

keel_error_t keel_scatter_result_merge_aggs(keel_scatter_result_t*          r,
                                        const keel_agg_col_spec_t* specs,
                                        uint16_t                   nspecs)
{
    if (!r || !specs || nspecs == 0) return KEEL_OK;
    if (r->row_count == 0 || r->ncols == 0) return KEEL_OK;

    /* Reject AVG and binary-format aggregate columns up front */
    for (uint16_t i = 0; i < nspecs; i++) {
        if (specs[i].func == KEEL_AGG_AVG)
            return KEEL_ERR_NOT_SUPPORTED;
        int16_t ci = specs[i].col_index;
        if (ci >= 0 && (uint16_t)ci < r->ncols &&
            r->cols[(uint16_t)ci].format != 0)
            return KEEL_ERR_NOT_SUPPORTED;
    }

    keel_error_t err = KEEL_OK;

    /* Allocate per-column accumulator state */
    col_acc_t* acc = (col_acc_t*)keel_malloc(r->ncols * sizeof(col_acc_t));
    if (!acc) return KEEL_ERR_NOMEM;
    memset(acc, 0, r->ncols * sizeof(col_acc_t));

    for (uint16_t c = 0; c < r->ncols; c++) {
        acc[c].func     = agg_func_for_col((int16_t)c, specs, nspecs);
        acc[c].is_float = (acc[c].func == KEEL_AGG_SUM)
                        && col_type_is_float(r->cols[c].type);
    }

    /* Single-pass accumulation over all shard rows */
    keel_scatter_result_iter_t it;
    err = keel_scatter_result_iter_init(&it, r);
    if (err != KEEL_OK) goto done;

    bool first_row = true;
    const keel_scatter_col_val_t* vals;
    while (keel_scatter_result_iter_next(&it, &vals)) {
        for (uint16_t c = 0; c < r->ncols; c++) {
            col_acc_t* a = &acc[c];

            if (a->func == KEEL_AGG_NONE) {
                /* Passthrough: capture first shard's value */
                if (first_row && vals[c].len >= 0) {
                    char* buf = (char*)keel_malloc((size_t)vals[c].len + 1);
                    if (buf) {
                        memcpy(buf, vals[c].data, (size_t)vals[c].len);
                        buf[vals[c].len] = '\0';
                        a->mm_data   = buf;
                        a->mm_len    = vals[c].len;
                        a->has_value = true;
                    }
                }
                continue;
            }

            /* NULL input */
            if (vals[c].len < 0) {
                /* COUNT treats NULL as 0 (no-op); SUM/MIN/MAX skip NULL */
                continue;
            }

            a->has_value = true;

            switch (a->func) {
            case KEEL_AGG_COUNT:
            case KEEL_AGG_SUM: {
                char tmp[48];
                size_t tl = (size_t)vals[c].len < sizeof(tmp) - 1
                            ? (size_t)vals[c].len : sizeof(tmp) - 1;
                memcpy(tmp, vals[c].data, tl);
                tmp[tl] = '\0';
                if (a->is_float)
                    a->dval += strtod(tmp, NULL);
                else
                    a->ival += strtoll(tmp, NULL, 10);
                break;
            }
            case KEEL_AGG_MIN:
            case KEEL_AGG_MAX: {
                bool update;
                if (a->mm_data == NULL) {
                    update = true;
                } else {
                    keel_scatter_col_val_t cur = { .len = a->mm_len,
                                              .data = a->mm_data };
                    int cmp = keel_scatter_col_cmp(r->cols[c].type,
                                               r->cols[c].format,
                                               &vals[c], &cur);
                    update = (a->func == KEEL_AGG_MIN) ? (cmp < 0) : (cmp > 0);
                }
                if (update) {
                    char* nb = (char*)keel_realloc(a->mm_data,
                                                    (size_t)vals[c].len + 1);
                    if (nb) {
                        memcpy(nb, vals[c].data, (size_t)vals[c].len);
                        nb[vals[c].len] = '\0';
                        a->mm_data = nb;
                        a->mm_len  = vals[c].len;
                    }
                }
                break;
            }
            default: break;
            }
        }
        first_row = false;
    }
    keel_scatter_result_iter_close(&it);

    /* Build the merged output column values */
    char** fmt_bufs = (char**)keel_malloc(r->ncols * sizeof(char*));
    if (!fmt_bufs) { err = KEEL_ERR_NOMEM; goto done; }
    memset(fmt_bufs, 0, r->ncols * sizeof(char*));

    keel_scatter_col_val_t* merged =
        (keel_scatter_col_val_t*)keel_malloc(r->ncols * sizeof(keel_scatter_col_val_t));
    if (!merged) {
        keel_free(fmt_bufs);
        err = KEEL_ERR_NOMEM;
        goto done;
    }

    for (uint16_t c = 0; c < r->ncols; c++) {
        col_acc_t* a = &acc[c];
        merged[c].len  = -1;    /* default: NULL */
        merged[c].data = NULL;

        switch (a->func) {
        case KEEL_AGG_NONE:
            if (a->has_value) {
                fmt_bufs[c] = a->mm_data;
                a->mm_data  = NULL; /* ownership transferred to fmt_bufs */
                merged[c].len  = a->mm_len;
                merged[c].data = fmt_bufs[c];
            }
            break;

        case KEEL_AGG_COUNT:
        case KEEL_AGG_SUM: {
            if (!a->has_value && a->func == KEEL_AGG_SUM) break; /* NULL */
            char* buf = (char*)keel_malloc(32);
            if (!buf) { err = KEEL_ERR_NOMEM; goto fmt_cleanup; }
            fmt_bufs[c] = buf;
            int n;
            if (a->is_float)
                n = snprintf(buf, 32, "%.17g", a->dval);
            else
                n = snprintf(buf, 32, "%lld", (long long)a->ival);
            merged[c].len  = (int32_t)(n > 0 ? n : 0);
            merged[c].data = buf;
            break;
        }
        case KEEL_AGG_MIN:
        case KEEL_AGG_MAX:
            if (a->has_value && a->mm_data) {
                fmt_bufs[c] = a->mm_data;
                a->mm_data  = NULL;
                merged[c].len  = a->mm_len;
                merged[c].data = fmt_bufs[c];
            }
            break;

        default: break;
        }
    }

    /* Replace result with the single merged row */
    if (!r->spilled) {
        /* Memory path: reset row_count, allocate new row in slab */
        size_t data_bytes = 0;
        for (uint16_t c = 0; c < r->ncols; c++)
            if (merged[c].len > 0) data_bytes += (size_t)merged[c].len;
        size_t row_sz = offsetof(keel_scatter_row_t, cols)
                        + (size_t)r->ncols * sizeof(keel_scatter_col_val_t)
                        + data_bytes;

        keel_scatter_row_t* row = (keel_scatter_row_t*)keel_arena_alloc(r->slab, row_sz);
        if (!row) { err = KEEL_ERR_NOMEM; goto fmt_cleanup; }

        row->ncols = r->ncols;
        char* raw  = (char*)row + offsetof(keel_scatter_row_t, cols)
                     + (size_t)r->ncols * sizeof(keel_scatter_col_val_t);
        for (uint16_t c = 0; c < r->ncols; c++) {
            row->cols[c].len = merged[c].len;
            if (merged[c].len > 0) {
                memcpy(raw, merged[c].data, (size_t)merged[c].len);
                row->cols[c].data = raw;
                raw += (size_t)merged[c].len;
            } else {
                row->cols[c].data = NULL;
            }
        }
        r->rows[0]   = row;
        r->row_count = 1;
    } else {
        /* Spill path: rewrite file with single merged row */
        off_t data_start = (off_t)spill_header_size(r->ncols);
        if (ftruncate(r->spill_fd, data_start) < 0 ||
            lseek(r->spill_fd, data_start, SEEK_SET) < 0) {
            err = KEEL_ERR_IO;
            goto fmt_cleanup;
        }
        r->write_buf_pos   = 0;
        r->spill_row_count = 0;
        r->spill_bytes     = 0;
        r->row_count       = 0;
        err = spill_append_row(r, merged);
        if (err == KEEL_OK) err = spill_write_eof(r);
    }

fmt_cleanup:
    keel_free(merged);
    for (uint16_t c = 0; c < r->ncols; c++) keel_free(fmt_bufs[c]);
    keel_free(fmt_bufs);

done:
    for (uint16_t c = 0; c < r->ncols; c++) keel_free(acc[c].mm_data);
    keel_free(acc);
    return err;
}

/* ============================================================================
 * Phase E — GROUP BY + partial aggregate hash-merge
 * ============================================================================ */

#define KEEL_GHT_INIT_CAP  64U

/** FNV-1a 64-bit hash over the GROUP BY key columns of @p vals. */
static uint64_t fnv1a_group_key(const keel_scatter_col_val_t*     vals,
                                  const keel_group_col_spec_t* keys,
                                  uint16_t                     nkeys)
{
    uint64_t h = UINT64_C(14695981039346656037);
    for (uint16_t i = 0; i < nkeys; i++) {
        int16_t ci = keys[i].col_index;
        if (ci < 0) continue;
        const keel_scatter_col_val_t* v = &vals[ci];
        if (v->len < 0) {
            h ^= 0xFFU; h *= UINT64_C(1099511628211);
        } else {
            for (int32_t j = 0; j < v->len; j++) {
                h ^= (uint64_t)(uint8_t)v->data[j];
                h *= UINT64_C(1099511628211);
            }
        }
        h ^= 0x1FU; h *= UINT64_C(1099511628211); /* inter-key separator */
    }
    return h;
}

/**
 * @brief One slot in the open-addressing GROUP BY hash table.
 *
 * key_blob holds the concatenated raw bytes of the GROUP BY key columns in
 * group_keys[] order.  key_lens[k] is the wire length of key k (−1 = NULL).
 * accs[ncols] are per-column aggregate accumulators (keel_malloc'd).
 */
typedef struct {
    bool       used;
    uint64_t   hash;
    char*      key_blob;                              /* arena-allocated */
    int32_t    key_lens[KEEL_SCATTER_MAX_GROUP_COLS]; /* per-key length  */
    col_acc_t* accs;                                  /* keel_malloc'd   */
} group_bucket_t;

/** True when the GROUP key columns of @p vals match the stored key in @p b. */
static bool group_key_eq(const group_bucket_t*        b,
                          const keel_scatter_col_val_t*     vals,
                          const keel_group_col_spec_t* keys,
                          uint16_t                     nkeys)
{
    const char* pos = b->key_blob;
    for (uint16_t i = 0; i < nkeys; i++) {
        int16_t ci = keys[i].col_index;
        if (ci < 0) continue;
        int32_t kl = b->key_lens[i];
        if (vals[ci].len != kl) return false;
        if (kl > 0) {
            if (memcmp(pos, vals[ci].data, (size_t)kl) != 0) return false;
            pos += (size_t)kl;
        }
    }
    return true;
}

/**
 * @brief Find the bucket for @p vals, or return the first free slot.
 * @p cap must be a power of 2.
 */
static group_bucket_t* ght_find_or_insert(group_bucket_t*              table,
                                            size_t                       cap,
                                            uint64_t                     hash,
                                            const keel_scatter_col_val_t*     vals,
                                            const keel_group_col_spec_t* keys,
                                            uint16_t                     nkeys,
                                            bool*                        is_new)
{
    size_t idx = (size_t)(hash & (cap - 1));
    for (size_t probe = 0; probe < cap; probe++) {
        group_bucket_t* b = &table[idx];
        if (!b->used) { *is_new = true;  return b; }
        if (b->hash == hash && group_key_eq(b, vals, keys, nkeys)) {
            *is_new = false; return b;
        }
        idx = (idx + 1) & (cap - 1);
    }
    return NULL; /* table full — should not happen with pre-sizing */
}

keel_error_t keel_scatter_result_group_aggs(keel_scatter_result_t*            r,
                                        const keel_group_col_spec_t* group_keys,
                                        uint16_t                     ngroup_keys,
                                        const keel_agg_col_spec_t*   agg_specs,
                                        uint16_t                     nagg_specs)
{
    if (!r || !group_keys || ngroup_keys == 0) return KEEL_OK;
    if (r->row_count == 0 || r->ncols == 0)   return KEEL_OK;

    /* Reject AVG and binary-format agg columns */
    for (uint16_t i = 0; i < nagg_specs; i++) {
        if (agg_specs[i].func == KEEL_AGG_AVG) return KEEL_ERR_NOT_SUPPORTED;
        int16_t ci = agg_specs[i].col_index;
        if (ci >= 0 && (uint16_t)ci < r->ncols &&
            r->cols[(uint16_t)ci].format != 0)
            return KEEL_ERR_NOT_SUPPORTED;
    }

    keel_error_t err = KEEL_OK;

    /* Hash table: next power-of-2 >= max(row_count * 4, KEEL_GHT_INIT_CAP) */
    size_t min_cap = r->row_count * 4;
    if (min_cap < KEEL_GHT_INIT_CAP) min_cap = KEEL_GHT_INIT_CAP;
    size_t cap = KEEL_GHT_INIT_CAP;
    while (cap < min_cap) cap <<= 1;

    group_bucket_t* table =
        (group_bucket_t*)keel_malloc(cap * sizeof(group_bucket_t));
    if (!table) return KEEL_ERR_NOMEM;
    memset(table, 0, cap * sizeof(group_bucket_t));

    /* Arena for GROUP key raw-byte storage */
    keel_arena_t* key_arena = keel_arena_create(256U * 1024U);
    if (!key_arena) { keel_free(table); return KEEL_ERR_NOMEM; }

    size_t ngroups = 0;

    /* ======================================================================
     * First pass: accumulate all shard rows into the hash table.
     * ====================================================================== */
    keel_scatter_result_iter_t it;
    err = keel_scatter_result_iter_init(&it, r);
    if (err != KEEL_OK) goto done;

    const keel_scatter_col_val_t* vals;
    while (keel_scatter_result_iter_next(&it, &vals)) {
        uint64_t hash = fnv1a_group_key(vals, group_keys, ngroup_keys);
        bool is_new;
        group_bucket_t* b = ght_find_or_insert(table, cap, hash,
                                                vals, group_keys, ngroup_keys,
                                                &is_new);
        if (!b) { err = KEEL_ERR_NOMEM; break; }

        if (is_new) {
            b->used = true;
            b->hash = hash;
            ngroups++;

            /* Copy GROUP key bytes into key_arena */
            size_t key_total = 0;
            for (uint16_t k = 0; k < ngroup_keys; k++) {
                int16_t ci = group_keys[k].col_index;
                int32_t kl = (ci >= 0 && (uint16_t)ci < r->ncols)
                             ? vals[ci].len : -1;
                b->key_lens[k] = kl;
                if (kl > 0) key_total += (size_t)kl;
            }
            b->key_blob = key_total > 0
                          ? (char*)keel_arena_alloc(key_arena, key_total)
                          : NULL;
            if (key_total > 0 && !b->key_blob) { err = KEEL_ERR_NOMEM; break; }
            if (b->key_blob) {
                char* dst = b->key_blob;
                for (uint16_t k = 0; k < ngroup_keys; k++) {
                    int16_t ci = group_keys[k].col_index;
                    if (b->key_lens[k] > 0 && ci >= 0) {
                        memcpy(dst, vals[ci].data, (size_t)b->key_lens[k]);
                        dst += (size_t)b->key_lens[k];
                    }
                }
            }

            /* Allocate per-column accumulators */
            b->accs = (col_acc_t*)keel_malloc(r->ncols * sizeof(col_acc_t));
            if (!b->accs) { err = KEEL_ERR_NOMEM; break; }
            memset(b->accs, 0, r->ncols * sizeof(col_acc_t));
            for (uint16_t c = 0; c < r->ncols; c++) {
                b->accs[c].func     = agg_func_for_col((int16_t)c,
                                                        agg_specs, nagg_specs);
                b->accs[c].is_float = (b->accs[c].func == KEEL_AGG_SUM)
                                    && col_type_is_float(r->cols[c].type);
            }
        }

        /* Accumulate this row into the bucket */
        for (uint16_t c = 0; c < r->ncols; c++) {
            col_acc_t* a = &b->accs[c];

            if (a->func == KEEL_AGG_NONE) {
                /* Passthrough: capture the first shard's value only */
                if (is_new && vals[c].len >= 0) {
                    char* buf = (char*)keel_malloc((size_t)vals[c].len + 1);
                    if (buf) {
                        memcpy(buf, vals[c].data, (size_t)vals[c].len);
                        buf[vals[c].len] = '\0';
                        a->mm_data   = buf;
                        a->mm_len    = vals[c].len;
                        a->has_value = true;
                    }
                }
                continue;
            }

            if (vals[c].len < 0) continue; /* NULL */
            a->has_value = true;

            switch (a->func) {
            case KEEL_AGG_COUNT:
            case KEEL_AGG_SUM: {
                char tmp[48];
                size_t tl = (size_t)vals[c].len < sizeof(tmp) - 1
                            ? (size_t)vals[c].len : sizeof(tmp) - 1;
                memcpy(tmp, vals[c].data, tl); tmp[tl] = '\0';
                if (a->is_float) a->dval += strtod(tmp, NULL);
                else             a->ival += strtoll(tmp, NULL, 10);
                break;
            }
            case KEEL_AGG_MIN:
            case KEEL_AGG_MAX: {
                bool update;
                if (!a->mm_data) {
                    update = true;
                } else {
                    keel_scatter_col_val_t cur = { .len = a->mm_len,
                                              .data = a->mm_data };
                    int cmp = keel_scatter_col_cmp(r->cols[c].type,
                                               r->cols[c].format,
                                               &vals[c], &cur);
                    update = (a->func == KEEL_AGG_MIN) ? (cmp < 0) : (cmp > 0);
                }
                if (update) {
                    char* nb = (char*)keel_realloc(a->mm_data,
                                                    (size_t)vals[c].len + 1);
                    if (nb) {
                        memcpy(nb, vals[c].data, (size_t)vals[c].len);
                        nb[vals[c].len] = '\0';
                        a->mm_data = nb;
                        a->mm_len  = vals[c].len;
                    }
                }
                break;
            }
            default: break;
            }
        }
    }
    keel_scatter_result_iter_close(&it);

    if (err != KEEL_OK) goto done;

    /* ======================================================================
     * Prepare output storage.
     * ====================================================================== */
    if (!r->spilled) {
        /* ngroups <= original row_count <= rows_cap; no realloc needed in
         * practice, but guard defensively. */
        if (ngroups > r->rows_cap) {
            keel_scatter_row_t** nr = (keel_scatter_row_t**)keel_realloc(
                r->rows, ngroups * sizeof *r->rows);
            if (!nr) { err = KEEL_ERR_NOMEM; goto done; }
            r->rows     = nr;
            r->rows_cap = ngroups;
        }
        r->row_count = 0;
    } else {
        off_t data_start = (off_t)spill_header_size(r->ncols);
        if (ftruncate(r->spill_fd, data_start) < 0 ||
            lseek(r->spill_fd, data_start, SEEK_SET) < 0) {
            err = KEEL_ERR_IO; goto done;
        }
        r->write_buf_pos   = 0;
        r->spill_row_count = 0;
        r->spill_bytes     = 0;
        r->row_count       = 0;
    }

    /* ======================================================================
     * Second pass: emit one output row per occupied bucket.
     * ====================================================================== */
    {
        char** fmt_bufs =
            (char**)keel_malloc(r->ncols * sizeof(char*));
        if (!fmt_bufs) { err = KEEL_ERR_NOMEM; goto done; }

        keel_scatter_col_val_t* merged =
            (keel_scatter_col_val_t*)keel_malloc(r->ncols * sizeof(keel_scatter_col_val_t));
        if (!merged) { keel_free(fmt_bufs); err = KEEL_ERR_NOMEM; goto done; }

        for (size_t slot = 0; slot < cap; slot++) {
            group_bucket_t* b = &table[slot];
            if (!b->used) continue;

            memset(fmt_bufs, 0, r->ncols * sizeof(char*));

            for (uint16_t c = 0; c < r->ncols; c++) {
                merged[c].len  = -1;
                merged[c].data = NULL;

                /* GROUP BY key column: reconstruct from key_blob */
                bool is_key = false;
                for (uint16_t k = 0; k < ngroup_keys; k++) {
                    if (group_keys[k].col_index != (int16_t)c) continue;
                    is_key = true;
                    /* Advance past earlier key columns in key_blob */
                    const char* pos = b->key_blob;
                    for (uint16_t j = 0; j < k; j++)
                        if (b->key_lens[j] > 0) pos += (size_t)b->key_lens[j];
                    merged[c].len  = b->key_lens[k];
                    merged[c].data = (b->key_lens[k] > 0) ? pos : NULL;
                    break;
                }
                if (is_key) continue;

                col_acc_t* a = &b->accs[c];
                switch (a->func) {
                case KEEL_AGG_NONE:
                    if (a->has_value) {
                        merged[c].len  = a->mm_len;
                        merged[c].data = a->mm_data;
                    }
                    break;
                case KEEL_AGG_COUNT:
                case KEEL_AGG_SUM: {
                    if (!a->has_value && a->func == KEEL_AGG_SUM) break;
                    char* buf = (char*)keel_malloc(32);
                    if (!buf) { err = KEEL_ERR_NOMEM; break; } /* breaks switch */
                    fmt_bufs[c] = buf;
                    int n;
                    if (a->is_float) n = snprintf(buf, 32, "%.17g", a->dval);
                    else             n = snprintf(buf, 32, "%lld", (long long)a->ival);
                    merged[c].len  = (int32_t)(n > 0 ? n : 0);
                    merged[c].data = buf;
                    break;
                }
                case KEEL_AGG_MIN:
                case KEEL_AGG_MAX:
                    if (a->has_value && a->mm_data) {
                        merged[c].len  = a->mm_len;
                        merged[c].data = a->mm_data;
                    }
                    break;
                default: break;
                }
            }

            if (err != KEEL_OK) {
                /* Free fmt_bufs before skipping the append */
                for (uint16_t c = 0; c < r->ncols; c++) {
                    keel_free(fmt_bufs[c]);
                    fmt_bufs[c] = NULL;
                }
                break;
            }

            /* Append merged row to output (merged[c].data may alias fmt_bufs[c]) */
            if (!r->spilled) {
                size_t data_bytes = 0;
                for (uint16_t c = 0; c < r->ncols; c++)
                    if (merged[c].len > 0) data_bytes += (size_t)merged[c].len;
                size_t row_sz = offsetof(keel_scatter_row_t, cols)
                                + (size_t)r->ncols * sizeof(keel_scatter_col_val_t)
                                + data_bytes;
                keel_scatter_row_t* row =
                    (keel_scatter_row_t*)keel_arena_alloc(r->slab, row_sz);
                if (!row) { err = KEEL_ERR_NOMEM; break; }
                row->ncols = r->ncols;
                char* raw  = (char*)row + offsetof(keel_scatter_row_t, cols)
                             + (size_t)r->ncols * sizeof(keel_scatter_col_val_t);
                for (uint16_t c = 0; c < r->ncols; c++) {
                    row->cols[c].len = merged[c].len;
                    if (merged[c].len > 0) {
                        memcpy(raw, merged[c].data, (size_t)merged[c].len);
                        row->cols[c].data = raw;
                        raw += (size_t)merged[c].len;
                    } else {
                        row->cols[c].data = NULL;
                    }
                }
                r->rows[r->row_count++] = row;
            } else {
                err = spill_append_row(r, merged);
            }

            /* Free fmt_bufs after row data has been copied to output */
            for (uint16_t c = 0; c < r->ncols; c++) {
                keel_free(fmt_bufs[c]);
                fmt_bufs[c] = NULL;
            }
            if (err != KEEL_OK) break;
        }

        if (r->spilled && err == KEEL_OK)
            err = spill_write_eof(r);

        keel_free(merged);
        keel_free(fmt_bufs);
    }

done:
    /* Free per-bucket accumulators (mm_data is keel_malloc'd) */
    for (size_t slot = 0; slot < cap; slot++) {
        if (table[slot].used && table[slot].accs) {
            for (uint16_t c = 0; c < r->ncols; c++)
                keel_free(table[slot].accs[c].mm_data);
            keel_free(table[slot].accs);
        }
    }
    keel_free(table);
    keel_arena_destroy(key_arena);
    return err;
}

/* ============================================================================
 * Phase H — HAVING post-filter and AVG finalize
 * ============================================================================ */

/** Evaluate whether @p cv satisfies @p pred's comparison. */
static bool having_row_passes(const keel_scatter_result_t*   r,
                               const keel_having_pred_t* pred,
                               const keel_scatter_col_val_t*  cv)
{
    if (cv->len < 0) return false; /* NULL never satisfies any comparison */

    keel_scatter_col_val_t lv = { .len = pred->literal_len, .data = pred->literal };
    uint32_t oid = ((uint16_t)pred->col_index < r->ncols)
                   ? r->cols[(uint16_t)pred->col_index].type
                   : KEEL_COL_TYPE_TEXT;
    int16_t  fmt = ((uint16_t)pred->col_index < r->ncols)
                   ? r->cols[(uint16_t)pred->col_index].format : 0;

    int cmp = keel_scatter_col_cmp(oid, fmt, cv, &lv);
    switch (pred->op) {
    case KEEL_CMP_EQ: return cmp == 0;
    case KEEL_CMP_NE: return cmp != 0;
    case KEEL_CMP_LT: return cmp < 0;
    case KEEL_CMP_LE: return cmp <= 0;
    case KEEL_CMP_GT: return cmp > 0;
    case KEEL_CMP_GE: return cmp >= 0;
    default:          return false;
    }
}

/** Return true when all @p npreds predicates pass for the given column values. */
static bool having_all_pass(const keel_scatter_result_t*   r,
                             const keel_having_pred_t* preds,
                             uint16_t                  npreds,
                             const keel_scatter_col_val_t*  vals,
                             uint16_t                  ncols)
{
    for (uint16_t p = 0; p < npreds; p++) {
        int16_t ci = preds[p].col_index;
        if (ci < 0 || (uint16_t)ci >= ncols) continue;
        if (!having_row_passes(r, &preds[p], &vals[(uint16_t)ci]))
            return false;
    }
    return true;
}

keel_error_t keel_scatter_result_apply_having(keel_scatter_result_t*         r,
                                          const keel_having_pred_t* preds,
                                          uint16_t                  npreds)
{
    if (!r || !preds || npreds == 0) return KEEL_OK;
    if (r->row_count == 0 || r->ncols == 0) return KEEL_OK;

    if (!r->spilled) {
        /* ------------------------------------------------------------------ */
        /* Memory path: compact rows[] in-place.                              */
        /* ------------------------------------------------------------------ */
        size_t kept = 0;
        for (size_t i = 0; i < r->row_count; i++) {
            keel_scatter_row_t* row = r->rows[i];
            if (having_all_pass(r, preds, npreds, row->cols, row->ncols))
                r->rows[kept++] = row;
        }
        r->row_count = kept;
        return KEEL_OK;
    }

    /* ---------------------------------------------------------------------- */
    /* Spill path: read all rows, keep passing ones, rewrite file.            */
    /* ---------------------------------------------------------------------- */
    keel_arena_t* tmp_slab = keel_arena_create(256U * 1024U);
    if (!tmp_slab) return KEEL_ERR_NOMEM;

    keel_scatter_row_t** tmp_rows =
        (keel_scatter_row_t**)keel_malloc(r->row_count * sizeof *tmp_rows);
    if (!tmp_rows) { keel_arena_destroy(tmp_slab); return KEEL_ERR_NOMEM; }

    keel_scatter_result_iter_t it;
    keel_error_t err = keel_scatter_result_iter_init(&it, r);
    if (err != KEEL_OK) {
        keel_free(tmp_rows);
        keel_arena_destroy(tmp_slab);
        return err;
    }

    size_t collected = 0;
    const keel_scatter_col_val_t* vals;
    while (keel_scatter_result_iter_next(&it, &vals)) {
        if (!having_all_pass(r, preds, npreds, vals, r->ncols)) continue;

        size_t data_bytes = 0;
        for (uint16_t i = 0; i < r->ncols; i++)
            if (vals[i].len > 0) data_bytes += (size_t)vals[i].len;
        size_t row_sz = offsetof(keel_scatter_row_t, cols)
                        + (size_t)r->ncols * sizeof(keel_scatter_col_val_t)
                        + data_bytes;

        keel_scatter_row_t* row = (keel_scatter_row_t*)keel_arena_alloc(tmp_slab, row_sz);
        if (!row) break; /* best effort */

        row->ncols = r->ncols;
        char* raw  = (char*)row + offsetof(keel_scatter_row_t, cols)
                     + (size_t)r->ncols * sizeof(keel_scatter_col_val_t);
        for (uint16_t i = 0; i < r->ncols; i++) {
            row->cols[i].len = vals[i].len;
            if (vals[i].len > 0) {
                row->cols[i].data = raw;
                memcpy(raw, vals[i].data, (size_t)vals[i].len);
                raw += (size_t)vals[i].len;
            } else {
                row->cols[i].data = NULL;
            }
        }
        tmp_rows[collected++] = row;
    }
    keel_scatter_result_iter_close(&it);

    off_t data_start = (off_t)spill_header_size(r->ncols);
    if (ftruncate(r->spill_fd, data_start) < 0 ||
        lseek(r->spill_fd, data_start, SEEK_SET) < 0) {
        keel_free(tmp_rows);
        keel_arena_destroy(tmp_slab);
        return KEEL_ERR_IO;
    }
    r->write_buf_pos   = 0;
    r->spill_row_count = 0;
    r->spill_bytes     = 0;
    r->row_count       = 0;

    for (size_t i = 0; i < collected; i++)
        spill_append_row(r, tmp_rows[i]->cols);
    err = spill_write_eof(r);

    keel_free(tmp_rows);
    keel_arena_destroy(tmp_slab);
    return err;
}

keel_error_t keel_scatter_result_finalize_avg(keel_scatter_result_t*               r,
                                          const keel_avg_finalize_spec_t* specs,
                                          uint16_t                        nspecs)
{
    if (!r || !specs || nspecs == 0 || r->row_count == 0) return KEEL_OK;

    if (!r->spilled) {
        /* ------------------------------------------------------------------ */
        /* Memory path: compute AVG in each row; update out_col in place.     */
        /* ------------------------------------------------------------------ */
        for (size_t ri = 0; ri < r->row_count; ri++) {
            keel_scatter_row_t* row = r->rows[ri];

            for (uint16_t s = 0; s < nspecs; s++) {
                const keel_avg_finalize_spec_t* sp = &specs[s];
                if (sp->sum_col < 0   || (uint16_t)sp->sum_col   >= row->ncols) continue;
                if (sp->count_col < 0 || (uint16_t)sp->count_col >= row->ncols) continue;
                if (sp->out_col < 0   || (uint16_t)sp->out_col   >= row->ncols) continue;

                const keel_scatter_col_val_t* sv = &row->cols[(uint16_t)sp->sum_col];
                const keel_scatter_col_val_t* cv = &row->cols[(uint16_t)sp->count_col];

                if (sv->len < 0 || cv->len < 0) {
                    row->cols[(uint16_t)sp->out_col].len  = -1;
                    row->cols[(uint16_t)sp->out_col].data = NULL;
                    continue;
                }

                char tmp[64];
                size_t sl = (size_t)sv->len < sizeof tmp - 1 ? (size_t)sv->len : sizeof tmp - 1;
                memcpy(tmp, sv->data, sl); tmp[sl] = '\0';
                double sum_d = strtod(tmp, NULL);

                size_t cl = (size_t)cv->len < sizeof tmp - 1 ? (size_t)cv->len : sizeof tmp - 1;
                memcpy(tmp, cv->data, cl); tmp[cl] = '\0';
                double cnt_d = strtod(tmp, NULL);

                if (cnt_d == 0.0) {
                    row->cols[(uint16_t)sp->out_col].len  = -1;
                    row->cols[(uint16_t)sp->out_col].data = NULL;
                    continue;
                }

                int n = snprintf(tmp, sizeof tmp, "%.17g", sum_d / cnt_d);
                int32_t avg_len = (int32_t)(n > 0 ? n : 0);

                /* Try to allocate from the slab; fall back to keel_malloc */
                char* buf = (char*)keel_arena_alloc(r->slab, (size_t)avg_len + 1);
                if (!buf) {
                    buf = (char*)keel_malloc((size_t)avg_len + 1);
                    if (!buf) return KEEL_ERR_NOMEM;
                }
                memcpy(buf, tmp, (size_t)avg_len);
                buf[avg_len] = '\0';
                row->cols[(uint16_t)sp->out_col].len  = avg_len;
                row->cols[(uint16_t)sp->out_col].data = buf;
            }
        }
        return KEEL_OK;
    }

    /* ---------------------------------------------------------------------- */
    /* Spill path: read all rows, compute AVG, rebuild the spill file.        */
    /* ---------------------------------------------------------------------- */
    keel_arena_t* tmp_slab = keel_arena_create(256U * 1024U);
    if (!tmp_slab) return KEEL_ERR_NOMEM;

    keel_scatter_row_t** tmp_rows =
        (keel_scatter_row_t**)keel_malloc(r->row_count * sizeof *tmp_rows);
    if (!tmp_rows) { keel_arena_destroy(tmp_slab); return KEEL_ERR_NOMEM; }

    keel_scatter_result_iter_t it;
    keel_error_t err = keel_scatter_result_iter_init(&it, r);
    if (err != KEEL_OK) {
        keel_free(tmp_rows);
        keel_arena_destroy(tmp_slab);
        return err;
    }

    /* Temporary mutable column-value array for computing AVG per row */
    keel_scatter_col_val_t* mvals =
        (keel_scatter_col_val_t*)keel_malloc(r->ncols * sizeof *mvals);
    if (!mvals) {
        keel_scatter_result_iter_close(&it);
        keel_free(tmp_rows);
        keel_arena_destroy(tmp_slab);
        return KEEL_ERR_NOMEM;
    }

    /* Per-spec formatted output buffers (reset each row) */
    char* fmt_bufs[KEEL_SCATTER_MAX_AVG_SPECS];
    memset(fmt_bufs, 0, sizeof fmt_bufs);

    size_t collected = 0;
    const keel_scatter_col_val_t* vals;
    while (keel_scatter_result_iter_next(&it, &vals) && err == KEEL_OK) {
        memcpy(mvals, vals, r->ncols * sizeof *mvals);
        memset(fmt_bufs, 0, sizeof fmt_bufs);

        /* Compute AVG for each spec, storing results in mvals and fmt_bufs */
        for (uint16_t s = 0; s < nspecs && err == KEEL_OK; s++) {
            const keel_avg_finalize_spec_t* sp = &specs[s];
            if (sp->sum_col < 0   || (uint16_t)sp->sum_col   >= r->ncols) continue;
            if (sp->count_col < 0 || (uint16_t)sp->count_col >= r->ncols) continue;
            if (sp->out_col < 0   || (uint16_t)sp->out_col   >= r->ncols) continue;

            const keel_scatter_col_val_t* sv = &mvals[(uint16_t)sp->sum_col];
            const keel_scatter_col_val_t* cv = &mvals[(uint16_t)sp->count_col];

            if (sv->len < 0 || cv->len < 0) {
                mvals[(uint16_t)sp->out_col].len  = -1;
                mvals[(uint16_t)sp->out_col].data = NULL;
                continue;
            }

            char tmp[64];
            size_t sl = (size_t)sv->len < sizeof tmp - 1 ? (size_t)sv->len : sizeof tmp - 1;
            memcpy(tmp, sv->data, sl); tmp[sl] = '\0';
            double sum_d = strtod(tmp, NULL);

            size_t cl = (size_t)cv->len < sizeof tmp - 1 ? (size_t)cv->len : sizeof tmp - 1;
            memcpy(tmp, cv->data, cl); tmp[cl] = '\0';
            double cnt_d = strtod(tmp, NULL);

            if (cnt_d == 0.0) {
                mvals[(uint16_t)sp->out_col].len  = -1;
                mvals[(uint16_t)sp->out_col].data = NULL;
                continue;
            }

            int n = snprintf(tmp, sizeof tmp, "%.17g", sum_d / cnt_d);
            int32_t avg_len = (int32_t)(n > 0 ? n : 0);
            char* buf = (char*)keel_malloc((size_t)avg_len + 1);
            if (!buf) { err = KEEL_ERR_NOMEM; break; }
            memcpy(buf, tmp, (size_t)avg_len); buf[avg_len] = '\0';
            fmt_bufs[s]                           = buf;
            mvals[(uint16_t)sp->out_col].len  = avg_len;
            mvals[(uint16_t)sp->out_col].data = buf;
        }

        if (err != KEEL_OK) {
            for (uint16_t s = 0; s < nspecs; s++) keel_free(fmt_bufs[s]);
            break;
        }

        /* Copy modified row into tmp_slab */
        size_t data_bytes = 0;
        for (uint16_t i = 0; i < r->ncols; i++)
            if (mvals[i].len > 0) data_bytes += (size_t)mvals[i].len;
        size_t row_sz = offsetof(keel_scatter_row_t, cols)
                        + (size_t)r->ncols * sizeof(keel_scatter_col_val_t)
                        + data_bytes;

        keel_scatter_row_t* row = (keel_scatter_row_t*)keel_arena_alloc(tmp_slab, row_sz);
        if (!row) {
            for (uint16_t s = 0; s < nspecs; s++) keel_free(fmt_bufs[s]);
            err = KEEL_ERR_NOMEM;
            break;
        }
        row->ncols = r->ncols;
        char* raw  = (char*)row + offsetof(keel_scatter_row_t, cols)
                     + (size_t)r->ncols * sizeof(keel_scatter_col_val_t);
        for (uint16_t i = 0; i < r->ncols; i++) {
            row->cols[i].len = mvals[i].len;
            if (mvals[i].len > 0) {
                row->cols[i].data = raw;
                memcpy(raw, mvals[i].data, (size_t)mvals[i].len);
                raw += (size_t)mvals[i].len;
            } else {
                row->cols[i].data = NULL;
            }
        }
        tmp_rows[collected++] = row;

        for (uint16_t s = 0; s < nspecs; s++) keel_free(fmt_bufs[s]);
    }
    keel_scatter_result_iter_close(&it);
    keel_free(mvals);

    if (err == KEEL_OK) {
        off_t data_start = (off_t)spill_header_size(r->ncols);
        if (ftruncate(r->spill_fd, data_start) < 0 ||
            lseek(r->spill_fd, data_start, SEEK_SET) < 0) {
            err = KEEL_ERR_IO;
        } else {
            r->write_buf_pos   = 0;
            r->spill_row_count = 0;
            r->spill_bytes     = 0;
            r->row_count       = 0;
            for (size_t i = 0; i < collected; i++)
                spill_append_row(r, tmp_rows[i]->cols);
            err = spill_write_eof(r);
        }
    }

    keel_free(tmp_rows);
    keel_arena_destroy(tmp_slab);
    return err;
}

/* ============================================================================
 * Phase F: Window function global recomputation
 * ============================================================================ */

/**
 * @brief Read a column value as double (for Tier 4 aggregate window functions).
 *
 * Parses the text-format column value.  Returns 0.0 for NULL or parse error.
 */
static double wc_col_as_double(const keel_scatter_row_t* row, int ci)
{
    if (ci < 0 || ci >= row->ncols) return 0.0;
    const keel_scatter_col_val_t* cv = &row->cols[ci];
    if (cv->len <= 0 || !cv->data) return 0.0;
    /* Column data is NOT null-terminated (columns share a contiguous buffer).
     * Copy to a temporary buffer before parsing to avoid reading past the end. */
    char tmp[64];
    size_t l = (size_t)cv->len;
    if (l >= sizeof(tmp)) l = sizeof(tmp) - 1;
    memcpy(tmp, cv->data, l);
    tmp[l] = '\0';
    return strtod(tmp, NULL);
}

/**
 * @brief Compare two column values as doubles (for MIN/MAX window functions).
 *
 * Returns -1 if a < b, +1 if a > b, 0 if equal.
 */
static int wc_col_cmp_double(const keel_scatter_row_t* ra, int ca,
                              const keel_scatter_row_t* rb, int cb)
{
    double va = wc_col_as_double(ra, ca);
    double vb = wc_col_as_double(rb, cb);
    if (va < vb) return -1;
    if (va > vb) return  1;
    return 0;
}

/**
 * @brief Write a text-format int64 into col_index of a memory-resident row.
 */
static keel_error_t wc_set_int64(keel_scatter_result_t* r,
                                   keel_scatter_row_t*    row,
                                   int16_t           col_idx,
                                   int64_t           value)
{
    char tmp[32];
    int n = snprintf(tmp, sizeof tmp, "%" PRId64, value);
    if (n <= 0) return KEEL_OK; /* degenerate */
    size_t blen = (size_t)n;

    /* Try slab first, then heap */
    char* buf = (char*)keel_arena_alloc(r->slab, blen + 1);
    if (!buf) {
        buf = (char*)keel_malloc(blen + 1);
        if (!buf) return KEEL_ERR_NOMEM;
    }
    memcpy(buf, tmp, blen + 1);
    row->cols[(uint16_t)col_idx].len  = (int32_t)blen;
    row->cols[(uint16_t)col_idx].data = buf;
    return KEEL_OK;
}

/**
 * @brief Write a text-format float64 into col_index of a memory-resident row.
 */
static keel_error_t wc_set_float64(keel_scatter_result_t* r,
                                     keel_scatter_row_t*    row,
                                     int16_t           col_idx,
                                     double            value)
{
    char tmp[32];
    int n = snprintf(tmp, sizeof tmp, "%.17g", value);
    if (n <= 0) return KEEL_OK;
    size_t blen = (size_t)n;
    char* buf = (char*)keel_arena_alloc(r->slab, blen + 1);
    if (!buf) {
        buf = (char*)keel_malloc(blen + 1);
        if (!buf) return KEEL_ERR_NOMEM;
    }
    memcpy(buf, tmp, blen + 1);
    row->cols[(uint16_t)col_idx].len  = (int32_t)blen;
    row->cols[(uint16_t)col_idx].data = buf;
    return KEEL_OK;
}

/**
 * @brief True if adjacent sorted rows are tied on all order_keys.
 *
 * Used by RANK / DENSE_RANK / CUME_DIST / PERCENT_RANK tie detection.
 */
static bool wc_rows_tied(const keel_scatter_result_t*       r,
                          const keel_scatter_row_t*          a,
                          const keel_scatter_row_t*          b,
                          const keel_sort_key_t*        keys,
                          uint16_t                      nkeys)
{
    for (uint16_t k = 0; k < nkeys; k++) {
        int16_t ci = keys[k].col_index;
        if (ci < 0 || (uint16_t)ci >= r->ncols) continue;
        int cmp = keel_scatter_col_cmp(r->cols[(uint16_t)ci].type,
                                   r->cols[(uint16_t)ci].format,
                                   &a->cols[(uint16_t)ci],
                                   &b->cols[(uint16_t)ci]);
        if (cmp != 0) return false;
    }
    return true;
}

/* ============================================================================
 * Tier 3 helpers: value-access window functions
 * (LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE)
 * ============================================================================ */

/**
 * Copy the value of column @p src_ci from @p src_row into column @p dst_ci
 * of @p dst_row.  The copied data is arena-allocated when possible, or
 * keel_malloc'd as a fallback (same pattern as wc_set_int64).
 */
static keel_error_t wc_copy_col(keel_scatter_result_t* r,
                                  keel_scatter_row_t*    dst_row, int dst_ci,
                                  keel_scatter_row_t*    src_row, int src_ci)
{
    if (src_ci < 0 || src_ci >= src_row->ncols) {
        dst_row->cols[dst_ci].len  = -1;
        dst_row->cols[dst_ci].data = NULL;
        return KEEL_OK;
    }
    const keel_scatter_col_val_t* sv = &src_row->cols[src_ci];
    if (sv->len <= 0) {
        dst_row->cols[dst_ci].len  = sv->len;
        dst_row->cols[dst_ci].data = NULL;
        return KEEL_OK;
    }
    /* Try slab first, then fall back to keel_malloc */
    char* buf = keel_arena_alloc(r->slab, (size_t)sv->len);
    if (!buf) {
        buf = keel_malloc((size_t)sv->len);
        if (!buf) return KEEL_ERR_NOMEM;
    }
    memcpy(buf, sv->data, (size_t)sv->len);
    dst_row->cols[dst_ci].len  = sv->len;
    dst_row->cols[dst_ci].data = buf;
    return KEEL_OK;
}

/**
 * Write a NULL or the spec's default_val into column @p ci of @p row.
 */
static keel_error_t wc_write_default(keel_scatter_result_t*             r,
                                      keel_scatter_row_t*                row,
                                      int                           ci,
                                      const keel_window_col_spec_t* spec)
{
    if (spec->default_val_len < 0) {
        row->cols[ci].len  = -1;
        row->cols[ci].data = NULL;
        return KEEL_OK;
    }
    size_t dlen = (size_t)spec->default_val_len;
    char*  buf  = keel_arena_alloc(r->slab, dlen ? dlen : 1);
    if (!buf) {
        buf = keel_malloc(dlen ? dlen : 1);
        if (!buf) return KEEL_ERR_NOMEM;
    }
    if (dlen) memcpy(buf, spec->default_val, dlen);
    row->cols[ci].len  = (int32_t)dlen;
    row->cols[ci].data = buf;
    return KEEL_OK;
}

/**
 * Compute frame-start row index for ROWS mode.
 * The returned value may be less than @p ps; caller must clamp to [ps, pe].
 */
static size_t wc_frame_start_idx(size_t cur, size_t ps,
                                   const keel_frame_bound_t* b)
{
    switch (b->type) {
    case KEEL_FRAME_UNBOUNDED_PRECEDING:
        return ps;
    case KEEL_FRAME_N_PRECEDING:
        return (b->n > 0 && cur >= (size_t)b->n) ? cur - (size_t)b->n : 0;
    case KEEL_FRAME_CURRENT_ROW:
        return cur;
    case KEEL_FRAME_N_FOLLOWING:
        return cur + (size_t)b->n; /* may exceed pe; caller clamps */
    case KEEL_FRAME_UNBOUNDED_FOLLOWING:
        return cur; /* degenerate: treat as current row */
    }
    return cur;
}

/**
 * Compute frame-end row index for ROWS mode.
 * The returned value may be outside [ps, pe]; caller must clamp.
 */
static size_t wc_frame_end_idx(size_t cur, size_t pe,
                                 const keel_frame_bound_t* b)
{
    switch (b->type) {
    case KEEL_FRAME_UNBOUNDED_PRECEDING:
        return cur; /* degenerate: treat as current row */
    case KEEL_FRAME_N_PRECEDING:
        return (b->n > 0 && cur >= (size_t)b->n) ? cur - (size_t)b->n : 0;
    case KEEL_FRAME_CURRENT_ROW:
        return cur;
    case KEEL_FRAME_N_FOLLOWING:
        return cur + (size_t)b->n; /* clamped to pe by caller */
    case KEEL_FRAME_UNBOUNDED_FOLLOWING:
        return pe;
    }
    return cur;
}

/**
 * @brief Apply one Tier 3 value-access window function to an in-memory result.
 *
 * Pre-condition: the result MUST already be sorted by
 * (spec->partition_keys, spec->order_keys) — the caller is responsible for
 * the sort.
 *
 * Algorithm (O(N)):
 *  1. One forward pass to compute part_lo[i] (start of row i's partition).
 *  2. One backward pass to compute part_hi[i] (end of row i's partition).
 *  3. One forward pass to write output values using frame or offset logic.
 */
static keel_error_t wc_apply_val_mem(keel_scatter_result_t*             r,
                                      const keel_window_col_spec_t* spec)
{
    size_t N = r->row_count;
    if (N == 0) return KEEL_OK;

    int dst_ci = spec->col_index;
    int src_ci = spec->source_col;

    /* When the source column was not found in the SELECT list (e.g.
     * FIRST_VALUE(score) OVER (...) AS fv — 'score' is the argument but only
     * 'fv' appears in the result), fall back to using the destination column
     * itself as the source.  Each shard already computed the correct local
     * value so we re-use it as the "source" for the global frame look-up. */
    if (src_ci < 0) src_ci = dst_ci;

    /* Partition boundary arrays */
    size_t* part_lo = (size_t*)keel_malloc(N * sizeof(size_t));
    size_t* part_hi = (size_t*)keel_malloc(N * sizeof(size_t));
    if (!part_lo || !part_hi) {
        keel_free(part_lo);
        keel_free(part_hi);
        return KEEL_ERR_NOMEM;
    }

    /* Forward pass: partition start */
    size_t cur_ps = 0;
    for (size_t i = 0; i < N; i++) {
        if (i > 0 && spec->npartition_keys > 0 &&
            !wc_rows_tied(r, r->rows[i - 1], r->rows[i],
                           spec->partition_keys, spec->npartition_keys))
            cur_ps = i;
        part_lo[i] = cur_ps;
    }

    /* Backward pass: partition end */
    size_t cur_pe = N - 1;
    for (size_t i_back = 0; i_back < N; i_back++) {
        size_t i = N - 1 - i_back;
        if (i_back > 0 && part_lo[i] != part_lo[i + 1])
            cur_pe = i;
        part_hi[i] = cur_pe;
    }

    keel_error_t err = KEEL_OK;

    for (size_t i = 0; i < N && err == KEEL_OK; i++) {
        size_t ps = part_lo[i];
        size_t pe = part_hi[i];
        keel_scatter_row_t* dst_row = r->rows[i];

        switch (spec->func) {

        /* ------------------------------------------------------------------ */
        case KEEL_WFUNC_LAG:
        case KEEL_WFUNC_LEAD: {
            int64_t offset = spec->val_offset > 0 ? spec->val_offset : 1;
            int64_t j_signed = (spec->func == KEEL_WFUNC_LEAD)
                               ? (int64_t)i + offset
                               : (int64_t)i - offset;
            /* Out of partition → write default or NULL */
            if (j_signed < (int64_t)ps || j_signed > (int64_t)pe) {
                err = wc_write_default(r, dst_row, dst_ci, spec);
            } else {
                err = wc_copy_col(r, dst_row, dst_ci, r->rows[(size_t)j_signed], src_ci);
            }
            break;
        }

        /* ------------------------------------------------------------------ */
        case KEEL_WFUNC_FIRST_VALUE: {
            size_t fs = wc_frame_start_idx(i, ps, &spec->frame_start);
            if (fs < ps) fs = ps;
            if (fs > pe) { err = wc_write_default(r, dst_row, dst_ci, spec); break; }
            err = wc_copy_col(r, dst_row, dst_ci, r->rows[fs], src_ci);
            break;
        }

        /* ------------------------------------------------------------------ */
        case KEEL_WFUNC_LAST_VALUE: {
            size_t fe = wc_frame_end_idx(i, pe, &spec->frame_end);
            if (fe < ps) { err = wc_write_default(r, dst_row, dst_ci, spec); break; }
            if (fe > pe) fe = pe;
            err = wc_copy_col(r, dst_row, dst_ci, r->rows[fe], src_ci);
            break;
        }

        /* ------------------------------------------------------------------ */
        case KEEL_WFUNC_NTH_VALUE: {
            int64_t nth = spec->val_offset > 0 ? spec->val_offset : 1;
            size_t fs = wc_frame_start_idx(i, ps, &spec->frame_start);
            size_t fe = wc_frame_end_idx(i, pe, &spec->frame_end);
            if (fs < ps) fs = ps;
            if (fe > pe) fe = pe;
            /* fs + (nth - 1) is the target row (1-based n) */
            if (nth <= 0 || (size_t)(nth - 1) > fe - fs) {
                err = wc_write_default(r, dst_row, dst_ci, spec); /* NULL: n > frame */
            } else {
                size_t target = fs + (size_t)(nth - 1);
                err = wc_copy_col(r, dst_row, dst_ci, r->rows[target], src_ci);
            }
            break;
        }

        /* ------------------------------------------------------------------ */
        /* Tier 4: aggregate window functions                                 */
        /* ------------------------------------------------------------------ */
        case KEEL_WFUNC_AGG_SUM:
        case KEEL_WFUNC_AGG_AVG: {
            size_t fs2 = wc_frame_start_idx(i, ps, &spec->frame_start);
            size_t fe2 = wc_frame_end_idx(i, pe, &spec->frame_end);
            if (fs2 < ps) fs2 = ps;
            if (fe2 > pe) fe2 = pe;
            double sum = 0.0;
            int64_t cnt2 = 0;
            for (size_t j = fs2; j <= fe2; j++) {
                if (src_ci >= 0 && src_ci < r->rows[j]->ncols &&
                    r->rows[j]->cols[src_ci].len > 0) {
                    sum += wc_col_as_double(r->rows[j], src_ci);
                    cnt2++;
                }
            }
            if (spec->func == KEEL_WFUNC_AGG_AVG)
                err = wc_set_float64(r, dst_row, dst_ci,
                                      cnt2 > 0 ? sum / (double)cnt2 : 0.0);
            else
                err = wc_set_float64(r, dst_row, dst_ci, sum);
            break;
        }

        case KEEL_WFUNC_AGG_COUNT: {
            size_t fs2 = wc_frame_start_idx(i, ps, &spec->frame_start);
            size_t fe2 = wc_frame_end_idx(i, pe, &spec->frame_end);
            if (fs2 < ps) fs2 = ps;
            if (fe2 > pe) fe2 = pe;
            int64_t cnt3 = 0;
            for (size_t j = fs2; j <= fe2; j++) {
                /* COUNT(*): every row counts; COUNT(col): skip NULLs */
                if (src_ci < 0 ||
                    (src_ci < r->rows[j]->ncols &&
                     r->rows[j]->cols[src_ci].len > 0))
                    cnt3++;
            }
            err = wc_set_int64(r, dst_row, dst_ci, cnt3);
            break;
        }

        case KEEL_WFUNC_AGG_MIN: {
            size_t fs2 = wc_frame_start_idx(i, ps, &spec->frame_start);
            size_t fe2 = wc_frame_end_idx(i, pe, &spec->frame_end);
            if (fs2 < ps) fs2 = ps;
            if (fe2 > pe) fe2 = pe;
            size_t best = fs2;
            for (size_t j = fs2 + 1; j <= fe2; j++) {
                if (wc_col_cmp_double(r->rows[j], src_ci,
                                       r->rows[best], src_ci) < 0)
                    best = j;
            }
            err = wc_copy_col(r, dst_row, dst_ci, r->rows[best], src_ci);
            break;
        }

        case KEEL_WFUNC_AGG_MAX: {
            size_t fs2 = wc_frame_start_idx(i, ps, &spec->frame_start);
            size_t fe2 = wc_frame_end_idx(i, pe, &spec->frame_end);
            if (fs2 < ps) fs2 = ps;
            if (fe2 > pe) fe2 = pe;
            size_t best = fs2;
            for (size_t j = fs2 + 1; j <= fe2; j++) {
                if (wc_col_cmp_double(r->rows[j], src_ci,
                                       r->rows[best], src_ci) > 0)
                    best = j;
            }
            err = wc_copy_col(r, dst_row, dst_ci, r->rows[best], src_ci);
            break;
        }

        default:
            break; /* unreachable for Tier 3/4 dispatch */
        }
    }

    keel_free(part_lo);
    keel_free(part_hi);
    return err;
}

/**
 * @brief Apply one window function spec to an in-memory sorted result.
 *
 * The result MUST already be sorted by spec->order_keys before this call.
 * Overwrites spec->col_index in every row with the globally correct value.
 */
static keel_error_t wc_apply_mem(keel_scatter_result_t*             r,
                                   const keel_window_col_spec_t* spec)
{
    size_t N = r->row_count;
    if (N == 0) return KEEL_OK;

    int16_t ci = spec->col_index;
    if (ci < 0 || (uint16_t)ci >= r->ncols) return KEEL_OK;

    keel_error_t err = KEEL_OK;

    switch (spec->func) {

    /* ------------------------------------------------------------------ */
    case KEEL_WFUNC_ROW_NUMBER:
        for (size_t i = 0; i < N && err == KEEL_OK; i++)
            err = wc_set_int64(r, r->rows[i], ci, (int64_t)(i + 1));
        break;

    /* ------------------------------------------------------------------ */
    case KEEL_WFUNC_RANK: {
        size_t i = 0;
        while (i < N && err == KEEL_OK) {
            /* Find tie group */
            size_t j = i + 1;
            while (j < N &&
                   wc_rows_tied(r, r->rows[i], r->rows[j],
                                 spec->order_keys, spec->norder_keys))
                j++;
            int64_t rank = (int64_t)(i + 1); /* rank = 1-based start of tie group */
            for (size_t k = i; k < j && err == KEEL_OK; k++)
                err = wc_set_int64(r, r->rows[k], ci, rank);
            i = j;
        }
        break;
    }

    /* ------------------------------------------------------------------ */
    case KEEL_WFUNC_DENSE_RANK: {
        int64_t dense = 0;
        for (size_t i = 0; i < N && err == KEEL_OK; i++) {
            if (i == 0 || !wc_rows_tied(r, r->rows[i - 1], r->rows[i],
                                         spec->order_keys, spec->norder_keys))
                dense++;
            err = wc_set_int64(r, r->rows[i], ci, dense);
        }
        break;
    }

    /* ------------------------------------------------------------------ */
    case KEEL_WFUNC_NTILE: {
        int64_t n = spec->ntile_n > 0 ? spec->ntile_n : 1;
        /* PostgreSQL semantics: first (N % n) buckets get ceil(N/n) rows,
         * the rest get floor(N/n) rows. */
        int64_t base  = (int64_t)N / n;
        int64_t extra = (int64_t)N % n; /* first 'extra' buckets get one more */
        int64_t bucket = 0;
        int64_t bucket_size = (bucket < extra) ? base + 1 : base;
        int64_t bucket_pos  = 0;
        for (size_t i = 0; i < N && err == KEEL_OK; i++) {
            if (bucket_pos >= bucket_size) {
                bucket++;
                bucket_size = (bucket < extra) ? base + 1 : base;
                bucket_pos  = 0;
            }
            err = wc_set_int64(r, r->rows[i], ci, bucket + 1); /* 1-based */
            bucket_pos++;
        }
        break;
    }

    /* ------------------------------------------------------------------ */
    case KEEL_WFUNC_PERCENT_RANK: {
        if (N == 1) {
            /* Single row: percent_rank = 0 */
            err = wc_set_float64(r, r->rows[0], ci, 0.0);
            break;
        }
        /* Walk tie groups; each group shares the same percent_rank = (group_start)/(N-1) */
        size_t i = 0;
        while (i < N && err == KEEL_OK) {
            size_t j = i + 1;
            while (j < N &&
                   wc_rows_tied(r, r->rows[i], r->rows[j],
                                 spec->order_keys, spec->norder_keys))
                j++;
            double pct = (double)i / (double)(N - 1);
            for (size_t k = i; k < j && err == KEEL_OK; k++)
                err = wc_set_float64(r, r->rows[k], ci, pct);
            i = j;
        }
        break;
    }

    /* ------------------------------------------------------------------ */
    case KEEL_WFUNC_CUME_DIST: {
        /* cume_dist = (rows with order_value <= current) / N.
         * Tied rows share the cume_dist of the last row in their tie group. */
        size_t i = 0;
        while (i < N && err == KEEL_OK) {
            size_t j = i + 1;
            while (j < N &&
                   wc_rows_tied(r, r->rows[i], r->rows[j],
                                 spec->order_keys, spec->norder_keys))
                j++;
            double cd = (double)j / (double)N;
            for (size_t k = i; k < j && err == KEEL_OK; k++)
                err = wc_set_float64(r, r->rows[k], ci, cd);
            i = j;
        }
        break;
    }

    } /* switch */

    return err;
}

keel_error_t keel_scatter_result_window_compute(keel_scatter_result_t*             r,
                                            const keel_window_col_spec_t* specs,
                                            uint16_t                      nspecs)
{
    if (!r || !specs || nspecs == 0 || r->row_count == 0) return KEEL_OK;

    for (uint16_t s = 0; s < nspecs; s++) {
        const keel_window_col_spec_t* sp = &specs[s];

        /* Determine tier: LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE → Tier 3
         * AGG_SUM/AGG_COUNT/AGG_MIN/AGG_MAX/AGG_AVG → Tier 4
         * (Tier 4 also uses wc_apply_val_mem, same as Tier 3) */
        bool is_tier3 = (sp->func == KEEL_WFUNC_LAG         ||
                         sp->func == KEEL_WFUNC_LEAD        ||
                         sp->func == KEEL_WFUNC_FIRST_VALUE ||
                         sp->func == KEEL_WFUNC_LAST_VALUE  ||
                         sp->func == KEEL_WFUNC_NTH_VALUE   ||
                         sp->func == KEEL_WFUNC_AGG_SUM     ||
                         sp->func == KEEL_WFUNC_AGG_COUNT   ||
                         sp->func == KEEL_WFUNC_AGG_MIN     ||
                         sp->func == KEEL_WFUNC_AGG_MAX     ||
                         sp->func == KEEL_WFUNC_AGG_AVG);

        /* Build combined sort keys:
         *   Tier 3: (partition_keys, order_keys) — groups partitions, orders within
         *   Tier 2: order_keys only */
        keel_sort_key_t combined[KEEL_SCATTER_MAX_ORDER_KEYS * 2];
        uint16_t        ncombined = 0;
        if (is_tier3) {
            for (uint16_t k = 0; k < sp->npartition_keys &&
                                  ncombined < KEEL_SCATTER_MAX_ORDER_KEYS * 2; k++)
                combined[ncombined++] = sp->partition_keys[k];
        }
        for (uint16_t k = 0; k < sp->norder_keys &&
                              ncombined < KEEL_SCATTER_MAX_ORDER_KEYS * 2; k++)
            combined[ncombined++] = sp->order_keys[k];

        /* When all order keys were unresolved (source column not in SELECT),
         * fall back to sorting by the window-function result column itself.
         * This handles cases like FIRST_VALUE(x) OVER (ORDER BY x) where the
         * SELECT list is just the window function alias with no bare 'x'. */
        bool any_resolved = false;
        for (uint16_t k = 0; k < ncombined; k++) {
            if (combined[k].col_index != KEEL_SORT_COL_UNRESOLVED) {
                any_resolved = true;
                break;
            }
        }
        if (!any_resolved && ncombined == 0 && is_tier3) {
            /* No keys at all — add window col as the sort key */
            combined[0].col_index = sp->col_index;
            combined[0].dir       = KEEL_SORT_ASC;
            combined[0].nulls     = KEEL_SORT_NULLS_DEFAULT;
            ncombined = 1;
        } else if (!any_resolved && ncombined > 0) {
            /* All UNRESOLVED — replace with window col */
            for (uint16_t k = 0; k < ncombined; k++)
                if (combined[k].col_index == KEEL_SORT_COL_UNRESOLVED)
                    combined[k].col_index = sp->col_index;
        }

        /* Step 1: sort by the combined key sequence. */
        if (ncombined > 0) {
            keel_error_t serr = keel_scatter_result_sort(r, combined, ncombined);
            if (serr != KEEL_OK) return serr;
        }

        /* Step 2: spill path — re-materialise into memory for in-place writes,
         * then rebuild the spill file.  For the common case (result fits in
         * memory) this branch is never taken. */
        if (r->spilled) {
            /* Load all rows into a temp arena + pointer array */
            keel_arena_t* tmp_slab = keel_arena_create(256U * 1024U);
            if (!tmp_slab) return KEEL_ERR_NOMEM;

            keel_scatter_row_t** tmp_rows =
                (keel_scatter_row_t**)keel_malloc(r->row_count * sizeof *tmp_rows);
            if (!tmp_rows) { keel_arena_destroy(tmp_slab); return KEEL_ERR_NOMEM; }

            keel_scatter_result_iter_t it;
            keel_error_t err = keel_scatter_result_iter_init(&it, r);
            if (err != KEEL_OK) {
                keel_free(tmp_rows);
                keel_arena_destroy(tmp_slab);
                return err;
            }

            size_t ncols = r->ncols;
            size_t collected = 0;
            const keel_scatter_col_val_t* vals;
            while (keel_scatter_result_iter_next(&it, &vals) && err == KEEL_OK) {
                size_t row_sz = sizeof(keel_scatter_row_t)
                              + ncols * sizeof(keel_scatter_col_val_t);
                size_t data_sz = 0;
                for (size_t c = 0; c < ncols; c++)
                    if (vals[c].len > 0) data_sz += (size_t)vals[c].len;
                keel_scatter_row_t* row =
                    (keel_scatter_row_t*)keel_arena_alloc(tmp_slab, row_sz + data_sz);
                if (!row) { err = KEEL_ERR_NOMEM; break; }
                row->ncols = (uint16_t)ncols;
                char* dp = (char*)row + row_sz;
                for (size_t c = 0; c < ncols; c++) {
                    row->cols[c].len  = vals[c].len;
                    if (vals[c].len > 0 && vals[c].data) {
                        memcpy(dp, vals[c].data, (size_t)vals[c].len);
                        row->cols[c].data = dp;
                        dp += (size_t)vals[c].len;
                    } else {
                        row->cols[c].data = NULL;
                    }
                }
                tmp_rows[collected++] = row;
            }
            keel_scatter_result_iter_close(&it);

            if (err != KEEL_OK) {
                keel_free(tmp_rows);
                keel_arena_destroy(tmp_slab);
                return err;
            }

            /* Fake in-memory result backed by tmp_slab */
            keel_scatter_result_t fake = *r;
            fake.spilled   = false;
            fake.slab      = tmp_slab;
            fake.rows      = tmp_rows;
            fake.rows_cap  = collected;
            fake.row_count = collected;
            fake.mem_bytes = 0;

            err = is_tier3 ? wc_apply_val_mem(&fake, sp)
                           : wc_apply_mem(&fake, sp);

            if (err == KEEL_OK) {
                /* Rebuild the spill file from modified tmp_rows */
                off_t data_start = (off_t)spill_header_size(r->ncols);
                if (ftruncate(r->spill_fd, data_start) < 0 ||
                    lseek(r->spill_fd, data_start, SEEK_SET) < 0) {
                    err = KEEL_ERR_IO;
                } else {
                    r->write_buf_pos   = 0;
                    r->spill_row_count = 0;
                    r->spill_bytes     = 0;
                    r->row_count       = 0;
                    for (size_t i = 0; i < collected; i++)
                        spill_append_row(r, tmp_rows[i]->cols);
                    err = spill_write_eof(r);
                }
            }

            keel_free(tmp_rows);
            keel_arena_destroy(tmp_slab);
            if (err != KEEL_OK) return err;
            continue;
        }

        /* Step 3: memory path — apply window function directly. */
        keel_error_t merr = is_tier3 ? wc_apply_val_mem(r, sp)
                                     : wc_apply_mem(r, sp);
        if (merr != KEEL_OK) return merr;
    }

    return KEEL_OK;
}
