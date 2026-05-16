/**
 * @file print.h
 * @brief Public API for the low-level configurable print/output facility.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This module provides a unified print facility that can be configured
 * for buffered or unbuffered output. All output in KEEL should use these
 * functions instead of raw printf/fprintf.
 *
 * Features:
 * - Configurable buffering mode (buffered/unbuffered/line-buffered)
 * - Thread-safe output
 * - Configurable output destinations (stdout, stderr, file)
 * - Optional timestamps
 * - Flush control
 */

#ifndef KEEL_PRINT_H
#define KEEL_PRINT_H

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Buffer Mode
 * ============================================================================ */

typedef enum keel_buffer_mode {
    KEEL_BUFFER_NONE = 0,    /* Unbuffered - flush after every write */
    KEEL_BUFFER_LINE,        /* Line buffered - flush on newline */
    KEEL_BUFFER_FULL,        /* Fully buffered - flush when buffer full or explicit */
} keel_buffer_mode_t;

/* ============================================================================
 * Output Stream
 * ============================================================================ */

typedef enum keel_stream {
    KEEL_STREAM_STDOUT = 0,  /* Standard output */
    KEEL_STREAM_STDERR,      /* Standard error */
    KEEL_STREAM_FILE,        /* Custom file */
} keel_stream_t;

/* ============================================================================
 * Print Configuration
 * ============================================================================ */

typedef struct keel_print_config {
    keel_buffer_mode_t   buffer_mode;        /* Buffering mode */
    size_t              buffer_size;        /* Buffer size (0 = default 8KB) */
    keel_stream_t        default_stream;     /* Default output stream */
    const char*         log_file;           /* Log file path (if stream is FILE) */
    bool                timestamps;         /* Add timestamps to output */
    bool                colors;             /* Enable ANSI colors (if terminal) */
} keel_print_config_t;

/* Default configuration */
#define KEEL_PRINT_CONFIG_DEFAULT { \
    .buffer_mode = KEEL_BUFFER_LINE, \
    .buffer_size = 0, \
    .default_stream = KEEL_STREAM_STDOUT, \
    .log_file = NULL, \
    .timestamps = false, \
    .colors = true, \
}

/* ============================================================================
 * Initialization & Cleanup
 * ============================================================================ */

/**
 * Initialize the print facility with given configuration.
 * Must be called before any keel_print functions.
 *
 * @param config Configuration options, or `NULL` for defaults.
 * @return `0` on success, `-1` on error.
 */
int keel_print_init(const keel_print_config_t* config);

/**
 * Shutdown the print facility, flushing all buffers.
 *
 * @return
 */
void keel_print_shutdown(void);

/**
 * Reconfigure the print facility at runtime.
 *
 * @param config New configuration.
 * @return `0` on success, `-1` on error.
 */
int keel_print_configure(const keel_print_config_t* config);

/**
 * Get current configuration.
 *
 * @param[out] config Caller-provided output configuration buffer.
 * @return
 */
void keel_print_get_config(keel_print_config_t* config);

/* ============================================================================
 * Core Print Functions
 * ============================================================================ */

/**
 * Print formatted output to stdout (like printf).
 *
 * @param fmt Format string.
 * @param ... Arguments.
 * @return Number of characters written, or `-1` on error.
 */
int keel_printf(const char* fmt, ...) __attribute__((format(printf, 1, 2)));

/**
 * Print formatted output to specified stream (like fprintf).
 *
 * @param stream Output stream (`KEEL_STREAM_*`).
 * @param fmt Format string.
 * @param ... Arguments.
 * @return Number of characters written, or `-1` on error.
 */
int keel_fprintf(keel_stream_t stream, const char* fmt, ...) __attribute__((format(printf, 2, 3)));

/**
 * Print formatted output to stderr.
 *
 * @param fmt Format string.
 * @param ... Arguments.
 * @return Number of characters written, or `-1` on error.
 */
int keel_eprintf(const char* fmt, ...) __attribute__((format(printf, 1, 2)));

/**
 * Print formatted output with va_list (like vprintf).
 *
 * @param fmt Format string.
 * @param args Argument list.
 * @return Number of characters written, or `-1` on error.
 */
int keel_vprintf(const char* fmt, va_list args);

/**
 * Print formatted output to stream with va_list (like vfprintf).
 *
 * @param stream Output stream.
 * @param fmt Format string.
 * @param args Argument list.
 * @return Number of characters written, or `-1` on error.
 */
int keel_vfprintf(keel_stream_t stream, const char* fmt, va_list args);

/* ============================================================================
 * Buffer Control
 * ============================================================================ */

/**
 * Flush all output buffers.
 *
 * @return
 */
void keel_print_flush(void);

/**
 * Flush specific stream buffer.
 *
 * @param stream Stream to flush.
 * @return
 */
void keel_print_flush_stream(keel_stream_t stream);

/**
 * Set buffer mode at runtime.
 *
 * @param mode New buffer mode.
 * @return
 */
void keel_print_set_buffer_mode(keel_buffer_mode_t mode);

/**
 * Get current buffer mode.
 *
 * @return Current buffer mode.
 */
keel_buffer_mode_t keel_print_get_buffer_mode(void);

/* ============================================================================
 * Convenience Macros
 * ============================================================================ */

/**
 * Debug print macro - only outputs when KEEL_DEBUG_PRINT is defined.
 * Automatically includes file/line information.
 */
#ifdef KEEL_DEBUG_PRINT
    #define keel_dprintf(fmt, ...) \
        keel_eprintf("[DEBUG %s:%d] " fmt, __FILE__, __LINE__, ##__VA_ARGS__)
#else
    #define keel_dprintf(fmt, ...) ((void)0)
#endif

/**
 * Error print macro - always outputs to stderr with prefix.
 */
#define keel_error(fmt, ...) \
    keel_eprintf("ERROR: " fmt, ##__VA_ARGS__)

/**
 * Warning print macro - outputs to stderr with prefix.
 */
#define keel_warn(fmt, ...) \
    keel_eprintf("WARNING: " fmt, ##__VA_ARGS__)

/**
 * Info print macro - outputs to stdout.
 */
#define keel_info(fmt, ...) \
    keel_printf("INFO: " fmt, ##__VA_ARGS__)

#ifdef __cplusplus
}
#endif

#endif /* KEEL_PRINT_H */
