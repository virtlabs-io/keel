/**
 * @file print.c
 * @brief KEEL Print Facility Implementation
 *
 * Thread-safe, configurable print facility with buffering support.
 */

#include "keel/log/print.h"
#include "keel/mem/mem.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>

/* ============================================================================
 * Constants
 * ============================================================================ */

#define DEFAULT_BUFFER_SIZE     (8 * 1024)  /* 8KB default buffer */
#define MAX_TIMESTAMP_LEN       32
#define MAX_FORMAT_LEN          4096

/* ============================================================================
 * Internal State
 * ============================================================================ */

typedef struct stream_buffer {
    char*           data;           /* Buffer data */
    size_t          size;           /* Buffer capacity */
    size_t          used;           /* Bytes used */
    pthread_mutex_t lock;           /* Thread safety */
    FILE*           file;           /* Output file handle */
    bool            is_tty;         /* Is a terminal */
} stream_buffer_t;

typedef struct print_state {
    bool                initialized;
    keel_print_config_t  config;
    stream_buffer_t     stdout_buf;
    stream_buffer_t     stderr_buf;
    stream_buffer_t     file_buf;
    FILE*               log_file;
} print_state_t;

static print_state_t g_print = {0};

/* ============================================================================
 * Internal Functions
 * ============================================================================ */

/**
 * @brief Initialize a stream buffer.
 *
 * Allocates the backing memory and mutex for @p buf, associates it with
 * @p file, and detects whether the file is a TTY.
 *
 * @param buf   Buffer to initialize.
 * @param file  Output file handle to associate with the buffer.
 * @param size  Desired buffer capacity in bytes; 0 uses DEFAULT_BUFFER_SIZE.
 * @return 0 on success, -1 on allocation or mutex-init failure.
 */
static int buffer_init(stream_buffer_t* buf, FILE* file, size_t size) {
    buf->size = size > 0 ? size : DEFAULT_BUFFER_SIZE;
    buf->data = (char*)keel_malloc(buf->size);
    if (!buf->data) {
        return -1;
    }
    buf->used = 0;
    buf->file = file;
    buf->is_tty = file ? isatty(fileno(file)) : false;
    
    if (pthread_mutex_init(&buf->lock, NULL) != 0) {
        keel_free(buf->data);
        buf->data = NULL;
        return -1;
    }
    
    return 0;
}

/**
 * @brief Flush and destroy a stream buffer.
 *
 * Flushes any pending data to the associated file, frees the backing
 * memory, and destroys the mutex.  Safe to call on a zeroed struct.
 *
 * @param buf  Buffer to destroy.
 */
static void buffer_destroy(stream_buffer_t* buf) {
    if (buf->data) {
        /* Flush any remaining data */
        if (buf->used > 0 && buf->file) {
            fwrite(buf->data, 1, buf->used, buf->file);
            fflush(buf->file);
        }
        keel_free(buf->data);
        buf->data = NULL;
    }
    buf->used = 0;
    buf->size = 0;
    pthread_mutex_destroy(&buf->lock);
}

/**
 * @brief Write buffered data to the file without acquiring the lock.
 *
 * Must be called with @p buf->lock already held by the caller.
 * Resets the used-byte counter to 0 after writing.
 *
 * @param buf  Buffer to flush.
 */
static void buffer_flush_unlocked(stream_buffer_t* buf) {
    if (buf->used > 0 && buf->file && buf->data) {
        fwrite(buf->data, 1, buf->used, buf->file);
        fflush(buf->file);
        buf->used = 0;
    }
}

/**
 * @brief Thread-safe flush of a stream buffer.
 *
 * Acquires @p buf->lock, delegates to buffer_flush_unlocked(), then
 * releases the lock.
 *
 * @param buf  Buffer to flush.
 */
static void buffer_flush(stream_buffer_t* buf) {
    pthread_mutex_lock(&buf->lock);
    buffer_flush_unlocked(buf);
    pthread_mutex_unlock(&buf->lock);
}

/**
 * @brief Write data to a stream buffer according to the buffering mode.
 *
 * Supports three modes:
 *   - KEEL_BUFFER_NONE  – unbuffered, written directly to the file.
 *   - KEEL_BUFFER_LINE  – line-buffered, flushed on each newline.
 *   - KEEL_BUFFER_FULL  – fully buffered, flushed only when the buffer is full.
 *
 * @param buf   Destination buffer.
 * @param data  Bytes to write.
 * @param len   Number of bytes in @p data.
 * @param mode  Buffering mode to use.
 * @return Number of bytes accepted (equal to @p len), or -1 on invalid input.
 */
static int buffer_write(stream_buffer_t* buf, const char* data, size_t len, 
                        keel_buffer_mode_t mode) {
    if (!buf || !buf->data || !data) {
        return -1;
    }
    
    pthread_mutex_lock(&buf->lock);
    
    int written = (int)len;
    
    switch (mode) {
        case KEEL_BUFFER_NONE:
            /* Unbuffered: write directly */
            if (buf->file) {
                fwrite(data, 1, len, buf->file);
                fflush(buf->file);
            }
            break;
            
        case KEEL_BUFFER_LINE:
            /* Line buffered: buffer until newline */
            for (size_t i = 0; i < len; i++) {
                if (buf->used >= buf->size) {
                    buffer_flush_unlocked(buf);
                }
                buf->data[buf->used++] = data[i];
                if (data[i] == '\n') {
                    buffer_flush_unlocked(buf);
                }
            }
            break;
            
        case KEEL_BUFFER_FULL:
            /* Full buffered: buffer until full */
            while (len > 0) {
                size_t space = buf->size - buf->used;
                size_t chunk = len < space ? len : space;
                
                memcpy(buf->data + buf->used, data, chunk);
                buf->used += chunk;
                data += chunk;
                len -= chunk;
                
                if (buf->used >= buf->size) {
                    buffer_flush_unlocked(buf);
                }
            }
            break;
    }
    
    pthread_mutex_unlock(&buf->lock);
    return written;
}

/**
 * @brief Map a stream identifier to its corresponding buffer.
 *
 * @param stream  Logical stream (KEEL_STREAM_STDOUT, KEEL_STREAM_STDERR,
 *                or KEEL_STREAM_FILE).
 * @return Pointer to the matching stream_buffer_t.  Falls back to
 *         stdout_buf for unknown values.
 */
static stream_buffer_t* get_stream_buffer(keel_stream_t stream) {
    switch (stream) {
        case KEEL_STREAM_STDOUT:
            return &g_print.stdout_buf;
        case KEEL_STREAM_STDERR:
            return &g_print.stderr_buf;
        case KEEL_STREAM_FILE:
            return &g_print.file_buf;
        default:
            return &g_print.stdout_buf;
    }
}

/**
 * @brief Write a formatted timestamp prefix into @p buf.
 *
 * Produces a string of the form "[YYYY-MM-DD HH:MM:SS] " using
 * the current local time.
 *
 * @param buf      Destination character buffer.
 * @param bufsize  Capacity of @p buf in bytes.
 */
static void add_timestamp(char* buf, size_t bufsize) {
    time_t now = time(NULL);
    struct tm tm_storage;
    struct tm* tm_info = localtime_r(&now, &tm_storage);
    strftime(buf, bufsize, "[%Y-%m-%d %H:%M:%S] ", tm_info);
}

/* ============================================================================
 * Public API - Initialization
 * ============================================================================ */

/**
 * @brief Initialize the print facility.
 *
 * Allocates buffers for stdout, stderr, and (optionally) a log file.
 * Calling this function when already initialized is a no-op.
 *
 * @param config  Configuration to apply, or NULL to use
 *                KEEL_PRINT_CONFIG_DEFAULT.
 * @return 0 on success, -1 if any buffer or file-open operation fails.
 */
int keel_print_init(const keel_print_config_t* config) {
    if (g_print.initialized) {
        return 0;  /* Already initialized */
    }
    
    /* Apply default config if none provided */
    if (config) {
        g_print.config = *config;
    } else {
        keel_print_config_t defaults = KEEL_PRINT_CONFIG_DEFAULT;
        g_print.config = defaults;
    }
    
    size_t bufsize = g_print.config.buffer_size > 0 
                   ? g_print.config.buffer_size 
                   : DEFAULT_BUFFER_SIZE;
    
    /* Initialize stdout buffer */
    if (buffer_init(&g_print.stdout_buf, stdout, bufsize) != 0) {
        return -1;
    }
    
    /* Initialize stderr buffer */
    if (buffer_init(&g_print.stderr_buf, stderr, bufsize) != 0) {
        buffer_destroy(&g_print.stdout_buf);
        return -1;
    }
    
    /* Initialize file buffer if needed */
    if (g_print.config.log_file) {
        g_print.log_file = fopen(g_print.config.log_file, "a");
        if (!g_print.log_file) {
            buffer_destroy(&g_print.stdout_buf);
            buffer_destroy(&g_print.stderr_buf);
            return -1;
        }
        if (buffer_init(&g_print.file_buf, g_print.log_file, bufsize) != 0) {
            fclose(g_print.log_file);
            buffer_destroy(&g_print.stdout_buf);
            buffer_destroy(&g_print.stderr_buf);
            return -1;
        }
    }
    
    g_print.initialized = true;
    return 0;
}

/**
 * @brief Shut down the print facility and release all resources.
 *
 * Flushes and destroys all stream buffers and closes the log file if
 * one was opened.  Safe to call when not initialized.
 */
void keel_print_shutdown(void) {
    if (!g_print.initialized) {
        return;
    }
    
    /* Flush and destroy all buffers */
    buffer_destroy(&g_print.stdout_buf);
    buffer_destroy(&g_print.stderr_buf);
    
    if (g_print.log_file) {
        buffer_destroy(&g_print.file_buf);
        fclose(g_print.log_file);
        g_print.log_file = NULL;
    }
    
    g_print.initialized = false;
}

/**
 * @brief Update the print configuration at runtime.
 *
 * Applies changes to buffering mode, timestamps, colors, and log file
 * immediately.  Buffer-size changes are ignored after initialization.
 *
 * @param config  New configuration to apply.  Must not be NULL.
 * @return 0 on success, -1 if @p config is NULL.
 */
int keel_print_configure(const keel_print_config_t* config) {
    if (!config) {
        return -1;
    }
    
    /* Update buffer mode - takes effect immediately */
    g_print.config.buffer_mode = config->buffer_mode;
    g_print.config.timestamps = config->timestamps;
    g_print.config.colors = config->colors;
    
    /* If buffer size changed, we'd need to reallocate - skip for now */
    
    /* If log file changed, close old and open new */
    if (config->log_file != g_print.config.log_file) {
        if (g_print.log_file) {
            buffer_destroy(&g_print.file_buf);
            fclose(g_print.log_file);
            g_print.log_file = NULL;
        }
        
        if (config->log_file) {
            g_print.log_file = fopen(config->log_file, "a");
            if (g_print.log_file) {
                size_t bufsize = config->buffer_size > 0 
                               ? config->buffer_size 
                               : DEFAULT_BUFFER_SIZE;
                buffer_init(&g_print.file_buf, g_print.log_file, bufsize);
            }
        }
        g_print.config.log_file = config->log_file;
    }
    
    return 0;
}

/**
 * @brief Retrieve the current print configuration.
 *
 * Copies the active configuration into @p config.  Does nothing if
 * @p config is NULL.
 *
 * @param config  Output parameter that receives the current configuration.
 */
void keel_print_get_config(keel_print_config_t* config) {
    if (config) {
        *config = g_print.config;
    }
}

/* ============================================================================
 * Public API - Print Functions
 * ============================================================================ */

/**
 * @brief Format and write a message to the specified stream (va_list form).
 *
 * Auto-initializes the print facility with default settings if it has not
 * yet been initialized.  Falls back to raw vfprintf() if initialization
 * fails.
 *
 * @param stream  Target stream (KEEL_STREAM_STDOUT, KEEL_STREAM_STDERR,
 *                or KEEL_STREAM_FILE).
 * @param fmt     printf-style format string.  Must not be NULL.
 * @param args    Variadic argument list.
 * @return Number of bytes written, or -1 on error.
 */
int keel_vfprintf(keel_stream_t stream, const char* fmt, va_list args) {
    if (!fmt) {
        return -1;
    }
    
    /* Auto-initialize with defaults if not done */
    if (!g_print.initialized) {
        if (keel_print_init(NULL) != 0) {
            /* Fallback to raw output */
            FILE* f = (stream == KEEL_STREAM_STDERR) ? stderr : stdout;
            return vfprintf(f, fmt, args);
        }
    }
    
    stream_buffer_t* buf = get_stream_buffer(stream);
    
    /* Format the message */
    char formatted[MAX_FORMAT_LEN];
    char* output = formatted;
    size_t offset = 0;
    
    /* Add timestamp if configured */
    if (g_print.config.timestamps) {
        add_timestamp(formatted, MAX_TIMESTAMP_LEN);
        offset = strlen(formatted);
    }
    
    /* Format the user message */
    int len = vsnprintf(formatted + offset, MAX_FORMAT_LEN - offset, fmt, args);
    if (len < 0) {
        return -1;
    }
    
    size_t total_len = offset + (size_t)len;
    if (total_len >= MAX_FORMAT_LEN) {
        total_len = MAX_FORMAT_LEN - 1;
    }
    
    return buffer_write(buf, output, total_len, g_print.config.buffer_mode);
}

/**
 * @brief Format and write a message to stdout (va_list form).
 *
 * Convenience wrapper around keel_vfprintf() targeting KEEL_STREAM_STDOUT.
 *
 * @param fmt   printf-style format string.
 * @param args  Variadic argument list.
 * @return Number of bytes written, or -1 on error.
 */
int keel_vprintf(const char* fmt, va_list args) {
    return keel_vfprintf(KEEL_STREAM_STDOUT, fmt, args);
}

/**
 * @brief Format and write a message to stdout.
 *
 * Variadic wrapper around keel_vfprintf() targeting KEEL_STREAM_STDOUT.
 *
 * @param fmt  printf-style format string.
 * @param ...  Additional arguments matching @p fmt.
 * @return Number of bytes written, or -1 on error.
 */
int keel_printf(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    int result = keel_vfprintf(KEEL_STREAM_STDOUT, fmt, args);
    va_end(args);
    return result;
}

/**
 * @brief Format and write a message to the specified stream.
 *
 * Variadic wrapper around keel_vfprintf().
 *
 * @param stream  Target stream.
 * @param fmt     printf-style format string.
 * @param ...     Additional arguments matching @p fmt.
 * @return Number of bytes written, or -1 on error.
 */
int keel_fprintf(keel_stream_t stream, const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    int result = keel_vfprintf(stream, fmt, args);
    va_end(args);
    return result;
}

/**
 * @brief Format and write a message to stderr.
 *
 * Variadic wrapper around keel_vfprintf() targeting KEEL_STREAM_STDERR.
 *
 * @param fmt  printf-style format string.
 * @param ...  Additional arguments matching @p fmt.
 * @return Number of bytes written, or -1 on error.
 */
int keel_eprintf(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    int result = keel_vfprintf(KEEL_STREAM_STDERR, fmt, args);
    va_end(args);
    return result;
}

/* ============================================================================
 * Public API - Buffer Control
 * ============================================================================ */

/**
 * @brief Flush all output streams.
 *
 * Flushes stdout, stderr, and the log file (if open).  If the facility
 * is not initialized, falls back to fflush(stdout) and fflush(stderr).
 */
void keel_print_flush(void) {
    if (!g_print.initialized) {
        fflush(stdout);
        fflush(stderr);
        return;
    }
    
    buffer_flush(&g_print.stdout_buf);
    buffer_flush(&g_print.stderr_buf);
    if (g_print.log_file) {
        buffer_flush(&g_print.file_buf);
    }
}

/**
 * @brief Flush a single output stream.
 *
 * If the facility is not initialized, falls back to fflush() on the
 * underlying C FILE for that stream.
 *
 * @param stream  Stream to flush.
 */
void keel_print_flush_stream(keel_stream_t stream) {
    if (!g_print.initialized) {
        FILE* f = (stream == KEEL_STREAM_STDERR) ? stderr : stdout;
        fflush(f);
        return;
    }
    
    stream_buffer_t* buf = get_stream_buffer(stream);
    buffer_flush(buf);
}

/**
 * @brief Set the active buffering mode.
 *
 * Changes take effect immediately for all subsequent write calls.
 *
 * @param mode  New buffering mode (KEEL_BUFFER_NONE, KEEL_BUFFER_LINE,
 *              or KEEL_BUFFER_FULL).
 */
void keel_print_set_buffer_mode(keel_buffer_mode_t mode) {
    g_print.config.buffer_mode = mode;
}

/**
 * @brief Retrieve the active buffering mode.
 *
 * @return The current keel_buffer_mode_t value.
 */
keel_buffer_mode_t keel_print_get_buffer_mode(void) {
    return g_print.config.buffer_mode;
}
