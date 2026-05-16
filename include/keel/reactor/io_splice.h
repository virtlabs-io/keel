/**
 * @file io_splice.h
 * @brief Public API for zero-copy splice helpers and portable fallback transfer paths.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This API exposes small, explicit building blocks for protocol paths that need
 * to inspect socket data without consuming it and, when possible, forward bytes
 * without userspace copying.
 *
 * Typical use cases:
 * - `MSG_PEEK` inspection of protocol headers before making a routing decision
 * - socket-to-socket forwarding via `splice()` on Linux
 * - socket/pipe buffering that preserves a consistent result contract on
 *   platforms that must fall back to normal read/write or send semantics
 */

#ifndef KEEL_IO_SPLICE_H
#define KEEL_IO_SPLICE_H

#include "keel_types.h"
#include "keel_error.h"

#include <sys/types.h>  /* For ssize_t */

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Platform Detection
 * ============================================================================ */

/* Check for splice() availability */
#if defined(__linux__) && defined(__GLIBC__)
    #define KEEL_HAVE_SPLICE 1
#else
    #define KEEL_HAVE_SPLICE 0
#endif

/* Check for sendfile() */
#if defined(__linux__) || defined(__FreeBSD__) || defined(__APPLE__)
    #define KEEL_HAVE_SENDFILE 1
#else
    #define KEEL_HAVE_SENDFILE 0
#endif

/* Check for vmsplice() - Linux only */
#if defined(__linux__) && defined(__GLIBC__)
    #define KEEL_HAVE_VMSPLICE 1
#else
    #define KEEL_HAVE_VMSPLICE 0
#endif

/* ============================================================================
 * Splice Pipe Handle
 * ============================================================================ */

/**
 * @brief Splice pipe for zero-copy transfers
 *
 * On Linux, splice() requires an intermediate pipe. This structure
 * manages a pipe pair optimized for zero-copy operations.
 */
typedef struct keel_splice_pipe {
    int     pipe_fds[2];    /**< Pipe file descriptors [read, write] */
    size_t  capacity;       /**< Pipe buffer capacity */
    size_t  pending;        /**< Bytes pending in pipe */
    bool    valid;          /**< Pipe is valid and usable */
} keel_splice_pipe_t;

/**
 * @brief Create a splice pipe
 *
 * Creates a pipe optimized for splice operations with the maximum
 * available buffer size.
 *
 * @param pipe  Pipe structure to initialize
 * @param size  Requested pipe size (0 = system default)
 * @return KEEL_OK on success
 */
keel_error_t keel_splice_pipe_create(keel_splice_pipe_t* pipe, size_t size);

/**
 * @brief Destroy a splice pipe
 *
 * @param pipe  Pipe to destroy
 */
void keel_splice_pipe_destroy(keel_splice_pipe_t* pipe);

/**
 * @brief Get pipe capacity
 *
 * Returns the actual pipe buffer size, which may differ from requested.
 *
 * @param pipe  Pipe to query
 * @return Pipe capacity in bytes, or 0 if invalid
 */
size_t keel_splice_pipe_capacity(const keel_splice_pipe_t* pipe);

/* ============================================================================
 * Peek Operations (MSG_PEEK)
 * ============================================================================ */

/**
 * @brief Peek result status
 */
typedef enum keel_peek_status {
    KEEL_PEEK_OK = 0,        /**< Peek successful, data available */
    KEEL_PEEK_CLOSED,        /**< Connection closed by peer */
    KEEL_PEEK_WOULDBLOCK,    /**< No data available (non-blocking) */
    KEEL_PEEK_ERROR,         /**< I/O error occurred */
} keel_peek_status_t;

/**
 * @brief Peek result structure
 */
typedef struct keel_peek_result {
    keel_peek_status_t status;   /**< Peek status */
    size_t            len;      /**< Bytes peeked */
    int               error;    /**< errno if status == KEEL_PEEK_ERROR */
} keel_peek_result_t;

/**
 * @brief Peek at socket data without consuming
 *
 * Uses MSG_PEEK to examine incoming data without removing it from
 * the socket's receive buffer. Ideal for protocol header inspection.
 *
 * @param fd    Socket file descriptor
 * @param buf   Buffer to store peeked data
 * @param len   Maximum bytes to peek
 * @return Peek result with status and bytes read
 *
 * @note This is a non-blocking operation. If no data is available,
 *       returns KEEL_PEEK_WOULDBLOCK.
 *
 * Example:
 * @code
 * uint8_t header[5];
 * keel_peek_result_t res = keel_peek(client_fd, header, 5);
 * if (res.status == KEEL_PEEK_OK && res.len >= 5) {
 *     // Parse header and make routing decision
 *     int msg_type = header[0];
 *     int msg_len = (header[1] << 24) | (header[2] << 16) | 
 *                   (header[3] << 8) | header[4];
 * }
 * @endcode
 */
keel_peek_result_t keel_peek(int fd, void* buf, size_t len);

/**
 * @brief Peek at socket data with timeout
 *
 * Like keel_peek() but waits up to the specified timeout for data.
 *
 * @param fd        Socket file descriptor
 * @param buf       Buffer to store peeked data
 * @param len       Maximum bytes to peek
 * @param timeout_ms Timeout in milliseconds (-1 = infinite)
 * @return Peek result
 */
keel_peek_result_t keel_peek_timed(int fd, void* buf, size_t len, int timeout_ms);

/* ============================================================================
 * Splice/Zero-Copy Transfer Operations
 * ============================================================================ */

/**
 * @brief Transfer result structure
 */
typedef struct keel_transfer_result {
    size_t      bytes;      /**< Bytes transferred */
    keel_error_t error;      /**< Error code if any */
    int         sys_errno;  /**< System errno if error */
    bool        eof;        /**< End-of-file reached on source */
} keel_transfer_result_t;

/**
 * @brief Transfer flags
 */
typedef enum keel_transfer_flags {
    KEEL_TRANSFER_NONE       = 0,
    KEEL_TRANSFER_NONBLOCK   = (1 << 0),   /**< Non-blocking operation */
    KEEL_TRANSFER_MORE       = (1 << 1),   /**< More data coming (TCP_CORK hint) */
    KEEL_TRANSFER_MOVE       = (1 << 2),   /**< Move pages (SPLICE_F_MOVE) */
    KEEL_TRANSFER_GIFT       = (1 << 3),   /**< Gift pages (SPLICE_F_GIFT) */
} keel_transfer_flags_t;

/**
 * @brief Zero-copy transfer between sockets using splice
 *
 * Transfers data from source socket to destination socket without
 * copying through user space. Uses an intermediate pipe for buffering.
 *
 * @param src_fd    Source socket file descriptor
 * @param dst_fd    Destination socket file descriptor
 * @param pipe      Splice pipe for intermediate buffering
 * @param len       Maximum bytes to transfer (0 = pipe capacity)
 * @param flags     Transfer flags
 * @return Transfer result
 *
 * @note On non-Linux platforms, falls back to read/write.
 *
 * Example (proxy forward):
 * @code
 * keel_splice_pipe_t pipe;
 * keel_splice_pipe_create(&pipe, 64 * 1024);
 * 
 * keel_transfer_result_t res = keel_splice_transfer(
 *     client_fd, backend_fd, &pipe, 0, KEEL_TRANSFER_NONBLOCK
 * );
 * if (res.bytes > 0) {
 *     total_forwarded += res.bytes;
 * }
 * @endcode
 */
keel_transfer_result_t keel_splice_transfer(
    int                     src_fd,
    int                     dst_fd,
    keel_splice_pipe_t*      pipe,
    size_t                  len,
    keel_transfer_flags_t    flags
);

/**
 * @brief Zero-copy transfer from socket to pipe
 *
 * Moves data from socket receive buffer to pipe without copying.
 * The data can then be transferred to another socket with keel_splice_to_socket().
 *
 * @param src_fd    Source socket file descriptor
 * @param pipe      Destination pipe
 * @param len       Maximum bytes to transfer
 * @param flags     Transfer flags
 * @return Transfer result
 */
keel_transfer_result_t keel_splice_from_socket(
    int                     src_fd,
    keel_splice_pipe_t*      pipe,
    size_t                  len,
    keel_transfer_flags_t    flags
);

/**
 * @brief Zero-copy transfer from pipe to socket
 *
 * Moves data from pipe to socket send buffer without copying.
 *
 * @param pipe      Source pipe
 * @param dst_fd    Destination socket file descriptor
 * @param len       Maximum bytes to transfer (0 = all pending)
 * @param flags     Transfer flags
 * @return Transfer result
 */
keel_transfer_result_t keel_splice_to_socket(
    keel_splice_pipe_t*      pipe,
    int                     dst_fd,
    size_t                  len,
    keel_transfer_flags_t    flags
);

/**
 * @brief Transfer from user buffer to socket via pipe (vmsplice + splice)
 *
 * This is a zero-copy write from user memory to a socket. On Linux,
 * uses vmsplice() to gift pages to the pipe, then splice() to socket.
 *
 * @param buf       Source buffer
 * @param len       Buffer length
 * @param dst_fd    Destination socket
 * @param pipe      Intermediate pipe
 * @param flags     Transfer flags
 * @return Transfer result
 *
 * @warning If using KEEL_TRANSFER_GIFT, the buffer MUST remain valid
 *          until the kernel has consumed the data. Use with caution.
 */
keel_transfer_result_t keel_splice_from_buffer(
    const void*             buf,
    size_t                  len,
    int                     dst_fd,
    keel_splice_pipe_t*      pipe,
    keel_transfer_flags_t    flags
);

/* ============================================================================
 * Registered Buffers (io_uring integration)
 * ============================================================================ */

/**
 * @brief Registered buffer set for io_uring
 *
 * Pre-registered buffers avoid per-operation buffer mapping overhead.
 */
typedef struct keel_registered_bufs {
    void**      bufs;           /**< Array of buffer pointers */
    size_t*     sizes;          /**< Array of buffer sizes */
    size_t      count;          /**< Number of buffers */
    size_t      buf_size;       /**< Individual buffer size */
    bool        registered;     /**< Registered with io_uring */
    void*       ring;           /**< io_uring ring (if registered) */
} keel_registered_bufs_t;

/**
 * @brief Create registered buffer set
 *
 * Allocates page-aligned buffers suitable for registration with io_uring.
 *
 * @param bufs      Buffer set to initialize
 * @param count     Number of buffers
 * @param buf_size  Size of each buffer
 * @return KEEL_OK on success
 */
keel_error_t keel_registered_bufs_create(
    keel_registered_bufs_t*  bufs,
    size_t                  count,
    size_t                  buf_size
);

/**
 * @brief Destroy registered buffer set
 *
 * Unregisters from io_uring if registered, then frees buffers.
 *
 * @param bufs  Buffer set to destroy
 */
void keel_registered_bufs_destroy(keel_registered_bufs_t* bufs);

/**
 * @brief Get a buffer from the set
 *
 * @param bufs  Buffer set
 * @param idx   Buffer index
 * @return Buffer pointer, or NULL if index out of range
 */
void* keel_registered_bufs_get(const keel_registered_bufs_t* bufs, size_t idx);

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/**
 * @brief Check if splice() is available on this platform
 */
bool keel_splice_available(void);

/**
 * @brief Check if vmsplice() is available
 */
bool keel_vmsplice_available(void);

/**
 * @brief Get maximum pipe capacity on this system
 *
 * @return Maximum pipe capacity in bytes
 */
size_t keel_pipe_max_capacity(void);

/**
 * @brief Set pipe capacity (Linux only)
 *
 * Attempts to increase pipe buffer size for better throughput.
 *
 * @param fd    Pipe file descriptor
 * @param size  Requested size
 * @return Actual size set, or -1 on error
 */
ssize_t keel_pipe_set_capacity(int fd, size_t size);

/**
 * @brief Statistics for splice operations
 */
typedef struct keel_splice_stats {
    uint64_t    peek_count;         /**< Number of peek operations */
    uint64_t    peek_bytes;         /**< Total bytes peeked */
    uint64_t    splice_count;       /**< Number of splice operations */
    uint64_t    splice_bytes;       /**< Bytes transferred via splice */
    uint64_t    fallback_count;     /**< Fallback (copy) operations */
    uint64_t    fallback_bytes;     /**< Bytes transferred via fallback */
} keel_splice_stats_t;

/**
 * @brief Get global splice statistics
 */
keel_splice_stats_t keel_splice_get_stats(void);

/**
 * @brief Reset splice statistics
 */
void keel_splice_reset_stats(void);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_IO_SPLICE_H */
