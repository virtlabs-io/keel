/**
 * @file net.c
 * @brief Shared network utilities (protocol-agnostic)
 *
 * All synchronous connect code has been removed.
 * Backend connections now use io_uring via backend_connect_async.c.
 */
