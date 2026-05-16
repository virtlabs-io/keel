/**
 * @file backend_auth.h
 * @brief Compatibility placeholder for backend-auth declarations.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The historical synchronous backend-auth surface has been retired in favor of
 * protocol-specific pure helpers plus the asynchronous backend-connect state
 * machine. This header remains as a stable include target for code that conceptually
 * depends on "backend auth" as a subsystem boundary, even though the concrete APIs
 * now live in protocol-specific headers such as `pg_backend_auth.h` and
 * `mysql_backend_auth.h`.
 */

#ifndef KEEL_BACKEND_AUTH_H
#define KEEL_BACKEND_AUTH_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* All sync functions removed. See backend_connect_async.c for async path. */

#ifdef __cplusplus
}
#endif

#endif /* KEEL_BACKEND_AUTH_H */
