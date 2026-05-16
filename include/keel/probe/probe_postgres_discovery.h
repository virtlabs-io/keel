/**
 * @file probe_postgres_discovery.h
 * @brief PostgreSQL-specific discovery probe implementation.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Provides @ref keel_pg_discovery_probe, a concrete implementation of
 * @ref keel_discovery_probe_fn for PostgreSQL backends.  Callers wire this
 * into keel_discovery_config_t::probe_fn at startup:
 *
 * @code
 * keel_discovery_config_t cfg = keel_discovery_config_default();
 * cfg.probe_fn = keel_pg_discovery_probe;
 * keel_discovery_t* disc = keel_discovery_create(&cfg);
 * @endcode
 *
 * This header MUST NOT be included from any file in keelcore (src/core/).
 * It belongs exclusively to the probe layer and higher-level wiring code.
 */

#ifndef KEEL_PROBE_POSTGRES_DISCOVERY_H
#define KEEL_PROBE_POSTGRES_DISCOVERY_H

#include "keel/core/router_discovery.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief PostgreSQL wire-protocol server probe.
 *
 * Implements keel_discovery_probe_fn for PostgreSQL backends:
 *
 * 1. Opens a TCP connection to host:port.
 * 2. Performs PostgreSQL startup + authentication (trust or cleartext password).
 * 3. Executes a role/state query to determine primary vs. replica.
 * 4. For replicas: queries replication lag; marks DEGRADED if thresholds exceeded.
 * 5. Terminates the connection and populates @p info.
 *
 * Auth modes supported: trust (type 0), cleartext password (type 3).
 * For MD5/SCRAM environments, configure a dedicated discovery user with trust.
 *
 * @return KEEL_OK always; @p info->health reflects the actual outcome.
 */
keel_error_t keel_pg_discovery_probe(
    const char*                          host,
    uint16_t                             port,
    const char*                          user,
    const char*                          pass,
    const char*                          dbname,
    const keel_discovery_probe_params_t* params,
    keel_server_info_t*                  info
);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_PROBE_POSTGRES_DISCOVERY_H */
