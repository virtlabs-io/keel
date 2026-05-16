/**
 * @file pg_scatter.h
 * @brief PostgreSQL-specific scatter adapter: OID ↔ keel_col_type_t mapping.
 *
 * The keel core scatter-merge engine uses the DB-agnostic keel_col_type_t
 * enum defined in scatter_store.h.  When the PostgreSQL protocol layer reads
 * a RowDescription message from a backend, it uses keel_pg_oid_to_col_type()
 * to translate the wire OID into the generic type.  When it writes a
 * RowDescription back to a client it uses keel_pg_col_type_to_oid() for the
 * reverse mapping.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * GNU Affero General Public License v3.0
 */

#ifndef KEEL_PROTO_POSTGRES_PG_SCATTER_H
#define KEEL_PROTO_POSTGRES_PG_SCATTER_H

#include "keel/core/scatter_store.h"

/* ============================================================================
 * Well-known PostgreSQL type OIDs  (pg_type.oid)
 * ============================================================================ */

#define KEEL_PG_OID_BOOL          16U
#define KEEL_PG_OID_INT8          20U
#define KEEL_PG_OID_INT2          21U
#define KEEL_PG_OID_INT4          23U
#define KEEL_PG_OID_OID           26U
#define KEEL_PG_OID_FLOAT4       700U
#define KEEL_PG_OID_FLOAT8       701U
#define KEEL_PG_OID_CHAR          18U
#define KEEL_PG_OID_NAME          19U
#define KEEL_PG_OID_TEXT          25U
#define KEEL_PG_OID_BPCHAR      1042U
#define KEEL_PG_OID_VARCHAR     1043U
#define KEEL_PG_OID_DATE        1082U
#define KEEL_PG_OID_TIME        1083U
#define KEEL_PG_OID_TIMESTAMP   1114U
#define KEEL_PG_OID_TIMESTAMPTZ 1184U
#define KEEL_PG_OID_NUMERIC     1700U
#define KEEL_PG_OID_UUID        2950U
#define KEEL_PG_OID_BYTEA         17U
#define KEEL_PG_OID_TEXT_ARRAY  1009U
#define KEEL_PG_OID_JSONB       3802U

/* ============================================================================
 * Mapping functions
 * ============================================================================ */

/**
 * @brief Map a PostgreSQL wire OID to the generic keel_col_type_t.
 *
 * Returns KEEL_COL_TYPE_UNKNOWN for any OID not explicitly listed above.
 */
keel_col_type_t keel_pg_oid_to_col_type(uint32_t oid);

/**
 * @brief Map a generic keel_col_type_t back to a canonical PostgreSQL OID.
 *
 * Used when re-encoding a RowDescription message for a PostgreSQL client.
 * Returns KEEL_PG_OID_TEXT for KEEL_COL_TYPE_UNKNOWN.
 */
uint32_t keel_pg_col_type_to_oid(keel_col_type_t type);

#endif /* KEEL_PROTO_POSTGRES_PG_SCATTER_H */
