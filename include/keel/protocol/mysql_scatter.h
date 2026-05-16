/**
 * @file mysql_scatter.h
 * @brief MySQL-specific scatter adapter: wire type ↔ keel_col_type_t mapping.
 *
 * The keel core scatter-merge engine uses the DB-agnostic keel_col_type_t
 * enum defined in scatter_store.h.  When the MySQL protocol layer reads
 * column metadata from a backend it uses keel_mysql_type_to_col_type() to
 * translate the MySQL wire type byte into the generic type.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * GNU Affero General Public License v3.0
 */

#ifndef KEEL_PROTO_MYSQL_MYSQL_SCATTER_H
#define KEEL_PROTO_MYSQL_MYSQL_SCATTER_H

#include <stdint.h>
#include "keel/core/scatter_store.h"

/* ============================================================================
 * MySQL column type constants (MySQL wire protocol / enum_field_types)
 * ============================================================================ */

#define KEEL_MYSQL_TYPE_DECIMAL      0
#define KEEL_MYSQL_TYPE_TINY         1   /* TINYINT / BOOL */
#define KEEL_MYSQL_TYPE_SHORT        2   /* SMALLINT */
#define KEEL_MYSQL_TYPE_LONG         3   /* INT */
#define KEEL_MYSQL_TYPE_FLOAT        4
#define KEEL_MYSQL_TYPE_DOUBLE       5
#define KEEL_MYSQL_TYPE_NULL         6
#define KEEL_MYSQL_TYPE_TIMESTAMP    7
#define KEEL_MYSQL_TYPE_LONGLONG     8   /* BIGINT */
#define KEEL_MYSQL_TYPE_INT24        9   /* MEDIUMINT */
#define KEEL_MYSQL_TYPE_DATE        10
#define KEEL_MYSQL_TYPE_TIME        11
#define KEEL_MYSQL_TYPE_DATETIME    12
#define KEEL_MYSQL_TYPE_YEAR        13
#define KEEL_MYSQL_TYPE_VARCHAR     15
#define KEEL_MYSQL_TYPE_BIT         16
#define KEEL_MYSQL_TYPE_NEWDECIMAL 246
#define KEEL_MYSQL_TYPE_ENUM       247
#define KEEL_MYSQL_TYPE_SET        248
#define KEEL_MYSQL_TYPE_TINY_BLOB  249
#define KEEL_MYSQL_TYPE_MEDIUM_BLOB 250
#define KEEL_MYSQL_TYPE_LONG_BLOB  251
#define KEEL_MYSQL_TYPE_BLOB       252
#define KEEL_MYSQL_TYPE_VAR_STRING 253
#define KEEL_MYSQL_TYPE_STRING     254
#define KEEL_MYSQL_TYPE_GEOMETRY   255

/* ============================================================================
 * Mapping function
 * ============================================================================ */

/**
 * @brief Map a MySQL wire type byte to the generic keel_col_type_t.
 *
 * Returns KEEL_COL_TYPE_UNKNOWN for any type not explicitly listed above.
 */
keel_col_type_t keel_mysql_type_to_col_type(uint8_t mysql_type);

#endif /* KEEL_PROTO_MYSQL_MYSQL_SCATTER_H */
