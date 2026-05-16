/**
 * @file pg_scatter.c
 * @brief PostgreSQL OID ↔ keel_col_type_t mapping implementation.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * GNU Affero General Public License v3.0
 */

#include "keel/protocol/postgres/pg_scatter.h"

keel_col_type_t keel_pg_oid_to_col_type(uint32_t oid)
{
    switch (oid) {
    case KEEL_PG_OID_BOOL:                                     return KEEL_COL_TYPE_BOOL;
    case KEEL_PG_OID_INT2:                                     return KEEL_COL_TYPE_INT16;
    case KEEL_PG_OID_INT4: case KEEL_PG_OID_OID:              return KEEL_COL_TYPE_INT32;
    case KEEL_PG_OID_INT8:                                     return KEEL_COL_TYPE_INT64;
    case KEEL_PG_OID_FLOAT4:                                   return KEEL_COL_TYPE_FLOAT32;
    case KEEL_PG_OID_FLOAT8: case KEEL_PG_OID_NUMERIC:        return KEEL_COL_TYPE_FLOAT64;
    case KEEL_PG_OID_CHAR: case KEEL_PG_OID_NAME:
    case KEEL_PG_OID_TEXT: case KEEL_PG_OID_BPCHAR:
    case KEEL_PG_OID_VARCHAR:                                  return KEEL_COL_TYPE_TEXT;
    case KEEL_PG_OID_DATE:                                     return KEEL_COL_TYPE_DATE;
    case KEEL_PG_OID_TIME:                                     return KEEL_COL_TYPE_TIME;
    case KEEL_PG_OID_TIMESTAMP: case KEEL_PG_OID_TIMESTAMPTZ: return KEEL_COL_TYPE_TIMESTAMP;
    case KEEL_PG_OID_UUID:                                     return KEEL_COL_TYPE_UUID;
    case KEEL_PG_OID_BYTEA:                                    return KEEL_COL_TYPE_BYTES;
    case KEEL_PG_OID_TEXT_ARRAY:                               return KEEL_COL_TYPE_TEXT_ARRAY;
    case KEEL_PG_OID_JSONB:                                    return KEEL_COL_TYPE_JSONB;
    default:                                                    return KEEL_COL_TYPE_UNKNOWN;
    }
}

uint32_t keel_pg_col_type_to_oid(keel_col_type_t type)
{
    switch (type) {
    case KEEL_COL_TYPE_BOOL:      return KEEL_PG_OID_BOOL;
    case KEEL_COL_TYPE_INT16:     return KEEL_PG_OID_INT2;
    case KEEL_COL_TYPE_INT32:     return KEEL_PG_OID_INT4;
    case KEEL_COL_TYPE_INT64:     return KEEL_PG_OID_INT8;
    case KEEL_COL_TYPE_FLOAT32:   return KEEL_PG_OID_FLOAT4;
    case KEEL_COL_TYPE_FLOAT64:   return KEEL_PG_OID_FLOAT8;
    case KEEL_COL_TYPE_TEXT:      return KEEL_PG_OID_TEXT;
    case KEEL_COL_TYPE_DATE:      return KEEL_PG_OID_DATE;
    case KEEL_COL_TYPE_TIME:      return KEEL_PG_OID_TIME;
    case KEEL_COL_TYPE_TIMESTAMP: return KEEL_PG_OID_TIMESTAMP;
    case KEEL_COL_TYPE_UUID:      return KEEL_PG_OID_UUID;
    case KEEL_COL_TYPE_BYTES:     return KEEL_PG_OID_BYTEA;    case KEEL_COL_TYPE_TEXT_ARRAY: return KEEL_PG_OID_TEXT_ARRAY;
    case KEEL_COL_TYPE_JSONB:      return KEEL_PG_OID_JSONB;    default:                       return KEEL_PG_OID_TEXT;
    }
}
