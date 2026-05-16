/**
 * @file mysql_scatter.c
 * @brief MySQL wire type → keel_col_type_t mapping implementation.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * GNU Affero General Public License v3.0
 */

#include "keel/protocol/mysql_scatter.h"

keel_col_type_t keel_mysql_type_to_col_type(uint8_t mysql_type)
{
    switch (mysql_type) {
    case KEEL_MYSQL_TYPE_TINY:
        return KEEL_COL_TYPE_BOOL;   /* also used for TINYINT; caller may refine */
    case KEEL_MYSQL_TYPE_SHORT:
    case KEEL_MYSQL_TYPE_YEAR:
        return KEEL_COL_TYPE_INT16;
    case KEEL_MYSQL_TYPE_LONG:
    case KEEL_MYSQL_TYPE_INT24:
        return KEEL_COL_TYPE_INT32;
    case KEEL_MYSQL_TYPE_LONGLONG:
        return KEEL_COL_TYPE_INT64;
    case KEEL_MYSQL_TYPE_FLOAT:
        return KEEL_COL_TYPE_FLOAT32;
    case KEEL_MYSQL_TYPE_DOUBLE:
    case KEEL_MYSQL_TYPE_DECIMAL:
    case KEEL_MYSQL_TYPE_NEWDECIMAL:
        return KEEL_COL_TYPE_FLOAT64;
    case KEEL_MYSQL_TYPE_DATE:
        return KEEL_COL_TYPE_DATE;
    case KEEL_MYSQL_TYPE_TIME:
        return KEEL_COL_TYPE_TIME;
    case KEEL_MYSQL_TYPE_DATETIME:
    case KEEL_MYSQL_TYPE_TIMESTAMP:
        return KEEL_COL_TYPE_TIMESTAMP;
    case KEEL_MYSQL_TYPE_VARCHAR:
    case KEEL_MYSQL_TYPE_VAR_STRING:
    case KEEL_MYSQL_TYPE_STRING:
    case KEEL_MYSQL_TYPE_ENUM:
    case KEEL_MYSQL_TYPE_SET:
        return KEEL_COL_TYPE_TEXT;
    case KEEL_MYSQL_TYPE_BLOB:
    case KEEL_MYSQL_TYPE_TINY_BLOB:
    case KEEL_MYSQL_TYPE_MEDIUM_BLOB:
    case KEEL_MYSQL_TYPE_LONG_BLOB:
    case KEEL_MYSQL_TYPE_BIT:
    case KEEL_MYSQL_TYPE_GEOMETRY:
        return KEEL_COL_TYPE_BYTES;
    case KEEL_MYSQL_TYPE_NULL:
    default:
        return KEEL_COL_TYPE_UNKNOWN;
    }
}
