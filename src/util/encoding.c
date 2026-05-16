/**
 * @file encoding.c
 * @brief Hex and JSON string encoding helpers.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/util/encoding.h"

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>

size_t keel_json_escape(char *dst, size_t dst_size, const char *src) {
    if (!src) {
        if (dst_size > 0) dst[0] = '\0';
        return 0;
    }
    size_t w = 0;
    for (const char *p = src; *p && w + 6 < dst_size; p++) {
        switch (*p) {
        case '"':  dst[w++] = '\\'; dst[w++] = '"';  break;
        case '\\': dst[w++] = '\\'; dst[w++] = '\\'; break;
        case '\n': dst[w++] = '\\'; dst[w++] = 'n';  break;
        case '\r': dst[w++] = '\\'; dst[w++] = 'r';  break;
        case '\t': dst[w++] = '\\'; dst[w++] = 't';  break;
        default:
            if ((unsigned char)*p < 0x20) {
                w += (size_t)snprintf(dst + w, dst_size - w,
                                      "\\u%04x", (unsigned char)*p);
            } else {
                dst[w++] = *p;
            }
        }
    }
    if (w < dst_size) dst[w] = '\0';
    return w;
}
