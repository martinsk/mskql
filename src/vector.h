#ifndef VECTOR_H
#define VECTOR_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

/* Parse a pgvector-format text literal "[1.0, 2.0, 3.0]" into a float array.
 * Returns 0 on success, -1 on parse error, -2 on dimension mismatch.
 * out must point to at least expected_dim floats. */
static inline int vector_parse(const char *str, float *out, uint16_t expected_dim)
{
    if (!str || *str != '[') return -1;
    const char *p = str + 1;
    uint16_t count = 0;
    while (*p) {
        while (*p == ' ') p++;
        if (*p == ']') break;
        if (count > 0) {
            if (*p != ',') return -1;
            p++;
            while (*p == ' ') p++;
        }
        if (count >= expected_dim) return -2;
        char *end;
        out[count] = strtof(p, &end);
        if (end == p) return -1;
        p = end;
        count++;
    }
    if (*p != ']') return -1;
    if (count != expected_dim) return -2;
    return 0;
}

/* Parse a pgvector-format text literal and allocate the float array.
 * Returns the allocated array on success, NULL on error.
 * Sets *out_dim to the parsed dimension. */
static inline float *vector_parse_alloc(const char *str, uint16_t *out_dim)
{
    if (!str || *str != '[') return NULL;
    /* first pass: count dimensions */
    const char *p = str + 1;
    uint16_t count = 0;
    while (*p) {
        while (*p == ' ') p++;
        if (*p == ']') break;
        if (count > 0) {
            if (*p != ',') return NULL;
            p++;
            while (*p == ' ') p++;
        }
        char *end;
        (void)strtof(p, &end);
        if (end == p) return NULL;
        p = end;
        count++;
    }
    if (*p != ']' || count == 0) return NULL;
    /* second pass: parse values */
    float *vec = (float *)malloc(count * sizeof(float));
    if (!vec) return NULL;
    if (vector_parse(str, vec, count) != 0) {
        free(vec);
        return NULL;
    }
    *out_dim = count;
    return vec;
}

/* Format a float vector as pgvector text "[1,2,3]".
 * buf must have space for at least dim * 20 + 3 bytes.
 * Returns the number of bytes written (excluding NUL). */
static inline int vector_format(const float *vec, uint16_t dim, char *buf, size_t bufsz)
{
    size_t pos = 0;
    if (pos < bufsz) buf[pos++] = '[';
    for (uint16_t i = 0; i < dim; i++) {
        if (i > 0 && pos < bufsz) buf[pos++] = ',';
        int n = snprintf(buf + pos, bufsz - pos, "%g", (double)vec[i]);
        if (n > 0) pos += (size_t)n;
    }
    if (pos < bufsz) buf[pos++] = ']';
    if (pos < bufsz) buf[pos] = '\0';
    else if (bufsz > 0) buf[bufsz - 1] = '\0';
    return (int)pos;
}

#endif
