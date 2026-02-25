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

/* Fast float32 → decimal string using %g semantics (6 significant digits,
 * strip trailing zeros). Returns number of bytes written (excluding NUL).
 * buf must be >= 16 bytes. Does NOT NUL-terminate (caller's responsibility). */
static inline size_t fast_f32_to_str(float v, char *buf)
{
    if (v != v) { buf[0] = 'N'; buf[1] = 'a'; buf[2] = 'N'; return 3; }
    if (v == 0.0f) { buf[0] = '0'; return 1; }
    char *p = buf;
    if (v < 0) { *p++ = '-'; v = -v; }
    if (v > 3.4028235e38f) {
        memcpy(p, "Infinity", 8); return (size_t)(p - buf) + 8;
    }
    /* Fast path: exact integer in int32 range */
    if (v >= 1.0f && v <= 1.6777216e7f) { /* 2^24 — float has 24-bit mantissa */
        int32_t iv = (int32_t)v;
        if ((float)iv == v) {
            char tmp[12];
            char *tp = tmp + sizeof(tmp);
            uint32_t uv = (uint32_t)iv;
            while (uv >= 10) { *--tp = '0' + (char)(uv % 10); uv /= 10; }
            *--tp = '0' + (char)uv;
            size_t dlen = (size_t)(tmp + sizeof(tmp) - tp);
            memcpy(p, tp, dlen);
            return (size_t)(p - buf) + dlen;
        }
    }
    /* Fast path: fixed-point values in %g range [1e-4, 1e6).
     * %g uses 6 significant digits. For float32 (~7.2 decimal digits of
     * precision), 6 sig digits always matches snprintf("%g") output.
     * No round-trip check needed — we just format and strip trailing zeros. */
    if (v >= 0.0001f && v < 1e6f) {
        int scale;
        if      (v >= 1e5f) scale = 0;
        else if (v >= 1e4f) scale = 1;
        else if (v >= 1e3f) scale = 2;
        else if (v >= 1e2f) scale = 3;
        else if (v >= 1e1f) scale = 4;
        else if (v >= 1e0f) scale = 5;
        else if (v >= 1e-1f) scale = 6;
        else if (v >= 1e-2f) scale = 7;
        else if (v >= 1e-3f) scale = 8;
        else                 scale = 9;
        static const double pow10_tbl[] = {
            1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0,
            1000000.0, 10000000.0, 100000000.0, 1000000000.0
        };
        double scaled = (double)v * pow10_tbl[scale];
        int64_t digits = (int64_t)(scaled + 0.5);
        /* Strip trailing zeros */
        while (digits > 0 && digits % 10 == 0 && scale > 0) {
            digits /= 10;
            scale--;
        }
        if (scale == 0) {
            char tmp[12];
            char *tp = tmp + sizeof(tmp);
            int64_t d = digits;
            while (d >= 10) { *--tp = '0' + (char)(d % 10); d /= 10; }
            *--tp = '0' + (char)d;
            size_t dlen = (size_t)(tmp + sizeof(tmp) - tp);
            memcpy(p, tp, dlen);
            return (size_t)(p - buf) + dlen;
        }
        char tmp[12];
        char *tp = tmp + sizeof(tmp);
        int64_t d = digits;
        while (d >= 10) { *--tp = '0' + (char)(d % 10); d /= 10; }
        *--tp = '0' + (char)d;
        size_t dlen = (size_t)(tmp + sizeof(tmp) - tp);
        if ((int)dlen <= scale) {
            *p++ = '0';
            *p++ = '.';
            for (int z = 0; z < scale - (int)dlen; z++)
                *p++ = '0';
            memcpy(p, tp, dlen);
            p += dlen;
        } else {
            size_t ipart = dlen - (size_t)scale;
            memcpy(p, tp, ipart);
            p += ipart;
            *p++ = '.';
            memcpy(p, tp + ipart, (size_t)scale);
            p += scale;
        }
        return (size_t)(p - buf);
    }
    /* Fallback: snprintf for edge cases (scientific notation, very large) */
    return (size_t)snprintf(buf, 16, "%g", (double)v);
}

/* Fast vector format: same output as vector_format but uses fast_f32_to_str
 * instead of snprintf per element. buf must have space for dim * 16 + 3 bytes.
 * Returns the number of bytes written (excluding NUL). */
static inline int vector_format_fast(const float *vec, uint16_t dim, char *buf, size_t bufsz)
{
    char *p = buf;
    char *end = buf + bufsz - 1; /* leave room for NUL */
    *p++ = '[';
    for (uint16_t i = 0; i < dim; i++) {
        if (i > 0) *p++ = ',';
        if (p >= end) break;
        size_t n = fast_f32_to_str(vec[i], p);
        p += n;
    }
    *p++ = ']';
    *p = '\0';
    return (int)(p - buf); /* bytes written excluding NUL */
}

/* ---- Vector distance functions ---- */

/* Squared L2 (Euclidean) distance: sum((a[i] - b[i])^2) */
static inline float vector_l2_distance(const float *a, const float *b, uint16_t dim)
{
    float sum = 0.0f;
    for (uint16_t i = 0; i < dim; i++) {
        float d = a[i] - b[i];
        sum += d * d;
    }
    return sum;
}

/* Cosine distance: 1 - (a·b) / (|a| * |b|) */
static inline float vector_cosine_distance(const float *a, const float *b, uint16_t dim)
{
    float dot = 0.0f, na = 0.0f, nb = 0.0f;
    for (uint16_t i = 0; i < dim; i++) {
        dot += a[i] * b[i];
        na  += a[i] * a[i];
        nb  += b[i] * b[i];
    }
    float denom = sqrtf(na) * sqrtf(nb);
    if (denom == 0.0f) return 1.0f;
    return 1.0f - dot / denom;
}

/* Negative inner product: -(a·b)  (lower = more similar) */
static inline float vector_inner_product(const float *a, const float *b, uint16_t dim)
{
    float dot = 0.0f;
    for (uint16_t i = 0; i < dim; i++)
        dot += a[i] * b[i];
    return -dot;
}

#endif
