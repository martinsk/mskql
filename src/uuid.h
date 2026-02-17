#ifndef UUID_H
#define UUID_H

#include <stdint.h>
#include <string.h>

struct uuid_val {
    uint64_t hi;
    uint64_t lo;
};

static const struct uuid_val UUID_ZERO = {0, 0};

static inline int hex_digit(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

/* parse "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" → binary. returns 0 on success. */
static inline int uuid_parse(const char *str, struct uuid_val *out)
{
    if (!str) return -1;
    /* expected positions of dashes: 8, 13, 18, 23 */
    static const int dash_pos[] = {8, 13, 18, 23};
    int di = 0, nibble = 0;
    uint64_t hi = 0, lo = 0;
    for (int i = 0; i < 36; i++) {
        if (di < 4 && i == dash_pos[di]) {
            if (str[i] != '-') return -1;
            di++;
            continue;
        }
        int v = hex_digit(str[i]);
        if (v < 0) return -1;
        if (nibble < 16)
            hi = (hi << 4) | (uint64_t)v;
        else
            lo = (lo << 4) | (uint64_t)v;
        nibble++;
    }
    if (str[36] != '\0') return -1;
    if (nibble != 32) return -1;
    out->hi = hi;
    out->lo = lo;
    return 0;
}

/* binary → "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" (buf must be ≥37 bytes) */
static inline void uuid_format(const struct uuid_val *u, char buf[37])
{
    static const char hex[] = "0123456789abcdef";
    uint64_t hi = u->hi, lo = u->lo;
    /* hi: bits 63..0 → first 16 hex digits (positions 0-17 with dashes) */
    int pos = 0;
    for (int i = 15; i >= 0; i--) {
        buf[pos++] = hex[(hi >> (i * 4)) & 0xf];
        if (pos == 8 || pos == 13 || pos == 18 || pos == 23) buf[pos++] = '-';
    }
    /* lo: bits 63..0 → last 16 hex digits (positions 19-35 with dash at 23) */
    for (int i = 15; i >= 0; i--) {
        buf[pos++] = hex[(lo >> (i * 4)) & 0xf];
        if (pos == 23) buf[pos++] = '-';
    }
    buf[36] = '\0';
}

/* returns -1, 0, or 1 */
static inline int uuid_compare(struct uuid_val a, struct uuid_val b)
{
    if (a.hi < b.hi) return -1;
    if (a.hi > b.hi) return  1;
    if (a.lo < b.lo) return -1;
    if (a.lo > b.lo) return  1;
    return 0;
}

static inline int uuid_equal(struct uuid_val a, struct uuid_val b)
{
    return a.hi == b.hi && a.lo == b.lo;
}

static inline uint64_t uuid_hash(struct uuid_val u)
{
    uint64_t h = 14695981039346656037ULL;
    h ^= u.hi; h *= 1099511628211ULL;
    h ^= u.lo; h *= 1099511628211ULL;
    return h;
}

static inline int uuid_is_zero(struct uuid_val u)
{
    return u.hi == 0 && u.lo == 0;
}

#endif
