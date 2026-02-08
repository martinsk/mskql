#ifndef STRINGVIEW_H
#define STRINGVIEW_H

#include <stddef.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>

typedef struct {
    const char *data;
    size_t len;
} sv;

#define SV(cstr)       (sv){ .data = (cstr), .len = strlen(cstr) }
#define SV_LIT(lit)    (sv){ .data = (lit), .len = sizeof(lit) - 1 }
#define SV_NULL        (sv){ .data = NULL, .len = 0 }
#define SV_FMT         "%.*s"
#define SV_ARG(s)      (int)(s).len, (s).data

static inline sv sv_from(const char *data, size_t len)
{
    return (sv){ .data = data, .len = len };
}

static inline bool sv_eq(sv a, sv b)
{
    if (a.len != b.len) return false;
    return memcmp(a.data, b.data, a.len) == 0;
}

static inline bool sv_eq_cstr(sv a, const char *b)
{
    size_t blen = strlen(b);
    if (a.len != blen) return false;
    return memcmp(a.data, b, a.len) == 0;
}

static inline bool sv_eq_ignorecase(sv a, sv b)
{
    if (a.len != b.len) return false;
    for (size_t i = 0; i < a.len; i++) {
        if (tolower((unsigned char)a.data[i]) != tolower((unsigned char)b.data[i]))
            return false;
    }
    return true;
}

static inline bool sv_eq_ignorecase_cstr(sv a, const char *b)
{
    return sv_eq_ignorecase(a, SV(b));
}

static inline sv sv_trim(sv s)
{
    while (s.len > 0 && isspace((unsigned char)s.data[0])) {
        s.data++;
        s.len--;
    }
    while (s.len > 0 && isspace((unsigned char)s.data[s.len - 1])) {
        s.len--;
    }
    return s;
}

static inline char *sv_to_cstr(sv s)
{
    char *buf = malloc(s.len + 1);
    if (buf) {
        memcpy(buf, s.data, s.len);
        buf[s.len] = '\0';
    }
    return buf;
}

static inline bool sv_starts_with(sv s, sv prefix)
{
    if (prefix.len > s.len) return false;
    return memcmp(s.data, prefix.data, prefix.len) == 0;
}

static inline sv sv_chop_left(sv s, size_t n)
{
    if (n > s.len) n = s.len;
    return sv_from(s.data + n, s.len - n);
}

static inline sv sv_chop_right(sv s, size_t n)
{
    if (n > s.len) n = s.len;
    return sv_from(s.data, s.len - n);
}

static inline int sv_atoi(sv s)
{
    char buf[64];
    size_t n = s.len < sizeof(buf) - 1 ? s.len : sizeof(buf) - 1;
    memcpy(buf, s.data, n);
    buf[n] = '\0';
    return atoi(buf);
}

static inline double sv_atof(sv s)
{
    char buf[64];
    size_t n = s.len < sizeof(buf) - 1 ? s.len : sizeof(buf) - 1;
    memcpy(buf, s.data, n);
    buf[n] = '\0';
    return atof(buf);
}

static inline bool sv_contains_char(sv s, char c)
{
    for (size_t i = 0; i < s.len; i++)
        if (s.data[i] == c) return true;
    return false;
}

#endif
