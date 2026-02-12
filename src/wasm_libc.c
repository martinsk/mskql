/*
 * wasm_libc.c — freestanding libc implementation for wasm32 build.
 * Provides malloc/free, string ops, minimal snprintf, qsort, rand, time stubs.
 * Math and time functions are imported from JavaScript via env.* imports.
 */
#ifdef MSKQL_WASM
#include "wasm_libc.h"

/* ════════════════════════════════════════════════════════════════════
 *  JS imports (provided by mskql-wasm.js)
 * ════════════════════════════════════════════════════════════════════ */
__attribute__((import_module("env"), import_name("js_log")))
void js_log(const char *ptr, int len);

__attribute__((import_module("env"), import_name("js_time")))
double js_time(void);

__attribute__((import_module("env"), import_name("js_mktime")))
double js_mktime(int y, int m, int d, int h, int min, int sec);

__attribute__((import_module("env"), import_name("js_localtime")))
void js_localtime(double epoch, int *out7);

/* math imports */
__attribute__((import_module("env"), import_name("js_pow")))
double js_pow(double, double);
__attribute__((import_module("env"), import_name("js_sqrt")))
double js_sqrt(double);
__attribute__((import_module("env"), import_name("js_floor")))
double js_floor(double);
__attribute__((import_module("env"), import_name("js_ceil")))
double js_ceil(double);
__attribute__((import_module("env"), import_name("js_round")))
double js_round(double);
__attribute__((import_module("env"), import_name("js_fmod")))
double js_fmod(double, double);
__attribute__((import_module("env"), import_name("js_fabs")))
double js_fabs(double);
__attribute__((import_module("env"), import_name("js_log")))
double js_log_math(double);
__attribute__((import_module("env"), import_name("js_exp")))
double js_exp(double);
__attribute__((import_module("env"), import_name("js_sin")))
double js_sin(double);
__attribute__((import_module("env"), import_name("js_cos")))
double js_cos(double);

__attribute__((import_module("env"), import_name("js_abort")))
_Noreturn void js_abort(void);

/* ════════════════════════════════════════════════════════════════════
 *  WASM memory management
 * ════════════════════════════════════════════════════════════════════ */

/*
 * Simple allocator operating on WASM linear memory.
 * Uses a first-fit free list with coalescing.
 * Each block has an 8-byte header: [size:4][used:4]
 * Free blocks are linked via a next pointer stored in the payload.
 */

#define ALIGN 8
#define HDR_SIZE 8

/* WASM linear memory grows in 64KB pages */
extern unsigned char __heap_base;
static unsigned char *heap_start = 0;
static unsigned char *heap_end   = 0;

/* round up to alignment */
static inline size_t align_up(size_t n) {
    return (n + ALIGN - 1) & ~(ALIGN - 1);
}

/* block header: stored at ptr - HDR_SIZE */
static inline size_t blk_size(void *p) {
    unsigned char *h = (unsigned char *)p - HDR_SIZE;
    return *(size_t *)h;
}
static inline int blk_used(void *p) {
    unsigned char *h = (unsigned char *)p - HDR_SIZE;
    return *(int *)(h + 4);
}
static inline void blk_set(void *p, size_t sz, int used) {
    unsigned char *h = (unsigned char *)p - HDR_SIZE;
    *(size_t *)h = sz;
    *(int *)(h + 4) = used;
}

/* grow WASM memory */
static int grow_memory(size_t needed) {
    size_t pages = (needed + 65535) / 65536;
    int old = __builtin_wasm_memory_grow(0, pages);
    if (old == -1) return -1;
    heap_end = (unsigned char *)((unsigned long)(old + pages) * 65536);
    return 0;
}

static void heap_init(void) {
    if (heap_start) return;
    heap_start = (unsigned char *)align_up((size_t)&__heap_base);
    /* get current memory size */
    size_t cur_pages = __builtin_wasm_memory_size(0);
    heap_end = (unsigned char *)(cur_pages * 65536);
    /* create initial free block spanning all available heap */
    size_t avail = (size_t)(heap_end - heap_start) - HDR_SIZE;
    unsigned char *p = heap_start + HDR_SIZE;
    blk_set(p, avail, 0);
}

void *malloc(size_t size) {
    if (size == 0) return NULL;
    heap_init();
    size = align_up(size);

    /* first-fit scan */
    unsigned char *p = heap_start + HDR_SIZE;
    while (p < heap_end) {
        size_t bsz = blk_size(p);
        if (!blk_used(p) && bsz >= size) {
            /* split if remainder is large enough */
            if (bsz >= size + HDR_SIZE + ALIGN) {
                unsigned char *next = p + size + HDR_SIZE;
                blk_set(next, bsz - size - HDR_SIZE, 0);
                blk_set(p, size, 1);
            } else {
                blk_set(p, bsz, 1);
            }
            return p;
        }
        p += bsz + HDR_SIZE;
    }

    /* no fit — grow memory */
    size_t grow_sz = size + HDR_SIZE + 65536;
    if (grow_memory(grow_sz) == -1) return NULL;

    /* the old heap_end becomes a new free block */
    unsigned char *old_end = p; /* p == old heap_end after scan */
    /* but we need to check if the last block was free and extend it */
    /* simple approach: just create a new block at old_end */
    size_t new_avail = (size_t)(heap_end - old_end) - HDR_SIZE;
    unsigned char *np = old_end + HDR_SIZE;
    blk_set(np, new_avail, 0);

    /* retry allocation from this new block */
    if (new_avail >= size) {
        if (new_avail >= size + HDR_SIZE + ALIGN) {
            unsigned char *next = np + size + HDR_SIZE;
            blk_set(next, new_avail - size - HDR_SIZE, 0);
            blk_set(np, size, 1);
        } else {
            blk_set(np, new_avail, 1);
        }
        return np;
    }
    return NULL;
}

void free(void *ptr) {
    if (!ptr) return;
    blk_set(ptr, blk_size(ptr), 0);

    /* coalesce with next block */
    unsigned char *next = (unsigned char *)ptr + blk_size(ptr) + HDR_SIZE;
    if (next < heap_end && !blk_used(next)) {
        blk_set(ptr, blk_size(ptr) + HDR_SIZE + blk_size(next), 0);
    }
}

void *calloc(size_t nmemb, size_t size) {
    size_t total = nmemb * size;
    void *p = malloc(total);
    if (p) memset(p, 0, total);
    return p;
}

void *realloc(void *ptr, size_t size) {
    if (!ptr) return malloc(size);
    if (size == 0) { free(ptr); return NULL; }
    size_t old = blk_size(ptr);
    if (old >= size) return ptr;

    /* try to extend into next free block */
    unsigned char *next = (unsigned char *)ptr + old + HDR_SIZE;
    if (next < heap_end && !blk_used(next)) {
        size_t combined = old + HDR_SIZE + blk_size(next);
        if (combined >= align_up(size)) {
            size_t needed = align_up(size);
            if (combined >= needed + HDR_SIZE + ALIGN) {
                unsigned char *split = (unsigned char *)ptr + needed + HDR_SIZE;
                blk_set(split, combined - needed - HDR_SIZE, 0);
                blk_set(ptr, needed, 1);
            } else {
                blk_set(ptr, combined, 1);
            }
            return ptr;
        }
    }

    void *newp = malloc(size);
    if (!newp) return NULL;
    memcpy(newp, ptr, old < size ? old : size);
    free(ptr);
    return newp;
}

/* ════════════════════════════════════════════════════════════════════
 *  String / memory operations
 * ════════════════════════════════════════════════════════════════════ */

void *memcpy(void *dst, const void *src, size_t n) {
    unsigned char *d = (unsigned char *)dst;
    const unsigned char *s = (const unsigned char *)src;
    for (size_t i = 0; i < n; i++) d[i] = s[i];
    return dst;
}

void *memmove(void *dst, const void *src, size_t n) {
    unsigned char *d = (unsigned char *)dst;
    const unsigned char *s = (const unsigned char *)src;
    if (d < s) {
        for (size_t i = 0; i < n; i++) d[i] = s[i];
    } else {
        for (size_t i = n; i > 0; i--) d[i-1] = s[i-1];
    }
    return dst;
}

void *memset(void *s, int c, size_t n) {
    unsigned char *p = (unsigned char *)s;
    for (size_t i = 0; i < n; i++) p[i] = (unsigned char)c;
    return s;
}

int memcmp(const void *s1, const void *s2, size_t n) {
    const unsigned char *a = (const unsigned char *)s1;
    const unsigned char *b = (const unsigned char *)s2;
    for (size_t i = 0; i < n; i++) {
        if (a[i] != b[i]) return a[i] - b[i];
    }
    return 0;
}

size_t strlen(const char *s) {
    size_t n = 0;
    while (s[n]) n++;
    return n;
}

int strcmp(const char *s1, const char *s2) {
    while (*s1 && *s1 == *s2) { s1++; s2++; }
    return (unsigned char)*s1 - (unsigned char)*s2;
}

int strncmp(const char *s1, const char *s2, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (s1[i] != s2[i]) return (unsigned char)s1[i] - (unsigned char)s2[i];
        if (s1[i] == '\0') return 0;
    }
    return 0;
}

int strcasecmp(const char *s1, const char *s2) {
    while (*s1 && tolower((unsigned char)*s1) == tolower((unsigned char)*s2)) { s1++; s2++; }
    return tolower((unsigned char)*s1) - tolower((unsigned char)*s2);
}

int strncasecmp(const char *s1, const char *s2, size_t n) {
    for (size_t i = 0; i < n; i++) {
        int c1 = tolower((unsigned char)s1[i]);
        int c2 = tolower((unsigned char)s2[i]);
        if (c1 != c2) return c1 - c2;
        if (s1[i] == '\0') return 0;
    }
    return 0;
}

char *strchr(const char *s, int c) {
    while (*s) {
        if (*s == (char)c) return (char *)s;
        s++;
    }
    return (c == '\0') ? (char *)s : NULL;
}

char *strrchr(const char *s, int c) {
    const char *last = NULL;
    while (*s) {
        if (*s == (char)c) last = s;
        s++;
    }
    if (c == '\0') return (char *)s;
    return (char *)last;
}

char *strstr(const char *haystack, const char *needle) {
    size_t nlen = strlen(needle);
    if (nlen == 0) return (char *)haystack;
    while (*haystack) {
        if (strncmp(haystack, needle, nlen) == 0) return (char *)haystack;
        haystack++;
    }
    return NULL;
}

char *strdup(const char *s) {
    size_t n = strlen(s) + 1;
    char *d = (char *)malloc(n);
    if (d) memcpy(d, s, n);
    return d;
}

char *strndup(const char *s, size_t n) {
    size_t len = 0;
    while (len < n && s[len]) len++;
    char *d = (char *)malloc(len + 1);
    if (d) { memcpy(d, s, len); d[len] = '\0'; }
    return d;
}

char *strcpy(char *dst, const char *src) {
    char *d = dst;
    while ((*d++ = *src++));
    return dst;
}

char *strncpy(char *dst, const char *src, size_t n) {
    size_t i;
    for (i = 0; i < n && src[i]; i++) dst[i] = src[i];
    for (; i < n; i++) dst[i] = '\0';
    return dst;
}

char *strcat(char *dst, const char *src) {
    char *d = dst + strlen(dst);
    while ((*d++ = *src++));
    return dst;
}

/* ════════════════════════════════════════════════════════════════════
 *  Conversion
 * ════════════════════════════════════════════════════════════════════ */

int atoi(const char *s) {
    return (int)strtol(s, NULL, 10);
}

long long atoll(const char *s) {
    return strtoll(s, NULL, 10);
}

long strtol(const char *s, char **endptr, int base) {
    while (isspace((unsigned char)*s)) s++;
    int neg = 0;
    if (*s == '-') { neg = 1; s++; }
    else if (*s == '+') s++;

    if (base == 0) {
        if (s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) { base = 16; s += 2; }
        else if (s[0] == '0') { base = 8; }
        else base = 10;
    } else if (base == 16 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) {
        s += 2;
    }

    long result = 0;
    while (*s) {
        int digit;
        if (*s >= '0' && *s <= '9') digit = *s - '0';
        else if (*s >= 'a' && *s <= 'z') digit = *s - 'a' + 10;
        else if (*s >= 'A' && *s <= 'Z') digit = *s - 'A' + 10;
        else break;
        if (digit >= base) break;
        result = result * base + digit;
        s++;
    }
    if (endptr) *endptr = (char *)s;
    return neg ? -result : result;
}

long long strtoll(const char *s, char **endptr, int base) {
    while (isspace((unsigned char)*s)) s++;
    int neg = 0;
    if (*s == '-') { neg = 1; s++; }
    else if (*s == '+') s++;

    if (base == 0) {
        if (s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) { base = 16; s += 2; }
        else if (s[0] == '0') { base = 8; }
        else base = 10;
    } else if (base == 16 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) {
        s += 2;
    }

    long long result = 0;
    while (*s) {
        int digit;
        if (*s >= '0' && *s <= '9') digit = *s - '0';
        else if (*s >= 'a' && *s <= 'z') digit = *s - 'a' + 10;
        else if (*s >= 'A' && *s <= 'Z') digit = *s - 'A' + 10;
        else break;
        if (digit >= base) break;
        result = result * base + digit;
        s++;
    }
    if (endptr) *endptr = (char *)s;
    return neg ? -result : result;
}

double strtod(const char *s, char **endptr) {
    while (isspace((unsigned char)*s)) s++;
    int neg = 0;
    if (*s == '-') { neg = 1; s++; }
    else if (*s == '+') s++;

    double result = 0.0;
    while (isdigit((unsigned char)*s)) {
        result = result * 10.0 + (*s - '0');
        s++;
    }
    if (*s == '.') {
        s++;
        double frac = 0.1;
        while (isdigit((unsigned char)*s)) {
            result += (*s - '0') * frac;
            frac *= 0.1;
            s++;
        }
    }
    if (*s == 'e' || *s == 'E') {
        s++;
        int eneg = 0;
        if (*s == '-') { eneg = 1; s++; }
        else if (*s == '+') s++;
        int exp = 0;
        while (isdigit((unsigned char)*s)) {
            exp = exp * 10 + (*s - '0');
            s++;
        }
        double mul = 1.0;
        for (int i = 0; i < exp; i++) mul *= 10.0;
        if (eneg) result /= mul; else result *= mul;
    }
    if (endptr) *endptr = (char *)s;
    return neg ? -result : result;
}

double atof(const char *s) {
    return strtod(s, NULL);
}

/* ════════════════════════════════════════════════════════════════════
 *  Minimal snprintf
 *  Supports: %d %ld %lld %u %lu %llu %zu %x %s %c %g %f %p %%
 *  Flags: 0-pad, width, .precision, .*
 * ════════════════════════════════════════════════════════════════════ */

static int fmt_putc(char *buf, size_t size, size_t pos, char c) {
    if (pos < size) buf[pos] = c;
    return 1;
}

static int fmt_puts(char *buf, size_t size, size_t pos, const char *s, int maxlen) {
    int n = 0;
    for (int i = 0; s[i] && (maxlen < 0 || i < maxlen); i++) {
        if (pos + n < size) buf[pos + n] = s[i];
        n++;
    }
    return n;
}

static int fmt_int(char *buf, size_t size, size_t pos, long long val, int width, int zero_pad) {
    char tmp[24];
    int neg = 0;
    unsigned long long uval;
    if (val < 0) { neg = 1; uval = (unsigned long long)(-val); }
    else uval = (unsigned long long)val;

    int len = 0;
    do {
        tmp[len++] = '0' + (int)(uval % 10);
        uval /= 10;
    } while (uval);

    int total = neg + len;
    int pad = (width > total) ? width - total : 0;
    int n = 0;

    if (neg && zero_pad) { n += fmt_putc(buf, size, pos + n, '-'); }
    char pc = zero_pad ? '0' : ' ';
    for (int i = 0; i < pad; i++) n += fmt_putc(buf, size, pos + n, pc);
    if (neg && !zero_pad) { n += fmt_putc(buf, size, pos + n, '-'); }
    for (int i = len - 1; i >= 0; i--) n += fmt_putc(buf, size, pos + n, tmp[i]);
    return n;
}

static int fmt_uint(char *buf, size_t size, size_t pos, unsigned long long val, int width, int zero_pad) {
    char tmp[24];
    int len = 0;
    do {
        tmp[len++] = '0' + (int)(val % 10);
        val /= 10;
    } while (val);

    int pad = (width > len) ? width - len : 0;
    int n = 0;
    char pc = zero_pad ? '0' : ' ';
    for (int i = 0; i < pad; i++) n += fmt_putc(buf, size, pos + n, pc);
    for (int i = len - 1; i >= 0; i--) n += fmt_putc(buf, size, pos + n, tmp[i]);
    return n;
}

static int fmt_hex(char *buf, size_t size, size_t pos, unsigned long long val, int width, int zero_pad) {
    char tmp[20];
    const char *hex = "0123456789abcdef";
    int len = 0;
    do {
        tmp[len++] = hex[val & 0xf];
        val >>= 4;
    } while (val);

    int pad = (width > len) ? width - len : 0;
    int n = 0;
    char pc = zero_pad ? '0' : ' ';
    for (int i = 0; i < pad; i++) n += fmt_putc(buf, size, pos + n, pc);
    for (int i = len - 1; i >= 0; i--) n += fmt_putc(buf, size, pos + n, tmp[i]);
    return n;
}

static int fmt_double(char *buf, size_t size, size_t pos, double val, int prec, int use_g) {
    int n = 0;
    if (val < 0) { n += fmt_putc(buf, size, pos + n, '-'); val = -val; }

    if (use_g) {
        /* %g: strip trailing zeros */
        if (prec < 0) prec = 6;
        /* check if integer */
        long long ival = (long long)val;
        double diff = val - (double)ival;
        if (diff < 0) diff = -diff;
        if (diff < 1e-9 && val < 1e15) {
            /* print as integer */
            n += fmt_int(buf, size, pos + n, ival < 0 ? -ival : ival, 0, 0);
            return n;
        }
        /* print with enough digits, strip trailing zeros */
        if (prec > 15) prec = 15;
        /* integer part */
        unsigned long long ipart = (unsigned long long)val;
        double fpart = val - (double)ipart;
        n += fmt_uint(buf, size, pos + n, ipart, 0, 0);
        n += fmt_putc(buf, size, pos + n, '.');
        /* fractional digits */
        int start = n;
        for (int i = 0; i < prec; i++) {
            fpart *= 10.0;
            int d = (int)fpart;
            if (d > 9) d = 9;
            n += fmt_putc(buf, size, pos + n, '0' + d);
            fpart -= d;
        }
        /* strip trailing zeros (but keep at least one digit after dot) */
        while (n > start + 1) {
            size_t check = pos + n - 1;
            if (check < size && buf[check] == '0') n--;
            else break;
        }
        return n;
    }

    /* %f */
    if (prec < 0) prec = 6;
    unsigned long long ipart = (unsigned long long)val;
    double fpart = val - (double)ipart;
    n += fmt_uint(buf, size, pos + n, ipart, 0, 0);
    if (prec > 0) {
        n += fmt_putc(buf, size, pos + n, '.');
        for (int i = 0; i < prec; i++) {
            fpart *= 10.0;
            int d = (int)fpart;
            if (d > 9) d = 9;
            n += fmt_putc(buf, size, pos + n, '0' + d);
            fpart -= d;
        }
    }
    return n;
}

int vsnprintf(char *buf, size_t size, const char *fmt, va_list ap) {
    size_t pos = 0;
    while (*fmt) {
        if (*fmt != '%') {
            pos += fmt_putc(buf, size, pos, *fmt++);
            continue;
        }
        fmt++; /* skip % */

        /* flags */
        int zero_pad = 0;
        int left_align = 0;
        while (*fmt == '0' || *fmt == '-') {
            if (*fmt == '0') zero_pad = 1;
            if (*fmt == '-') left_align = 1;
            fmt++;
        }
        (void)left_align; /* not fully implemented */

        /* width */
        int width = 0;
        if (*fmt == '*') {
            width = va_arg(ap, int);
            fmt++;
        } else {
            while (isdigit((unsigned char)*fmt)) {
                width = width * 10 + (*fmt - '0');
                fmt++;
            }
        }

        /* precision */
        int prec = -1;
        if (*fmt == '.') {
            fmt++;
            prec = 0;
            if (*fmt == '*') {
                prec = va_arg(ap, int);
                fmt++;
            } else {
                while (isdigit((unsigned char)*fmt)) {
                    prec = prec * 10 + (*fmt - '0');
                    fmt++;
                }
            }
        }

        /* length modifier */
        int is_long = 0;  /* 1 = l, 2 = ll */
        int is_size = 0;
        if (*fmt == 'l') { is_long = 1; fmt++; if (*fmt == 'l') { is_long = 2; fmt++; } }
        else if (*fmt == 'z') { is_size = 1; fmt++; }
        else if (*fmt == 'h') { fmt++; if (*fmt == 'h') fmt++; } /* ignore h/hh */

        /* conversion */
        switch (*fmt) {
        case 'd': case 'i': {
            long long val;
            if (is_long == 2) val = va_arg(ap, long long);
            else if (is_long == 1) val = va_arg(ap, long);
            else if (is_size) val = (long long)va_arg(ap, size_t);
            else val = va_arg(ap, int);
            pos += fmt_int(buf, size, pos, val, width, zero_pad);
            break;
        }
        case 'u': {
            unsigned long long val;
            if (is_long == 2) val = va_arg(ap, unsigned long long);
            else if (is_long == 1) val = va_arg(ap, unsigned long);
            else if (is_size) val = va_arg(ap, size_t);
            else val = va_arg(ap, unsigned int);
            pos += fmt_uint(buf, size, pos, val, width, zero_pad);
            break;
        }
        case 'x': case 'X': {
            unsigned long long val;
            if (is_long == 2) val = va_arg(ap, unsigned long long);
            else if (is_long == 1) val = va_arg(ap, unsigned long);
            else val = va_arg(ap, unsigned int);
            pos += fmt_hex(buf, size, pos, val, width, zero_pad);
            break;
        }
        case 's': {
            const char *s = va_arg(ap, const char *);
            if (!s) s = "(null)";
            pos += fmt_puts(buf, size, pos, s, prec);
            break;
        }
        case 'c': {
            char c = (char)va_arg(ap, int);
            pos += fmt_putc(buf, size, pos, c);
            break;
        }
        case 'g': case 'G': {
            double val = va_arg(ap, double);
            pos += fmt_double(buf, size, pos, val, prec, 1);
            break;
        }
        case 'f': case 'F': {
            double val = va_arg(ap, double);
            pos += fmt_double(buf, size, pos, val, prec, 0);
            break;
        }
        case 'p': {
            void *p = va_arg(ap, void *);
            pos += fmt_puts(buf, size, pos, "0x", -1);
            pos += fmt_hex(buf, size, pos, (unsigned long long)(uintptr_t)p, 0, 0);
            break;
        }
        case '%':
            pos += fmt_putc(buf, size, pos, '%');
            break;
        default:
            pos += fmt_putc(buf, size, pos, '%');
            pos += fmt_putc(buf, size, pos, *fmt);
            break;
        }
        if (*fmt) fmt++;
    }
    if (size > 0) buf[pos < size ? pos : size - 1] = '\0';
    return (int)pos;
}

int snprintf(char *buf, size_t size, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, size, fmt, ap);
    va_end(ap);
    return n;
}

int sprintf(char *buf, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, (size_t)-1, fmt, ap);
    va_end(ap);
    return n;
}

/* fprintf — route to JS console */
int fprintf(FILE *stream, const char *fmt, ...) {
    (void)stream;
    char tmp[1024];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    js_log(tmp, n < (int)sizeof(tmp) ? n : (int)sizeof(tmp) - 1);
    return n;
}

/* minimal sscanf — only supports the patterns used in the codebase */
int sscanf(const char *str, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int count = 0;

    while (*fmt && *str) {
        if (*fmt == '%') {
            fmt++;
            if (*fmt == 'd') {
                int *p = va_arg(ap, int *);
                char *end;
                long val = strtol(str, &end, 10);
                if (end == str) break;
                *p = (int)val;
                str = end;
                count++;
                fmt++;
            } else if (*fmt == 'l' && *(fmt+1) == 'f') {
                double *p = va_arg(ap, double *);
                char *end;
                *p = strtod(str, &end);
                if (end == str) break;
                str = end;
                count++;
                fmt += 2;
            } else {
                break; /* unsupported */
            }
        } else if (*fmt == *str) {
            fmt++; str++;
        } else {
            break;
        }
    }
    va_end(ap);
    return count;
}

/* ════════════════════════════════════════════════════════════════════
 *  qsort (iterative quicksort)
 * ════════════════════════════════════════════════════════════════════ */

static void swap_bytes(void *a, void *b, size_t size) {
    unsigned char *pa = (unsigned char *)a;
    unsigned char *pb = (unsigned char *)b;
    for (size_t i = 0; i < size; i++) {
        unsigned char t = pa[i]; pa[i] = pb[i]; pb[i] = t;
    }
}

void qsort(void *base, size_t nmemb, size_t size,
           int (*compar)(const void *, const void *)) {
    if (nmemb < 2) return;
    /* simple insertion sort for small arrays */
    if (nmemb <= 16) {
        for (size_t i = 1; i < nmemb; i++) {
            for (size_t j = i; j > 0; j--) {
                void *a = (unsigned char *)base + (j-1) * size;
                void *b = (unsigned char *)base + j * size;
                if (compar(a, b) > 0) swap_bytes(a, b, size);
                else break;
            }
        }
        return;
    }
    /* quicksort with explicit stack */
    struct { size_t lo, hi; } stack[64];
    int sp = 0;
    stack[sp].lo = 0;
    stack[sp].hi = nmemb - 1;
    sp++;

    while (sp > 0) {
        sp--;
        size_t lo = stack[sp].lo;
        size_t hi = stack[sp].hi;
        if (lo >= hi) continue;

        /* median-of-three pivot */
        size_t mid = lo + (hi - lo) / 2;
        unsigned char *base_b = (unsigned char *)base;
        if (compar(base_b + lo * size, base_b + mid * size) > 0)
            swap_bytes(base_b + lo * size, base_b + mid * size, size);
        if (compar(base_b + lo * size, base_b + hi * size) > 0)
            swap_bytes(base_b + lo * size, base_b + hi * size, size);
        if (compar(base_b + mid * size, base_b + hi * size) > 0)
            swap_bytes(base_b + mid * size, base_b + hi * size, size);
        swap_bytes(base_b + mid * size, base_b + (hi - 1) * size, size);
        void *pivot = base_b + (hi - 1) * size;

        size_t i = lo;
        size_t j = hi - 1;
        for (;;) {
            while (compar(base_b + (++i) * size, pivot) < 0);
            while (j > lo && compar(base_b + (--j) * size, pivot) > 0);
            if (i >= j) break;
            swap_bytes(base_b + i * size, base_b + j * size, size);
        }
        swap_bytes(base_b + i * size, base_b + (hi - 1) * size, size);

        if (sp < 62) {
            stack[sp].lo = lo; stack[sp].hi = (i > 0) ? i - 1 : 0; sp++;
            stack[sp].lo = i + 1; stack[sp].hi = hi; sp++;
        }
    }
}

/* ════════════════════════════════════════════════════════════════════
 *  Math (wrappers around JS imports)
 * ════════════════════════════════════════════════════════════════════ */

double pow(double b, double e)   { return js_pow(b, e); }
double sqrt(double x)            { return js_sqrt(x); }
double floor(double x)           { return js_floor(x); }
double ceil(double x)            { return js_ceil(x); }
double round(double x)           { return js_round(x); }
double fmod(double x, double y)  { return js_fmod(x, y); }
double fabs(double x)            { return js_fabs(x); }
double log(double x)             { return js_log_math(x); }
double exp(double x)             { return js_exp(x); }
double sin(double x)             { return js_sin(x); }
double cos(double x)             { return js_cos(x); }
float  fabsf(float x)            { return (float)js_fabs((double)x); }

/* ════════════════════════════════════════════════════════════════════
 *  Time (wrappers around JS imports)
 * ════════════════════════════════════════════════════════════════════ */

static struct tm _tm_buf;

time_t time(time_t *t) {
    double now = js_time();
    time_t val = (time_t)now;
    if (t) *t = val;
    return val;
}

struct tm *localtime(const time_t *timep) {
    int fields[7];
    js_localtime((double)*timep, fields);
    _tm_buf.tm_sec  = fields[0];
    _tm_buf.tm_min  = fields[1];
    _tm_buf.tm_hour = fields[2];
    _tm_buf.tm_mday = fields[3];
    _tm_buf.tm_mon  = fields[4];
    _tm_buf.tm_year = fields[5];
    _tm_buf.tm_wday = fields[6];
    _tm_buf.tm_yday = 0;
    _tm_buf.tm_isdst = 0;
    return &_tm_buf;
}

time_t mktime(struct tm *tm) {
    double epoch = js_mktime(
        tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
        tm->tm_hour, tm->tm_min, tm->tm_sec
    );
    time_t t = (time_t)epoch;
    /* update the struct tm fields from the result */
    struct tm *updated = localtime(&t);
    *tm = *updated;
    return t;
}

double difftime(time_t t1, time_t t0) {
    return (double)(t1 - t0);
}

/* ════════════════════════════════════════════════════════════════════
 *  Random
 * ════════════════════════════════════════════════════════════════════ */

static unsigned int _rand_state = 1;

void srand(unsigned int seed) { _rand_state = seed; }

int rand(void) {
    _rand_state = _rand_state * 1103515245 + 12345;
    return (int)((_rand_state >> 16) & RAND_MAX);
}

/* ════════════════════════════════════════════════════════════════════
 *  Misc
 * ════════════════════════════════════════════════════════════════════ */

FILE *stderr = (FILE *)0;

_Noreturn void abort(void) {
    js_abort();
    __builtin_unreachable();
}

char *getenv(const char *name) {
    (void)name;
    return NULL;
}

#endif /* MSKQL_WASM */
