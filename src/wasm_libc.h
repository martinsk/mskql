/*
 * wasm_libc.h — freestanding libc for wasm32 build of mskql.
 * Provides type definitions and declarations for all standard library
 * functions used by the core source files. Implementations are in
 * wasm_libc.c; math and time functions are imported from JavaScript.
 *
 * Usage: clang --target=wasm32 -ffreestanding -nostdlib -include wasm_libc.h ...
 * This header is force-included BEFORE any source file, so it must
 * guard against the real system headers being pulled in.
 */
#ifndef WASM_LIBC_H
#define WASM_LIBC_H

/* With -nostdinc, no system headers are available.
 * This file provides everything the source files need. */

/* ── basic types ────────────────────────────────────────────────── */
typedef unsigned int       size_t;
typedef int                ssize_t;
typedef int                ptrdiff_t;
typedef long long          intmax_t;
typedef unsigned long long uintmax_t;

typedef signed char        int8_t;
typedef unsigned char      uint8_t;
typedef short              int16_t;
typedef unsigned short     uint16_t;
typedef int                int32_t;
typedef unsigned int       uint32_t;
typedef long long          int64_t;
typedef unsigned long long uint64_t;

typedef int                intptr_t;   /* wasm32: pointers are 32-bit */
typedef unsigned int       uintptr_t;  /* wasm32: pointers are 32-bit */

#define NULL ((void *)0)

#define INT32_MAX  0x7fffffff
#define INT32_MIN  (-INT32_MAX - 1)
#define UINT32_MAX 0xffffffffU
#define INT64_MAX  0x7fffffffffffffffLL
#define INT64_MIN  (-INT64_MAX - 1LL)
#define UINT64_MAX 0xffffffffffffffffULL
#define SIZE_MAX   UINT32_MAX

#define INT_MAX    INT32_MAX
#define INT_MIN    INT32_MIN
#define LONG_MAX   INT32_MAX
#define LONG_MIN   INT32_MIN
#define LLONG_MAX  INT64_MAX
#define LLONG_MIN  INT64_MIN

/* ── stdbool ────────────────────────────────────────────────────── */
#ifndef __cplusplus
typedef _Bool bool;
#define true  1
#define false 0
#endif

/* ── stdarg (compiler built-in) ─────────────────────────────────── */
typedef __builtin_va_list va_list;
#define va_start(ap, param) __builtin_va_start(ap, param)
#define va_end(ap)          __builtin_va_end(ap)
#define va_arg(ap, type)    __builtin_va_arg(ap, type)
#define va_copy(dst, src)   __builtin_va_copy(dst, src)

/* ── offsetof ───────────────────────────────────────────────────── */
#define offsetof(type, member) __builtin_offsetof(type, member)

/* ── FILE (opaque, only used for stderr) ────────────────────────── */
typedef struct _FILE FILE;
extern FILE *stderr;

/* ── memory ─────────────────────────────────────────────────────── */
void *malloc(size_t size);
void  free(void *ptr);
void *calloc(size_t nmemb, size_t size);
void *realloc(void *ptr, size_t size);

/* ── string / memory ops ────────────────────────────────────────── */
void  *memcpy(void *dst, const void *src, size_t n);
void  *memmove(void *dst, const void *src, size_t n);
void  *memset(void *s, int c, size_t n);
int    memcmp(const void *s1, const void *s2, size_t n);
size_t strlen(const char *s);
int    strcmp(const char *s1, const char *s2);
int    strncmp(const char *s1, const char *s2, size_t n);
int    strcasecmp(const char *s1, const char *s2);
int    strncasecmp(const char *s1, const char *s2, size_t n);
char  *strchr(const char *s, int c);
char  *strrchr(const char *s, int c);
char  *strstr(const char *haystack, const char *needle);
char  *strdup(const char *s);
char  *strndup(const char *s, size_t n);
char  *strcpy(char *dst, const char *src);
char  *strncpy(char *dst, const char *src, size_t n);
char  *strcat(char *dst, const char *src);

/* ── formatting ─────────────────────────────────────────────────── */
int snprintf(char *buf, size_t size, const char *fmt, ...);
int sprintf(char *buf, const char *fmt, ...);
int vsnprintf(char *buf, size_t size, const char *fmt, va_list ap);
int fprintf(FILE *stream, const char *fmt, ...);
int sscanf(const char *str, const char *fmt, ...);

/* ── ctype ──────────────────────────────────────────────────────── */
static inline int isdigit(int c)  { return c >= '0' && c <= '9'; }
static inline int isalpha(int c)  { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'); }
static inline int isalnum(int c)  { return isalpha(c) || isdigit(c); }
static inline int isspace(int c)  { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'; }
static inline int isupper(int c)  { return c >= 'A' && c <= 'Z'; }
static inline int islower(int c)  { return c >= 'a' && c <= 'z'; }
static inline int toupper(int c)  { return islower(c) ? c - 32 : c; }
static inline int tolower(int c)  { return isupper(c) ? c + 32 : c; }
static inline int isxdigit(int c) { return isdigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'); }

/* ── conversion ─────────────────────────────────────────────────── */
int       atoi(const char *s);
long long atoll(const char *s);
double    atof(const char *s);
long      strtol(const char *s, char **endptr, int base);
long long strtoll(const char *s, char **endptr, int base);
double    strtod(const char *s, char **endptr);

/* ── sorting ────────────────────────────────────────────────────── */
void qsort(void *base, size_t nmemb, size_t size,
           int (*compar)(const void *, const void *));

/* ── math (imported from JS) ────────────────────────────────────── */
double pow(double base, double exp);
double sqrt(double x);
double floor(double x);
double ceil(double x);
double round(double x);
double fmod(double x, double y);
double fabs(double x);
double log(double x);
double exp(double x);
double sin(double x);
double cos(double x);
float  fabsf(float x);

/* ── time ───────────────────────────────────────────────────────── */
typedef long long time_t;

struct tm {
    int tm_sec;
    int tm_min;
    int tm_hour;
    int tm_mday;
    int tm_mon;
    int tm_year;
    int tm_wday;
    int tm_yday;
    int tm_isdst;
};

time_t     time(time_t *t);
struct tm *localtime(const time_t *timep);
time_t     mktime(struct tm *tm);
double     difftime(time_t t1, time_t t0);

/* ── random ─────────────────────────────────────────────────────── */
#define RAND_MAX 0x7fffffff
int  rand(void);
void srand(unsigned int seed);

/* ── misc ───────────────────────────────────────────────────────── */
_Noreturn void abort(void);
char *getenv(const char *name);

/* ── signal stubs (for volatile sig_atomic_t in headers) ────────── */
typedef int sig_atomic_t;

#endif /* WASM_LIBC_H */
