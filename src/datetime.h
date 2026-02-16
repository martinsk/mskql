#ifndef DATETIME_H
#define DATETIME_H

#include <stdint.h>
#include <stddef.h>

/* ---- interval struct ---- */

struct interval {
    int32_t months;  /* years * 12 + months */
    int32_t days;
    int64_t usec;    /* hours/minutes/seconds as microseconds */
};

/* Microseconds per second / minute / hour / day */
#define USEC_PER_SEC   1000000LL
#define USEC_PER_MIN   (60LL * USEC_PER_SEC)
#define USEC_PER_HOUR  (3600LL * USEC_PER_SEC)
#define USEC_PER_DAY   (86400LL * USEC_PER_SEC)

/* Sentinel values for invalid parse results */
#define DATE_INVALID    INT32_MIN
#define TIMESTAMP_INVALID INT64_MIN
#define TIME_INVALID    INT64_MIN

/* PG epoch: 2000-01-01 as Unix timestamp (seconds since 1970-01-01) */
#define PG_EPOCH_UNIX   946684800LL

/* ---- parse functions (return sentinel on invalid input) ---- */

int32_t date_from_str(const char *s);
int64_t timestamp_from_str(const char *s);
int64_t time_from_str(const char *s);
struct interval interval_from_str(const char *s);

/* ---- format functions (write into caller-provided buf, zero malloc) ---- */

void date_to_str(int32_t days, char *buf, size_t len);
void timestamp_to_str(int64_t usec, char *buf, size_t len);
void timestamptz_to_str(int64_t usec, char *buf, size_t len);
void time_to_str(int64_t usec, char *buf, size_t len);
void interval_to_str(struct interval iv, char *buf, size_t len);

/* ---- extract / trunc (pure integer math, WASM-safe) ---- */

double date_extract(int32_t days, const char *field);
double timestamp_extract(int64_t usec, const char *field);
int32_t date_trunc_days(int32_t days, const char *field);
int64_t timestamp_trunc_usec(int64_t usec, const char *field);

/* ---- arithmetic ---- */

int32_t date_add_interval(int32_t days, struct interval iv);
int64_t timestamp_add_interval(int64_t usec, struct interval iv);

/* ---- conversion helpers ---- */

void days_to_ymd(int32_t days, int *y, int *m, int *d);
int32_t ymd_to_days(int y, int m, int d);

/* ---- inline helpers ---- */

static inline int interval_is_zero(struct interval iv)
{
    return iv.months == 0 && iv.days == 0 && iv.usec == 0;
}

/* normalize interval for comparison (approximate: 1 month = 30 days) */
static inline int64_t interval_to_usec_approx(struct interval iv)
{
    return (int64_t)iv.months * 30LL * USEC_PER_DAY +
           (int64_t)iv.days * USEC_PER_DAY +
           iv.usec;
}

static inline int interval_compare(struct interval a, struct interval b)
{
    int64_t au = interval_to_usec_approx(a);
    int64_t bu = interval_to_usec_approx(b);
    if (au < bu) return -1;
    if (au > bu) return  1;
    return 0;
}

static inline struct interval interval_add(struct interval a, struct interval b)
{
    struct interval r;
    r.months = a.months + b.months;
    r.days   = a.days + b.days;
    r.usec   = a.usec + b.usec;
    return r;
}

static inline struct interval interval_sub(struct interval a, struct interval b)
{
    struct interval r;
    r.months = a.months - b.months;
    r.days   = a.days - b.days;
    r.usec   = a.usec - b.usec;
    return r;
}

static inline struct interval interval_negate(struct interval iv)
{
    struct interval r;
    r.months = -iv.months;
    r.days   = -iv.days;
    r.usec   = -iv.usec;
    return r;
}

#endif
