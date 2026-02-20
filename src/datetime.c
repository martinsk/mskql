#include "datetime.h"
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <ctype.h>

/* ---- internal helpers ---- */

static int is_leap_year(int y)
{
    return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
}

static int days_in_month(int y, int m)
{
    static const int dim[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    if (m < 1 || m > 12) return 30;
    if (m == 2 && is_leap_year(y)) return 29;
    return dim[m - 1];
}

/* days since PG epoch (2000-01-01) → year/month/day */
void days_to_ymd(int32_t days, int *y, int *m, int *d)
{
    /* shift to a March-based calendar for easier leap year handling */
    int32_t j = days + 730120; /* days from year 0 (approx) to PG epoch + offset */
    /* use the algorithm from PostgreSQL's j2date */
    /* Julian day number from PG epoch days:
     * PG epoch 2000-01-01 = Julian day 2451545
     * We convert PG days → Julian day → Gregorian date */
    int32_t jd = days + 2451545; /* Julian day number */
    int32_t l, n, i, jj, dd, mm, yy;

    (void)j; /* suppress unused warning */

    l = jd + 68569;
    n = 4 * l / 146097;
    l = l - (146097 * n + 3) / 4;
    i = 4000 * (l + 1) / 1461001;
    l = l - 1461 * i / 4 + 31;
    jj = 80 * l / 2447;
    dd = l - 2447 * jj / 80;
    l = jj / 11;
    mm = jj + 2 - 12 * l;
    yy = 100 * (n - 49) + i + l;

    *y = (int)yy;
    *m = (int)mm;
    *d = (int)dd;
}

/* year/month/day → days since PG epoch (2000-01-01) */
int32_t ymd_to_days(int y, int m, int d)
{
    /* convert Gregorian date to Julian day number, then subtract PG epoch JD */
    int a = (14 - m) / 12;
    int yy = y + 4800 - a;
    int mm = m + 12 * a - 3;
    int32_t jd = d + (153 * mm + 2) / 5 + 365 * yy + yy / 4 - yy / 100 + yy / 400 - 32045;
    return jd - 2451545; /* subtract PG epoch Julian day */
}

/* ---- parse helpers ---- */

/* parse exactly n digits from *p, advance *p. returns -1 on failure. */
static int parse_digits(const char **p, int n)
{
    int val = 0;
    for (int i = 0; i < n; i++) {
        if (!isdigit((unsigned char)(*p)[i])) return -1;
        val = val * 10 + ((*p)[i] - '0');
    }
    *p += n;
    return val;
}

/* ---- parse functions ---- */

int32_t date_from_str(const char *s)
{
    if (!s) return DATE_INVALID;
    while (isspace((unsigned char)*s)) s++;

    const char *p = s;
    int neg = 0;
    if (*p == '-') { neg = 1; p++; }

    /* parse YYYY-MM-DD */
    int y = 0;
    while (isdigit((unsigned char)*p)) {
        y = y * 10 + (*p - '0');
        p++;
    }
    if (*p != '-') return DATE_INVALID;
    p++;
    int m = parse_digits(&p, 2);
    if (m < 0 || *p != '-') return DATE_INVALID;
    p++;
    int d = parse_digits(&p, 2);
    if (d < 0) return DATE_INVALID;

    if (neg) y = -y;
    if (m < 1 || m > 12 || d < 1 || d > 31) return DATE_INVALID;

    return ymd_to_days(y, m, d);
}

int64_t timestamp_from_str(const char *s)
{
    if (!s) return TIMESTAMP_INVALID;
    while (isspace((unsigned char)*s)) s++;

    /* parse date part */
    int32_t d = date_from_str(s);
    if (d == DATE_INVALID) return TIMESTAMP_INVALID;

    /* skip past date part (YYYY-MM-DD) */
    const char *p = s;
    if (*p == '-') p++; /* negative year */
    while (isdigit((unsigned char)*p)) p++;
    if (*p == '-') p++;
    p += 2; /* MM */
    if (*p == '-') p++;
    p += 2; /* DD */

    int hh = 0, mm = 0, ss = 0;
    if (*p == ' ' || *p == 'T' || *p == 't') {
        p++;
        int h = parse_digits(&p, 2);
        if (h < 0) return (int64_t)d * USEC_PER_DAY;
        hh = h;
        if (*p == ':') {
            p++;
            int mi = parse_digits(&p, 2);
            if (mi >= 0) mm = mi;
            if (*p == ':') {
                p++;
                int sc = parse_digits(&p, 2);
                if (sc >= 0) ss = sc;
            }
        }
    }

    return (int64_t)d * USEC_PER_DAY +
           (int64_t)hh * USEC_PER_HOUR +
           (int64_t)mm * USEC_PER_MIN +
           (int64_t)ss * USEC_PER_SEC;
}

int64_t time_from_str(const char *s)
{
    if (!s) return TIME_INVALID;
    while (isspace((unsigned char)*s)) s++;

    const char *p = s;
    int hh = parse_digits(&p, 2);
    if (hh < 0 || *p != ':') return TIME_INVALID;
    p++;
    int mm = parse_digits(&p, 2);
    if (mm < 0) return TIME_INVALID;
    int ss = 0;
    if (*p == ':') {
        p++;
        ss = parse_digits(&p, 2);
        if (ss < 0) ss = 0;
    }

    return (int64_t)hh * USEC_PER_HOUR +
           (int64_t)mm * USEC_PER_MIN +
           (int64_t)ss * USEC_PER_SEC;
}

struct interval interval_from_str(const char *s)
{
    struct interval iv = {0, 0, 0};
    if (!s) return iv;

    const char *p = s;
    while (*p) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;

        /* try HH:MM:SS pattern */
        {
            const char *save = p;
            int neg = 0;
            if (*p == '-') { neg = 1; p++; }
            if (isdigit((unsigned char)p[0]) && isdigit((unsigned char)p[1]) && p[2] == ':') {
                int hh = (p[0] - '0') * 10 + (p[1] - '0');
                p += 3;
                int mm = 0, ss = 0;
                if (isdigit((unsigned char)p[0]) && isdigit((unsigned char)p[1])) {
                    mm = (p[0] - '0') * 10 + (p[1] - '0');
                    p += 2;
                    if (*p == ':') {
                        p++;
                        if (isdigit((unsigned char)p[0]) && isdigit((unsigned char)p[1])) {
                            ss = (p[0] - '0') * 10 + (p[1] - '0');
                            p += 2;
                        }
                    }
                }
                int64_t t = (int64_t)hh * USEC_PER_HOUR +
                            (int64_t)mm * USEC_PER_MIN +
                            (int64_t)ss * USEC_PER_SEC;
                iv.usec += neg ? -t : t;
                continue;
            }
            p = save;
        }

        /* parse number + unit */
        int neg = 0;
        if (*p == '-') { neg = 1; p++; }
        if (!isdigit((unsigned char)*p) && *p != '.') { p++; continue; }

        double val = 0;
        while (isdigit((unsigned char)*p)) {
            val = val * 10 + (*p - '0');
            p++;
        }
        if (*p == '.') {
            p++;
            double frac = 0.1;
            while (isdigit((unsigned char)*p)) {
                val += (*p - '0') * frac;
                frac *= 0.1;
                p++;
            }
        }
        if (neg) val = -val;

        while (*p && isspace((unsigned char)*p)) p++;

        if (strncasecmp(p, "year", 4) == 0) {
            iv.months += (int32_t)(val * 12);
            p += 4; if (*p == 's') p++;
        } else if (strncasecmp(p, "mon", 3) == 0) {
            iv.months += (int32_t)val;
            while (*p && isalpha((unsigned char)*p)) p++;
        } else if (strncasecmp(p, "day", 3) == 0) {
            iv.days += (int32_t)val;
            p += 3; if (*p == 's') p++;
        } else if (strncasecmp(p, "hour", 4) == 0) {
            iv.usec += (int64_t)(val * USEC_PER_HOUR);
            p += 4; if (*p == 's') p++;
        } else if (strncasecmp(p, "minute", 6) == 0) {
            iv.usec += (int64_t)(val * USEC_PER_MIN);
            p += 6; if (*p == 's') p++;
        } else if (strncasecmp(p, "min", 3) == 0) {
            iv.usec += (int64_t)(val * USEC_PER_MIN);
            p += 3; if (*p == 's') p++;
        } else if (strncasecmp(p, "second", 6) == 0) {
            iv.usec += (int64_t)(val * USEC_PER_SEC);
            p += 6; if (*p == 's') p++;
        } else if (strncasecmp(p, "sec", 3) == 0) {
            iv.usec += (int64_t)(val * USEC_PER_SEC);
            p += 3; if (*p == 's') p++;
        } else {
            /* bare number — treat as seconds */
            iv.usec += (int64_t)(val * USEC_PER_SEC);
        }
    }
    return iv;
}

/* ---- format functions ---- */

void date_to_str(int32_t days, char *buf, size_t len)
{
    int y, m, d;
    days_to_ymd(days, &y, &m, &d);
    if (y < 0)
        snprintf(buf, len, "-%04d-%02d-%02d", -y, m, d);
    else
        snprintf(buf, len, "%04d-%02d-%02d", y, m, d);
}

void timestamp_to_str(int64_t usec, char *buf, size_t len)
{
    int32_t days;
    int64_t time_usec;

    if (usec >= 0) {
        days = (int32_t)(usec / USEC_PER_DAY);
        time_usec = usec % USEC_PER_DAY;
    } else {
        /* for negative timestamps, floor-divide */
        days = (int32_t)((usec - USEC_PER_DAY + 1) / USEC_PER_DAY);
        time_usec = usec - (int64_t)days * USEC_PER_DAY;
    }

    int y, mo, d;
    days_to_ymd(days, &y, &mo, &d);

    int hh = (int)(time_usec / USEC_PER_HOUR);
    time_usec %= USEC_PER_HOUR;
    int mm = (int)(time_usec / USEC_PER_MIN);
    time_usec %= USEC_PER_MIN;
    int ss = (int)(time_usec / USEC_PER_SEC);

    if (y < 0)
        snprintf(buf, len, "-%04d-%02d-%02d %02d:%02d:%02d", -y, mo, d, hh, mm, ss);
    else
        snprintf(buf, len, "%04d-%02d-%02d %02d:%02d:%02d", y, mo, d, hh, mm, ss);
}

void timestamptz_to_str(int64_t usec, char *buf, size_t len)
{
    /* Same as timestamp_to_str but appends +00 */
    int32_t days;
    int64_t time_usec;
    if (usec >= 0) {
        days = (int32_t)(usec / USEC_PER_DAY);
        time_usec = usec % USEC_PER_DAY;
    } else {
        days = (int32_t)((usec - USEC_PER_DAY + 1) / USEC_PER_DAY);
        time_usec = usec - (int64_t)days * USEC_PER_DAY;
    }

    int y, mo, d;
    days_to_ymd(days, &y, &mo, &d);

    int hh = (int)(time_usec / USEC_PER_HOUR);
    time_usec %= USEC_PER_HOUR;
    int mm = (int)(time_usec / USEC_PER_MIN);
    time_usec %= USEC_PER_MIN;
    int ss = (int)(time_usec / USEC_PER_SEC);

    if (y < 0)
        snprintf(buf, len, "-%04d-%02d-%02d %02d:%02d:%02d+00", -y, mo, d, hh, mm, ss);
    else
        snprintf(buf, len, "%04d-%02d-%02d %02d:%02d:%02d+00", y, mo, d, hh, mm, ss);
}

void time_to_str(int64_t usec, char *buf, size_t len)
{
    if (usec < 0) usec = 0;
    int hh = (int)(usec / USEC_PER_HOUR);
    usec %= USEC_PER_HOUR;
    int mm = (int)(usec / USEC_PER_MIN);
    usec %= USEC_PER_MIN;
    int ss = (int)(usec / USEC_PER_SEC);
    snprintf(buf, len, "%02d:%02d:%02d", hh, mm, ss);
}

void interval_to_str(struct interval iv, char *buf, size_t len)
{
    char *p = buf;
    size_t left = len;
    int wrote = 0;

    int neg_time = 0;
    int32_t months = iv.months;
    int32_t days = iv.days;
    int64_t usec = iv.usec;

    /* Normalize: borrow between days and usec when signs differ.
     * E.g. 2 days -12h → 1 day 12h;  -2 days 12h → -1 day -12h */
    if (days > 0 && usec < 0) {
        days--;
        usec += USEC_PER_DAY;
    } else if (days < 0 && usec > 0) {
        days++;
        usec -= USEC_PER_DAY;
    }

    /* handle negative usec */
    if (usec < 0) {
        neg_time = 1;
        usec = -usec;
    }

    int years = months / 12;
    months = months % 12;
    /* handle negative months */
    if (months < 0 && years > 0) { years--; months += 12; }
    if (months > 0 && years < 0) { years++; months -= 12; }

    if (years != 0) {
        int n = snprintf(p, left, "%d year%s ", years, (years == 1 || years == -1) ? "" : "s");
        p += n; left -= (size_t)n; wrote = 1;
    }
    if (months != 0) {
        int n = snprintf(p, left, "%d mon%s ", months, (months == 1 || months == -1) ? "" : "s");
        p += n; left -= (size_t)n; wrote = 1;
    }
    if (days != 0) {
        int n = snprintf(p, left, "%d day%s ", days, (days == 1 || days == -1) ? "" : "s");
        p += n; left -= (size_t)n; wrote = 1;
    }

    int64_t total_sec = usec / USEC_PER_SEC;
    int hh = (int)(total_sec / 3600);
    int mm = (int)((total_sec % 3600) / 60);
    int ss = (int)(total_sec % 60);

    if (hh != 0 || mm != 0 || ss != 0) {
        /* PostgreSQL-style: use HH:MM:SS only when multiple time components;
         * otherwise use human-readable units */
        int nparts = (hh != 0) + (mm != 0) + (ss != 0);
        if (nparts == 1 && !wrote) {
            /* single time component, no date parts — use human-readable */
            if (hh != 0) {
                int v = neg_time ? -hh : hh;
                snprintf(p, left, "%d hour%s", v, (v == 1 || v == -1) ? "" : "s");
            } else if (mm != 0) {
                int v = neg_time ? -mm : mm;
                snprintf(p, left, "%d minute%s", v, (v == 1 || v == -1) ? "" : "s");
            } else {
                int v = neg_time ? -ss : ss;
                snprintf(p, left, "%d sec%s", v, (v == 1 || v == -1) ? "" : "s");
            }
        } else {
            if (neg_time)
                snprintf(p, left, "-%02d:%02d:%02d", hh, mm, ss);
            else
                snprintf(p, left, "%02d:%02d:%02d", hh, mm, ss);
        }
    } else if (!wrote) {
        snprintf(p, left, "00:00:00");
    } else {
        /* trim trailing space */
        if (p > buf && p[-1] == ' ') p[-1] = '\0';
    }
}

/* ---- extract functions ---- */

double date_extract(int32_t days, const char *field)
{
    int y, m, d;
    days_to_ymd(days, &y, &m, &d);

    if (strcasecmp(field, "year") == 0) return (double)y;
    if (strcasecmp(field, "month") == 0) return (double)m;
    if (strcasecmp(field, "day") == 0) return (double)d;
    if (strcasecmp(field, "quarter") == 0) return (double)((m - 1) / 3 + 1);
    if (strcasecmp(field, "epoch") == 0) return (double)days * 86400.0 + (double)PG_EPOCH_UNIX;

    if (strcasecmp(field, "dow") == 0) {
        /* PG epoch 2000-01-01 is a Saturday (dow=6) */
        int dow = ((int)(days % 7) + 6) % 7;
        return (double)dow;
    }
    if (strcasecmp(field, "doy") == 0) {
        int32_t jan1 = ymd_to_days(y, 1, 1);
        return (double)(days - jan1 + 1);
    }
    if (strcasecmp(field, "week") == 0) {
        int32_t jan1 = ymd_to_days(y, 1, 1);
        return (double)((days - jan1) / 7 + 1);
    }

    return 0.0;
}

double timestamp_extract(int64_t usec, const char *field)
{
    int32_t days;
    int64_t time_usec;

    if (usec >= 0) {
        days = (int32_t)(usec / USEC_PER_DAY);
        time_usec = usec % USEC_PER_DAY;
    } else {
        days = (int32_t)((usec - USEC_PER_DAY + 1) / USEC_PER_DAY);
        time_usec = usec - (int64_t)days * USEC_PER_DAY;
    }

    if (strcasecmp(field, "hour") == 0)
        return (double)(time_usec / USEC_PER_HOUR);
    if (strcasecmp(field, "minute") == 0)
        return (double)((time_usec % USEC_PER_HOUR) / USEC_PER_MIN);
    if (strcasecmp(field, "second") == 0)
        return (double)((time_usec % USEC_PER_MIN) / USEC_PER_SEC);
    if (strcasecmp(field, "epoch") == 0)
        return (double)usec / (double)USEC_PER_SEC + (double)PG_EPOCH_UNIX;

    /* delegate date-level fields */
    return date_extract(days, field);
}

/* ---- trunc functions ---- */

int32_t date_trunc_days(int32_t days, const char *field)
{
    int y, m, d;
    days_to_ymd(days, &y, &m, &d);

    if (strcasecmp(field, "year") == 0) return ymd_to_days(y, 1, 1);
    if (strcasecmp(field, "quarter") == 0) {
        int qm = ((m - 1) / 3) * 3 + 1;
        return ymd_to_days(y, qm, 1);
    }
    if (strcasecmp(field, "month") == 0) return ymd_to_days(y, m, 1);
    if (strcasecmp(field, "week") == 0) {
        /* truncate to Monday */
        int dow = ((int)(days % 7) + 6) % 7; /* 0=Sun..6=Sat */
        int mon_offset = (dow == 0) ? 6 : dow - 1; /* days since Monday */
        return days - mon_offset;
    }
    /* day — no-op for date */
    return days;
}

int64_t timestamp_trunc_usec(int64_t usec, const char *field)
{
    if (strcasecmp(field, "hour") == 0) {
        if (usec >= 0) return usec - usec % USEC_PER_HOUR;
        return usec - ((usec % USEC_PER_HOUR) + USEC_PER_HOUR) % USEC_PER_HOUR;
    }
    if (strcasecmp(field, "minute") == 0) {
        if (usec >= 0) return usec - usec % USEC_PER_MIN;
        return usec - ((usec % USEC_PER_MIN) + USEC_PER_MIN) % USEC_PER_MIN;
    }
    if (strcasecmp(field, "second") == 0) {
        if (usec >= 0) return usec - usec % USEC_PER_SEC;
        return usec - ((usec % USEC_PER_SEC) + USEC_PER_SEC) % USEC_PER_SEC;
    }
    if (strcasecmp(field, "day") == 0) {
        if (usec >= 0) return usec - usec % USEC_PER_DAY;
        return usec - ((usec % USEC_PER_DAY) + USEC_PER_DAY) % USEC_PER_DAY;
    }

    /* date-level truncation: extract days, trunc, convert back */
    int32_t days;
    if (usec >= 0)
        days = (int32_t)(usec / USEC_PER_DAY);
    else
        days = (int32_t)((usec - USEC_PER_DAY + 1) / USEC_PER_DAY);

    int32_t td = date_trunc_days(days, field);
    return (int64_t)td * USEC_PER_DAY;
}

/* ---- arithmetic ---- */

/* calendar-aware: add months, then clamp day to end-of-month */
static int32_t add_months_to_date(int y, int m, int d, int32_t add_months)
{
    int total_months = y * 12 + (m - 1) + add_months;
    int ny = total_months / 12;
    int nm = total_months % 12 + 1;
    if (nm <= 0) { nm += 12; ny--; }
    int max_d = days_in_month(ny, nm);
    if (d > max_d) d = max_d;
    return ymd_to_days(ny, nm, d);
}

int32_t date_add_interval(int32_t days, struct interval iv)
{
    int y, m, d;
    days_to_ymd(days, &y, &m, &d);

    /* add months (calendar-aware) */
    if (iv.months != 0) {
        days = add_months_to_date(y, m, d, iv.months);
    }

    /* add days */
    days += iv.days;

    /* add sub-day component (rounded to whole days for DATE) */
    if (iv.usec != 0) {
        int64_t extra_days = iv.usec / USEC_PER_DAY;
        days += (int32_t)extra_days;
    }

    return days;
}

int64_t timestamp_add_interval(int64_t usec, struct interval iv)
{
    if (iv.months != 0) {
        /* extract date part, add months calendar-aware, reassemble */
        int32_t days;
        int64_t time_part;
        if (usec >= 0) {
            days = (int32_t)(usec / USEC_PER_DAY);
            time_part = usec % USEC_PER_DAY;
        } else {
            days = (int32_t)((usec - USEC_PER_DAY + 1) / USEC_PER_DAY);
            time_part = usec - (int64_t)days * USEC_PER_DAY;
        }

        int y, m, d;
        days_to_ymd(days, &y, &m, &d);
        days = add_months_to_date(y, m, d, iv.months);
        usec = (int64_t)days * USEC_PER_DAY + time_part;
    }

    /* add days and sub-day */
    usec += (int64_t)iv.days * USEC_PER_DAY;
    usec += iv.usec;

    return usec;
}
