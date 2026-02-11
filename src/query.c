#include "query.h"
#include "database.h"
#include "parser.h"
#include "plan.h"
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>

/* ---- date/time helpers ---- */

/* parse "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS" into struct tm, return 1 on success */
static int parse_datetime(const char *s, struct tm *out)
{
    memset(out, 0, sizeof(*out));
    if (!s) return 0;
    int y, mo, d, h = 0, mi = 0, sec = 0;
    int n = sscanf(s, "%d-%d-%d %d:%d:%d", &y, &mo, &d, &h, &mi, &sec);
    if (n < 3) return 0;
    out->tm_year = y - 1900;
    out->tm_mon = mo - 1;
    out->tm_mday = d;
    out->tm_hour = h;
    out->tm_min = mi;
    out->tm_sec = sec;
    out->tm_isdst = -1;
    return 1;
}

/* format struct tm as "YYYY-MM-DD HH:MM:SS" into buf (at least 20 bytes) */
static void format_timestamp(const struct tm *t, char *buf, size_t bufsz)
{
    snprintf(buf, bufsz, "%04d-%02d-%02d %02d:%02d:%02d",
             t->tm_year + 1900, t->tm_mon + 1, t->tm_mday,
             t->tm_hour, t->tm_min, t->tm_sec);
}

/* format struct tm as "YYYY-MM-DD" into buf (at least 11 bytes) */
static void format_date(const struct tm *t, char *buf, size_t bufsz)
{
    snprintf(buf, bufsz, "%04d-%02d-%02d",
             t->tm_year + 1900, t->tm_mon + 1, t->tm_mday);
}

/* parse an interval string like "1 year 2 months 3 days 04:05:06"
 * returns total seconds (approximate: 1 year=365.25 days, 1 month=30 days) */
static double parse_interval_to_seconds(const char *s)
{
    if (!s) return 0.0;
    double total = 0.0;
    const char *p = s;
    while (*p) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;
        /* try HH:MM:SS pattern */
        int hh, mm, ss;
        if (sscanf(p, "%d:%d:%d", &hh, &mm, &ss) == 3) {
            total += hh * 3600.0 + mm * 60.0 + ss;
            while (*p && !isspace((unsigned char)*p)) p++;
            continue;
        }
        /* try number + unit */
        char *end;
        double val = strtod(p, &end);
        if (end == p) { p++; continue; } /* skip non-numeric */
        p = end;
        while (*p && isspace((unsigned char)*p)) p++;
        if (strncasecmp(p, "year", 4) == 0) {
            total += val * 365.25 * 86400.0;
            p += 4; if (*p == 's') p++;
        } else if (strncasecmp(p, "mon", 3) == 0) {
            total += val * 30.0 * 86400.0;
            while (*p && isalpha((unsigned char)*p)) p++;
        } else if (strncasecmp(p, "day", 3) == 0) {
            total += val * 86400.0;
            p += 3; if (*p == 's') p++;
        } else if (strncasecmp(p, "hour", 4) == 0) {
            total += val * 3600.0;
            p += 4; if (*p == 's') p++;
        } else if (strncasecmp(p, "minute", 6) == 0) {
            total += val * 60.0;
            p += 6; if (*p == 's') p++;
        } else if (strncasecmp(p, "min", 3) == 0) {
            total += val * 60.0;
            p += 3; if (*p == 's') p++;
        } else if (strncasecmp(p, "second", 6) == 0) {
            total += val;
            p += 6; if (*p == 's') p++;
        } else if (strncasecmp(p, "sec", 3) == 0) {
            total += val;
            p += 3; if (*p == 's') p++;
        } else {
            /* bare number with no unit — treat as seconds */
            total += val;
        }
    }
    return total;
}

/* format a duration in seconds as a PostgreSQL-style interval string */
static void format_interval(double seconds, char *buf, size_t bufsz)
{
    int neg = (seconds < 0);
    if (neg) seconds = -seconds;
    int total_sec = (int)seconds;
    int days = total_sec / 86400;
    int rem = total_sec % 86400;
    int hours = rem / 3600;
    rem %= 3600;
    int mins = rem / 60;
    int secs = rem % 60;

    int years = days / 365;
    days %= 365;
    int months = days / 30;
    days %= 30;

    char *p = buf;
    size_t left = bufsz;
    if (neg) { *p++ = '-'; left--; }
    int wrote = 0;
    if (years > 0) {
        int n = snprintf(p, left, "%d year%s ", years, years != 1 ? "s" : "");
        p += n; left -= (size_t)n; wrote = 1;
    }
    if (months > 0) {
        int n = snprintf(p, left, "%d mon%s ", months, months != 1 ? "s" : "");
        p += n; left -= (size_t)n; wrote = 1;
    }
    if (days > 0) {
        int n = snprintf(p, left, "%d day%s ", days, days != 1 ? "s" : "");
        p += n; left -= (size_t)n; wrote = 1;
    }
    if (hours > 0 || mins > 0 || secs > 0) {
        snprintf(p, left, "%02d:%02d:%02d", hours, mins, secs);
    } else if (!wrote) {
        snprintf(p, left, "00:00:00");
    } else {
        /* trim trailing space */
        if (p > buf && p[-1] == ' ') p[-1] = '\0';
    }
}

/* forward declarations for cell helpers used by legacy eval functions */
static void cell_release(struct cell *c);
static void cell_release_rb(struct cell *c, struct bump_alloc *rb);
static struct cell cell_deep_copy(const struct cell *src);
static struct cell cell_deep_copy_rb(const struct cell *src, struct bump_alloc *rb);
static int cell_is_null(const struct cell *c);

/* SQL LIKE pattern matching: % = any sequence, _ = any single char */
static int like_match(const char *pattern, const char *text, int case_insensitive)
{
    while (*pattern) {
        if (*pattern == '%') {
            pattern++;
            if (*pattern == '\0') return 1;
            while (*text) {
                if (like_match(pattern, text, case_insensitive)) return 1;
                text++;
            }
            return 0;
        } else if (*pattern == '_') {
            if (*text == '\0') return 0;
            pattern++; text++;
        } else {
            char pc = *pattern, tc = *text;
            if (case_insensitive) { pc = tolower((unsigned char)pc); tc = tolower((unsigned char)tc); }
            if (pc != tc) return 0;
            pattern++; text++;
        }
    }
    return *text == '\0';
}

/* condition_free, query_free, and all query_*_free functions live in parser.c
 * (the allocating module) per JPL ownership rules. */

/* cell_cmp → use shared cell_compare from row.h (returns -2 for incompatible types) */

static int row_matches(struct table *t, struct where_clause *w, struct query_arena *arena, struct row *row, struct database *db);
static double cell_to_double(const struct cell *c);

int eval_condition(uint32_t cond_idx, struct query_arena *arena,
                   struct row *row, struct table *t,
                   struct database *db)
{
    if (cond_idx == IDX_NONE) return 1;
    struct condition *cond = &COND(arena, cond_idx);
    switch (cond->type) {
        case COND_AND:
            return eval_condition(cond->left, arena, row, t, db) &&
                   eval_condition(cond->right, arena, row, t, db);
        case COND_OR:
            return eval_condition(cond->left, arena, row, t, db) ||
                   eval_condition(cond->right, arena, row, t, db);
        case COND_NOT:
            return !eval_condition(cond->left, arena, row, t, db);
        case COND_MULTI_IN: {
            /* multi-column IN: (a, b) IN ((1,2), (3,4)) */
            int width = cond->multi_tuple_width;
            if (width <= 0) return 0;
            /* resolve column indices */
            int col_idxs[32];
            for (int ci = 0; ci < width && ci < 32; ci++) {
                /* strip table prefix from column name */
                sv col = ASV(arena, cond->multi_columns_start + (uint32_t)ci);
                for (size_t k = 0; k < col.len; k++) {
                    if (col.data[k] == '.') { col = sv_from(col.data + k + 1, col.len - k - 1); break; }
                }
                col_idxs[ci] = table_find_column_sv(t, col);
                if (col_idxs[ci] < 0) return 0;
            }
            int num_tuples = (int)cond->multi_values_count / width;
            int found = 0;
            for (int ti = 0; ti < num_tuples && !found; ti++) {
                int match = 1;
                for (int ci = 0; ci < width; ci++) {
                    struct cell *rc = &row->cells.items[col_idxs[ci]];
                    struct cell *vc = &ACELL(arena, cond->multi_values_start + (uint32_t)(ti * width + ci));
                    if (cell_compare(rc, vc) != 0) { match = 0; break; }
                }
                if (match) found = 1;
            }
            return cond->op == CMP_NOT_IN ? !found : found;
        }
        case COND_COMPARE: {
            /* EXISTS / NOT EXISTS */
            if (cond->op == CMP_EXISTS || cond->op == CMP_NOT_EXISTS) {
                /* correlated: subquery_sql still set → execute per-row */
                if (cond->subquery_sql != IDX_NONE && db) {
                    const char *sql_tmpl = ASTRING(arena, cond->subquery_sql);
                    /* substitute outer table.col references with literal values */
                    char sql_buf[4096];
                    strncpy(sql_buf, sql_tmpl, sizeof(sql_buf) - 1);
                    sql_buf[sizeof(sql_buf) - 1] = '\0';
                    for (size_t ci = 0; ci < t->columns.count; ci++) {
                        const char *cname = t->columns.items[ci].name;
                        char ref[256];
                        snprintf(ref, sizeof(ref), "%s.%s", t->name, cname);
                        size_t rlen = strlen(ref);
                        char lit[256];
                        struct cell *cv = &row->cells.items[ci];
                        if (cv->is_null || (column_type_is_text(cv->type) && !cv->value.as_text))
                            snprintf(lit, sizeof(lit), "NULL");
                        else if (cv->type == COLUMN_TYPE_INT)
                            snprintf(lit, sizeof(lit), "%d", cv->value.as_int);
                        else if (cv->type == COLUMN_TYPE_FLOAT)
                            snprintf(lit, sizeof(lit), "%g", cv->value.as_float);
                        else if (column_type_is_text(cv->type) && cv->value.as_text)
                            snprintf(lit, sizeof(lit), "'%s'", cv->value.as_text);
                        else
                            continue;
                        size_t llen = strlen(lit);
                        char *pos;
                        while ((pos = strstr(sql_buf, ref)) != NULL) {
                            char after = pos[rlen];
                            if (isalnum((unsigned char)after) || after == '_') break;
                            size_t cur_len = strlen(sql_buf);
                            if (cur_len - rlen + llen >= sizeof(sql_buf) - 1) break;
                            memmove(pos + llen, pos + rlen, cur_len - (size_t)(pos - sql_buf) - rlen + 1);
                            memcpy(pos, lit, llen);
                        }
                    }
                    struct query sq = {0};
                    int has_rows = 0;
                    if (query_parse(sql_buf, &sq) == 0) {
                        struct rows sq_result = {0};
                        if (db_exec(db, &sq, &sq_result, NULL) == 0) {
                            has_rows = (sq_result.count > 0);
                            for (size_t i = 0; i < sq_result.count; i++)
                                row_free(&sq_result.data[i]);
                            free(sq_result.data);
                        }
                    }
                    query_free(&sq);
                    return cond->op == CMP_EXISTS ? has_rows : !has_rows;
                }
                /* pre-resolved (non-correlated) */
                if (cond->op == CMP_EXISTS)
                    return cond->value.value.as_int != 0;
                return cond->value.value.as_int == 0;
            }
            struct cell lhs_tmp = {0};
            struct cell *c;
            if (cond->lhs_expr != IDX_NONE) {
                lhs_tmp = eval_expr(cond->lhs_expr, arena, t, row, NULL, NULL);
                c = &lhs_tmp;
            } else {
                int col_idx = table_find_column_sv(t, cond->column);
                if (col_idx < 0) return 0;
                c = &row->cells.items[col_idx];
            }
            if (cond->op == CMP_IS_NULL)
                return c->is_null || (column_type_is_text(c->type)
                       ? (c->value.as_text == NULL) : 0);
            if (cond->op == CMP_IS_NOT_NULL)
                return !c->is_null && (column_type_is_text(c->type)
                       ? (c->value.as_text != NULL) : 1);
            /* IN / NOT IN */
            if (cond->op == CMP_IN || cond->op == CMP_NOT_IN) {
                /* SQL standard: NULL IN (...) → UNKNOWN (false);
                 * NULL NOT IN (...) → UNKNOWN (false) */
                if (c->is_null || (column_type_is_text(c->type) && !c->value.as_text))
                    return 0;
                int found = 0;
                for (uint32_t i = 0; i < cond->in_values_count; i++) {
                    struct cell *iv = &ACELL(arena, cond->in_values_start + i);
                    /* skip NULL values in the IN list */
                    if (iv->is_null) continue;
                    if (cell_compare(c, iv) == 0) { found = 1; break; }
                }
                return cond->op == CMP_IN ? found : !found;
            }
            /* BETWEEN */
            if (cond->op == CMP_BETWEEN) {
                int lo = cell_compare(c, &cond->value);
                int hi = cell_compare(c, &cond->between_high);
                if (lo == -2 || hi == -2) return 0;
                return lo >= 0 && hi <= 0;
            }
            /* IS DISTINCT FROM / IS NOT DISTINCT FROM */
            if (cond->op == CMP_IS_DISTINCT || cond->op == CMP_IS_NOT_DISTINCT) {
                /* NULL-safe equality: NULL IS NOT DISTINCT FROM NULL → true */
                int a_null = c->is_null || (column_type_is_text(c->type)
                             && !c->value.as_text);
                int b_null = column_type_is_text(cond->value.type)
                             ? (!cond->value.value.as_text) : 0;
                if (a_null && b_null) {
                    return cond->op == CMP_IS_NOT_DISTINCT; /* both NULL → not distinct */
                }
                if (a_null || b_null) {
                    return cond->op == CMP_IS_DISTINCT; /* one NULL → distinct */
                }
                int eq = (cell_compare(c, &cond->value) == 0);
                return cond->op == CMP_IS_DISTINCT ? !eq : eq;
            }
            /* LIKE / ILIKE */
            if (cond->op == CMP_LIKE || cond->op == CMP_ILIKE) {
                if (!column_type_is_text(c->type) || !c->value.as_text)
                    return 0;
                if (!cond->value.value.as_text) return 0;
                return like_match(cond->value.value.as_text, c->value.as_text,
                                  cond->op == CMP_ILIKE);
            }
            /* ANY/ALL/SOME: col op ANY(ARRAY[...]) */
            if (cond->is_any || cond->is_all) {
                if (c->is_null || (column_type_is_text(c->type) && !c->value.as_text))
                    return 0;
                for (uint32_t i = 0; i < cond->array_values_count; i++) {
                    struct cell *av = &ACELL(arena, cond->array_values_start + i);
                    int r = cell_compare(c, av);
                    int match = 0;
                    if (r != -2) {
                        switch (cond->op) {
                            case CMP_EQ: match = (r == 0); break;
                            case CMP_NE: match = (r != 0); break;
                            case CMP_LT: match = (r < 0); break;
                            case CMP_GT: match = (r > 0); break;
                            case CMP_LE: match = (r <= 0); break;
                            case CMP_GE: match = (r >= 0); break;
                            case CMP_IS_NULL:
                            case CMP_IS_NOT_NULL:
                            case CMP_IN:
                            case CMP_NOT_IN:
                            case CMP_BETWEEN:
                            case CMP_LIKE:
                            case CMP_ILIKE:
                            case CMP_IS_DISTINCT:
                            case CMP_IS_NOT_DISTINCT:
                            case CMP_EXISTS:
                            case CMP_NOT_EXISTS:
                                break;
                        }
                    }
                    if (cond->is_any && match) return 1;
                    if (cond->is_all && !match) return 0;
                }
                return cond->is_all ? 1 : 0;
            }
            /* column-to-column comparison (JOIN ON conditions) */
            if (cond->rhs_column.len > 0) {
                int rhs_col = table_find_column_sv(t, cond->rhs_column);
                if (rhs_col < 0) return 0;
                struct cell *rhs = &row->cells.items[rhs_col];
                if (c->is_null || (column_type_is_text(c->type) && !c->value.as_text))
                    return 0;
                if (rhs->is_null || (column_type_is_text(rhs->type) && !rhs->value.as_text))
                    return 0;
                int r = cell_compare(c, rhs);
                if (r == -2) return 0;
                switch (cond->op) {
                    case CMP_EQ: return r == 0;
                    case CMP_NE: return r != 0;
                    case CMP_LT: return r < 0;
                    case CMP_GT: return r > 0;
                    case CMP_LE: return r <= 0;
                    case CMP_GE: return r >= 0;
                    case CMP_IS_NULL:
                    case CMP_IS_NOT_NULL:
                    case CMP_IN:
                    case CMP_NOT_IN:
                    case CMP_BETWEEN:
                    case CMP_LIKE:
                    case CMP_ILIKE:
                    case CMP_IS_DISTINCT:
                    case CMP_IS_NOT_DISTINCT:
                    case CMP_EXISTS:
                    case CMP_NOT_EXISTS:
                        return 0;
                }
            }
            /* SQL three-valued logic: any comparison with NULL → UNKNOWN (false) */
            if (c->is_null || (column_type_is_text(c->type) && !c->value.as_text))
                return 0;
            int r = cell_compare(c, &cond->value);
            if (r == -2) return 0;
            switch (cond->op) {
                case CMP_EQ: return r == 0;
                case CMP_NE: return r != 0;
                case CMP_LT: return r < 0;
                case CMP_GT: return r > 0;
                case CMP_LE: return r <= 0;
                case CMP_GE: return r >= 0;
                case CMP_IS_NULL:
                case CMP_IS_NOT_NULL:
                case CMP_IN:
                case CMP_NOT_IN:
                case CMP_BETWEEN:
                case CMP_LIKE:
                case CMP_ILIKE:
                case CMP_IS_DISTINCT:
                case CMP_IS_NOT_DISTINCT:
                case CMP_EXISTS:
                case CMP_NOT_EXISTS:
                    return 0;
            }
        }
    }
    return 0;
}

/* cell_match → use shared cell_equal from row.h */

static double resolve_operand(sv tok_sv, struct table *t, struct row *src);

/* check if sv starts with a keyword (case-insensitive) */
static int sv_starts_with_ci(sv s, const char *prefix)
{
    size_t plen = strlen(prefix);
    if (s.len < plen) return 0;
    for (size_t i = 0; i < plen; i++) {
        if (tolower((unsigned char)s.data[i]) != tolower((unsigned char)prefix[i])) return 0;
    }
    /* must be followed by non-alnum or end */
    if (s.len > plen && isalnum((unsigned char)s.data[plen])) return 0;
    return 1;
}

/* get the body of FUNC(...) — the part between the outer parens */
static sv func_body(sv expr)
{
    size_t start = 0;
    while (start < expr.len && expr.data[start] != '(') start++;
    start++; /* skip '(' */
    size_t end = expr.len;
    if (end > 0 && expr.data[end - 1] == ')') end--;
    return sv_from(expr.data + start, end - start);
}

/* split function arguments on commas respecting parentheses */
static int split_func_args(sv body, sv *out, int max_args)
{
    int nargs = 0;
    int depth = 0;
    size_t arg_start = 0;
    for (size_t i = 0; i <= body.len && nargs < max_args; i++) {
        char c = (i < body.len) ? body.data[i] : '\0';
        if (c == '(') depth++;
        else if (c == ')') depth--;
        else if ((c == ',' && depth == 0) || i == body.len) {
            out[nargs++] = sv_from(body.data + arg_start, i - arg_start);
            arg_start = i + 1;
        }
    }
    return nargs;
}

/* forward declarations for mutual recursion */
static struct cell eval_scalar_func(sv expr, struct table *t, struct row *src);
static struct cell eval_coalesce(sv expr, struct table *t, struct row *src);

/* resolve a single argument expression to a cell value.
 * Handles: string literals, number literals, column references,
 * NULL keyword, and nested function calls (UPPER, LOWER, LENGTH,
 * COALESCE, NULLIF, GREATEST, LEAST). */
static struct cell resolve_arg(sv arg, struct table *t, struct row *src)
{
    /* trim */
    while (arg.len > 0 && (arg.data[0] == ' ' || arg.data[0] == '\t'))
        { arg.data++; arg.len--; }
    while (arg.len > 0 && (arg.data[arg.len-1] == ' ' || arg.data[arg.len-1] == '\t'))
        arg.len--;

    /* NULL keyword */
    if (arg.len == 4 && strncasecmp(arg.data, "NULL", 4) == 0) {
        struct cell c = { .type = COLUMN_TYPE_TEXT, .is_null = 1 };
        return c;
    }

    /* nested function call: look for FUNC(...) pattern */
    if (arg.len > 2) {
        size_t paren = 0;
        for (size_t k = 0; k < arg.len; k++) {
            if (arg.data[k] == '(') { paren = k; break; }
        }
        if (paren > 0 && arg.data[arg.len - 1] == ')') {
            sv fname = sv_from(arg.data, paren);
            while (fname.len > 0 && fname.data[fname.len-1] == ' ') fname.len--;
            if (sv_eq_ignorecase_cstr(fname, "UPPER") ||
                sv_eq_ignorecase_cstr(fname, "LOWER") ||
                sv_eq_ignorecase_cstr(fname, "LENGTH") ||
                sv_eq_ignorecase_cstr(fname, "COALESCE") ||
                sv_eq_ignorecase_cstr(fname, "NULLIF") ||
                sv_eq_ignorecase_cstr(fname, "GREATEST") ||
                sv_eq_ignorecase_cstr(fname, "LEAST") ||
                sv_eq_ignorecase_cstr(fname, "TRIM") ||
                sv_eq_ignorecase_cstr(fname, "SUBSTRING")) {
                return eval_scalar_func(arg, t, src);
            }
        }
    }

    /* string literal */
    if (arg.len >= 2 && arg.data[0] == '\'') {
        struct cell c = {0};
        c.type = COLUMN_TYPE_TEXT;
        /* caller owns returned text — see JPL contract at cell_release() */
        c.value.as_text = malloc(arg.len - 1);
        memcpy(c.value.as_text, arg.data + 1, arg.len - 2);
        c.value.as_text[arg.len - 2] = '\0';
        return c;
    }

    /* column reference */
    sv col = arg;
    for (size_t k = 0; k < col.len; k++) {
        if (col.data[k] == '.') { col = sv_from(col.data + k + 1, col.len - k - 1); break; }
    }
    for (size_t j = 0; j < t->columns.count; j++) {
        if (sv_eq_cstr(col, t->columns.items[j].name)) {
            struct cell *sc = &src->cells.items[j];
            if (sc->is_null) {
                struct cell c = { .type = sc->type, .is_null = 1 };
                return c;
            }
            /* caller owns returned cell — see JPL contract at cell_release() */
            return cell_deep_copy(sc);
        }
    }

    /* number literal */
    if (arg.len > 0 && (isdigit((unsigned char)arg.data[0]) || arg.data[0] == '-')) {
        char buf[64];
        size_t n = arg.len < 63 ? arg.len : 63;
        memcpy(buf, arg.data, n); buf[n] = '\0';
        struct cell c = {0};
        if (strchr(buf, '.')) {
            c.type = COLUMN_TYPE_FLOAT;
            c.value.as_float = atof(buf);
        } else {
            c.type = COLUMN_TYPE_INT;
            c.value.as_int = atoi(buf);
        }
        return c;
    }

    struct cell null_cell = { .type = COLUMN_TYPE_TEXT, .is_null = 1 };
    return null_cell;
}

/* evaluate COALESCE(arg1, arg2, ...) — returns first non-NULL argument */
static struct cell eval_coalesce(sv expr, struct table *t, struct row *src)
{
    sv body = func_body(expr);
    sv args[32];
    int n = split_func_args(body, args, 32);

    for (int i = 0; i < n; i++) {
        struct cell c = resolve_arg(args[i], t, src);
        if (!cell_is_null(&c))
            return c; /* ownership transfers to caller */
        cell_release(&c);
    }

    /* all NULL */
    struct cell null_cell = { .type = COLUMN_TYPE_TEXT, .is_null = 1 };
    return null_cell;
}

/* evaluate scalar functions: NULLIF, GREATEST, LEAST, UPPER, LOWER, LENGTH */
static struct cell eval_scalar_func(sv expr, struct table *t, struct row *src)
{
    /* determine function name */
    size_t paren = 0;
    for (size_t k = 0; k < expr.len; k++) {
        if (expr.data[k] == '(') { paren = k; break; }
    }
    sv fname = sv_from(expr.data, paren);
    while (fname.len > 0 && fname.data[fname.len-1] == ' ') fname.len--;

    sv body = func_body(expr);

    if (sv_eq_ignorecase_cstr(fname, "COALESCE")) {
        return eval_coalesce(expr, t, src);
    }

    if (sv_eq_ignorecase_cstr(fname, "NULLIF")) {
        sv args[2];
        int n = split_func_args(body, args, 2);
        if (n < 2) {
            struct cell null_cell = { .type = COLUMN_TYPE_TEXT, .is_null = 1 };
            return null_cell;
        }
        struct cell a = resolve_arg(args[0], t, src);
        struct cell b = resolve_arg(args[1], t, src);
        /* if a == b, return NULL; else return a */
        if (!cell_is_null(&a) && !cell_is_null(&b) && cell_equal(&a, &b)) {
            cell_release(&a);
            cell_release(&b);
            struct cell null_cell = { .type = a.type, .is_null = 1 };
            return null_cell;
        }
        cell_release(&b);
        return a; /* ownership transfers to caller */
    }

    if (sv_eq_ignorecase_cstr(fname, "GREATEST") || sv_eq_ignorecase_cstr(fname, "LEAST")) {
        int is_greatest = sv_eq_ignorecase_cstr(fname, "GREATEST");
        sv args[32];
        int n = split_func_args(body, args, 32);
        if (n == 0) {
            struct cell null_cell = { .type = COLUMN_TYPE_TEXT, .is_null = 1 };
            return null_cell;
        }
        struct cell best = resolve_arg(args[0], t, src);
        int best_null = cell_is_null(&best);
        for (int i = 1; i < n; i++) {
            struct cell cur = resolve_arg(args[i], t, src);
            int cur_null = cell_is_null(&cur);
            if (best_null) {
                cell_release(&best);
                best = cur;
                best_null = cur_null;
                continue;
            }
            if (cur_null) { cell_release(&cur); continue; }
            int cmp = cell_compare(&cur, &best);
            if ((is_greatest && cmp > 0) || (!is_greatest && cmp < 0)) {
                cell_release(&best);
                best = cur;
            } else {
                cell_release(&cur);
            }
        }
        return best; /* ownership transfers to caller */
    }

    if (sv_eq_ignorecase_cstr(fname, "UPPER") || sv_eq_ignorecase_cstr(fname, "LOWER")) {
        int is_upper = sv_eq_ignorecase_cstr(fname, "UPPER");
        struct cell arg = resolve_arg(body, t, src);
        if (arg.is_null) return arg;
        if (column_type_is_text(arg.type) && arg.value.as_text) {
            for (char *p = arg.value.as_text; *p; p++)
                *p = is_upper ? toupper((unsigned char)*p) : tolower((unsigned char)*p);
        }
        return arg;
    }

    if (sv_eq_ignorecase_cstr(fname, "LENGTH")) {
        struct cell arg = resolve_arg(body, t, src);
        struct cell result = {0};
        result.type = COLUMN_TYPE_INT;
        if (cell_is_null(&arg)) {
            result.is_null = 1;
        } else if (column_type_is_text(arg.type) && arg.value.as_text) {
            result.value.as_int = (int)strlen(arg.value.as_text);
            cell_release(&arg);
        } else {
            result.value.as_int = 0;
        }
        return result;
    }

    if (sv_eq_ignorecase_cstr(fname, "TRIM")) {
        struct cell arg = resolve_arg(body, t, src);
        if (arg.is_null) return arg;
        if (column_type_is_text(arg.type) && arg.value.as_text) {
            char *s = arg.value.as_text;
            while (*s == ' ' || *s == '\t') s++;
            char *e = s + strlen(s);
            while (e > s && (e[-1] == ' ' || e[-1] == '\t')) e--;
            char *trimmed = malloc((size_t)(e - s) + 1);
            memcpy(trimmed, s, (size_t)(e - s));
            trimmed[e - s] = '\0';
            free(arg.value.as_text); /* replace in-place, not a cross-scope transfer */
            arg.value.as_text = trimmed;
        }
        return arg;
    }

    struct cell null_cell = { .type = COLUMN_TYPE_TEXT, .is_null = 1 };
    return null_cell;
}

// TODO: eval_case_when does manual keyword scanning with strncasecmp on raw sv;
// could reuse the lexer and condition parser for more robust and maintainable parsing
/* evaluate CASE WHEN cond THEN val [WHEN cond THEN val]* [ELSE val] END */
static struct cell eval_case_when(sv expr, struct table *t, struct row *src)
{
    /* skip "CASE " */
    size_t pos = 4;
    while (pos < expr.len && (expr.data[pos] == ' ' || expr.data[pos] == '\t')) pos++;

    sv rest = sv_from(expr.data + pos, expr.len - pos);

    /* strip trailing END (case-insensitive) */
    while (rest.len > 0 && (rest.data[rest.len-1] == ' ' || rest.data[rest.len-1] == '\t')) rest.len--;
    if (rest.len >= 3) {
        sv tail = sv_from(rest.data + rest.len - 3, 3);
        if (sv_eq_ignorecase_cstr(tail, "END")) rest.len -= 3;
    }
    while (rest.len > 0 && (rest.data[rest.len-1] == ' ' || rest.data[rest.len-1] == '\t')) rest.len--;

    /* parse WHEN...THEN...ELSE blocks by scanning for keywords */
    /* simple approach: find WHEN, THEN, ELSE keywords at word boundaries */
    const char *p = rest.data;
    const char *end = rest.data + rest.len;

    while (p < end) {
        /* skip whitespace */
        while (p < end && (*p == ' ' || *p == '\t')) p++;
        if (p >= end) break;

        /* check for WHEN */
        if ((size_t)(end - p) >= 4 && strncasecmp(p, "WHEN", 4) == 0 &&
            (p + 4 >= end || p[4] == ' ' || p[4] == '\t')) {
            p += 4;
            while (p < end && (*p == ' ' || *p == '\t')) p++;

            /* find THEN */
            const char *then_pos = NULL;
            for (const char *s = p; s + 4 <= end; s++) {
                if (strncasecmp(s, "THEN", 4) == 0 &&
                    (s == p || s[-1] == ' ' || s[-1] == '\t') &&
                    (s + 4 >= end || s[4] == ' ' || s[4] == '\t')) {
                    then_pos = s; break;
                }
            }
            if (!then_pos) break;

            sv cond_sv = sv_from(p, (size_t)(then_pos - p));
            while (cond_sv.len > 0 && (cond_sv.data[cond_sv.len-1] == ' ')) cond_sv.len--;

            const char *val_start = then_pos + 4;
            while (val_start < end && (*val_start == ' ' || *val_start == '\t')) val_start++;

            /* find next WHEN or ELSE or end */
            const char *val_end = end;
            for (const char *s = val_start; s + 4 <= end; s++) {
                if ((strncasecmp(s, "WHEN", 4) == 0 || strncasecmp(s, "ELSE", 4) == 0) &&
                    (s == val_start || s[-1] == ' ' || s[-1] == '\t') &&
                    (s + 4 >= end || s[4] == ' ' || s[4] == '\t')) {
                    val_end = s; break;
                }
            }

            sv val_sv = sv_from(val_start, (size_t)(val_end - val_start));
            while (val_sv.len > 0 && (val_sv.data[val_sv.len-1] == ' ')) val_sv.len--;

            /* evaluate condition: simple "col op value" or "col IS [NOT] NULL" */
            int cond_match = 0;
            /* check for IS NULL / IS NOT NULL */
            {
                const char *is_pos = NULL;
                for (const char *s = cond_sv.data; s + 2 <= cond_sv.data + cond_sv.len; s++) {
                    if ((s == cond_sv.data || s[-1] == ' ' || s[-1] == '\t') &&
                        strncasecmp(s, "IS", 2) == 0 &&
                        (s + 2 >= cond_sv.data + cond_sv.len || s[2] == ' ' || s[2] == '\t')) {
                        is_pos = s; break;
                    }
                }
                if (is_pos) {
                    sv col_part = sv_from(cond_sv.data, (size_t)(is_pos - cond_sv.data));
                    while (col_part.len > 0 && col_part.data[col_part.len-1] == ' ') col_part.len--;
                    const char *after_is = is_pos + 2;
                    while (after_is < cond_sv.data + cond_sv.len && (*after_is == ' ' || *after_is == '\t')) after_is++;
                    int is_not_null = 0;
                    if ((size_t)(cond_sv.data + cond_sv.len - after_is) >= 3 &&
                        strncasecmp(after_is, "NOT", 3) == 0) {
                        is_not_null = 1;
                        after_is += 3;
                        while (after_is < cond_sv.data + cond_sv.len && (*after_is == ' ' || *after_is == '\t')) after_is++;
                    }
                    if ((size_t)(cond_sv.data + cond_sv.len - after_is) >= 4 &&
                        strncasecmp(after_is, "NULL", 4) == 0) {
                        struct cell col_val = resolve_arg(col_part, t, src);
                        int col_is_null = cell_is_null(&col_val);
                        cell_release(&col_val);
                        cond_match = is_not_null ? !col_is_null : col_is_null;
                        goto case_cond_done;
                    }
                }
            }
            for (size_t i = 0; i < cond_sv.len; i++) {
                char cc = cond_sv.data[i];
                /* skip operators inside quoted strings */
                if (cc == '\'') { i++; while (i < cond_sv.len && cond_sv.data[i] != '\'') i++; continue; }
                if (cc == '=' || cc == '>' || cc == '<' || cc == '!') {
                    sv col_part = sv_from(cond_sv.data, i);
                    while (col_part.len > 0 && col_part.data[col_part.len-1] == ' ') col_part.len--;
                    size_t op_end = i + 1;
                    if (op_end < cond_sv.len && cond_sv.data[op_end] == '=') op_end++;
                    sv val_part = sv_from(cond_sv.data + op_end, cond_sv.len - op_end);
                    while (val_part.len > 0 && val_part.data[0] == ' ') { val_part.data++; val_part.len--; }

                    /* check if either side is a string literal */
                    int is_text_cmp = 0;
                    if (val_part.len >= 2 && val_part.data[0] == '\'') is_text_cmp = 1;

                    if (is_text_cmp) {
                        /* resolve LHS as text from column */
                        const char *lhs_text = NULL;
                        sv col_sv = col_part;
                        for (size_t k = 0; k < col_sv.len; k++) {
                            if (col_sv.data[k] == '.') { col_sv = sv_from(col_sv.data + k + 1, col_sv.len - k - 1); break; }
                        }
                        for (size_t j = 0; j < t->columns.count; j++) {
                            if (sv_eq_cstr(col_sv, t->columns.items[j].name)) {
                                struct cell *sc = &src->cells.items[j];
                                if (column_type_is_text(sc->type) && sc->value.as_text)
                                    lhs_text = sc->value.as_text;
                                break;
                            }
                        }
                        /* extract RHS string (strip quotes) */
                        char rhs_buf[256];
                        size_t rlen = val_part.len - 2 < 255 ? val_part.len - 2 : 255;
                        memcpy(rhs_buf, val_part.data + 1, rlen);
                        rhs_buf[rlen] = '\0';

                        if (lhs_text) {
                            int scmp = strcmp(lhs_text, rhs_buf);
                            if (cc == '=' && op_end == i + 1) cond_match = (scmp == 0);
                            else if (cc == '!' && op_end == i + 2) cond_match = (scmp != 0);
                            else if (cc == '>' && op_end == i + 1) cond_match = (scmp > 0);
                            else if (cc == '>' && op_end == i + 2) cond_match = (scmp >= 0);
                            else if (cc == '<' && op_end == i + 1) cond_match = (scmp < 0);
                            else if (cc == '<' && op_end == i + 2) cond_match = (scmp <= 0);
                        }
                    } else {
                        double lhs = resolve_operand(col_part, t, src);
                        double rhs = resolve_operand(val_part, t, src);

                        if (cc == '=' && op_end == i + 1) cond_match = (lhs == rhs);
                        else if (cc == '!' && op_end == i + 2) cond_match = (lhs != rhs);
                        else if (cc == '>' && op_end == i + 1) cond_match = (lhs > rhs);
                        else if (cc == '>' && op_end == i + 2) cond_match = (lhs >= rhs);
                        else if (cc == '<' && op_end == i + 1) cond_match = (lhs < rhs);
                        else if (cc == '<' && op_end == i + 2) cond_match = (lhs <= rhs);
                    }
                    break;
                }
            }

case_cond_done:
            if (cond_match) {
                return resolve_arg(val_sv, t, src);
            }

            p = val_end;
            continue;
        }

        /* check for ELSE */
        if ((size_t)(end - p) >= 4 && strncasecmp(p, "ELSE", 4) == 0 &&
            (p + 4 >= end || p[4] == ' ' || p[4] == '\t')) {
            p += 4;
            while (p < end && (*p == ' ' || *p == '\t')) p++;
            sv else_val = sv_from(p, (size_t)(end - p));
            while (else_val.len > 0 && (else_val.data[else_val.len-1] == ' ')) else_val.len--;

            return resolve_arg(else_val, t, src);
        }

        break;
    }

    struct cell null_cell = { .type = COLUMN_TYPE_TEXT, .is_null = 1 };
    return null_cell;
}

/* check if an sv segment contains an arithmetic operator */
static int has_arith_op(sv s)
{
    int in_quote = 0;
    int paren_depth = 0;
    for (size_t i = 0; i < s.len; i++) {
        char c = s.data[i];
        if (c == '\'' && !in_quote) { in_quote = 1; continue; }
        if (c == '\'' && in_quote) { in_quote = 0; continue; }
        if (in_quote) continue;
        if (c == '(') { paren_depth++; continue; }
        if (c == ')') { paren_depth--; continue; }
        if (paren_depth > 0) continue;
        if (c == '+' || c == '/' || c == '-' || c == '%') return 1;
        if (c == '*') {
            /* distinguish SELECT * from multiplication: * is arith only if not alone */
            if (s.len > 1) return 1;
        }
        if (c == '|' && i + 1 < s.len && s.data[i+1] == '|') return 1;
    }
    return 0;
}

/* check if a single operand token resolves to a NULL column */
static int resolve_operand_is_null(sv tok_sv, struct table *t, struct row *src)
{
    while (tok_sv.len > 0 && (tok_sv.data[0] == ' ' || tok_sv.data[0] == '\t'))
        { tok_sv.data++; tok_sv.len--; }
    while (tok_sv.len > 0 && (tok_sv.data[tok_sv.len-1] == ' ' || tok_sv.data[tok_sv.len-1] == '\t'))
        tok_sv.len--;
    if (tok_sv.len == 0) return 0;
    /* number literals are never NULL */
    char first = tok_sv.data[0];
    if (isdigit((unsigned char)first) || (first == '-' && tok_sv.len > 1)) return 0;
    /* string literals are never NULL */
    if (first == '\'') return 0;
    /* column reference */
    for (size_t k = 0; k < tok_sv.len; k++) {
        if (tok_sv.data[k] == '.') { tok_sv = sv_from(tok_sv.data + k + 1, tok_sv.len - k - 1); break; }
    }
    for (size_t j = 0; j < t->columns.count; j++) {
        if (sv_eq_cstr(tok_sv, t->columns.items[j].name)) {
            struct cell *c = &src->cells.items[j];
            return c->is_null || (column_type_is_text(c->type) && !c->value.as_text);
        }
    }
    return 0;
}

/* resolve a single token (column name or literal) to a double */
static double resolve_operand(sv tok_sv, struct table *t, struct row *src)
{
    /* trim whitespace */
    while (tok_sv.len > 0 && (tok_sv.data[0] == ' ' || tok_sv.data[0] == '\t'))
        { tok_sv.data++; tok_sv.len--; }
    while (tok_sv.len > 0 && (tok_sv.data[tok_sv.len-1] == ' ' || tok_sv.data[tok_sv.len-1] == '\t'))
        tok_sv.len--;
    if (tok_sv.len == 0) return 0.0;

    /* try as number literal */
    char first = tok_sv.data[0];
    if (isdigit((unsigned char)first) || (first == '-' && tok_sv.len > 1)) {
        char buf[64];
        size_t n = tok_sv.len < 63 ? tok_sv.len : 63;
        memcpy(buf, tok_sv.data, n);
        buf[n] = '\0';
        return atof(buf);
    }

    /* strip table prefix */
    for (size_t k = 0; k < tok_sv.len; k++) {
        if (tok_sv.data[k] == '.') {
            tok_sv = sv_from(tok_sv.data + k + 1, tok_sv.len - k - 1);
            break;
        }
    }

    /* look up column */
    for (size_t j = 0; j < t->columns.count; j++) {
        if (sv_eq_cstr(tok_sv, t->columns.items[j].name)) {
            struct cell *c = &src->cells.items[j];
            if (c->type == COLUMN_TYPE_INT) return (double)c->value.as_int;
            if (c->type == COLUMN_TYPE_FLOAT) return c->value.as_float;
            return 0.0;
        }
    }
    return 0.0;
}

/* evaluate a simple arithmetic expression: operand [op operand]* */
static struct cell eval_arith_expr(sv expr, struct table *t, struct row *src)
{
    struct cell result = {0};
    result.type = COLUMN_TYPE_FLOAT;

    /* check for || (string concat) operator first */
    int has_concat = 0;
    {
        int in_q = 0, pd = 0;
        for (size_t i = 0; i + 1 < expr.len; i++) {
            if (expr.data[i] == '\'' && !in_q) { in_q = 1; continue; }
            if (expr.data[i] == '\'' && in_q) { in_q = 0; continue; }
            if (in_q) continue;
            if (expr.data[i] == '(') { pd++; continue; }
            if (expr.data[i] == ')') { pd--; continue; }
            if (pd > 0) continue;
            if (expr.data[i] == '|' && expr.data[i+1] == '|') { has_concat = 1; break; }
        }
    }
    if (has_concat) {
        /* string concatenation: split on || and join */
        /* SQL standard: if any operand is NULL, result is NULL */
        char buf[4096] = {0};
        size_t buf_len = 0;
        size_t seg_start = 0;
        int in_q = 0, pd = 0;
        int any_null = 0;
        for (size_t i = 0; i <= expr.len; i++) {
            if (i < expr.len && expr.data[i] == '\'' && !in_q) { in_q = 1; continue; }
            if (i < expr.len && expr.data[i] == '\'' && in_q) { in_q = 0; continue; }
            if (in_q) continue;
            if (i < expr.len && expr.data[i] == '(') { pd++; continue; }
            if (i < expr.len && expr.data[i] == ')') { pd--; continue; }
            if (pd > 0) continue;
            int is_concat = (i + 1 < expr.len && expr.data[i] == '|' && expr.data[i+1] == '|');
            if (is_concat || i == expr.len) {
                sv seg = sv_from(expr.data + seg_start, i - seg_start);
                while (seg.len > 0 && (seg.data[0] == ' ' || seg.data[0] == '\t')) { seg.data++; seg.len--; }
                while (seg.len > 0 && (seg.data[seg.len-1] == ' ' || seg.data[seg.len-1] == '\t')) seg.len--;
                if (seg.len >= 2 && seg.data[0] == '\'') {
                    /* string literal */
                    size_t n = seg.len - 2 < sizeof(buf) - buf_len - 1 ? seg.len - 2 : sizeof(buf) - buf_len - 1;
                    memcpy(buf + buf_len, seg.data + 1, n);
                    buf_len += n;
                } else {
                    /* column reference — check for NULL */
                    if (resolve_operand_is_null(seg, t, src)) { any_null = 1; break; }
                    sv col = seg;
                    for (size_t k = 0; k < col.len; k++) {
                        if (col.data[k] == '.') { col = sv_from(col.data + k + 1, col.len - k - 1); break; }
                    }
                    for (size_t j = 0; j < t->columns.count; j++) {
                        if (sv_eq_cstr(col, t->columns.items[j].name)) {
                            struct cell *sc = &src->cells.items[j];
                            if (column_type_is_text(sc->type) && sc->value.as_text) {
                                size_t slen = strlen(sc->value.as_text);
                                size_t n = slen < sizeof(buf) - buf_len - 1 ? slen : sizeof(buf) - buf_len - 1;
                                memcpy(buf + buf_len, sc->value.as_text, n);
                                buf_len += n;
                            } else if (sc->type == COLUMN_TYPE_INT) {
                                buf_len += snprintf(buf + buf_len, sizeof(buf) - buf_len, "%d", sc->value.as_int);
                            } else if (sc->type == COLUMN_TYPE_FLOAT) {
                                buf_len += snprintf(buf + buf_len, sizeof(buf) - buf_len, "%g", sc->value.as_float);
                            }
                            break;
                        }
                    }
                }
                if (is_concat) { i++; seg_start = i + 1; }
            }
        }
        if (any_null) {
            result.type = COLUMN_TYPE_TEXT;
            result.is_null = 1;
            return result;
        }
        buf[buf_len] = '\0';
        result.type = COLUMN_TYPE_TEXT;
        /* caller owns returned text — see JPL contract at cell_release() */
        result.value.as_text = strdup(buf);
        return result;
    }

    /* tokenize: split on +, -, *, /, % keeping operators */
    double vals[32];
    char ops[32];
    int nvals = 0, nops = 0;
    int arith_has_null = 0;

    size_t start = 0;
    for (size_t i = 0; i <= expr.len && nvals < 32; i++) {
        char c = (i < expr.len) ? expr.data[i] : '\0';
        int is_op = (c == '+' || c == '-' || c == '/' || c == '*' || c == '%');
        /* '-' is unary (not binary) only when no operand text precedes it */
        if (c == '-' && is_op) {
            /* check if there is any non-whitespace content since 'start' */
            int has_operand = 0;
            for (size_t k = start; k < i; k++) {
                if (expr.data[k] != ' ' && expr.data[k] != '\t') { has_operand = 1; break; }
            }
            if (!has_operand) is_op = 0;
        }

        if (is_op || i == expr.len) {
            sv operand = sv_from(expr.data + start, i - start);
            if (resolve_operand_is_null(operand, t, src)) arith_has_null = 1;
            vals[nvals++] = resolve_operand(operand, t, src);
            if (is_op && nops < 32) ops[nops++] = c;
            start = i + 1;
        }
    }

    /* SQL standard: any NULL operand → NULL result */
    if (arith_has_null) {
        result.type = COLUMN_TYPE_INT;
        result.is_null = 1;
        return result;
    }

    /* evaluate: first pass for *, / and % */
    for (int i = 0; i < nops; i++) {
        if (ops[i] == '*' || ops[i] == '/' || ops[i] == '%') {
            if (ops[i] == '*') vals[i] = vals[i] * vals[i+1];
            else if (ops[i] == '/') vals[i] = (vals[i+1] != 0.0) ? vals[i] / vals[i+1] : 0.0;
            else vals[i] = (vals[i+1] != 0.0) ? (double)((int)vals[i] % (int)vals[i+1]) : 0.0;
            /* shift remaining */
            for (int j = i+1; j < nvals - 1; j++) vals[j] = vals[j+1];
            for (int j = i; j < nops - 1; j++) ops[j] = ops[j+1];
            nvals--; nops--; i--;
        }
    }
    /* second pass for + and - */
    double v = (nvals > 0) ? vals[0] : 0.0;
    for (int i = 0; i < nops; i++) {
        if (ops[i] == '+') v += vals[i+1];
        else if (ops[i] == '-') v -= vals[i+1];
    }

    /* output as int if result is a whole number and no float literals involved */
    if (v == (double)(int)v) {
        result.type = COLUMN_TYPE_INT;
        result.value.as_int = (int)v;
    } else {
        result.value.as_float = v;
    }
    return result;
}

/* ---------------------------------------------------------------------------
 * AST-based expression evaluator — walks the expression tree produced by
 * parse_expr and returns a cell value.
 *
 * JPL ownership contract for cell text:
 *   - Factory functions (cell_make_text, cell_deep_copy, cell_to_text)
 *     allocate text via strdup/malloc.  Ownership transfers to the caller.
 *   - The caller must either:
 *       (a) pass the cell into a result row (row_free releases the text), or
 *       (b) call cell_release() to free the text when discarding the cell.
 *   - cell_release() is the single canonical release function; all discard
 *     paths must go through it so ownership is auditable.
 * ------------------------------------------------------------------------- */

/* Release any owned text in a cell.  After this call the cell must not be
 * used.  This is the ONLY function that should free cell text outside of
 * row_free (which iterates cells at end-of-life). */
static void cell_release(struct cell *c)
{
    if (column_type_is_text(c->type) && c->value.as_text) {
        free(c->value.as_text);
        c->value.as_text = NULL;
    }
}

/* When rb is non-NULL, text lives in the bump — just zero the pointer. */
static void cell_release_rb(struct cell *c, struct bump_alloc *rb)
{
    if (rb) {
        c->value.as_text = NULL;
    } else {
        cell_release(c);
    }
}

static struct cell cell_make_null(void)
{
    struct cell c = {0};
    c.type = COLUMN_TYPE_TEXT;
    c.is_null = 1;
    return c;
}

static struct cell cell_make_int(int v)
{
    struct cell c = {0};
    c.type = COLUMN_TYPE_INT;
    c.value.as_int = v;
    return c;
}

static struct cell cell_make_float(double v)
{
    struct cell c = {0};
    c.type = COLUMN_TYPE_FLOAT;
    c.value.as_float = v;
    return c;
}


/* Deep-copy a cell, strdup'ing any text.  Caller owns the copy and must
 * either push it into a result row or call cell_release(). */
static struct cell cell_deep_copy(const struct cell *src)
{
    struct cell c = { .type = src->type, .is_null = src->is_null };
    if (column_type_is_text(src->type) && src->value.as_text)
        c.value.as_text = strdup(src->value.as_text);
    else
        c.value = src->value;
    return c;
}

/* When rb is non-NULL, copy text into the bump slab instead of strdup. */
static struct cell cell_deep_copy_rb(const struct cell *src, struct bump_alloc *rb)
{
    if (!rb) return cell_deep_copy(src);
    struct cell c = { .type = src->type, .is_null = src->is_null };
    if (column_type_is_text(src->type) && src->value.as_text)
        c.value.as_text = bump_strdup(rb, src->value.as_text);
    else
        c.value = src->value;
    return c;
}

static int cell_is_null(const struct cell *c)
{
    return c->is_null || (column_type_is_text(c->type) && !c->value.as_text);
}

static double cell_to_double_val(const struct cell *c)
{
    if (c->type == COLUMN_TYPE_INT)    return (double)c->value.as_int;
    if (c->type == COLUMN_TYPE_FLOAT)  return c->value.as_float;
    if (c->type == COLUMN_TYPE_BIGINT) return (double)c->value.as_bigint;
    return 0.0;
}

/* Return a malloc'd NUL-terminated string representation of the cell.
 * Caller owns the returned pointer and must free() it. */
static char *cell_to_text(const struct cell *c)
{
    if (cell_is_null(c)) return NULL;
    if (column_type_is_text(c->type) && c->value.as_text)
        return strdup(c->value.as_text);
    char buf[64];
    if (c->type == COLUMN_TYPE_INT)
        snprintf(buf, sizeof(buf), "%d", c->value.as_int);
    else if (c->type == COLUMN_TYPE_FLOAT)
        snprintf(buf, sizeof(buf), "%g", c->value.as_float);
    else if (c->type == COLUMN_TYPE_BIGINT)
        snprintf(buf, sizeof(buf), "%lld", c->value.as_bigint);
    else if (c->type == COLUMN_TYPE_BOOLEAN)
        snprintf(buf, sizeof(buf), "%s", c->value.as_bool ? "true" : "false");
    else
        buf[0] = '\0';
    return strdup(buf);
}

/* When rb is non-NULL, allocate the text from the bump slab. */
static char *cell_to_text_rb(const struct cell *c, struct bump_alloc *rb)
{
    if (!rb) return cell_to_text(c);
    if (cell_is_null(c)) return NULL;
    if (column_type_is_text(c->type) && c->value.as_text)
        return bump_strdup(rb, c->value.as_text);
    char buf[64];
    if (c->type == COLUMN_TYPE_INT)
        snprintf(buf, sizeof(buf), "%d", c->value.as_int);
    else if (c->type == COLUMN_TYPE_FLOAT)
        snprintf(buf, sizeof(buf), "%g", c->value.as_float);
    else if (c->type == COLUMN_TYPE_BIGINT)
        snprintf(buf, sizeof(buf), "%lld", c->value.as_bigint);
    else if (c->type == COLUMN_TYPE_BOOLEAN)
        snprintf(buf, sizeof(buf), "%s", c->value.as_bool ? "true" : "false");
    else
        buf[0] = '\0';
    return bump_strdup(rb, buf);
}

struct cell eval_expr(uint32_t expr_idx, struct query_arena *arena,
                      struct table *t, struct row *row,
                      struct database *db, struct bump_alloc *rb)
{
    if (expr_idx == IDX_NONE) return cell_make_null();
    struct expr *e = &EXPR(arena, expr_idx);

    switch (e->type) {

    case EXPR_LITERAL:
        return cell_deep_copy_rb(&e->literal, rb);

    case EXPR_COLUMN_REF: {
        if (!t || !row) return cell_make_null();
        sv col = e->column_ref.column;
        int idx = table_find_column_sv(t, col);
        if (idx < 0 && e->column_ref.table.len > 0) {
            /* try table.column qualified lookup */
            char qname[256];
            snprintf(qname, sizeof(qname), SV_FMT "." SV_FMT,
                     SV_ARG(e->column_ref.table), SV_ARG(col));
            sv qsv = sv_from(qname, strlen(qname));
            idx = table_find_column_sv(t, qsv);
        }
        if (idx < 0) return cell_make_null();
        return cell_deep_copy_rb(&row->cells.items[idx], rb);
    }

    case EXPR_UNARY_OP: {
        struct cell operand = eval_expr(e->unary.operand, arena, t, row, db, rb);
        if (cell_is_null(&operand)) return operand;
        if (e->unary.op == OP_NEG) {
            if (operand.type == COLUMN_TYPE_INT) {
                operand.value.as_int = -operand.value.as_int;
            } else if (operand.type == COLUMN_TYPE_FLOAT) {
                operand.value.as_float = -operand.value.as_float;
            } else if (operand.type == COLUMN_TYPE_BIGINT) {
                operand.value.as_bigint = -operand.value.as_bigint;
            }
        }
        return operand;
    }

    case EXPR_BINARY_OP: {
        struct cell lhs = eval_expr(e->binary.left, arena, t, row, db, rb);
        struct cell rhs = eval_expr(e->binary.right, arena, t, row, db, rb);

        /* string concatenation */
        if (e->binary.op == OP_CONCAT) {
            /* SQL standard: NULL || anything = NULL */
            if (cell_is_null(&lhs) || cell_is_null(&rhs)) {
                cell_release_rb(&lhs, rb);
                cell_release_rb(&rhs, rb);
                return cell_make_null();
            }
            char *ls = cell_to_text_rb(&lhs, rb);
            char *rs = cell_to_text_rb(&rhs, rb);
            size_t llen = ls ? strlen(ls) : 0;
            size_t rlen = rs ? strlen(rs) : 0;
            char *buf = rb ? (char *)bump_alloc(rb, llen + rlen + 1)
                           : malloc(llen + rlen + 1);
            if (ls) memcpy(buf, ls, llen);
            if (rs) memcpy(buf + llen, rs, rlen);
            buf[llen + rlen] = '\0';
            if (!rb) { free(ls); free(rs); }
            cell_release_rb(&lhs, rb);
            cell_release_rb(&rhs, rb);
            /* caller owns returned text — see JPL contract at cell_release() */
            struct cell c = {0};
            c.type = COLUMN_TYPE_TEXT;
            c.value.as_text = buf;
            return c;
        }

        /* arithmetic: NULL propagation */
        if (cell_is_null(&lhs) || cell_is_null(&rhs)) {
            cell_release_rb(&lhs, rb);
            cell_release_rb(&rhs, rb);
            struct cell c = {0};
            c.type = COLUMN_TYPE_INT;
            c.is_null = 1;
            return c;
        }

        /* date/time arithmetic: date/timestamp +/- interval, timestamp - timestamp */
        {
            int lhs_is_dt = (lhs.type == COLUMN_TYPE_DATE || lhs.type == COLUMN_TYPE_TIMESTAMP ||
                             lhs.type == COLUMN_TYPE_TIMESTAMPTZ);
            int rhs_is_dt = (rhs.type == COLUMN_TYPE_DATE || rhs.type == COLUMN_TYPE_TIMESTAMP ||
                             rhs.type == COLUMN_TYPE_TIMESTAMPTZ);
            int lhs_is_interval = (lhs.type == COLUMN_TYPE_INTERVAL);
            int rhs_is_interval = (rhs.type == COLUMN_TYPE_INTERVAL);

            /* date/timestamp +/- interval */
            if (lhs_is_dt && rhs_is_interval && (e->binary.op == OP_ADD || e->binary.op == OP_SUB)) {
                const char *dt_str = (column_type_is_text(lhs.type) && lhs.value.as_text) ? lhs.value.as_text : "";
                const char *iv_str = (column_type_is_text(rhs.type) && rhs.value.as_text) ? rhs.value.as_text : "";
                struct tm tm_val;
                if (parse_datetime(dt_str, &tm_val)) {
                    double secs = parse_interval_to_seconds(iv_str);
                    time_t t_val = mktime(&tm_val);
                    if (e->binary.op == OP_ADD) t_val += (time_t)secs;
                    else t_val -= (time_t)secs;
                    struct tm *result_tm = localtime(&t_val);
                    char buf[32];
                    if (lhs.type == COLUMN_TYPE_DATE)
                        format_date(result_tm, buf, sizeof(buf));
                    else
                        format_timestamp(result_tm, buf, sizeof(buf));
                    cell_release_rb(&lhs, rb);
                    cell_release_rb(&rhs, rb);
                    struct cell r = {0};
                    r.type = lhs.type;
                    r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
                    return r;
                }
            }

            /* interval + date/timestamp */
            if (lhs_is_interval && rhs_is_dt && e->binary.op == OP_ADD) {
                const char *iv_str = (column_type_is_text(lhs.type) && lhs.value.as_text) ? lhs.value.as_text : "";
                const char *dt_str = (column_type_is_text(rhs.type) && rhs.value.as_text) ? rhs.value.as_text : "";
                struct tm tm_val;
                if (parse_datetime(dt_str, &tm_val)) {
                    double secs = parse_interval_to_seconds(iv_str);
                    time_t t_val = mktime(&tm_val);
                    t_val += (time_t)secs;
                    struct tm *result_tm = localtime(&t_val);
                    char buf[32];
                    if (rhs.type == COLUMN_TYPE_DATE)
                        format_date(result_tm, buf, sizeof(buf));
                    else
                        format_timestamp(result_tm, buf, sizeof(buf));
                    cell_release_rb(&lhs, rb);
                    cell_release_rb(&rhs, rb);
                    struct cell r = {0};
                    r.type = rhs.type;
                    r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
                    return r;
                }
            }

            /* timestamp - timestamp = interval */
            if (lhs_is_dt && rhs_is_dt && e->binary.op == OP_SUB) {
                const char *sa = (column_type_is_text(lhs.type) && lhs.value.as_text) ? lhs.value.as_text : "";
                const char *sb = (column_type_is_text(rhs.type) && rhs.value.as_text) ? rhs.value.as_text : "";
                struct tm tm_a, tm_b;
                if (parse_datetime(sa, &tm_a) && parse_datetime(sb, &tm_b)) {
                    time_t ta = mktime(&tm_a);
                    time_t tb = mktime(&tm_b);
                    double diff = difftime(ta, tb);
                    char buf[128];
                    format_interval(diff, buf, sizeof(buf));
                    cell_release_rb(&lhs, rb);
                    cell_release_rb(&rhs, rb);
                    struct cell r = {0};
                    r.type = COLUMN_TYPE_INTERVAL;
                    r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
                    return r;
                }
            }

            /* date/timestamp + integer days */
            if (lhs_is_dt && (rhs.type == COLUMN_TYPE_INT || rhs.type == COLUMN_TYPE_BIGINT) &&
                e->binary.op == OP_ADD) {
                const char *dt_str = (column_type_is_text(lhs.type) && lhs.value.as_text) ? lhs.value.as_text : "";
                struct tm tm_val;
                if (parse_datetime(dt_str, &tm_val)) {
                    int days = (rhs.type == COLUMN_TYPE_INT) ? rhs.value.as_int : (int)rhs.value.as_bigint;
                    time_t t_val = mktime(&tm_val);
                    t_val += (time_t)days * 86400;
                    struct tm *result_tm = localtime(&t_val);
                    char buf[32];
                    if (lhs.type == COLUMN_TYPE_DATE)
                        format_date(result_tm, buf, sizeof(buf));
                    else
                        format_timestamp(result_tm, buf, sizeof(buf));
                    cell_release_rb(&lhs, rb);
                    cell_release_rb(&rhs, rb);
                    struct cell r = {0};
                    r.type = lhs.type;
                    r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
                    return r;
                }
            }

            /* date/timestamp - integer days */
            if (lhs_is_dt && (rhs.type == COLUMN_TYPE_INT || rhs.type == COLUMN_TYPE_BIGINT) &&
                e->binary.op == OP_SUB) {
                const char *dt_str = (column_type_is_text(lhs.type) && lhs.value.as_text) ? lhs.value.as_text : "";
                struct tm tm_val;
                if (parse_datetime(dt_str, &tm_val)) {
                    int days = (rhs.type == COLUMN_TYPE_INT) ? rhs.value.as_int : (int)rhs.value.as_bigint;
                    time_t t_val = mktime(&tm_val);
                    t_val -= (time_t)days * 86400;
                    struct tm *result_tm = localtime(&t_val);
                    char buf[32];
                    if (lhs.type == COLUMN_TYPE_DATE)
                        format_date(result_tm, buf, sizeof(buf));
                    else
                        format_timestamp(result_tm, buf, sizeof(buf));
                    cell_release_rb(&lhs, rb);
                    cell_release_rb(&rhs, rb);
                    struct cell r = {0};
                    r.type = lhs.type;
                    r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
                    return r;
                }
            }
        }

        double lv = cell_to_double_val(&lhs);
        double rv = cell_to_double_val(&rhs);
        double result_v = 0.0;
        switch (e->binary.op) {
            case OP_ADD: result_v = lv + rv; break;
            case OP_SUB: result_v = lv - rv; break;
            case OP_MUL: result_v = lv * rv; break;
            case OP_DIV: result_v = (rv != 0.0) ? lv / rv : 0.0; break;
            case OP_MOD: result_v = (rv != 0.0) ? (double)((long long)lv % (long long)rv) : 0.0; break;
            case OP_CONCAT: break; /* handled above */
            case OP_NEG: break;    /* not a binary op */
        }

        int use_float = (lhs.type == COLUMN_TYPE_FLOAT || rhs.type == COLUMN_TYPE_FLOAT);
        cell_release_rb(&lhs, rb);
        cell_release_rb(&rhs, rb);

        if (use_float || result_v != (double)(int)result_v)
            return cell_make_float(result_v);
        return cell_make_int((int)result_v);
    }

    case EXPR_FUNC_CALL: {
        enum expr_func fn = e->func_call.func;
        uint32_t nargs = e->func_call.args_count;
        uint32_t args_start = e->func_call.args_start;

        if (fn == FUNC_COALESCE) {
            for (uint32_t i = 0; i < nargs; i++) {
                struct cell c = eval_expr(FUNC_ARG(arena, args_start, i), arena, t, row, db, rb);
                if (!cell_is_null(&c)) return c; /* ownership transfers to caller */
                cell_release_rb(&c, rb);
            }
            return cell_make_null();
        }

        if (fn == FUNC_NULLIF) {
            if (nargs < 2) return cell_make_null();
            struct cell a = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
            struct cell b = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
            if (!cell_is_null(&a) && !cell_is_null(&b) && cell_equal(&a, &b)) {
                cell_release_rb(&a, rb);
                cell_release_rb(&b, rb);
                struct cell n = { .type = a.type, .is_null = 1 };
                return n;
            }
            cell_release_rb(&b, rb);
            return a; /* ownership transfers to caller */
        }

        if (fn == FUNC_GREATEST || fn == FUNC_LEAST) {
            int is_greatest = (fn == FUNC_GREATEST);
            if (nargs == 0) return cell_make_null();
            struct cell best = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
            int best_null = cell_is_null(&best);
            for (uint32_t i = 1; i < nargs; i++) {
                struct cell cur = eval_expr(FUNC_ARG(arena, args_start, i), arena, t, row, db, rb);
                int cur_null = cell_is_null(&cur);
                if (best_null) {
                    cell_release_rb(&best, rb);
                    best = cur; best_null = cur_null;
                    continue;
                }
                if (cur_null) {
                    cell_release_rb(&cur, rb);
                    continue;
                }
                int cmp = cell_compare(&cur, &best);
                if ((is_greatest && cmp > 0) || (!is_greatest && cmp < 0)) {
                    cell_release_rb(&best, rb);
                    best = cur;
                } else {
                    cell_release_rb(&cur, rb);
                }
            }
            return best;
        }

        if (fn == FUNC_UPPER || fn == FUNC_LOWER) {
            if (nargs == 0) return cell_make_null();
            struct cell arg = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
            if (cell_is_null(&arg)) return arg;
            if (column_type_is_text(arg.type) && arg.value.as_text) {
                int is_upper = (fn == FUNC_UPPER);
                for (char *p = arg.value.as_text; *p; p++)
                    *p = is_upper ? toupper((unsigned char)*p) : tolower((unsigned char)*p);
            }
            return arg;
        }

        if (fn == FUNC_LENGTH) {
            if (nargs == 0) return cell_make_null();
            struct cell arg = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
            if (cell_is_null(&arg)) {
                struct cell r = {0}; r.type = COLUMN_TYPE_INT; r.is_null = 1;
                return r;
            }
            if (column_type_is_text(arg.type) && arg.value.as_text) {
                int len = (int)strlen(arg.value.as_text);
                cell_release_rb(&arg, rb);
                return cell_make_int(len);
            }
            return cell_make_int(0);
        }

        if (fn == FUNC_TRIM) {
            if (nargs == 0) return cell_make_null();
            struct cell arg = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
            if (cell_is_null(&arg)) return arg;
            if (column_type_is_text(arg.type) && arg.value.as_text) {
                char *s = arg.value.as_text;
                while (*s == ' ' || *s == '\t') s++;
                char *end = s + strlen(s);
                while (end > s && (end[-1] == ' ' || end[-1] == '\t')) end--;
                size_t tlen = (size_t)(end - s);
                char *trimmed = rb ? (char *)bump_alloc(rb, tlen + 1)
                                   : malloc(tlen + 1);
                memcpy(trimmed, s, tlen);
                trimmed[tlen] = '\0';
                if (!rb) free(arg.value.as_text);
                arg.value.as_text = trimmed;
            }
            return arg; /* ownership transfers to caller */
        }

        if (fn == FUNC_NEXTVAL || fn == FUNC_CURRVAL) {
            if (nargs == 0 || !db) return cell_make_null();
            struct cell arg = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
            if (cell_is_null(&arg)) return cell_make_null();
            const char *seq_name = NULL;
            if (column_type_is_text(arg.type) && arg.value.as_text)
                seq_name = arg.value.as_text;
            if (!seq_name) { cell_release_rb(&arg, rb); return cell_make_null(); }
            /* find sequence in database */
            struct sequence *seq = NULL;
            for (size_t si = 0; si < db->sequences.count; si++) {
                if (strcmp(db->sequences.items[si].name, seq_name) == 0) {
                    seq = &db->sequences.items[si];
                    break;
                }
            }
            cell_release_rb(&arg, rb);
            if (!seq) {
                fprintf(stderr, "sequence '%s' not found\n", seq_name);
                return cell_make_null();
            }
            if (fn == FUNC_NEXTVAL) {
                long long val;
                if (!seq->has_been_called) {
                    val = seq->current_value;
                    seq->has_been_called = 1;
                } else {
                    seq->current_value += seq->increment;
                    val = seq->current_value;
                }
                struct cell r = {0};
                r.type = COLUMN_TYPE_BIGINT;
                r.value.as_bigint = val;
                return r;
            } else {
                /* currval */
                if (!seq->has_been_called) {
                    fprintf(stderr, "currval: sequence '%s' not yet called\n", seq_name);
                    return cell_make_null();
                }
                struct cell r = {0};
                r.type = COLUMN_TYPE_BIGINT;
                r.value.as_bigint = seq->current_value;
                return r;
            }
        }

        if (fn == FUNC_GEN_RANDOM_UUID) {
            /* generate a v4 UUID: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx */
            static int uuid_seeded = 0;
            if (!uuid_seeded) { srand((unsigned)time(NULL)); uuid_seeded = 1; }
            char buf[37];
            const char *hex = "0123456789abcdef";
            for (int i = 0; i < 36; i++) {
                if (i == 8 || i == 13 || i == 18 || i == 23) buf[i] = '-';
                else if (i == 14) buf[i] = '4'; /* version 4 */
                else if (i == 19) buf[i] = hex[8 + (rand() & 3)]; /* variant 10xx */
                else buf[i] = hex[rand() & 15];
            }
            buf[36] = '\0';
            struct cell r = {0};
            r.type = COLUMN_TYPE_UUID;
            r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
            return r;
        }

        /* ---- date/time functions ---- */

        if (fn == FUNC_NOW || fn == FUNC_CURRENT_TIMESTAMP || fn == FUNC_CURRENT_DATE) {
            time_t now = time(NULL);
            struct tm *lt = localtime(&now);
            char buf[32];
            if (fn == FUNC_CURRENT_DATE) {
                format_date(lt, buf, sizeof(buf));
                struct cell r = {0};
                r.type = COLUMN_TYPE_DATE;
                r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
                return r;
            } else {
                format_timestamp(lt, buf, sizeof(buf));
                struct cell r = {0};
                r.type = COLUMN_TYPE_TIMESTAMP;
                r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
                return r;
            }
        }

        if (fn == FUNC_EXTRACT || fn == FUNC_DATE_PART) {
            if (nargs < 2) return cell_make_null();
            struct cell field_c = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
            struct cell src_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
            if (cell_is_null(&src_c)) {
                cell_release_rb(&field_c, rb);
                cell_release_rb(&src_c, rb);
                struct cell r = {0}; r.type = COLUMN_TYPE_FLOAT; r.is_null = 1;
                return r;
            }
            const char *field = (column_type_is_text(field_c.type) && field_c.value.as_text)
                                ? field_c.value.as_text : "";
            const char *src = (column_type_is_text(src_c.type) && src_c.value.as_text)
                              ? src_c.value.as_text : "";
            struct tm tm_val;
            double result_v = 0.0;
            if (src_c.type == COLUMN_TYPE_INTERVAL) {
                /* EXTRACT from interval */
                double secs = parse_interval_to_seconds(src);
                if (strcasecmp(field, "epoch") == 0) result_v = secs;
                else if (strcasecmp(field, "hour") == 0) result_v = (int)(secs / 3600) % 24;
                else if (strcasecmp(field, "minute") == 0) result_v = (int)(secs / 60) % 60;
                else if (strcasecmp(field, "second") == 0) result_v = (int)secs % 60;
                else if (strcasecmp(field, "day") == 0) result_v = (int)(secs / 86400);
            } else if (parse_datetime(src, &tm_val)) {
                if (strcasecmp(field, "year") == 0) result_v = tm_val.tm_year + 1900;
                else if (strcasecmp(field, "month") == 0) result_v = tm_val.tm_mon + 1;
                else if (strcasecmp(field, "day") == 0) result_v = tm_val.tm_mday;
                else if (strcasecmp(field, "hour") == 0) result_v = tm_val.tm_hour;
                else if (strcasecmp(field, "minute") == 0) result_v = tm_val.tm_min;
                else if (strcasecmp(field, "second") == 0) result_v = tm_val.tm_sec;
                else if (strcasecmp(field, "dow") == 0) {
                    mktime(&tm_val);
                    result_v = tm_val.tm_wday;
                } else if (strcasecmp(field, "doy") == 0) {
                    mktime(&tm_val);
                    result_v = tm_val.tm_yday + 1;
                } else if (strcasecmp(field, "epoch") == 0) {
                    result_v = (double)mktime(&tm_val);
                } else if (strcasecmp(field, "quarter") == 0) {
                    result_v = (tm_val.tm_mon / 3) + 1;
                } else if (strcasecmp(field, "week") == 0) {
                    mktime(&tm_val);
                    result_v = (tm_val.tm_yday / 7) + 1;
                }
            }
            cell_release_rb(&field_c, rb);
            cell_release_rb(&src_c, rb);
            /* EXTRACT returns float in PostgreSQL */
            struct cell r = {0};
            r.type = COLUMN_TYPE_FLOAT;
            r.value.as_float = result_v;
            return r;
        }

        if (fn == FUNC_DATE_TRUNC) {
            if (nargs < 2) return cell_make_null();
            struct cell field_c = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
            struct cell src_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
            if (cell_is_null(&src_c)) {
                cell_release_rb(&field_c, rb);
                cell_release_rb(&src_c, rb);
                return src_c;
            }
            const char *field = (column_type_is_text(field_c.type) && field_c.value.as_text)
                                ? field_c.value.as_text : "";
            const char *src = (column_type_is_text(src_c.type) && src_c.value.as_text)
                              ? src_c.value.as_text : "";
            struct tm tm_val;
            if (parse_datetime(src, &tm_val)) {
                if (strcasecmp(field, "year") == 0) {
                    tm_val.tm_mon = 0; tm_val.tm_mday = 1;
                    tm_val.tm_hour = 0; tm_val.tm_min = 0; tm_val.tm_sec = 0;
                } else if (strcasecmp(field, "month") == 0) {
                    tm_val.tm_mday = 1;
                    tm_val.tm_hour = 0; tm_val.tm_min = 0; tm_val.tm_sec = 0;
                } else if (strcasecmp(field, "day") == 0) {
                    tm_val.tm_hour = 0; tm_val.tm_min = 0; tm_val.tm_sec = 0;
                } else if (strcasecmp(field, "hour") == 0) {
                    tm_val.tm_min = 0; tm_val.tm_sec = 0;
                } else if (strcasecmp(field, "minute") == 0) {
                    tm_val.tm_sec = 0;
                }
                /* second: no truncation needed */
                char buf[32];
                format_timestamp(&tm_val, buf, sizeof(buf));
                cell_release_rb(&field_c, rb);
                cell_release_rb(&src_c, rb);
                struct cell r = {0};
                r.type = COLUMN_TYPE_TIMESTAMP;
                r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
                return r;
            }
            cell_release_rb(&field_c, rb);
            cell_release_rb(&src_c, rb);
            return cell_make_null();
        }

        if (fn == FUNC_AGE) {
            if (nargs < 1) return cell_make_null();
            struct cell a_c = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
            struct cell b_c;
            if (nargs >= 2) {
                b_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
            } else {
                /* AGE(timestamp) = AGE(CURRENT_DATE, timestamp) */
                time_t now = time(NULL);
                struct tm *lt = localtime(&now);
                char buf[32];
                format_timestamp(lt, buf, sizeof(buf));
                b_c = a_c;
                a_c = (struct cell){0};
                a_c.type = COLUMN_TYPE_TIMESTAMP;
                a_c.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
            }
            if (cell_is_null(&a_c) || cell_is_null(&b_c)) {
                cell_release_rb(&a_c, rb);
                cell_release_rb(&b_c, rb);
                struct cell r = {0}; r.type = COLUMN_TYPE_INTERVAL; r.is_null = 1;
                return r;
            }
            struct tm tm_a, tm_b;
            const char *sa = (column_type_is_text(a_c.type) && a_c.value.as_text) ? a_c.value.as_text : "";
            const char *sb = (column_type_is_text(b_c.type) && b_c.value.as_text) ? b_c.value.as_text : "";
            if (parse_datetime(sa, &tm_a) && parse_datetime(sb, &tm_b)) {
                time_t ta = mktime(&tm_a);
                time_t tb = mktime(&tm_b);
                double diff = difftime(ta, tb);
                char buf[128];
                format_interval(diff, buf, sizeof(buf));
                cell_release_rb(&a_c, rb);
                cell_release_rb(&b_c, rb);
                struct cell r = {0};
                r.type = COLUMN_TYPE_INTERVAL;
                r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
                return r;
            }
            cell_release_rb(&a_c, rb);
            cell_release_rb(&b_c, rb);
            return cell_make_null();
        }

        if (fn == FUNC_TO_CHAR) {
            if (nargs < 2) return cell_make_null();
            struct cell src_c = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
            struct cell fmt_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
            if (cell_is_null(&src_c)) {
                cell_release_rb(&fmt_c, rb);
                return src_c;
            }
            const char *src = (column_type_is_text(src_c.type) && src_c.value.as_text)
                              ? src_c.value.as_text : "";
            const char *fmt = (column_type_is_text(fmt_c.type) && fmt_c.value.as_text)
                              ? fmt_c.value.as_text : "";
            struct tm tm_val;
            if (parse_datetime(src, &tm_val)) {
                /* simple PG format conversion: YYYY, MM, DD, HH24, MI, SS */
                char buf[256] = {0};
                char *out = buf;
                const char *fp = fmt;
                while (*fp && (size_t)(out - buf) < sizeof(buf) - 10) {
                    if (strncasecmp(fp, "YYYY", 4) == 0) {
                        out += sprintf(out, "%04d", tm_val.tm_year + 1900);
                        fp += 4;
                    } else if (strncasecmp(fp, "MM", 2) == 0) {
                        out += sprintf(out, "%02d", tm_val.tm_mon + 1);
                        fp += 2;
                    } else if (strncasecmp(fp, "DD", 2) == 0) {
                        out += sprintf(out, "%02d", tm_val.tm_mday);
                        fp += 2;
                    } else if (strncasecmp(fp, "HH24", 4) == 0) {
                        out += sprintf(out, "%02d", tm_val.tm_hour);
                        fp += 4;
                    } else if (strncasecmp(fp, "HH", 2) == 0) {
                        out += sprintf(out, "%02d", tm_val.tm_hour);
                        fp += 2;
                    } else if (strncasecmp(fp, "MI", 2) == 0) {
                        out += sprintf(out, "%02d", tm_val.tm_min);
                        fp += 2;
                    } else if (strncasecmp(fp, "SS", 2) == 0) {
                        out += sprintf(out, "%02d", tm_val.tm_sec);
                        fp += 2;
                    } else {
                        *out++ = *fp++;
                    }
                }
                *out = '\0';
                cell_release_rb(&src_c, rb);
                cell_release_rb(&fmt_c, rb);
                struct cell r = {0};
                r.type = COLUMN_TYPE_TEXT;
                r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
                return r;
            }
            cell_release_rb(&src_c, rb);
            cell_release_rb(&fmt_c, rb);
            return cell_make_null();
        }

        if (fn == FUNC_SUBSTRING) {
            if (nargs < 2) return cell_make_null();
            struct cell str = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
            struct cell from_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
            if (cell_is_null(&str)) {
                cell_release_rb(&from_c, rb);
                return str;
            }
            int from = (int)cell_to_double_val(&from_c);
            if (from < 1) from = 1;
            from--; /* convert to 0-based */
            cell_release_rb(&from_c, rb);

            if (column_type_is_text(str.type) && str.value.as_text) {
                int slen = (int)strlen(str.value.as_text);
                int len = slen - from;
                if (nargs >= 3) {
                    struct cell len_c = eval_expr(FUNC_ARG(arena, args_start, 2), arena, t, row, db, rb);
                    len = (int)cell_to_double_val(&len_c);
                    cell_release_rb(&len_c, rb);
                }
                if (from >= slen || len <= 0) {
                    if (!rb) free(str.value.as_text);
                    str.value.as_text = rb ? bump_strdup(rb, "") : strdup("");
                    return str;
                }
                if (from + len > slen) len = slen - from;
                char *sub = rb ? (char *)bump_alloc(rb, (size_t)len + 1)
                               : malloc((size_t)len + 1);
                memcpy(sub, str.value.as_text + from, (size_t)len);
                sub[len] = '\0';
                if (!rb) free(str.value.as_text);
                str.value.as_text = sub;
            }
            return str;
        }

        return cell_make_null();
    }

    case EXPR_CASE_WHEN: {
        for (uint32_t i = 0; i < e->case_when.branches_count; i++) {
            struct case_when_branch *b = &ABRANCH(arena, e->case_when.branches_start + i);
            if (eval_condition(b->cond_idx, arena, row, t, NULL))
                return eval_expr(b->then_expr_idx, arena, t, row, db, rb);
        }
        if (e->case_when.else_expr != IDX_NONE)
            return eval_expr(e->case_when.else_expr, arena, t, row, db, rb);
        return cell_make_null();
    }

    case EXPR_SUBQUERY: {
        if (!db || e->subquery.sql_idx == IDX_NONE) return cell_make_null();
        const char *orig_sql = ASTRING(arena, e->subquery.sql_idx);
        /* correlated subquery: substitute outer column refs with literals */
        char sql_buf[2048];
        size_t sql_len = strlen(orig_sql);
        if (sql_len >= sizeof(sql_buf)) sql_len = sizeof(sql_buf) - 1;
        memcpy(sql_buf, orig_sql, sql_len);
        sql_buf[sql_len] = '\0';

        if (t && row) {
            for (size_t ci = 0; ci < t->columns.count; ci++) {
                const char *cname = t->columns.items[ci].name;
                char ref[256];
                snprintf(ref, sizeof(ref), "%s.%s", t->name, cname);
                size_t rlen = strlen(ref);
                char lit[256];
                struct cell *cv = &row->cells.items[ci];
                if (cell_is_null(cv)) {
                    snprintf(lit, sizeof(lit), "NULL");
                } else if (cv->type == COLUMN_TYPE_INT) {
                    snprintf(lit, sizeof(lit), "%d", cv->value.as_int);
                } else if (cv->type == COLUMN_TYPE_FLOAT) {
                    snprintf(lit, sizeof(lit), "%g", cv->value.as_float);
                } else if (column_type_is_text(cv->type) && cv->value.as_text) {
                    snprintf(lit, sizeof(lit), "'%s'", cv->value.as_text);
                } else {
                    continue;
                }
                size_t llen = strlen(lit);
                char *pos;
                while ((pos = strstr(sql_buf, ref)) != NULL) {
                    char after = pos[rlen];
                    if (isalnum((unsigned char)after) || after == '_') break;
                    size_t cur_len = strlen(sql_buf);
                    if (cur_len - rlen + llen >= sizeof(sql_buf) - 1) break;
                    memmove(pos + llen, pos + rlen,
                            cur_len - (size_t)(pos - sql_buf) - rlen + 1);
                    memcpy(pos, lit, llen);
                }
            }
        }

        struct query sub_q = {0};
        if (query_parse(sql_buf, &sub_q) == 0) {
            struct rows sub_rows = {0};
            if (db_exec(db, &sub_q, &sub_rows, NULL) == 0 && sub_rows.count > 0
                && sub_rows.data[0].cells.count > 0) {
                struct cell result = cell_deep_copy_rb(&sub_rows.data[0].cells.items[0], rb);
                for (size_t ri = 0; ri < sub_rows.count; ri++)
                    row_free(&sub_rows.data[ri]);
                free(sub_rows.data);
                query_free(&sub_q);
                return result;
            }
            for (size_t ri = 0; ri < sub_rows.count; ri++)
                row_free(&sub_rows.data[ri]);
            free(sub_rows.data);
        }
        query_free(&sub_q);
        return cell_make_null();
    }

    case EXPR_CAST: {
        struct cell src = eval_expr(e->cast.operand, arena, t, row, db, rb);
        if (cell_is_null(&src)) {
            src.type = e->cast.target;
            return src;
        }
        enum column_type target = e->cast.target;

        /* already the right type? */
        if (src.type == target) return src;

        /* numeric → numeric conversions */
        if ((target == COLUMN_TYPE_INT || target == COLUMN_TYPE_BIGINT ||
             target == COLUMN_TYPE_FLOAT || target == COLUMN_TYPE_NUMERIC) &&
            (src.type == COLUMN_TYPE_INT || src.type == COLUMN_TYPE_BIGINT ||
             src.type == COLUMN_TYPE_FLOAT || src.type == COLUMN_TYPE_NUMERIC)) {
            double v = cell_to_double_val(&src);
            cell_release_rb(&src, rb);
            struct cell r = {0};
            r.type = target;
            if (target == COLUMN_TYPE_INT) {
                r.value.as_int = (int)v;
            } else if (target == COLUMN_TYPE_BIGINT) {
                r.value.as_bigint = (long long)v;
            } else if (target == COLUMN_TYPE_FLOAT) {
                r.value.as_float = v;
            } else { /* NUMERIC */
                r.value.as_float = v;
            }
            return r;
        }

        /* numeric → text */
        if (column_type_is_text(target) &&
            (src.type == COLUMN_TYPE_INT || src.type == COLUMN_TYPE_BIGINT ||
             src.type == COLUMN_TYPE_FLOAT || src.type == COLUMN_TYPE_NUMERIC ||
             src.type == COLUMN_TYPE_BOOLEAN)) {
            char buf[128];
            if (src.type == COLUMN_TYPE_INT)
                snprintf(buf, sizeof(buf), "%d", src.value.as_int);
            else if (src.type == COLUMN_TYPE_BIGINT)
                snprintf(buf, sizeof(buf), "%lld", src.value.as_bigint);
            else if (src.type == COLUMN_TYPE_FLOAT || src.type == COLUMN_TYPE_NUMERIC)
                snprintf(buf, sizeof(buf), "%g", src.value.as_float);
            else /* BOOLEAN */
                snprintf(buf, sizeof(buf), "%s", src.value.as_bool ? "true" : "false");
            cell_release_rb(&src, rb);
            struct cell r = {0};
            r.type = target;
            r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
            return r;
        }

        /* text → numeric */
        if ((target == COLUMN_TYPE_INT || target == COLUMN_TYPE_BIGINT ||
             target == COLUMN_TYPE_FLOAT || target == COLUMN_TYPE_NUMERIC) &&
            column_type_is_text(src.type) && src.value.as_text) {
            const char *s = src.value.as_text;
            struct cell r = {0};
            r.type = target;
            if (target == COLUMN_TYPE_INT) {
                r.value.as_int = atoi(s);
            } else if (target == COLUMN_TYPE_BIGINT) {
                r.value.as_bigint = atoll(s);
            } else { /* FLOAT or NUMERIC */
                r.value.as_float = atof(s);
            }
            cell_release_rb(&src, rb);
            return r;
        }

        /* text → boolean */
        if (target == COLUMN_TYPE_BOOLEAN && column_type_is_text(src.type) && src.value.as_text) {
            const char *s = src.value.as_text;
            int bval = (strcasecmp(s, "true") == 0 || strcasecmp(s, "t") == 0 ||
                        strcasecmp(s, "yes") == 0 || strcasecmp(s, "1") == 0);
            cell_release_rb(&src, rb);
            struct cell r = {0};
            r.type = COLUMN_TYPE_BOOLEAN;
            r.value.as_bool = bval;
            return r;
        }

        /* boolean → int */
        if (target == COLUMN_TYPE_INT && src.type == COLUMN_TYPE_BOOLEAN) {
            int v = src.value.as_bool ? 1 : 0;
            return cell_make_int(v);
        }

        /* text → text-like (DATE, TIMESTAMP, etc.) — just change the type tag */
        if (column_type_is_text(target) && column_type_is_text(src.type)) {
            src.type = target;
            return src;
        }

        /* fallback: convert to text first, then to target */
        char *txt = cell_to_text_rb(&src, rb);
        cell_release_rb(&src, rb);
        struct cell r = {0};
        r.type = target;
        if (column_type_is_text(target)) {
            r.value.as_text = txt;
        } else if (target == COLUMN_TYPE_INT) {
            r.value.as_int = txt ? atoi(txt) : 0;
            if (!rb && txt) free(txt);
        } else if (target == COLUMN_TYPE_BIGINT) {
            r.value.as_bigint = txt ? atoll(txt) : 0;
            if (!rb && txt) free(txt);
        } else if (target == COLUMN_TYPE_FLOAT || target == COLUMN_TYPE_NUMERIC) {
            r.value.as_float = txt ? atof(txt) : 0.0;
            if (!rb && txt) free(txt);
        } else if (target == COLUMN_TYPE_BOOLEAN) {
            r.value.as_bool = (txt && (strcasecmp(txt, "true") == 0 || strcmp(txt, "1") == 0)) ? 1 : 0;
            if (!rb && txt) free(txt);
        } else {
            r.value.as_text = txt; /* DATE, TIMESTAMP, etc. */
        }
        return r;
    }

    }
    return cell_make_null();
}

/* JPL ownership: emit_row receives cells with owned text from eval_expr,
 * eval_scalar_func, and resolve_arg.  It pushes them into the result row
 * without copying — ownership transfers from the producer to the result row.
 * row_free() is the single release point for all cell text in the result.
 * The select_all path strdup's source cells (alloc+push in same scope).
 * When rb is non-NULL, all text is bump-allocated (bulk-freed). */
static void emit_row(struct table *t, struct query_select *s, struct query_arena *arena,
                     struct row *src, struct rows *result, int select_all,
                     struct database *db, struct bump_alloc *rb)
{
    struct row dst = {0};
    da_init(&dst.cells);

    if (select_all) {
        for (size_t j = 0; j < src->cells.count; j++) {
            struct cell c = src->cells.items[j];
            struct cell copy = { .type = c.type, .is_null = c.is_null };
            if (column_type_is_text(c.type) && c.value.as_text)
                copy.value.as_text = rb ? bump_strdup(rb, c.value.as_text)
                                        : strdup(c.value.as_text);
            else
                copy.value = c.value;
            da_push(&dst.cells, copy);
        }
    } else if (s->parsed_columns_count > 0) {
        /* AST-based column evaluation */
        for (uint32_t i = 0; i < s->parsed_columns_count; i++) {
            struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + i];
            struct cell c = eval_expr(sc->expr_idx, arena, t, src, db, rb);
            da_push(&dst.cells, c);
        }
    } else {
        /* legacy: walk comma-separated column list (used by aggregate/window paths) */
        sv cols = s->columns;
        while (cols.len > 0) {
            /* trim leading whitespace */
            while (cols.len > 0 && (cols.data[0] == ' ' || cols.data[0] == '\t'))
                { cols.data++; cols.len--; }
            /* find end of this column name (comma or end), respecting parens and CASE..END */
            size_t end = 0;
            int paren_depth = 0;
            int case_depth = 0;
            while (end < cols.len) {
                char ch = cols.data[end];
                if (ch == '(') paren_depth++;
                else if (ch == ')') { if (paren_depth > 0) paren_depth--; }
                else if (paren_depth == 0 && case_depth == 0 && ch == ',') break;
                /* track CASE...END */
                if (paren_depth == 0 && end + 4 <= cols.len &&
                    strncasecmp(cols.data + end, "CASE", 4) == 0 &&
                    (end == 0 || cols.data[end-1] == ' ' || cols.data[end-1] == ',') &&
                    (end + 4 >= cols.len || cols.data[end+4] == ' '))
                    case_depth++;
                if (paren_depth == 0 && case_depth > 0 && end + 3 <= cols.len &&
                    strncasecmp(cols.data + end, "END", 3) == 0 &&
                    (end == 0 || cols.data[end-1] == ' ') &&
                    (end + 3 >= cols.len || cols.data[end+3] == ' ' || cols.data[end+3] == ','))
                    case_depth--;
                end++;
            }
            sv one = sv_from(cols.data, end);
            /* trim trailing whitespace */
            while (one.len > 0 && (one.data[one.len - 1] == ' ' || one.data[one.len - 1] == '\t'))
                one.len--;

            /* strip column alias: "col AS alias" -> "col" (skip for COALESCE/CASE) */
            if (!sv_starts_with_ci(one, "COALESCE") && !sv_starts_with_ci(one, "CASE")) {
                for (size_t k = 0; k + 1 < one.len; k++) {
                    if ((one.data[k] == ' ' || one.data[k] == '\t') &&
                        (k + 3 <= one.len) &&
                        (one.data[k+1] == 'A' || one.data[k+1] == 'a') &&
                        (one.data[k+2] == 'S' || one.data[k+2] == 's') &&
                        (k + 3 == one.len || one.data[k+3] == ' ' || one.data[k+3] == '\t')) {
                        one.len = k;
                        while (one.len > 0 && (one.data[one.len-1] == ' ' || one.data[one.len-1] == '\t'))
                            one.len--;
                        break;
                    }
                }
            }

            /* strip table prefix (e.g. t1.col -> col), but not for parenthesized exprs */
            if (one.len == 0 || one.data[0] != '(') {
                for (size_t k = 0; k < one.len; k++) {
                    if (one.data[k] == '.') {
                        one = sv_from(one.data + k + 1, one.len - k - 1);
                        break;
                    }
                }
            }

            /* subquery in SELECT list: (SELECT ...) */
            if (one.len > 2 && one.data[0] == '(' && db) {
                /* check if it starts with (SELECT */
                sv inner = sv_from(one.data + 1, one.len - 1);
                while (inner.len > 0 && (inner.data[0] == ' ' || inner.data[0] == '\t'))
                    { inner.data++; inner.len--; }
                if (inner.len >= 6 && strncasecmp(inner.data, "SELECT", 6) == 0) {
                    /* extract SQL between parens */
                    size_t sql_start = 1;
                    while (sql_start < one.len && (one.data[sql_start] == ' ' || one.data[sql_start] == '\t'))
                        sql_start++;
                    size_t sql_end = one.len;
                    if (sql_end > 0 && one.data[sql_end - 1] == ')') sql_end--;
                    char sql_buf[2048];
                    size_t sql_len = sql_end - sql_start;
                    if (sql_len >= sizeof(sql_buf)) sql_len = sizeof(sql_buf) - 1;
                    memcpy(sql_buf, one.data + sql_start, sql_len);
                    sql_buf[sql_len] = '\0';
                    /* correlated subquery: substitute outer column refs with literals */
                    for (size_t ci = 0; ci < t->columns.count; ci++) {
                        const char *cname = t->columns.items[ci].name;
                        /* build "table.col" reference pattern using the table name */
                        char ref[256];
                        snprintf(ref, sizeof(ref), "%s.%s", t->name, cname);
                        size_t rlen = strlen(ref);
                        /* build literal replacement */
                        char lit[256];
                        struct cell *cv = &src->cells.items[ci];
                        if (cv->is_null || (column_type_is_text(cv->type) && !cv->value.as_text)) {
                            snprintf(lit, sizeof(lit), "NULL");
                        } else if (cv->type == COLUMN_TYPE_INT) {
                            snprintf(lit, sizeof(lit), "%d", cv->value.as_int);
                        } else if (cv->type == COLUMN_TYPE_FLOAT) {
                            snprintf(lit, sizeof(lit), "%g", cv->value.as_float);
                        } else if (column_type_is_text(cv->type) && cv->value.as_text) {
                            snprintf(lit, sizeof(lit), "'%s'", cv->value.as_text);
                        } else {
                            continue;
                        }
                        size_t llen = strlen(lit);
                        /* replace all occurrences of ref in sql_buf */
                        char *pos;
                        while ((pos = strstr(sql_buf, ref)) != NULL) {
                            /* ensure it's a word boundary */
                            char after = pos[rlen];
                            if (isalnum((unsigned char)after) || after == '_') break;
                            size_t cur_len = strlen(sql_buf);
                            if (cur_len - rlen + llen >= sizeof(sql_buf) - 1) break;
                            memmove(pos + llen, pos + rlen, cur_len - (size_t)(pos - sql_buf) - rlen + 1);
                            memcpy(pos, lit, llen);
                        }
                    }
                    struct query sub_q = {0};
                    if (query_parse(sql_buf, &sub_q) == 0) {
                        struct rows sub_rows = {0};
                        if (db_exec(db, &sub_q, &sub_rows, NULL) == 0 && sub_rows.count > 0
                            && sub_rows.data[0].cells.count > 0) {
                            struct cell *sc = &sub_rows.data[0].cells.items[0];
                            struct cell copy = { .type = sc->type, .is_null = sc->is_null };
                            if (column_type_is_text(sc->type) && sc->value.as_text)
                                copy.value.as_text = rb ? bump_strdup(rb, sc->value.as_text)
                                                        : strdup(sc->value.as_text);
                            else
                                copy.value = sc->value;
                            da_push(&dst.cells, copy);
                        } else {
                            struct cell null_cell = { .type = COLUMN_TYPE_TEXT, .is_null = 1 };
                            da_push(&dst.cells, null_cell);
                        }
                        for (size_t ri = 0; ri < sub_rows.count; ri++)
                            row_free(&sub_rows.data[ri]);
                        free(sub_rows.data);
                    } else {
                        struct cell null_cell = { .type = COLUMN_TYPE_TEXT, .is_null = 1 };
                        da_push(&dst.cells, null_cell);
                    }
                    query_free(&sub_q);
                    goto next_col;
                }
            }
            if (sv_starts_with_ci(one, "NULLIF") ||
                sv_starts_with_ci(one, "GREATEST") ||
                sv_starts_with_ci(one, "LEAST") ||
                sv_starts_with_ci(one, "UPPER") ||
                sv_starts_with_ci(one, "LOWER") ||
                sv_starts_with_ci(one, "LENGTH") ||
                sv_starts_with_ci(one, "TRIM")) {
                struct cell c = eval_scalar_func(one, t, src);
                da_push(&dst.cells, c);
            } else if (sv_starts_with_ci(one, "COALESCE")) {
                struct cell c = eval_coalesce(one, t, src);
                da_push(&dst.cells, c);
            } else if (sv_starts_with_ci(one, "CASE")) {
                struct cell c = eval_case_when(one, t, src);
                da_push(&dst.cells, c);
            } else if (has_arith_op(one)) {
                struct cell c = eval_arith_expr(one, t, src);
                da_push(&dst.cells, c);
            } else {
                for (size_t j = 0; j < t->columns.count; j++) {
                    if (sv_eq_cstr(one, t->columns.items[j].name)) {
                        struct cell c = src->cells.items[j];
                        struct cell copy = { .type = c.type, .is_null = c.is_null };
                        if (column_type_is_text(c.type) && c.value.as_text)
                            copy.value.as_text = rb ? bump_strdup(rb, c.value.as_text)
                                                    : strdup(c.value.as_text);
                        else
                            copy.value = c.value;
                        da_push(&dst.cells, copy);
                        break;
                    }
                }
            }

next_col:
            if (end < cols.len) end++; /* skip comma */
            cols = sv_from(cols.data + end, cols.len - end);
        }
    }

    rows_push(result, dst);
}

int query_aggregate(struct table *t, struct query_select *s, struct query_arena *arena, struct rows *result)
{
    /* find WHERE column index if applicable (legacy path) */
    int where_col = -1;
    if (s->where.has_where && s->where.where_cond == IDX_NONE) {
        for (size_t j = 0; j < t->columns.count; j++) {
            if (sv_eq_cstr(s->where.where_column, t->columns.items[j].name)) {
                where_col = (int)j;
                break;
            }
        }
        if (where_col < 0) {
            fprintf(stderr, "WHERE column '" SV_FMT "' not found\n",
                    SV_ARG(s->where.where_column));
            return -1;
        }
    }

    uint32_t naggs = s->aggregates_count;
    /* resolve column index for each aggregate */
    int *agg_col = bump_calloc(&arena->scratch, naggs, sizeof(int));
    for (uint32_t a = 0; a < naggs; a++) {
        struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
        if (sv_eq_cstr(ae->column, "*")) {
            agg_col[a] = -1; /* COUNT(*) doesn't need a column */
        } else {
            agg_col[a] = -1;
            for (size_t j = 0; j < t->columns.count; j++) {
                if (sv_eq_cstr(ae->column, t->columns.items[j].name)) {
                    agg_col[a] = (int)j;
                    break;
                }
            }
            if (agg_col[a] < 0) {
                fprintf(stderr, "aggregate column '" SV_FMT "' not found\n",
                        SV_ARG(ae->column));
                return -1;
            }
        }
    }

    /* accumulate — single allocation for all aggregate arrays
     * layout: double[3*N] | size_t[N] | int[N]  (descending alignment) */
    size_t _nagg = naggs;
    size_t _agg_alloc = _nagg * (3 * sizeof(double) + sizeof(size_t) + sizeof(int));
    char *_agg_buf = bump_calloc(&arena->scratch, 1, _agg_alloc ? _agg_alloc : 1);
    double *sums = (double *)_agg_buf;
    double *mins = sums + _nagg;
    double *maxs = mins + _nagg;
    size_t *nonnull_count = (size_t *)(maxs + _nagg);
    int *minmax_init = (int *)(nonnull_count + _nagg);
    size_t row_count = 0;

    for (size_t i = 0; i < t->rows.count; i++) {
        if (s->where.has_where) {
            if (s->where.where_cond != IDX_NONE) {
                if (!eval_condition(s->where.where_cond, arena, &t->rows.items[i], t, NULL))
                    continue;
            } else if (where_col >= 0) {
                if (!cell_equal(&t->rows.items[i].cells.items[where_col],
                                &s->where.where_value))
                    continue;
            }
        }
        row_count++;
        for (uint32_t a = 0; a < naggs; a++) {
            if (agg_col[a] < 0) continue;
            struct cell *c = &t->rows.items[i].cells.items[agg_col[a]];
            /* skip NULL values (SQL standard) */
            if (c->is_null || (column_type_is_text(c->type) && !c->value.as_text))
                continue;
            nonnull_count[a]++;
            double v = cell_to_double(c);
            sums[a] += v;
            if (!minmax_init[a] || v < mins[a]) mins[a] = v;
            if (!minmax_init[a] || v > maxs[a]) maxs[a] = v;
            minmax_init[a] = 1;
        }
    }

    /* build result row */
    struct row dst = {0};
    da_init(&dst.cells);
    for (uint32_t a = 0; a < naggs; a++) {
        struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
        struct cell c = {0};
        int col_is_float = (agg_col[a] >= 0 &&
                            t->columns.items[agg_col[a]].type == COLUMN_TYPE_FLOAT);
        switch (ae->func) {
            case AGG_COUNT:
                c.type = COLUMN_TYPE_INT;
                if (ae->has_distinct && agg_col[a] >= 0) {
                    /* COUNT(DISTINCT col): count unique non-NULL values */
                    int distinct_count = 0;
                    struct cell *seen = NULL;
                    if (nonnull_count[a] > 0)
                        seen = bump_calloc(&arena->scratch, nonnull_count[a], sizeof(struct cell));
                    for (size_t i = 0; i < t->rows.count; i++) {
                        if (s->where.has_where) {
                            if (s->where.where_cond != IDX_NONE) {
                                if (!eval_condition(s->where.where_cond, arena, &t->rows.items[i], t, NULL))
                                    continue;
                            } else if (where_col >= 0) {
                                if (!cell_equal(&t->rows.items[i].cells.items[where_col], &s->where.where_value))
                                    continue;
                            }
                        }
                        struct cell *cv = &t->rows.items[i].cells.items[agg_col[a]];
                        if (cv->is_null || (column_type_is_text(cv->type) && !cv->value.as_text))
                            continue;
                        int dup = 0;
                        for (int d = 0; d < distinct_count; d++) {
                            if (cell_compare(cv, &seen[d]) == 0) { dup = 1; break; }
                        }
                        if (!dup) { seen[distinct_count++] = *cv; }
                    }
                    c.value.as_int = distinct_count;
                } else {
                    /* COUNT(*) counts all rows; COUNT(col) counts non-NULL */
                    c.value.as_int = (agg_col[a] < 0) ? (int)row_count : (int)nonnull_count[a];
                }
                break;
            case AGG_SUM:
                if (nonnull_count[a] == 0) {
                    c.type = col_is_float ? COLUMN_TYPE_FLOAT : COLUMN_TYPE_INT;
                    c.is_null = 1;
                } else if (col_is_float) {
                    c.type = COLUMN_TYPE_FLOAT;
                    c.value.as_float = sums[a];
                } else {
                    c.type = COLUMN_TYPE_INT;
                    c.value.as_int = (int)sums[a];
                }
                break;
            case AGG_AVG:
                c.type = COLUMN_TYPE_FLOAT;
                if (nonnull_count[a] == 0) {
                    c.is_null = 1;
                } else {
                    c.value.as_float = sums[a] / (double)nonnull_count[a];
                }
                break;
            case AGG_MIN:
            case AGG_MAX: {
                if (nonnull_count[a] == 0) {
                    c.type = col_is_float ? COLUMN_TYPE_FLOAT : COLUMN_TYPE_INT;
                    c.is_null = 1;
                } else {
                    double val = (ae->func == AGG_MIN) ? mins[a] : maxs[a];
                    if (col_is_float) {
                        c.type = COLUMN_TYPE_FLOAT;
                        c.value.as_float = val;
                    } else {
                        c.type = COLUMN_TYPE_INT;
                        c.value.as_int = (int)val;
                    }
                }
                break;
            }
            case AGG_NONE:
                break;
        }
        da_push(&dst.cells, c);
    }
    rows_push(result, dst);

    return 0;
}

/* copy_cell_into forward decl removed — using shared cell_copy from row.h */

/* find_col_idx → use shared table_find_column_sv from table.h */

/* qsort context for multi-column ORDER BY (single-threaded, so static is fine) */
struct sort_ctx {
    int *cols;
    int *descs;
    size_t ncols;
    struct rows *rows;     /* for sorting rows directly */
    struct table *table;   /* for sorting indices into table rows */
    size_t *indices;       /* index array (when sorting indices) */
};
static struct sort_ctx _sort_ctx;

/* cell_compare → now shared from row.h */

/* qsort comparator: compare two struct row* by multi-column ORDER BY */
static int cmp_rows_multi(const void *a, const void *b)
{
    const struct row *ra = (const struct row *)a;
    const struct row *rb = (const struct row *)b;
    for (size_t k = 0; k < _sort_ctx.ncols; k++) {
        int ci = _sort_ctx.cols[k];
        if (ci < 0) continue;
        int cmp = cell_compare(&ra->cells.items[ci], &rb->cells.items[ci]);
        if (_sort_ctx.descs[k]) cmp = -cmp;
        if (cmp != 0) return cmp;
    }
    return 0;
}

/* qsort comparator: compare two size_t indices into _sort_ctx.table->rows */
static int cmp_indices_multi(const void *a, const void *b)
{
    size_t ia = *(const size_t *)a;
    size_t ib = *(const size_t *)b;
    struct table *t = _sort_ctx.table;
    for (size_t k = 0; k < _sort_ctx.ncols; k++) {
        int ci = _sort_ctx.cols[k];
        if (ci < 0) continue;
        int cmp = cell_compare(&t->rows.items[ia].cells.items[ci],
                               &t->rows.items[ib].cells.items[ci]);
        if (_sort_ctx.descs[k]) cmp = -cmp;
        if (cmp != 0) return cmp;
    }
    return 0;
}

static double cell_to_double(const struct cell *c)
{
    switch (c->type) {
        case COLUMN_TYPE_INT:     return (double)c->value.as_int;
        case COLUMN_TYPE_FLOAT:   return c->value.as_float;
        case COLUMN_TYPE_BOOLEAN: return (double)c->value.as_bool;
        case COLUMN_TYPE_BIGINT:  return (double)c->value.as_bigint;
        case COLUMN_TYPE_NUMERIC: return c->value.as_numeric;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID:
            return 0.0;
    }
    return 0.0;
}

/* sort helper for window ORDER BY */
struct sort_entry { size_t idx; const struct cell *key; };
static int _sort_entry_desc; /* 0=ASC, 1=DESC */

static int sort_entry_cmp(const void *a, const void *b)
{
    const struct sort_entry *sa = a, *sb = b;
    int cmp = cell_compare(sa->key, sb->key);
    return _sort_entry_desc ? -cmp : cmp;
}

static int query_window(struct table *t, struct query_select *s, struct query_arena *arena, struct rows *result)
{
    size_t nrows = t->rows.count;
    size_t nexprs = s->select_exprs_count;

    /* resolve column indices for plain columns and window args — single allocation */
    int *_win_buf = bump_calloc(&arena->scratch, 4 * nexprs + 1, sizeof(int));
    int *col_idx = _win_buf;
    int *part_idx = _win_buf + nexprs;
    int *ord_idx = _win_buf + 2 * nexprs;
    int *arg_idx = _win_buf + 3 * nexprs;

    for (size_t e = 0; e < nexprs; e++) {
        struct select_expr *se = &arena->select_exprs.items[s->select_exprs_start + e];
        col_idx[e] = -1;
        part_idx[e] = -1;
        ord_idx[e] = -1;
        arg_idx[e] = -1;

        if (se->kind == SEL_COLUMN) {
            col_idx[e] = table_find_column_sv(t, se->column);
            if (col_idx[e] < 0) {
                fprintf(stderr, "column '" SV_FMT "' not found\n", SV_ARG(se->column));
                return -1;
            }
        } else {
            if (se->win.has_partition) {
                part_idx[e] = table_find_column_sv(t, se->win.partition_col);
                if (part_idx[e] < 0) {
                    fprintf(stderr, "partition column '" SV_FMT "' not found\n",
                            SV_ARG(se->win.partition_col));
                    return -1;
                }
            }
            if (se->win.has_order) {
                ord_idx[e] = table_find_column_sv(t, se->win.order_col);
                if (ord_idx[e] < 0) {
                    fprintf(stderr, "order column '" SV_FMT "' not found\n",
                            SV_ARG(se->win.order_col));
                    return -1;
                }
            }
            if (se->win.arg_column.len > 0 && !sv_eq_cstr(se->win.arg_column, "*")) {
                arg_idx[e] = table_find_column_sv(t, se->win.arg_column);
                if (arg_idx[e] < 0) {
                    fprintf(stderr, "window arg column '" SV_FMT "' not found\n",
                            SV_ARG(se->win.arg_column));
                    return -1;
                }
            }
        }
    }

    /* collect rows matching WHERE (or all rows if no WHERE) */
    size_t *win_match_items = (size_t *)bump_alloc(&arena->scratch,
                               (nrows ? nrows : 1) * sizeof(size_t));
    size_t nmatch = 0;
    for (size_t i = 0; i < nrows; i++) {
        if (!row_matches(t, &s->where, arena, &t->rows.items[i], NULL))
            continue;
        win_match_items[nmatch++] = i;
    }

    /* build sorted row index (by first ORDER BY we find, or original order) */
    struct sort_entry *sorted = bump_calloc(&arena->scratch, nmatch ? nmatch : 1, sizeof(struct sort_entry));
    int global_ord = -1;
    for (size_t e = 0; e < nexprs; e++) {
        if (arena->select_exprs.items[s->select_exprs_start + e].kind == SEL_WINDOW && ord_idx[e] >= 0) {
            global_ord = ord_idx[e];
            break;
        }
    }
    for (size_t i = 0; i < nmatch; i++) {
        sorted[i].idx = win_match_items[i];
        if (global_ord >= 0)
            sorted[i].key = &t->rows.items[win_match_items[i]].cells.items[global_ord];
        else
            sorted[i].key = NULL;
    }
    int global_ord_desc = 0;
    for (size_t e = 0; e < nexprs; e++) {
        if (arena->select_exprs.items[s->select_exprs_start + e].kind == SEL_WINDOW && ord_idx[e] >= 0) {
            global_ord_desc = arena->select_exprs.items[s->select_exprs_start + e].win.order_desc;
            break;
        }
    }
    if (global_ord >= 0) {
        _sort_entry_desc = global_ord_desc;
        qsort(sorted, nmatch, sizeof(struct sort_entry), sort_entry_cmp);
    }

    /* for each row (in sorted order), compute all expressions */
    for (size_t ri = 0; ri < nmatch; ri++) {
        size_t row_i = sorted[ri].idx;
        struct row *src = &t->rows.items[row_i];
        struct row dst = {0};
        da_init(&dst.cells);

        for (size_t e = 0; e < nexprs; e++) {
            struct select_expr *se = &arena->select_exprs.items[s->select_exprs_start + e];
            struct cell c = {0};

            if (se->kind == SEL_COLUMN) {
                cell_copy(&c, &src->cells.items[col_idx[e]]);
            } else {
                /* compute window value */
                /* find partition peers: rows with same partition column value */
                size_t part_count = 0;
                size_t part_rank = 0;
                int rank_set = 0;

                for (size_t rj = 0; rj < nmatch; rj++) {
                    size_t j = sorted[rj].idx;
                    struct row *peer = &t->rows.items[j];

                    /* check partition match */
                    if (se->win.has_partition && part_idx[e] >= 0) {
                        if (!cell_equal_nullsafe(&src->cells.items[part_idx[e]],
                                        &peer->cells.items[part_idx[e]]))
                            continue;
                    }
                    part_count++;

                    /* for ROW_NUMBER/RANK, track position */
                    if (j == row_i && !rank_set) {
                        part_rank = part_count;
                        rank_set = 1;
                    }
                }

                switch (se->win.func) {
                    case WIN_ROW_NUMBER:
                        c.type = COLUMN_TYPE_INT;
                        c.value.as_int = (int)part_rank;
                        break;
                    case WIN_RANK:
                        c.type = COLUMN_TYPE_INT;
                        /* RANK: count peers with strictly better order key + 1 */
                        if (se->win.has_order && ord_idx[e] >= 0) {
                            int rank = 1;
                            for (size_t rj = 0; rj < nmatch; rj++) {
                                size_t j = sorted[rj].idx;
                                if (se->win.has_partition && part_idx[e] >= 0) {
                                    if (!cell_equal_nullsafe(&src->cells.items[part_idx[e]],
                                                    &t->rows.items[j].cells.items[part_idx[e]]))
                                        continue;
                                }
                                int cmp = cell_compare(&t->rows.items[j].cells.items[ord_idx[e]],
                                                       &src->cells.items[ord_idx[e]]);
                                /* ASC: count peers with smaller key; DESC: count peers with larger key */
                                if (se->win.order_desc ? (cmp > 0) : (cmp < 0))
                                    rank++;
                            }
                            c.value.as_int = rank;
                        } else {
                            c.value.as_int = (int)part_rank;
                        }
                        break;
                    case WIN_SUM:
                    case WIN_COUNT:
                    case WIN_AVG: {
                        /* build partition-local index for frame support */
                        size_t pi[4096];
                        size_t pn = 0;
                        size_t my_pos = 0;
                        for (size_t rj = 0; rj < nmatch && pn < 4096; rj++) {
                            size_t j = sorted[rj].idx;
                            if (se->win.has_partition && part_idx[e] >= 0 &&
                                !cell_equal_nullsafe(&src->cells.items[part_idx[e]],
                                                &t->rows.items[j].cells.items[part_idx[e]]))
                                continue;
                            if (j == row_i) my_pos = pn;
                            pi[pn++] = j;
                        }
                        /* compute frame bounds */
                        size_t fs = 0, fe = pn; /* default: entire partition */
                        if (se->win.has_frame) {
                            switch (se->win.frame_start) {
                                case FRAME_UNBOUNDED_PRECEDING: fs = 0; break;
                                case FRAME_CURRENT_ROW: fs = my_pos; break;
                                case FRAME_N_PRECEDING: fs = (my_pos >= (size_t)se->win.frame_start_n) ? my_pos - (size_t)se->win.frame_start_n : 0; break;
                                case FRAME_N_FOLLOWING: fs = my_pos + (size_t)se->win.frame_start_n; break;
                                case FRAME_UNBOUNDED_FOLLOWING: fs = pn; break;
                            }
                            switch (se->win.frame_end) {
                                case FRAME_UNBOUNDED_FOLLOWING: fe = pn; break;
                                case FRAME_CURRENT_ROW: fe = my_pos + 1; break;
                                case FRAME_N_FOLLOWING: fe = my_pos + (size_t)se->win.frame_end_n + 1; if (fe > pn) fe = pn; break;
                                case FRAME_N_PRECEDING: fe = (my_pos >= (size_t)se->win.frame_end_n) ? my_pos - (size_t)se->win.frame_end_n + 1 : 0; break;
                                case FRAME_UNBOUNDED_PRECEDING: fe = 0; break;
                            }
                            if (fs > pn) fs = pn;
                        }
                        /* aggregate within frame */
                        double frame_sum = 0.0;
                        int frame_nn = 0;
                        int frame_count = 0;
                        for (size_t fi = fs; fi < fe; fi++) {
                            size_t j = pi[fi];
                            frame_count++;
                            if (arg_idx[e] >= 0) {
                                struct cell *ac = &t->rows.items[j].cells.items[arg_idx[e]];
                                if (!ac->is_null && !(column_type_is_text(ac->type) && !ac->value.as_text)) {
                                    frame_sum += cell_to_double(ac);
                                    frame_nn++;
                                }
                            }
                        }
                        if (se->win.func == WIN_SUM) {
                            if (arg_idx[e] >= 0 &&
                                t->columns.items[arg_idx[e]].type == COLUMN_TYPE_FLOAT) {
                                c.type = COLUMN_TYPE_FLOAT;
                                c.value.as_float = frame_sum;
                            } else {
                                c.type = COLUMN_TYPE_INT;
                                c.value.as_int = (int)frame_sum;
                            }
                        } else if (se->win.func == WIN_COUNT) {
                            c.type = COLUMN_TYPE_INT;
                            c.value.as_int = (arg_idx[e] >= 0) ? frame_nn : frame_count;
                        } else { /* WIN_AVG */
                            c.type = COLUMN_TYPE_FLOAT;
                            if (frame_nn > 0) {
                                c.value.as_float = frame_sum / (double)frame_nn;
                            } else {
                                c.is_null = 1;
                            }
                        }
                        break;
                    }
                    case WIN_DENSE_RANK: {
                        c.type = COLUMN_TYPE_INT;
                        if (se->win.has_order && ord_idx[e] >= 0) {
                            /* count distinct order key values that are strictly less */
                            int rank = 1;
                            /* collect unique order key values from partition peers */
                            for (size_t rj = 0; rj < nmatch; rj++) {
                                size_t j = sorted[rj].idx;
                                if (se->win.has_partition && part_idx[e] >= 0 &&
                                    !cell_equal_nullsafe(&src->cells.items[part_idx[e]],
                                                    &t->rows.items[j].cells.items[part_idx[e]]))
                                    continue;
                                int cmp = cell_compare(&t->rows.items[j].cells.items[ord_idx[e]],
                                                       &src->cells.items[ord_idx[e]]);
                                if (se->win.order_desc ? (cmp > 0) : (cmp < 0)) {
                                    /* check if this value is distinct from all previously counted */
                                    int is_dup = 0;
                                    for (size_t rk = 0; rk < rj; rk++) {
                                        size_t k = sorted[rk].idx;
                                        if (se->win.has_partition && part_idx[e] >= 0 &&
                                            !cell_equal_nullsafe(&src->cells.items[part_idx[e]],
                                                            &t->rows.items[k].cells.items[part_idx[e]]))
                                            continue;
                                        if (cell_equal_nullsafe(&t->rows.items[j].cells.items[ord_idx[e]],
                                                                &t->rows.items[k].cells.items[ord_idx[e]])) {
                                            is_dup = 1; break;
                                        }
                                    }
                                    if (!is_dup) rank++;
                                }
                            }
                            c.value.as_int = rank;
                        } else {
                            c.value.as_int = (int)part_rank;
                        }
                        break;
                    }
                    case WIN_NTILE: {
                        c.type = COLUMN_TYPE_INT;
                        int nbuckets = se->win.offset > 0 ? se->win.offset : 1;
                        /* part_rank is 1-based position within partition */
                        int bucket = (int)(((part_rank - 1) * (size_t)nbuckets) / part_count) + 1;
                        c.value.as_int = bucket;
                        break;
                    }
                    case WIN_PERCENT_RANK: {
                        c.type = COLUMN_TYPE_FLOAT;
                        if (part_count <= 1) {
                            c.value.as_float = 0.0;
                        } else if (se->win.has_order && ord_idx[e] >= 0) {
                            /* rank - 1 / (partition_count - 1) */
                            int rank = 1;
                            for (size_t rj = 0; rj < nmatch; rj++) {
                                size_t j = sorted[rj].idx;
                                if (se->win.has_partition && part_idx[e] >= 0 &&
                                    !cell_equal_nullsafe(&src->cells.items[part_idx[e]],
                                                    &t->rows.items[j].cells.items[part_idx[e]]))
                                    continue;
                                int cmp = cell_compare(&t->rows.items[j].cells.items[ord_idx[e]],
                                                       &src->cells.items[ord_idx[e]]);
                                if (se->win.order_desc ? (cmp > 0) : (cmp < 0))
                                    rank++;
                            }
                            c.value.as_float = (double)(rank - 1) / (double)(part_count - 1);
                        } else {
                            c.value.as_float = 0.0;
                        }
                        break;
                    }
                    case WIN_CUME_DIST: {
                        c.type = COLUMN_TYPE_FLOAT;
                        if (se->win.has_order && ord_idx[e] >= 0) {
                            /* count of peers with order key <= current / partition_count */
                            int le_count = 0;
                            for (size_t rj = 0; rj < nmatch; rj++) {
                                size_t j = sorted[rj].idx;
                                if (se->win.has_partition && part_idx[e] >= 0 &&
                                    !cell_equal_nullsafe(&src->cells.items[part_idx[e]],
                                                    &t->rows.items[j].cells.items[part_idx[e]]))
                                    continue;
                                int cmp = cell_compare(&t->rows.items[j].cells.items[ord_idx[e]],
                                                       &src->cells.items[ord_idx[e]]);
                                if (se->win.order_desc ? (cmp >= 0) : (cmp <= 0))
                                    le_count++;
                            }
                            c.value.as_float = (double)le_count / (double)part_count;
                        } else {
                            c.value.as_float = 1.0;
                        }
                        break;
                    }
                    case WIN_LAG:
                    case WIN_LEAD: {
                        /* find the row at offset positions before (LAG) or after (LEAD) */
                        c.type = COLUMN_TYPE_INT;
                        c.is_null = 1;
                        int offset = se->win.offset;
                        /* build partition-local sorted index */
                        size_t part_pos = 0;
                        size_t target_pos = 0;
                        int found_self = 0;
                        size_t part_indices[4096];
                        size_t pn = 0;
                        for (size_t rj = 0; rj < nmatch && pn < 4096; rj++) {
                            size_t j = sorted[rj].idx;
                            if (se->win.has_partition && part_idx[e] >= 0 &&
                                !cell_equal_nullsafe(&src->cells.items[part_idx[e]],
                                                &t->rows.items[j].cells.items[part_idx[e]]))
                                continue;
                            if (j == row_i && !found_self) {
                                part_pos = pn;
                                found_self = 1;
                            }
                            part_indices[pn++] = j;
                        }
                        if (se->win.func == WIN_LAG)
                            target_pos = (part_pos >= (size_t)offset) ? part_pos - (size_t)offset : pn; /* pn = out of range */
                        else
                            target_pos = part_pos + (size_t)offset;
                        if (target_pos < pn && arg_idx[e] >= 0) {
                            size_t tj = part_indices[target_pos];
                            cell_copy(&c, &t->rows.items[tj].cells.items[arg_idx[e]]);
                        }
                        break;
                    }
                    case WIN_FIRST_VALUE:
                    case WIN_LAST_VALUE: {
                        c.type = COLUMN_TYPE_INT;
                        c.is_null = 1;
                        size_t first_j = 0, last_j = 0;
                        int found_any = 0;
                        for (size_t rj = 0; rj < nmatch; rj++) {
                            size_t j = sorted[rj].idx;
                            if (se->win.has_partition && part_idx[e] >= 0 &&
                                !cell_equal_nullsafe(&src->cells.items[part_idx[e]],
                                                &t->rows.items[j].cells.items[part_idx[e]]))
                                continue;
                            if (!found_any) { first_j = j; found_any = 1; }
                            last_j = j;
                        }
                        if (found_any && arg_idx[e] >= 0) {
                            size_t tj = (se->win.func == WIN_FIRST_VALUE) ? first_j : last_j;
                            cell_copy(&c, &t->rows.items[tj].cells.items[arg_idx[e]]);
                        }
                        break;
                    }
                    case WIN_NTH_VALUE: {
                        c.type = COLUMN_TYPE_INT;
                        c.is_null = 1;
                        int nth = se->win.offset; /* 1-based */
                        size_t count = 0;
                        for (size_t rj = 0; rj < nmatch; rj++) {
                            size_t j = sorted[rj].idx;
                            if (se->win.has_partition && part_idx[e] >= 0 &&
                                !cell_equal_nullsafe(&src->cells.items[part_idx[e]],
                                                &t->rows.items[j].cells.items[part_idx[e]]))
                                continue;
                            count++;
                            if ((int)count == nth && arg_idx[e] >= 0) {
                                cell_copy(&c, &t->rows.items[j].cells.items[arg_idx[e]]);
                                break;
                            }
                        }
                        break;
                    }
                }
            }
            da_push(&dst.cells, c);
        }
        rows_push(result, dst);
    }

    /* apply outer ORDER BY if present */
    if (s->has_order_by && result->count > 1) {
        /* build a temporary table descriptor from the result for sorting */
        /* The result columns correspond to select_exprs; we need to find
         * the ORDER BY column index within the result columns */
        for (uint32_t oi = 0; oi < s->order_by_count; oi++) {
            struct order_by_item *ob = &arena->order_items.items[s->order_by_start + oi];
            sv ord_name = ob->column;
            int ord_desc = ob->desc;
            int res_col = -1;
            /* find matching column in select_exprs */
            for (size_t e = 0; e < nexprs; e++) {
                if (arena->select_exprs.items[s->select_exprs_start + e].kind == SEL_COLUMN) {
                    sv col = arena->select_exprs.items[s->select_exprs_start + e].column;
                    /* strip table prefix from both */
                    sv bare_col = col, bare_ord = ord_name;
                    for (size_t k = 0; k < col.len; k++)
                        if (col.data[k] == '.') { bare_col = sv_from(col.data+k+1, col.len-k-1); break; }
                    for (size_t k = 0; k < ord_name.len; k++)
                        if (ord_name.data[k] == '.') { bare_ord = sv_from(ord_name.data+k+1, ord_name.len-k-1); break; }
                    if (sv_eq_ignorecase(bare_col, bare_ord)) { res_col = (int)e; break; }
                }
            }
            if (res_col < 0) continue;
            /* simple bubble sort on result rows by res_col */
            for (size_t i = 0; i < result->count - 1; i++) {
                for (size_t j = i + 1; j < result->count; j++) {
                    int cmp = cell_compare(&result->data[i].cells.items[res_col],
                                           &result->data[j].cells.items[res_col]);
                    if (ord_desc ? (cmp < 0) : (cmp > 0)) {
                        struct row tmp = result->data[i];
                        result->data[i] = result->data[j];
                        result->data[j] = tmp;
                    }
                }
            }
            break; /* only sort by first ORDER BY column for now */
        }
    }

    return 0;
}

/* helper: resolve a result column index by name in grouped output */
static int grp_find_result_col(struct table *t, int *grp_cols, size_t ngrp,
                               struct query_select *s, struct query_arena *arena, sv name)
{
    size_t agg_n = s->aggregates_count;
    /* result layout depends on agg_before_cols:
     *   default:          [grp0..grpN, agg0..aggN]
     *   agg_before_cols:  [agg0..aggN, grp0..grpN] */
    size_t grp_offset = s->agg_before_cols ? agg_n : 0;
    size_t agg_offset = s->agg_before_cols ? 0 : ngrp;

    /* check group columns */
    for (size_t k = 0; k < ngrp; k++) {
        if (grp_cols[k] >= 0 && sv_eq_cstr(name, t->columns.items[grp_cols[k]].name))
            return (int)(grp_offset + k);
    }
    /* check aggregate names */
    for (size_t a = 0; a < agg_n; a++) {
        struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
        const char *agg_name = "?";
        switch (ae->func) {
            case AGG_SUM:   agg_name = "sum";   break;
            case AGG_COUNT: agg_name = "count"; break;
            case AGG_AVG:   agg_name = "avg";   break;
            case AGG_MIN:   agg_name = "min";   break;
            case AGG_MAX:   agg_name = "max";   break;
            case AGG_NONE: break;
        }
        if (sv_eq_cstr(name, agg_name))
            return (int)(agg_offset + a);
    }
    return -1;
}

int query_group_by(struct table *t, struct query_select *s, struct query_arena *arena, struct rows *result)
{
    /* resolve GROUP BY column indices */
    size_t ngrp = s->group_by_count;
    if (ngrp == 0) ngrp = 1; /* backward compat: single group_by_col */
    int grp_cols[32];
    if (s->group_by_count > 0) {
        for (size_t k = 0; k < ngrp && k < 32; k++) {
            sv gbcol = ASV(arena, s->group_by_start + (uint32_t)k);
            grp_cols[k] = table_find_column_sv(t, gbcol);
            if (grp_cols[k] < 0) {
                fprintf(stderr, "GROUP BY column '" SV_FMT "' not found\n",
                        SV_ARG(gbcol));
                return -1;
            }
        }
    } else {
        grp_cols[0] = table_find_column_sv(t, s->group_by_col);
        if (grp_cols[0] < 0) {
            fprintf(stderr, "GROUP BY column '" SV_FMT "' not found\n",
                    SV_ARG(s->group_by_col));
            return -1;
        }
    }
    if (ngrp > 32) ngrp = 32;

    /* collect matching rows (scratch-allocated) */
    size_t max_rows = t->rows.count ? t->rows.count : 1;
    size_t *matching = (size_t *)bump_alloc(&arena->scratch, max_rows * sizeof(size_t));
    size_t matching_count = 0;
    for (size_t i = 0; i < t->rows.count; i++) {
        if (s->where.has_where && s->where.where_cond != IDX_NONE) {
            if (!eval_condition(s->where.where_cond, arena, &t->rows.items[i], t, NULL))
                continue;
        }
        matching[matching_count++] = i;
    }

    /* find distinct group keys (compare all group columns as a tuple) */
    size_t *group_starts = (size_t *)bump_alloc(&arena->scratch, max_rows * sizeof(size_t));
    size_t group_starts_count = 0;

    for (size_t m = 0; m < matching_count; m++) {
        size_t ri = matching[m];
        int found = 0;
        for (size_t g = 0; g < group_starts_count; g++) {
            size_t gi = matching[group_starts[g]];
            int eq = 1;
            for (size_t k = 0; k < ngrp; k++) {
                if (!cell_equal_nullsafe(&t->rows.items[ri].cells.items[grp_cols[k]],
                                &t->rows.items[gi].cells.items[grp_cols[k]])) {
                    eq = 0; break;
                }
            }
            if (eq) { found = 1; break; }
        }
        if (!found) group_starts[group_starts_count++] = m;
    }

    /* pre-allocate aggregate accumulators in a single allocation */
    size_t agg_n = s->aggregates_count;
    void *_grp_buf = NULL;
    double *sums = NULL, *gmins = NULL, *gmaxs = NULL;
    int *gminmax_init = NULL, *gagg_cols = NULL;
    size_t *gnonnull = NULL;
    if (agg_n > 0) {
        /* layout: double[3*N] | size_t[N] | int[2*N]  (descending alignment) */
        size_t _grp_alloc = 3 * agg_n * sizeof(double) + agg_n * sizeof(size_t) + 2 * agg_n * sizeof(int);
        _grp_buf = bump_alloc(&arena->scratch, _grp_alloc);
        memset(_grp_buf, 0, _grp_alloc);
        sums          = (double *)_grp_buf;
        gmins         = sums + agg_n;
        gmaxs         = gmins + agg_n;
        gnonnull      = (size_t *)(gmaxs + agg_n);
        gminmax_init  = (int *)(gnonnull + agg_n);
        gagg_cols     = gminmax_init + agg_n;
    }

    /* resolve aggregate column indices once */
    for (size_t a = 0; a < agg_n; a++) {
        struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
        if (sv_eq_cstr(ae->column, "*"))
            gagg_cols[a] = -1;
        else
            gagg_cols[a] = table_find_column_sv(t, ae->column);
    }

    /* build HAVING tmp_t once (columns don't change between groups).
     * JPL ownership: column names are strdup'd here and freed by
     * column_free at the end of this function (same scope).
     * Column order must match result row layout (agg_before_cols). */
    struct table having_t = {0};
    int has_having_t = 0;
    if (s->has_having && s->having_cond != IDX_NONE) {
        has_having_t = 1;
        da_init(&having_t.columns);
        da_init(&having_t.rows);
        da_init(&having_t.indexes);
        if (!s->agg_before_cols) {
            for (size_t k = 0; k < ngrp; k++) {
                struct column col_grp = { .name = strdup(t->columns.items[grp_cols[k]].name),
                                          .type = t->columns.items[grp_cols[k]].type,
                                          .enum_type_name = NULL };
                da_push(&having_t.columns, col_grp);
            }
        }
        for (size_t a = 0; a < agg_n; a++) {
            struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
            const char *agg_name = "?";
            switch (ae->func) {
                case AGG_SUM:   agg_name = "sum";   break;
                case AGG_COUNT: agg_name = "count"; break;
                case AGG_AVG:   agg_name = "avg";   break;
                case AGG_MIN:   agg_name = "min";   break;
                case AGG_MAX:   agg_name = "max";   break;
                case AGG_NONE: break;
            }
            int ac_idx = gagg_cols[a];
            enum column_type ctype = (ac_idx >= 0 &&
                t->columns.items[ac_idx].type == COLUMN_TYPE_FLOAT)
                ? COLUMN_TYPE_FLOAT : COLUMN_TYPE_INT;
            if (ae->func == AGG_AVG)
                ctype = COLUMN_TYPE_FLOAT;
            if (ae->func == AGG_COUNT)
                ctype = COLUMN_TYPE_INT;
            struct column col_a = { .name = strdup(agg_name),
                                    .type = ctype,
                                    .enum_type_name = NULL };
            da_push(&having_t.columns, col_a);
        }
        if (s->agg_before_cols) {
            for (size_t k = 0; k < ngrp; k++) {
                struct column col_grp = { .name = strdup(t->columns.items[grp_cols[k]].name),
                                          .type = t->columns.items[grp_cols[k]].type,
                                          .enum_type_name = NULL };
                da_push(&having_t.columns, col_grp);
            }
        }
    }

    /* for each group, compute aggregates */
    for (size_t g = 0; g < group_starts_count; g++) {
        size_t first_ri = matching[group_starts[g]];

        /* reset accumulators */
        size_t grp_count = 0;
        if (agg_n > 0) {
            memset(sums, 0, agg_n * sizeof(double));
            memset(gminmax_init, 0, agg_n * sizeof(int));
            memset(gnonnull, 0, agg_n * sizeof(size_t));
        }

        for (size_t m = 0; m < matching_count; m++) {
            size_t ri = matching[m];
            /* check if this row belongs to the current group */
            int eq = 1;
            for (size_t k = 0; k < ngrp; k++) {
                if (!cell_equal_nullsafe(&t->rows.items[ri].cells.items[grp_cols[k]],
                                &t->rows.items[first_ri].cells.items[grp_cols[k]])) {
                    eq = 0; break;
                }
            }
            if (!eq) continue;
            grp_count++;
            for (size_t a = 0; a < agg_n; a++) {
                int ac = gagg_cols[a];
                if (ac < 0) continue;
                struct cell *gc = &t->rows.items[ri].cells.items[ac];
                /* skip NULL values (SQL standard) */
                if (gc->is_null || (column_type_is_text(gc->type) && !gc->value.as_text))
                    continue;
                gnonnull[a]++;
                {
                    double v = cell_to_double(gc);
                    sums[a] += v;
                    if (!gminmax_init[a] || v < gmins[a]) gmins[a] = v;
                    if (!gminmax_init[a] || v > gmaxs[a]) gmaxs[a] = v;
                    gminmax_init[a] = 1;
                }
            }
        }

        /* build result row: group key columns + aggregates (or reversed if agg_before_cols) */
        struct row dst = {0};
        da_init(&dst.cells);

        if (!s->agg_before_cols) {
            /* default: group key columns first */
            for (size_t k = 0; k < ngrp; k++) {
                struct cell gc;
                cell_copy(&gc, &t->rows.items[first_ri].cells.items[grp_cols[k]]);
                da_push(&dst.cells, gc);
            }
        }

        /* add aggregate values */
        for (size_t a = 0; a < agg_n; a++) {
            struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
            struct cell c = {0};
            int ac_idx = gagg_cols[a];
            int col_is_float = (ac_idx >= 0 &&
                                t->columns.items[ac_idx].type == COLUMN_TYPE_FLOAT);
            switch (ae->func) {
                case AGG_COUNT:
                    c.type = COLUMN_TYPE_INT;
                    /* COUNT(*) counts all rows; COUNT(col) counts non-NULL */
                    c.value.as_int = (gagg_cols[a] < 0) ? (int)grp_count : (int)gnonnull[a];
                    break;
                case AGG_SUM:
                    if (gnonnull[a] == 0) {
                        c.type = col_is_float ? COLUMN_TYPE_FLOAT : COLUMN_TYPE_INT;
                        c.is_null = 1;
                    } else if (col_is_float) {
                        c.type = COLUMN_TYPE_FLOAT;
                        c.value.as_float = sums[a];
                    } else {
                        c.type = COLUMN_TYPE_INT;
                        c.value.as_int = (int)sums[a];
                    }
                    break;
                case AGG_AVG:
                    c.type = COLUMN_TYPE_FLOAT;
                    if (gnonnull[a] == 0) {
                        c.is_null = 1;
                    } else {
                        c.value.as_float = sums[a] / (double)gnonnull[a];
                    }
                    break;
                case AGG_MIN:
                case AGG_MAX: {
                    if (gnonnull[a] == 0) {
                        c.type = col_is_float ? COLUMN_TYPE_FLOAT : COLUMN_TYPE_INT;
                        c.is_null = 1;
                    } else {
                        double val = (ae->func == AGG_MIN) ? gmins[a] : gmaxs[a];
                        if (col_is_float) {
                            c.type = COLUMN_TYPE_FLOAT;
                            c.value.as_float = val;
                        } else {
                            c.type = COLUMN_TYPE_INT;
                            c.value.as_int = (int)val;
                        }
                    }
                    break;
                }
                case AGG_NONE:
                    break;
            }
            da_push(&dst.cells, c);
        }

        if (s->agg_before_cols) {
            /* aggregates first, then group key columns */
            for (size_t k = 0; k < ngrp; k++) {
                struct cell gc;
                cell_copy(&gc, &t->rows.items[first_ri].cells.items[grp_cols[k]]);
                da_push(&dst.cells, gc);
            }
        }

        /* HAVING filter */
        if (has_having_t) {
            int passes = eval_condition(s->having_cond, arena, &dst, &having_t, NULL);
            if (!passes) {
                row_free(&dst);
                continue;
            }
        }

        rows_push(result, dst);
    }

    /* matching and group_starts are scratch-allocated — no free needed */
    if (has_having_t) {
        for (size_t i = 0; i < having_t.columns.count; i++)
            column_free(&having_t.columns.items[i]);
        da_free(&having_t.columns);
    }

    /* ORDER BY on grouped results (multi-column) */
    if (s->has_order_by && s->order_by_count > 0 && result->count > 1) {
        int ord_res[32];
        int ord_descs[32];
        size_t nord = s->order_by_count < 32 ? s->order_by_count : 32;
        for (size_t k = 0; k < nord; k++) {
            struct order_by_item *obi = &arena->order_items.items[s->order_by_start + k];
            ord_res[k] = grp_find_result_col(t, grp_cols, ngrp, s, arena,
                                             obi->column);
            ord_descs[k] = obi->desc;
        }
        _sort_ctx = (struct sort_ctx){ .cols = ord_res, .descs = ord_descs, .ncols = nord };
        qsort(result->data, result->count, sizeof(struct row), cmp_rows_multi);
    }

    /* LIMIT / OFFSET on grouped results */
    if (s->has_offset || s->has_limit) {
        size_t start = s->has_offset ? (size_t)s->offset_count : 0;
        if (start > result->count) start = result->count;
        size_t end = result->count;
        if (s->has_limit) {
            size_t lim = (size_t)s->limit_count;
            if (start + lim < end) end = start + lim;
        }
        struct rows trimmed = {0};
        for (size_t i = start; i < end; i++) {
            rows_push(&trimmed, result->data[i]);
            result->data[i] = (struct row){0};
        }
        rows_free(result);
        *result = trimmed;
    }

    return 0;
}

static int row_matches(struct table *t, struct where_clause *w, struct query_arena *arena, struct row *row, struct database *db)
{
    if (!w->has_where) return 1;
    if (w->where_cond != IDX_NONE)
        return eval_condition(w->where_cond, arena, row, t, db);
    /* legacy single-column = value */
    int where_col = -1;
    for (size_t j = 0; j < t->columns.count; j++) {
        if (sv_eq_cstr(w->where_column, t->columns.items[j].name)) {
            where_col = (int)j; break;
        }
    }
    if (where_col < 0) return 0;
    return cell_equal(&row->cells.items[where_col], &w->where_value);
}

static int query_select_exec(struct table *t, struct query_select *s, struct query_arena *arena, struct rows *result, struct database *db, struct bump_alloc *rb)
{
    /* dispatch to window path if select_exprs are present */
    if (s->select_exprs_count > 0)
        return query_window(t, s, arena, result);

    /* dispatch to GROUP BY path */
    if (s->has_group_by) {
        if (s->group_by_rollup || s->group_by_cube) {
            /* ROLLUP/CUBE: run query_group_by for each grouping set */
            uint32_t orig_count = s->group_by_count;
            uint32_t orig_start = s->group_by_start;
            int orig_rollup = s->group_by_rollup;
            int orig_cube = s->group_by_cube;

            /* generate grouping sets */
            /* ROLLUP(a,b,c): (a,b,c), (a,b), (a), () — n+1 sets */
            /* CUBE(a,b,c): all 2^n subsets */
            uint32_t nsets = 0;
            uint32_t sets[256]; /* bitmask per set */
            if (s->group_by_rollup) {
                for (uint32_t i = 0; i <= orig_count; i++) {
                    uint32_t mask = 0;
                    for (uint32_t j = 0; j < orig_count - i; j++)
                        mask |= (1u << j);
                    sets[nsets++] = mask;
                }
            } else { /* CUBE */
                uint32_t total = 1u << orig_count;
                for (uint32_t m = total; m > 0; m--)
                    sets[nsets++] = m - 1;
            }

            /* resolve group column indices for NULL-out */
            int grp_col_idx[32];
            for (uint32_t k = 0; k < orig_count && k < 32; k++) {
                sv gbcol = ASV(arena, orig_start + k);
                grp_col_idx[k] = table_find_column_sv(t, gbcol);
            }

            s->group_by_rollup = 0;
            s->group_by_cube = 0;

            for (uint32_t si = 0; si < nsets; si++) {
                uint32_t mask = sets[si];
                /* build temporary sv list for this set's active columns */
                uint32_t tmp_start = (uint32_t)arena->svs.count;
                uint32_t tmp_count = 0;
                for (uint32_t k = 0; k < orig_count; k++) {
                    if (mask & (1u << k)) {
                        arena_push_sv(arena, ASV(arena, orig_start + k));
                        tmp_count++;
                    }
                }
                if (tmp_count == 0) {
                    /* grand total: run as plain aggregate (no GROUP BY) */
                    s->has_group_by = 0;
                    struct rows sub = {0};
                    query_aggregate(t, s, arena, &sub);
                    s->has_group_by = 1;
                    /* prepend NULL group columns if agg_before_cols is 0 */
                    for (size_t r = 0; r < sub.count; r++) {
                        if (!s->agg_before_cols) {
                            /* insert NULL cells for each group column at the front */
                            struct row newrow = {0};
                            da_init(&newrow.cells);
                            for (uint32_t k = 0; k < orig_count; k++) {
                                struct cell nc = {0};
                                nc.type = (grp_col_idx[k] >= 0) ? t->columns.items[grp_col_idx[k]].type : COLUMN_TYPE_TEXT;
                                nc.is_null = 1;
                                da_push(&newrow.cells, nc);
                            }
                            for (size_t ci = 0; ci < sub.data[r].cells.count; ci++) {
                                struct cell dup;
                                cell_copy(&dup, &sub.data[r].cells.items[ci]);
                                da_push(&newrow.cells, dup);
                            }
                            row_free(&sub.data[r]);
                            rows_push(result, newrow);
                        } else {
                            rows_push(result, sub.data[r]);
                            sub.data[r] = (struct row){0};
                        }
                    }
                    free(sub.data);
                } else {
                    s->group_by_start = tmp_start;
                    s->group_by_count = tmp_count;
                    s->group_by_col = ASV(arena, tmp_start);
                    struct rows sub = {0};
                    query_group_by(t, s, arena, &sub);
                    /* NULL-out columns not in this grouping set */
                    for (size_t r = 0; r < sub.count; r++) {
                        if (!s->agg_before_cols) {
                            /* group columns are at the front of the row */
                            /* we need to expand to orig_count group cols, inserting NULLs for missing ones */
                            struct row newrow = {0};
                            da_init(&newrow.cells);
                            size_t sub_grp_i = 0;
                            for (uint32_t k = 0; k < orig_count; k++) {
                                if (mask & (1u << k)) {
                                    if (sub_grp_i < sub.data[r].cells.count) {
                                        struct cell dup;
                                        cell_copy(&dup, &sub.data[r].cells.items[sub_grp_i]);
                                        da_push(&newrow.cells, dup);
                                    }
                                    sub_grp_i++;
                                } else {
                                    struct cell nc = {0};
                                    nc.type = (grp_col_idx[k] >= 0) ? t->columns.items[grp_col_idx[k]].type : COLUMN_TYPE_TEXT;
                                    nc.is_null = 1;
                                    da_push(&newrow.cells, nc);
                                }
                            }
                            /* append aggregate columns */
                            for (size_t ci = sub_grp_i; ci < sub.data[r].cells.count; ci++) {
                                struct cell dup;
                                cell_copy(&dup, &sub.data[r].cells.items[ci]);
                                da_push(&newrow.cells, dup);
                            }
                            row_free(&sub.data[r]);
                            rows_push(result, newrow);
                        } else {
                            rows_push(result, sub.data[r]);
                            sub.data[r] = (struct row){0};
                        }
                    }
                    free(sub.data);
                }
            }

            /* restore original state */
            s->group_by_start = orig_start;
            s->group_by_count = orig_count;
            s->group_by_rollup = orig_rollup;
            s->group_by_cube = orig_cube;
            return 0;
        }
        return query_group_by(t, s, arena, result);
    }

    /* dispatch to aggregate path if aggregates are present */
    if (s->aggregates_count > 0)
        return query_aggregate(t, s, arena, result);

    int select_all = sv_eq_cstr(s->columns, "*");

    /* try index lookup for simple equality WHERE on indexed column */
    if (s->where.has_where && s->where.where_cond != IDX_NONE
        && COND(arena, s->where.where_cond).type == COND_COMPARE
        && COND(arena, s->where.where_cond).op == CMP_EQ && !s->has_order_by) {
        int where_col = table_find_column_sv(t, COND(arena, s->where.where_cond).column);
        if (where_col >= 0) {
            for (size_t idx = 0; idx < t->indexes.count; idx++) {
                if (strcmp(t->indexes.items[idx].column_name,
                           t->columns.items[where_col].name) == 0) {
                    size_t *ids = NULL;
                    size_t id_count = 0;
                    index_lookup(&t->indexes.items[idx], &COND(arena, s->where.where_cond).value,
                                 &ids, &id_count);
                    for (size_t k = 0; k < id_count; k++) {
                        if (ids[k] < t->rows.count)
                            emit_row(t, s, arena, &t->rows.items[ids[k]], result, select_all, db, rb);
                    }
                    return 0;
                }
            }
        }
    }

    /* try block-oriented plan executor for simple queries */
    {
        uint32_t plan_root = plan_build_select(t, s, arena, db);
        if (plan_root != IDX_NONE) {
            struct plan_exec_ctx ctx;
            plan_exec_init(&ctx, arena, db, plan_root);
            return plan_exec_to_rows(&ctx, plan_root, result, rb);
        }
    }

    /* full scan — collect matching row indices (scratch-allocated) */
    size_t *match_items = (size_t *)bump_alloc(&arena->scratch,
                           (t->rows.count ? t->rows.count : 1) * sizeof(size_t));
    size_t match_count = 0;
    for (size_t i = 0; i < t->rows.count; i++) {
        if (!row_matches(t, &s->where, arena, &t->rows.items[i], db))
            continue;
        match_items[match_count++] = i;
    }

    /* ORDER BY — sort indices using the original table data (multi-column) */
    int order_on_result = 0; /* set to 1 if we need to sort projected rows */
    int result_ord_cols[32];
    int result_ord_descs[32];
    size_t result_nord = 0;
    if (s->has_order_by && s->order_by_count > 0) {
        /* resolve column indices for all ORDER BY items */
        int ord_cols[32];
        int ord_descs[32];
        size_t nord = s->order_by_count < 32 ? s->order_by_count : 32;
        int all_resolved = 1;
        for (size_t k = 0; k < nord; k++) {
            struct order_by_item *obi = &arena->order_items.items[s->order_by_start + k];
            ord_cols[k] = table_find_column_sv(t, obi->column);
            /* if not found, try resolving as a SELECT alias */
            if (ord_cols[k] < 0 && s->columns.len > 0) {
                ord_cols[k] = resolve_alias_to_column(t, s->columns, obi->column);
            }
            /* if still not found, try resolving as a parsed_columns alias (output position) */
            if (ord_cols[k] < 0 && s->parsed_columns_count > 0) {
                for (uint32_t pc = 0; pc < s->parsed_columns_count; pc++) {
                    struct select_column *scp = &arena->select_cols.items[s->parsed_columns_start + pc];
                    if (scp->alias.len > 0 &&
                        sv_eq_ignorecase(obi->column, scp->alias)) {
                        result_ord_cols[k] = (int)pc;
                        result_ord_descs[k] = obi->desc;
                        order_on_result = 1;
                        ord_cols[k] = -2; /* mark as alias-resolved */
                        break;
                    }
                }
            }
            if (ord_cols[k] < 0 && ord_cols[k] != -2) all_resolved = 0;
            ord_descs[k] = obi->desc;
        }
        if (order_on_result) {
            /* defer sorting to after projection */
            result_nord = nord;
            for (size_t k = 0; k < nord; k++) {
                if (ord_cols[k] != -2) {
                    /* table column — find its position in parsed_columns output */
                    result_ord_cols[k] = -1;
                    for (uint32_t pc = 0; pc < s->parsed_columns_count; pc++) {
                        struct select_column *scp = &arena->select_cols.items[s->parsed_columns_start + pc];
                        if (scp->expr_idx != IDX_NONE && EXPR(arena, scp->expr_idx).type == EXPR_COLUMN_REF) {
                            int ci = table_find_column_sv(t, EXPR(arena, scp->expr_idx).column_ref.column);
                            if (ci == ord_cols[k]) { result_ord_cols[k] = (int)pc; break; }
                        }
                    }
                    result_ord_descs[k] = ord_descs[k];
                }
            }
        } else if (all_resolved) {
            _sort_ctx = (struct sort_ctx){ .cols = ord_cols, .descs = ord_descs,
                                           .ncols = nord, .table = t };
            qsort(match_items, match_count, sizeof(size_t), cmp_indices_multi);
        }
    }

    /* project into result rows */
    struct rows tmp = {0};
    for (size_t i = 0; i < match_count; i++) {
        emit_row(t, s, arena, &t->rows.items[match_items[i]], &tmp, select_all, db, rb);
    }

    /* sort projected result rows if ORDER BY references a SELECT alias */
    if (order_on_result && result_nord > 0 && tmp.count > 1) {
        _sort_ctx = (struct sort_ctx){ .cols = result_ord_cols, .descs = result_ord_descs,
                                       .ncols = result_nord };
        qsort(tmp.data, tmp.count, sizeof(struct row), cmp_rows_multi);
    }

    // TODO: DISTINCT dedup is O(n^2); could sort rows first then deduplicate
    // in a single linear pass for O(n log n) performance
    // TODO: CONTAINER REUSE: the row-equality loop here is the same pattern used in
    // UNION/INTERSECT/EXCEPT in database.c; extract a shared rows_equal helper into row.c
    /* DISTINCT: deduplicate before LIMIT (SQL semantics) */
    if (s->has_distinct && tmp.count > 1) {
        struct rows deduped = {0};
        for (size_t i = 0; i < tmp.count; i++) {
            int dup = 0;
            for (size_t j = 0; j < deduped.count; j++) {
                if (row_equal_nullsafe(&tmp.data[i], &deduped.data[j])) { dup = 1; break; }
            }
            if (!dup) {
                rows_push(&deduped, tmp.data[i]);
                tmp.data[i] = (struct row){0};
            }
        }
        for (size_t i = 0; i < tmp.count; i++) {
            if (tmp.data[i].cells.items) {
                if (rb) da_free(&tmp.data[i].cells);
                else    row_free(&tmp.data[i]);
            }
        }
        free(tmp.data);
        tmp = deduped;
    }

    /* DISTINCT ON: keep first row per distinct value of the ON columns */
    if (s->has_distinct_on && s->distinct_on_count > 0 && tmp.count > 1) {
        /* resolve DISTINCT ON column indices */
        int don_cols[16];
        uint32_t don_n = s->distinct_on_count < 16 ? s->distinct_on_count : 16;
        for (uint32_t di = 0; di < don_n; di++) {
            sv col_name = ASV(arena, s->distinct_on_start + di);
            don_cols[di] = table_find_column_sv(t, col_name);
        }
        struct rows deduped = {0};
        for (size_t i = 0; i < tmp.count; i++) {
            int dup = 0;
            for (size_t j = 0; j < deduped.count; j++) {
                int same = 1;
                for (uint32_t di = 0; di < don_n; di++) {
                    int ci = don_cols[di];
                    if (ci < 0) continue;
                    if ((size_t)ci >= tmp.data[i].cells.count || (size_t)ci >= deduped.data[j].cells.count) {
                        same = 0; break;
                    }
                    if (!cell_equal_nullsafe(&tmp.data[i].cells.items[ci],
                                             &deduped.data[j].cells.items[ci])) {
                        same = 0; break;
                    }
                }
                if (same) { dup = 1; break; }
            }
            if (!dup) {
                rows_push(&deduped, tmp.data[i]);
                tmp.data[i] = (struct row){0};
            }
        }
        for (size_t i = 0; i < tmp.count; i++) {
            if (tmp.data[i].cells.items) {
                if (rb) da_free(&tmp.data[i].cells);
                else    row_free(&tmp.data[i]);
            }
        }
        free(tmp.data);
        tmp = deduped;
    }

    /* OFFSET / LIMIT */
    size_t start = 0;
    size_t end = tmp.count;
    if (s->has_offset) {
        start = (size_t)s->offset_count;
        if (start > tmp.count) start = tmp.count;
    }
    if (s->has_limit) {
        size_t lim = (size_t)s->limit_count;
        if (start + lim < end) end = start + lim;
    }

    for (size_t i = start; i < end; i++) {
        rows_push(result, tmp.data[i]);
        /* null out so we don't double-free */
        tmp.data[i].cells.items = NULL;
        tmp.data[i].cells.count = 0;
    }

    /* free unused rows */
    for (size_t i = 0; i < tmp.count; i++) {
        if (tmp.data[i].cells.items) {
            if (rb) da_free(&tmp.data[i].cells);
            else    row_free(&tmp.data[i]);
        }
    }
    free(tmp.data);

    return 0;
}

static void rebuild_indexes(struct table *t)
{
    for (size_t idx = 0; idx < t->indexes.count; idx++) {
        struct index *ix = &t->indexes.items[idx];
        int col_idx = ix->column_idx;
        index_reset(ix);
        /* re-insert all rows */
        for (size_t r = 0; r < t->rows.count; r++) {
            if ((size_t)col_idx < t->rows.items[r].cells.count)
                index_insert(ix, &t->rows.items[r].cells.items[col_idx], r);
        }
    }
}

static void emit_returning_row(struct table *t, struct row *src,
                               sv returning_columns, int return_all,
                               struct rows *result)
{
    struct row ret = {0};
    da_init(&ret.cells);
    if (return_all) {
        for (size_t c = 0; c < src->cells.count; c++) {
            struct cell cp;
            cell_copy(&cp, &src->cells.items[c]);
            da_push(&ret.cells, cp);
        }
    } else {
        sv cols = returning_columns;
        while (cols.len > 0) {
            size_t end = 0;
            while (end < cols.len && cols.data[end] != ',') end++;
            sv one = sv_trim(sv_from(cols.data, end));
            for (size_t j = 0; j < t->columns.count; j++) {
                if (sv_eq_cstr(one, t->columns.items[j].name)) {
                    struct cell cp;
                    cell_copy(&cp, &src->cells.items[j]);
                    da_push(&ret.cells, cp);
                    break;
                }
            }
            if (end < cols.len) end++;
            cols = sv_from(cols.data + end, cols.len - end);
        }
    }
    rows_push(result, ret);
}

static int query_delete_exec(struct table *t, struct query_delete *d, struct query_arena *arena, struct rows *result)
{
    int has_ret = (d->has_returning && d->returning_columns.len > 0);
    int return_all = has_ret && sv_eq_cstr(d->returning_columns, "*");
    size_t deleted = 0;
    for (size_t i = 0; i < t->rows.count; ) {
        if (row_matches(t, &d->where, arena, &t->rows.items[i], NULL)) {
            /* capture row for RETURNING before freeing */
            if (has_ret && result)
                emit_returning_row(t, &t->rows.items[i], d->returning_columns, return_all, result);
            row_free(&t->rows.items[i]);
            for (size_t j = i; j + 1 < t->rows.count; j++)
                t->rows.items[j] = t->rows.items[j + 1];
            t->rows.count--;
            deleted++;
            t->generation++;
        } else {
            i++;
        }
    }
    /* rebuild indexes after row removal */
    if (deleted > 0 && t->indexes.count > 0)
        rebuild_indexes(t);

    /* store deleted count for command tag (only if not RETURNING) */
    if (!has_ret && result) {
        struct row r = {0};
        da_init(&r.cells);
        struct cell c = { .type = COLUMN_TYPE_INT };
        c.value.as_int = (int)deleted;
        da_push(&r.cells, c);
        rows_push(result, r);
    }
    return 0;
}

static int query_update_exec(struct table *t, struct query_update *u, struct query_arena *arena, struct rows *result, struct database *db)
{
    int has_ret = (u->has_returning && u->returning_columns.len > 0);
    int return_all = has_ret && sv_eq_cstr(u->returning_columns, "*");
    size_t updated = 0;
    for (size_t i = 0; i < t->rows.count; i++) {
        if (!row_matches(t, &u->where, arena, &t->rows.items[i], db))
            continue;
        updated++;
        /* evaluate all SET expressions against the pre-update row snapshot
         * before applying any changes (SQL standard: SET a=b, b=a swaps) */
        uint32_t nsc = u->set_clauses_count;
        struct cell *new_vals = bump_calloc(&arena->scratch, nsc, sizeof(struct cell));
        int *col_idxs = bump_calloc(&arena->scratch, nsc, sizeof(int));
        for (uint32_t sc = 0; sc < nsc; sc++) {
            struct set_clause *scp = &arena->set_clauses.items[u->set_clauses_start + sc];
            col_idxs[sc] = table_find_column_sv(t, scp->column);
            if (col_idxs[sc] < 0) { new_vals[sc] = (struct cell){0}; continue; }
            if (scp->expr_idx != IDX_NONE) {
                new_vals[sc] = eval_expr(scp->expr_idx, arena, t, &t->rows.items[i], db, NULL);
            } else {
                cell_copy(&new_vals[sc], &scp->value);
            }
        }
        /* now apply all new values */
        for (size_t sc = 0; sc < nsc; sc++) {
            if (col_idxs[sc] < 0) continue;
            struct cell *dst = &t->rows.items[i].cells.items[col_idxs[sc]];
            if (column_type_is_text(dst->type) && dst->value.as_text)
                free(dst->value.as_text);
            *dst = new_vals[sc];
        }
        /* capture row for RETURNING after SET */
        if (has_ret && result)
            emit_returning_row(t, &t->rows.items[i], u->returning_columns, return_all, result);
    }
    /* rebuild indexes after cell mutation */
    if (updated > 0) {
        t->generation++;
        if (t->indexes.count > 0)
            rebuild_indexes(t);
    }

    if (!has_ret && result) {
        struct row r = {0};
        da_init(&r.cells);
        struct cell c = { .type = COLUMN_TYPE_INT };
        c.value.as_int = (int)updated;
        da_push(&r.cells, c);
        rows_push(result, r);
    }
    return 0;
}

/* copy_cell_into → use shared cell_copy from row.h */

static int query_insert_exec(struct table *t, struct query_insert *ins, struct query_arena *arena, struct rows *result, struct database *db)
{
    int has_returning = (ins->returning_columns.len > 0);
    int return_all = has_returning && sv_eq_cstr(ins->returning_columns, "*");

    for (uint32_t r = 0; r < ins->insert_rows_count; r++) {
        struct row *src = &arena->rows.items[ins->insert_rows_start + r];

        /* resolve expression sentinel cells (is_null==2) in source row */
        for (size_t ci = 0; ci < src->cells.count; ci++) {
            if (src->cells.items[ci].is_null == 2) {
                uint32_t ei = (uint32_t)src->cells.items[ci].value.as_int;
                struct cell val = eval_expr(ei, arena, t, NULL, db, NULL);
                src->cells.items[ci] = val;
            }
        }

        struct row copy = {0};
        da_init(&copy.cells);

        if (ins->insert_columns_count > 0) {
            /* column list provided: build full-width row with defaults/NULLs,
             * then place each value at the correct column position */
            for (size_t ci = 0; ci < t->columns.count; ci++) {
                if (t->columns.items[ci].has_default && t->columns.items[ci].default_value) {
                    struct cell dup;
                    cell_copy(&dup, t->columns.items[ci].default_value);
                    da_push(&copy.cells, dup);
                } else {
                    struct cell null_cell = {0};
                    null_cell.type = t->columns.items[ci].type;
                    null_cell.is_null = 1;
                    da_push(&copy.cells, null_cell);
                }
            }
            for (uint32_t vi = 0; vi < ins->insert_columns_count && vi < (uint32_t)src->cells.count; vi++) {
                sv col_name = ASV(arena, ins->insert_columns_start + vi);
                int ci = table_find_column_sv(t, col_name);
                if (ci < 0) continue;
                cell_free_text(&copy.cells.items[ci]);
                cell_copy(&copy.cells.items[ci], &src->cells.items[vi]);
            }
        } else {
            for (size_t i = 0; i < src->cells.count; i++) {
                struct cell dup;
                cell_copy(&dup, &src->cells.items[i]);
                da_push(&copy.cells, dup);
            }
            /* pad with DEFAULT or NULL if fewer values than columns */
            while (copy.cells.count < t->columns.count) {
                size_t ci = copy.cells.count;
                if (t->columns.items[ci].has_default && t->columns.items[ci].default_value) {
                    struct cell dup;
                    cell_copy(&dup, t->columns.items[ci].default_value);
                    da_push(&copy.cells, dup);
                } else {
                    struct cell null_cell = {0};
                    null_cell.type = t->columns.items[ci].type;
                    null_cell.is_null = 1;
                    da_push(&copy.cells, null_cell);
                }
            }
        }
        /* type coercion: promote TEXT cells to the column's temporal/UUID type */
        for (size_t i = 0; i < t->columns.count && i < copy.cells.count; i++) {
            struct cell *c = &copy.cells.items[i];
            enum column_type ct = t->columns.items[i].type;
            if (c->type == COLUMN_TYPE_TEXT && !c->is_null && c->value.as_text &&
                (ct == COLUMN_TYPE_DATE || ct == COLUMN_TYPE_TIME ||
                 ct == COLUMN_TYPE_TIMESTAMP || ct == COLUMN_TYPE_TIMESTAMPTZ ||
                 ct == COLUMN_TYPE_INTERVAL || ct == COLUMN_TYPE_UUID)) {
                c->type = ct;
            }
        }
        /* auto-increment SERIAL/BIGSERIAL columns */
        for (size_t i = 0; i < t->columns.count && i < copy.cells.count; i++) {
            if (t->columns.items[i].is_serial) {
                struct cell *c = &copy.cells.items[i];
                int is_null = c->is_null || (column_type_is_text(c->type) && !c->value.as_text);
                if (is_null) {
                    long long val = t->columns.items[i].serial_next++;
                    c->is_null = 0;
                    if (t->columns.items[i].type == COLUMN_TYPE_BIGINT) {
                        c->type = COLUMN_TYPE_BIGINT;
                        c->value.as_bigint = val;
                    } else {
                        c->type = COLUMN_TYPE_INT;
                        c->value.as_int = (int)val;
                    }
                } else {
                    /* user provided a value — update serial_next if needed */
                    long long v = (c->type == COLUMN_TYPE_BIGINT) ? c->value.as_bigint : c->value.as_int;
                    if (v >= t->columns.items[i].serial_next)
                        t->columns.items[i].serial_next = v + 1;
                }
            }
        }
        /* enforce NOT NULL constraints */
        for (size_t i = 0; i < t->columns.count && i < copy.cells.count; i++) {
            if (t->columns.items[i].not_null) {
                struct cell *c = &copy.cells.items[i];
                int is_null = c->is_null || (column_type_is_text(c->type) && !c->value.as_text);
                if (is_null) {
                    fprintf(stderr, "NOT NULL constraint violated for column '%s'\n",
                            t->columns.items[i].name);
                    row_free(&copy);
                    return -1;
                }
            }
        }
        /* enforce UNIQUE constraints */
        for (size_t i = 0; i < t->columns.count && i < copy.cells.count; i++) {
            if (t->columns.items[i].is_unique) {
                struct cell *new_c = &copy.cells.items[i];
                /* SQL standard: NULLs are not considered duplicates */
                if (new_c->is_null || (column_type_is_text(new_c->type) && !new_c->value.as_text))
                    continue;
                for (size_t ri = 0; ri < t->rows.count; ri++) {
                    struct cell *existing = &t->rows.items[ri].cells.items[i];
                    if (cell_compare(new_c, existing) == 0) {
                        fprintf(stderr, "UNIQUE constraint violated for column '%s'\n",
                                t->columns.items[i].name);
                        row_free(&copy);
                        return -1;
                    }
                }
            }
        }
        da_push(&t->rows, copy);
        t->generation++;

        /* update indexes */
        size_t new_row_id = t->rows.count - 1;
        for (size_t ix = 0; ix < t->indexes.count; ix++) {
            int col_idx = t->indexes.items[ix].column_idx;
            if (col_idx >= 0 && (size_t)col_idx < t->rows.items[new_row_id].cells.count) {
                index_insert(&t->indexes.items[ix],
                             &t->rows.items[new_row_id].cells.items[col_idx],
                             new_row_id);
            }
        }

        if (has_returning && result)
            emit_returning_row(t, &t->rows.items[t->rows.count - 1], ins->returning_columns, return_all, result);
    }

    return 0;
}

int query_exec(struct table *t, struct query *q, struct rows *result, struct database *db, struct bump_alloc *rb)
{
    switch (q->query_type) {
        case QUERY_TYPE_CREATE:
        case QUERY_TYPE_DROP:
        case QUERY_TYPE_CREATE_INDEX:
        case QUERY_TYPE_DROP_INDEX:
        case QUERY_TYPE_CREATE_TYPE:
        case QUERY_TYPE_DROP_TYPE:
        case QUERY_TYPE_ALTER:
        case QUERY_TYPE_BEGIN:
        case QUERY_TYPE_COMMIT:
        case QUERY_TYPE_ROLLBACK:
        case QUERY_TYPE_CREATE_SEQUENCE:
        case QUERY_TYPE_DROP_SEQUENCE:
        case QUERY_TYPE_CREATE_VIEW:
        case QUERY_TYPE_DROP_VIEW:
            return -1;
        case QUERY_TYPE_SELECT:
            return query_select_exec(t, &q->select, &q->arena, result, db, rb);
        case QUERY_TYPE_INSERT:
            return query_insert_exec(t, &q->insert, &q->arena, result, db);
        case QUERY_TYPE_DELETE:
            return query_delete_exec(t, &q->del, &q->arena, result);
        case QUERY_TYPE_UPDATE:
            return query_update_exec(t, &q->update, &q->arena, result, db);
    }
    return -1;
}
