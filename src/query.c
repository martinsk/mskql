#include "query.h"
#include "database.h"
#include "parser.h"
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

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

static int row_matches(struct table *t, struct where_clause *w, struct row *row);
static double cell_to_double(const struct cell *c);

int eval_condition(struct condition *cond, struct row *row,
                   struct table *t)
{
    if (!cond) return 1;
    switch (cond->type) {
        case COND_AND:
            return eval_condition(cond->left, row, t) &&
                   eval_condition(cond->right, row, t);
        case COND_OR:
            return eval_condition(cond->left, row, t) ||
                   eval_condition(cond->right, row, t);
        case COND_NOT:
            return !eval_condition(cond->left, row, t);
        case COND_MULTI_IN: {
            /* multi-column IN: (a, b) IN ((1,2), (3,4)) */
            int width = cond->multi_tuple_width;
            if (width <= 0) return 0;
            /* resolve column indices */
            int col_idxs[32];
            for (int ci = 0; ci < width && ci < 32; ci++) {
                /* strip table prefix from column name */
                sv col = cond->multi_columns.items[ci];
                for (size_t k = 0; k < col.len; k++) {
                    if (col.data[k] == '.') { col = sv_from(col.data + k + 1, col.len - k - 1); break; }
                }
                col_idxs[ci] = table_find_column_sv(t, col);
                if (col_idxs[ci] < 0) return 0;
            }
            int num_tuples = (int)cond->multi_values.count / width;
            int found = 0;
            for (int ti = 0; ti < num_tuples && !found; ti++) {
                int match = 1;
                for (int ci = 0; ci < width; ci++) {
                    struct cell *rc = &row->cells.items[col_idxs[ci]];
                    struct cell *vc = &cond->multi_values.items[ti * width + ci];
                    if (cell_compare(rc, vc) != 0) { match = 0; break; }
                }
                if (match) found = 1;
            }
            return cond->op == CMP_NOT_IN ? !found : found;
        }
        case COND_COMPARE: {
            /* EXISTS / NOT EXISTS — no column reference needed */
            if (cond->op == CMP_EXISTS)
                return cond->value.value.as_int != 0;
            if (cond->op == CMP_NOT_EXISTS)
                return cond->value.value.as_int == 0;
            int col_idx = table_find_column_sv(t, cond->column);
            if (col_idx < 0) return 0;
            struct cell *c = &row->cells.items[col_idx];
            if (cond->op == CMP_IS_NULL)
                return c->is_null || (column_type_is_text(c->type)
                       ? (c->value.as_text == NULL) : 0);
            if (cond->op == CMP_IS_NOT_NULL)
                return !c->is_null && (column_type_is_text(c->type)
                       ? (c->value.as_text != NULL) : 1);
            /* IN / NOT IN */
            if (cond->op == CMP_IN || cond->op == CMP_NOT_IN) {
                /* SQL standard: NULL IN (...) → UNKNOWN (false) */
                if (c->is_null || (column_type_is_text(c->type) && !c->value.as_text))
                    return cond->op == CMP_NOT_IN ? 1 : 0;
                int found = 0;
                for (size_t i = 0; i < cond->in_values.count; i++) {
                    /* skip NULL values in the IN list */
                    if (cond->in_values.items[i].is_null) continue;
                    if (cell_compare(c, &cond->in_values.items[i]) == 0) { found = 1; break; }
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
                for (size_t i = 0; i < cond->array_values.count; i++) {
                    int r = cell_compare(c, &cond->array_values.items[i]);
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
            struct cell copy = { .type = sc->type };
            if (column_type_is_text(sc->type) && sc->value.as_text)
                copy.value.as_text = strdup(sc->value.as_text);
            else
                copy.value = sc->value;
            return copy;
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
        if (!c.is_null && !(column_type_is_text(c.type) && !c.value.as_text))
            return c;
        if (column_type_is_text(c.type) && c.value.as_text) free(c.value.as_text);
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
        if (!a.is_null && !b.is_null && cell_equal(&a, &b)) {
            if (column_type_is_text(a.type) && a.value.as_text) free(a.value.as_text);
            if (column_type_is_text(b.type) && b.value.as_text) free(b.value.as_text);
            struct cell null_cell = { .type = a.type, .is_null = 1 };
            return null_cell;
        }
        if (column_type_is_text(b.type) && b.value.as_text) free(b.value.as_text);
        return a;
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
        int best_null = best.is_null || (column_type_is_text(best.type) && !best.value.as_text);
        for (int i = 1; i < n; i++) {
            struct cell cur = resolve_arg(args[i], t, src);
            int cur_null = cur.is_null || (column_type_is_text(cur.type) && !cur.value.as_text);
            if (best_null) {
                if (column_type_is_text(best.type) && best.value.as_text) free(best.value.as_text);
                best = cur;
                best_null = cur_null;
                continue;
            }
            if (cur_null) { if (column_type_is_text(cur.type) && cur.value.as_text) free(cur.value.as_text); continue; }
            int cmp = cell_compare(&cur, &best);
            if ((is_greatest && cmp > 0) || (!is_greatest && cmp < 0)) {
                if (column_type_is_text(best.type) && best.value.as_text) free(best.value.as_text);
                best = cur;
            } else {
                if (column_type_is_text(cur.type) && cur.value.as_text) free(cur.value.as_text);
            }
        }
        return best;
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
        if (arg.is_null || (column_type_is_text(arg.type) && !arg.value.as_text)) {
            result.is_null = 1;
        } else if (column_type_is_text(arg.type) && arg.value.as_text) {
            result.value.as_int = (int)strlen(arg.value.as_text);
            free(arg.value.as_text);
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
            free(arg.value.as_text);
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
                        int col_is_null = col_val.is_null || (column_type_is_text(col_val.type) && !col_val.value.as_text);
                        if (column_type_is_text(col_val.type) && col_val.value.as_text) free(col_val.value.as_text);
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

static void emit_row(struct table *t, struct query_select *s, struct row *src,
                     struct rows *result, int select_all, struct database *db)
{
    struct row dst = {0};
    da_init(&dst.cells);

    if (select_all) {
        for (size_t j = 0; j < src->cells.count; j++) {
            struct cell c = src->cells.items[j];
            struct cell copy = { .type = c.type, .is_null = c.is_null };
            if (column_type_is_text(c.type) && c.value.as_text)
                copy.value.as_text = strdup(c.value.as_text);
            else
                copy.value = c.value;
            da_push(&dst.cells, copy);
        }
    } else {
        /* walk comma-separated column list */
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
                        if (db_exec(db, &sub_q, &sub_rows) == 0 && sub_rows.count > 0
                            && sub_rows.data[0].cells.count > 0) {
                            struct cell *sc = &sub_rows.data[0].cells.items[0];
                            struct cell copy = { .type = sc->type, .is_null = sc->is_null };
                            if (column_type_is_text(sc->type) && sc->value.as_text)
                                copy.value.as_text = strdup(sc->value.as_text);
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
                            copy.value.as_text = strdup(c.value.as_text);
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

static int query_aggregate(struct table *t, struct query_select *s, struct rows *result)
{
    /* find WHERE column index if applicable (legacy path) */
    int where_col = -1;
    if (s->where.has_where && !s->where.where_cond) {
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

    /* resolve column index for each aggregate */
    int *agg_col = calloc(s->aggregates.count, sizeof(int));
    for (size_t a = 0; a < s->aggregates.count; a++) {
        if (sv_eq_cstr(s->aggregates.items[a].column, "*")) {
            agg_col[a] = -1; /* COUNT(*) doesn't need a column */
        } else {
            agg_col[a] = -1;
            for (size_t j = 0; j < t->columns.count; j++) {
                if (sv_eq_cstr(s->aggregates.items[a].column, t->columns.items[j].name)) {
                    agg_col[a] = (int)j;
                    break;
                }
            }
            if (agg_col[a] < 0) {
                fprintf(stderr, "aggregate column '" SV_FMT "' not found\n",
                        SV_ARG(s->aggregates.items[a].column));
                free(agg_col);
                return -1;
            }
        }
    }

    /* accumulate — single allocation for all aggregate arrays
     * layout: double[3*N] | size_t[N] | int[N]  (descending alignment) */
    size_t _nagg = s->aggregates.count;
    size_t _agg_alloc = _nagg * (3 * sizeof(double) + sizeof(size_t) + sizeof(int));
    char *_agg_buf = calloc(1, _agg_alloc ? _agg_alloc : 1);
    double *sums = (double *)_agg_buf;
    double *mins = sums + _nagg;
    double *maxs = mins + _nagg;
    size_t *nonnull_count = (size_t *)(maxs + _nagg);
    int *minmax_init = (int *)(nonnull_count + _nagg);
    size_t row_count = 0;

    for (size_t i = 0; i < t->rows.count; i++) {
        if (s->where.has_where) {
            if (s->where.where_cond) {
                if (!eval_condition(s->where.where_cond, &t->rows.items[i], t))
                    continue;
            } else if (where_col >= 0) {
                if (!cell_equal(&t->rows.items[i].cells.items[where_col],
                                &s->where.where_value))
                    continue;
            }
        }
        row_count++;
        for (size_t a = 0; a < s->aggregates.count; a++) {
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
    for (size_t a = 0; a < s->aggregates.count; a++) {
        struct cell c = {0};
        int col_is_float = (agg_col[a] >= 0 &&
                            t->columns.items[agg_col[a]].type == COLUMN_TYPE_FLOAT);
        switch (s->aggregates.items[a].func) {
            case AGG_COUNT:
                c.type = COLUMN_TYPE_INT;
                /* COUNT(*) counts all rows; COUNT(col) counts non-NULL */
                c.value.as_int = (agg_col[a] < 0) ? (int)row_count : (int)nonnull_count[a];
                break;
            case AGG_SUM:
                if (col_is_float) {
                    c.type = COLUMN_TYPE_FLOAT;
                    c.value.as_float = sums[a];
                } else {
                    c.type = COLUMN_TYPE_INT;
                    c.value.as_int = (int)sums[a];
                }
                break;
            case AGG_AVG:
                c.type = COLUMN_TYPE_FLOAT;
                c.value.as_float = nonnull_count[a] > 0 ? sums[a] / (double)nonnull_count[a] : 0.0;
                break;
            case AGG_MIN:
            case AGG_MAX: {
                double val = (s->aggregates.items[a].func == AGG_MIN) ? mins[a] : maxs[a];
                if (col_is_float) {
                    c.type = COLUMN_TYPE_FLOAT;
                    c.value.as_float = val;
                } else {
                    c.type = COLUMN_TYPE_INT;
                    c.value.as_int = (int)val;
                }
                break;
            }
            case AGG_NONE:
                break;
        }
        da_push(&dst.cells, c);
    }
    rows_push(result, dst);

    free(_agg_buf);
    free(agg_col);
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
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_UUID:
            return 0.0;
    }
    return 0.0;
}

/* sort helper for window ORDER BY */
struct sort_entry { size_t idx; const struct cell *key; };

static int sort_entry_cmp(const void *a, const void *b)
{
    const struct sort_entry *sa = a, *sb = b;
    return cell_compare(sa->key, sb->key);
}

static int query_window(struct table *t, struct query_select *s, struct rows *result)
{
    size_t nrows = t->rows.count;
    size_t nexprs = s->select_exprs.count;

    /* resolve column indices for plain columns and window args — single allocation */
    int *_win_buf = calloc(4 * nexprs + 1, sizeof(int));
    int *col_idx = _win_buf;
    int *part_idx = _win_buf + nexprs;
    int *ord_idx = _win_buf + 2 * nexprs;
    int *arg_idx = _win_buf + 3 * nexprs;

    for (size_t e = 0; e < nexprs; e++) {
        struct select_expr *se = &s->select_exprs.items[e];
        col_idx[e] = -1;
        part_idx[e] = -1;
        ord_idx[e] = -1;
        arg_idx[e] = -1;

        if (se->kind == SEL_COLUMN) {
            col_idx[e] = table_find_column_sv(t, se->column);
            if (col_idx[e] < 0) {
                fprintf(stderr, "column '" SV_FMT "' not found\n", SV_ARG(se->column));
                free(_win_buf);
                return -1;
            }
        } else {
            if (se->win.has_partition) {
                part_idx[e] = table_find_column_sv(t, se->win.partition_col);
                if (part_idx[e] < 0) {
                    fprintf(stderr, "partition column '" SV_FMT "' not found\n",
                            SV_ARG(se->win.partition_col));
                    free(_win_buf);
                    return -1;
                }
            }
            if (se->win.has_order) {
                ord_idx[e] = table_find_column_sv(t, se->win.order_col);
                if (ord_idx[e] < 0) {
                    fprintf(stderr, "order column '" SV_FMT "' not found\n",
                            SV_ARG(se->win.order_col));
                    free(_win_buf);
                    return -1;
                }
            }
            if (se->win.arg_column.len > 0 && !sv_eq_cstr(se->win.arg_column, "*")) {
                arg_idx[e] = table_find_column_sv(t, se->win.arg_column);
                if (arg_idx[e] < 0) {
                    fprintf(stderr, "window arg column '" SV_FMT "' not found\n",
                            SV_ARG(se->win.arg_column));
                    free(_win_buf);
                    return -1;
                }
            }
        }
    }

    /* collect rows matching WHERE (or all rows if no WHERE) */
    DYNAMIC_ARRAY(size_t) match_idx;
    da_init(&match_idx);
    for (size_t i = 0; i < nrows; i++) {
        if (!row_matches(t, &s->where, &t->rows.items[i]))
            continue;
        da_push(&match_idx, i);
    }
    size_t nmatch = match_idx.count;

    /* build sorted row index (by first ORDER BY we find, or original order) */
    struct sort_entry *sorted = calloc(nmatch, sizeof(struct sort_entry));
    int global_ord = -1;
    for (size_t e = 0; e < nexprs; e++) {
        if (s->select_exprs.items[e].kind == SEL_WINDOW && ord_idx[e] >= 0) {
            global_ord = ord_idx[e];
            break;
        }
    }
    for (size_t i = 0; i < nmatch; i++) {
        sorted[i].idx = match_idx.items[i];
        if (global_ord >= 0)
            sorted[i].key = &t->rows.items[match_idx.items[i]].cells.items[global_ord];
        else
            sorted[i].key = NULL;
    }
    if (global_ord >= 0)
        qsort(sorted, nmatch, sizeof(struct sort_entry), sort_entry_cmp);

    /* for each row (in sorted order), compute all expressions */
    for (size_t ri = 0; ri < nmatch; ri++) {
        size_t row_i = sorted[ri].idx;
        struct row *src = &t->rows.items[row_i];
        struct row dst = {0};
        da_init(&dst.cells);

        for (size_t e = 0; e < nexprs; e++) {
            struct select_expr *se = &s->select_exprs.items[e];
            struct cell c = {0};

            if (se->kind == SEL_COLUMN) {
                cell_copy(&c, &src->cells.items[col_idx[e]]);
            } else {
                /* compute window value */
                /* find partition peers: rows with same partition column value */
                size_t part_count = 0;
                size_t part_rank = 0;
                double part_sum = 0.0;
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

                    /* for SUM/AVG, accumulate */
                    if (arg_idx[e] >= 0) {
                        part_sum += cell_to_double(&peer->cells.items[arg_idx[e]]);
                    }

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
                        if (arg_idx[e] >= 0 &&
                            t->columns.items[arg_idx[e]].type == COLUMN_TYPE_FLOAT) {
                            c.type = COLUMN_TYPE_FLOAT;
                            c.value.as_float = part_sum;
                        } else {
                            c.type = COLUMN_TYPE_INT;
                            c.value.as_int = (int)part_sum;
                        }
                        break;
                    case WIN_COUNT:
                        c.type = COLUMN_TYPE_INT;
                        c.value.as_int = (int)part_count;
                        break;
                    case WIN_AVG:
                        c.type = COLUMN_TYPE_FLOAT;
                        c.value.as_float = part_count > 0 ? part_sum / (double)part_count : 0.0;
                        break;
                }
            }
            da_push(&dst.cells, c);
        }
        rows_push(result, dst);
    }

    da_free(&match_idx);
    free(sorted);
    free(_win_buf);

    /* apply outer ORDER BY if present */
    if (s->has_order_by && result->count > 1) {
        /* build a temporary table descriptor from the result for sorting */
        /* The result columns correspond to select_exprs; we need to find
         * the ORDER BY column index within the result columns */
        for (size_t oi = 0; oi < s->order_by_items.count; oi++) {
            sv ord_name = s->order_by_items.items[oi].column;
            int ord_desc = s->order_by_items.items[oi].desc;
            int res_col = -1;
            /* find matching column in select_exprs */
            for (size_t e = 0; e < nexprs; e++) {
                if (s->select_exprs.items[e].kind == SEL_COLUMN) {
                    sv col = s->select_exprs.items[e].column;
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
                               struct query_select *s, sv name)
{
    /* check group columns first */
    for (size_t k = 0; k < ngrp; k++) {
        if (grp_cols[k] >= 0 && sv_eq_cstr(name, t->columns.items[grp_cols[k]].name))
            return (int)k;
    }
    /* check aggregate names */
    for (size_t a = 0; a < s->aggregates.count; a++) {
        const char *agg_name = "?";
        switch (s->aggregates.items[a].func) {
            case AGG_SUM:   agg_name = "sum";   break;
            case AGG_COUNT: agg_name = "count"; break;
            case AGG_AVG:   agg_name = "avg";   break;
            case AGG_MIN:   agg_name = "min";   break;
            case AGG_MAX:   agg_name = "max";   break;
            case AGG_NONE: break;
        }
        if (sv_eq_cstr(name, agg_name))
            return (int)(ngrp + a);
    }
    return -1;
}

static int query_group_by(struct table *t, struct query_select *s, struct rows *result)
{
    /* resolve GROUP BY column indices */
    size_t ngrp = s->group_by_cols.count;
    if (ngrp == 0) ngrp = 1; /* backward compat: single group_by_col */
    int grp_cols[32];
    if (s->group_by_cols.count > 0) {
        for (size_t k = 0; k < ngrp && k < 32; k++) {
            grp_cols[k] = table_find_column_sv(t, s->group_by_cols.items[k]);
            if (grp_cols[k] < 0) {
                fprintf(stderr, "GROUP BY column '" SV_FMT "' not found\n",
                        SV_ARG(s->group_by_cols.items[k]));
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

    /* collect matching rows */
    DYNAMIC_ARRAY(size_t) matching;
    da_init(&matching);
    for (size_t i = 0; i < t->rows.count; i++) {
        if (s->where.has_where && s->where.where_cond) {
            if (!eval_condition(s->where.where_cond, &t->rows.items[i], t))
                continue;
        }
        da_push(&matching, i);
    }

    /* find distinct group keys (compare all group columns as a tuple) */
    DYNAMIC_ARRAY(size_t) group_starts;
    da_init(&group_starts);

    for (size_t m = 0; m < matching.count; m++) {
        size_t ri = matching.items[m];
        int found = 0;
        for (size_t g = 0; g < group_starts.count; g++) {
            size_t gi = matching.items[group_starts.items[g]];
            int eq = 1;
            for (size_t k = 0; k < ngrp; k++) {
                if (!cell_equal_nullsafe(&t->rows.items[ri].cells.items[grp_cols[k]],
                                &t->rows.items[gi].cells.items[grp_cols[k]])) {
                    eq = 0; break;
                }
            }
            if (eq) { found = 1; break; }
        }
        if (!found) da_push(&group_starts, m);
    }

    /* pre-allocate aggregate accumulators in a single allocation */
    size_t agg_n = s->aggregates.count;
    void *_grp_buf = NULL;
    double *sums = NULL, *gmins = NULL, *gmaxs = NULL;
    int *gminmax_init = NULL, *gagg_cols = NULL;
    size_t *gnonnull = NULL;
    if (agg_n > 0) {
        /* layout: double[3*N] | size_t[N] | int[2*N]  (descending alignment) */
        _grp_buf = malloc(3 * agg_n * sizeof(double) + agg_n * sizeof(size_t) + 2 * agg_n * sizeof(int));
        sums          = (double *)_grp_buf;
        gmins         = sums + agg_n;
        gmaxs         = gmins + agg_n;
        gnonnull      = (size_t *)(gmaxs + agg_n);
        gminmax_init  = (int *)(gnonnull + agg_n);
        gagg_cols     = gminmax_init + agg_n;
    }

    /* resolve aggregate column indices once */
    for (size_t a = 0; a < agg_n; a++) {
        if (sv_eq_cstr(s->aggregates.items[a].column, "*"))
            gagg_cols[a] = -1;
        else
            gagg_cols[a] = table_find_column_sv(t, s->aggregates.items[a].column);
    }

    /* build HAVING tmp_t once (columns don't change between groups) */
    struct table having_t = {0};
    int has_having_t = 0;
    if (s->has_having && s->having_cond) {
        has_having_t = 1;
        da_init(&having_t.columns);
        da_init(&having_t.rows);
        da_init(&having_t.indexes);
        for (size_t k = 0; k < ngrp; k++) {
            struct column col_grp = { .name = strdup(t->columns.items[grp_cols[k]].name),
                                      .type = t->columns.items[grp_cols[k]].type,
                                      .enum_type_name = NULL };
            da_push(&having_t.columns, col_grp);
        }
        for (size_t a = 0; a < s->aggregates.count; a++) {
            const char *agg_name = "?";
            switch (s->aggregates.items[a].func) {
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
            if (s->aggregates.items[a].func == AGG_AVG)
                ctype = COLUMN_TYPE_FLOAT;
            if (s->aggregates.items[a].func == AGG_COUNT)
                ctype = COLUMN_TYPE_INT;
            struct column col_a = { .name = strdup(agg_name),
                                    .type = ctype,
                                    .enum_type_name = NULL };
            da_push(&having_t.columns, col_a);
        }
    }

    /* for each group, compute aggregates */
    for (size_t g = 0; g < group_starts.count; g++) {
        size_t first_ri = matching.items[group_starts.items[g]];

        /* reset accumulators */
        size_t grp_count = 0;
        if (agg_n > 0) {
            memset(sums, 0, agg_n * sizeof(double));
            memset(gminmax_init, 0, agg_n * sizeof(int));
            memset(gnonnull, 0, agg_n * sizeof(size_t));
        }

        for (size_t m = 0; m < matching.count; m++) {
            size_t ri = matching.items[m];
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
            for (size_t a = 0; a < s->aggregates.count; a++) {
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

        /* build result row: group key columns + aggregates */
        struct row dst = {0};
        da_init(&dst.cells);

        /* add group key columns */
        for (size_t k = 0; k < ngrp; k++) {
            struct cell gc;
            cell_copy(&gc, &t->rows.items[first_ri].cells.items[grp_cols[k]]);
            da_push(&dst.cells, gc);
        }

        /* add aggregate values */
        for (size_t a = 0; a < s->aggregates.count; a++) {
            struct cell c = {0};
            int ac_idx = gagg_cols[a];
            int col_is_float = (ac_idx >= 0 &&
                                t->columns.items[ac_idx].type == COLUMN_TYPE_FLOAT);
            switch (s->aggregates.items[a].func) {
                case AGG_COUNT:
                    c.type = COLUMN_TYPE_INT;
                    /* COUNT(*) counts all rows; COUNT(col) counts non-NULL */
                    c.value.as_int = (gagg_cols[a] < 0) ? (int)grp_count : (int)gnonnull[a];
                    break;
                case AGG_SUM:
                    if (col_is_float) {
                        c.type = COLUMN_TYPE_FLOAT;
                        c.value.as_float = sums[a];
                    } else {
                        c.type = COLUMN_TYPE_INT;
                        c.value.as_int = (int)sums[a];
                    }
                    break;
                case AGG_AVG:
                    c.type = COLUMN_TYPE_FLOAT;
                    c.value.as_float = gnonnull[a] > 0 ? sums[a] / (double)gnonnull[a] : 0.0;
                    break;
                case AGG_MIN:
                case AGG_MAX: {
                    double val = (s->aggregates.items[a].func == AGG_MIN) ? gmins[a] : gmaxs[a];
                    if (col_is_float) {
                        c.type = COLUMN_TYPE_FLOAT;
                        c.value.as_float = val;
                    } else {
                        c.type = COLUMN_TYPE_INT;
                        c.value.as_int = (int)val;
                    }
                    break;
                }
                case AGG_NONE:
                    break;
            }
            da_push(&dst.cells, c);
        }
        /* HAVING filter */
        if (has_having_t) {
            int passes = eval_condition(s->having_cond, &dst, &having_t);
            if (!passes) {
                row_free(&dst);
                continue;
            }
        }

        rows_push(result, dst);
    }

    da_free(&matching);
    da_free(&group_starts);
    free(_grp_buf);
    if (has_having_t) {
        for (size_t i = 0; i < having_t.columns.count; i++)
            column_free(&having_t.columns.items[i]);
        da_free(&having_t.columns);
    }

    /* ORDER BY on grouped results (multi-column) */
    if (s->has_order_by && s->order_by_items.count > 0 && result->count > 1) {
        int ord_res[32];
        int ord_descs[32];
        size_t nord = s->order_by_items.count < 32 ? s->order_by_items.count : 32;
        for (size_t k = 0; k < nord; k++) {
            ord_res[k] = grp_find_result_col(t, grp_cols, ngrp, s,
                                             s->order_by_items.items[k].column);
            ord_descs[k] = s->order_by_items.items[k].desc;
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

static int row_matches(struct table *t, struct where_clause *w, struct row *row)
{
    if (!w->has_where) return 1;
    if (w->where_cond)
        return eval_condition(w->where_cond, row, t);
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

static int query_select_exec(struct table *t, struct query_select *s, struct rows *result, struct database *db)
{
    /* dispatch to window path if select_exprs are present */
    if (s->select_exprs.count > 0)
        return query_window(t, s, result);

    /* dispatch to GROUP BY path */
    if (s->has_group_by)
        return query_group_by(t, s, result);

    /* dispatch to aggregate path if aggregates are present */
    if (s->aggregates.count > 0)
        return query_aggregate(t, s, result);

    int select_all = sv_eq_cstr(s->columns, "*");

    /* try index lookup for simple equality WHERE on indexed column */
    if (s->where.has_where && s->where.where_cond && s->where.where_cond->type == COND_COMPARE
        && s->where.where_cond->op == CMP_EQ && !s->has_order_by) {
        int where_col = table_find_column_sv(t, s->where.where_cond->column);
        if (where_col >= 0) {
            for (size_t idx = 0; idx < t->indexes.count; idx++) {
                if (strcmp(t->indexes.items[idx].column_name,
                           t->columns.items[where_col].name) == 0) {
                    size_t *ids = NULL;
                    size_t id_count = 0;
                    index_lookup(&t->indexes.items[idx], &s->where.where_cond->value,
                                 &ids, &id_count);
                    for (size_t k = 0; k < id_count; k++) {
                        if (ids[k] < t->rows.count)
                            emit_row(t, s, &t->rows.items[ids[k]], result, select_all, db);
                    }
                    return 0;
                }
            }
        }
    }

    /* full scan — collect matching row indices */
    DYNAMIC_ARRAY(size_t) match_idx;
    da_init(&match_idx);
    for (size_t i = 0; i < t->rows.count; i++) {
        if (!row_matches(t, &s->where, &t->rows.items[i]))
            continue;
        da_push(&match_idx, i);
    }

    /* ORDER BY — sort indices using the original table data (multi-column) */
    if (s->has_order_by && s->order_by_items.count > 0) {
        /* resolve column indices for all ORDER BY items */
        int ord_cols[32];
        int ord_descs[32];
        size_t nord = s->order_by_items.count < 32 ? s->order_by_items.count : 32;
        for (size_t k = 0; k < nord; k++) {
            ord_cols[k] = table_find_column_sv(t, s->order_by_items.items[k].column);
            /* if not found, try resolving as a SELECT alias */
            if (ord_cols[k] < 0 && s->columns.len > 0) {
                ord_cols[k] = resolve_alias_to_column(t, s->columns,
                                                       s->order_by_items.items[k].column);
            }
            ord_descs[k] = s->order_by_items.items[k].desc;
        }
        _sort_ctx = (struct sort_ctx){ .cols = ord_cols, .descs = ord_descs,
                                       .ncols = nord, .table = t };
        qsort(match_idx.items, match_idx.count, sizeof(size_t), cmp_indices_multi);
    }

    /* project into result rows */
    struct rows tmp = {0};
    for (size_t i = 0; i < match_idx.count; i++) {
        emit_row(t, s, &t->rows.items[match_idx.items[i]], &tmp, select_all, db);
    }
    da_free(&match_idx);

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
            if (tmp.data[i].cells.items) row_free(&tmp.data[i]);
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
        if (tmp.data[i].cells.items) row_free(&tmp.data[i]);
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

static int query_delete_exec(struct table *t, struct query_delete *d, struct rows *result)
{
    int has_ret = (d->has_returning && d->returning_columns.len > 0);
    int return_all = has_ret && sv_eq_cstr(d->returning_columns, "*");
    size_t deleted = 0;
    for (size_t i = 0; i < t->rows.count; ) {
        if (row_matches(t, &d->where, &t->rows.items[i])) {
            /* capture row for RETURNING before freeing */
            if (has_ret && result)
                emit_returning_row(t, &t->rows.items[i], d->returning_columns, return_all, result);
            row_free(&t->rows.items[i]);
            for (size_t j = i; j + 1 < t->rows.count; j++)
                t->rows.items[j] = t->rows.items[j + 1];
            t->rows.count--;
            deleted++;
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

static int query_update_exec(struct table *t, struct query_update *u, struct rows *result)
{
    int has_ret = (u->has_returning && u->returning_columns.len > 0);
    int return_all = has_ret && sv_eq_cstr(u->returning_columns, "*");
    size_t updated = 0;
    for (size_t i = 0; i < t->rows.count; i++) {
        if (!row_matches(t, &u->where, &t->rows.items[i]))
            continue;
        updated++;
        for (size_t sc = 0; sc < u->set_clauses.count; sc++) {
            int col_idx = table_find_column_sv(t, u->set_clauses.items[sc].column);
            if (col_idx < 0) continue;
            struct cell *dst = &t->rows.items[i].cells.items[col_idx];
            /* free old text */
            if (column_type_is_text(dst->type) && dst->value.as_text)
                free(dst->value.as_text);
            if (u->set_clauses.items[sc].has_expr) {
                /* evaluate expression per-row */
                struct cell val = eval_arith_expr(u->set_clauses.items[sc].value_expr, t, &t->rows.items[i]);
                *dst = val;
            } else {
                /* copy literal value */
                cell_copy(dst, &u->set_clauses.items[sc].value);
            }
        }
        /* capture row for RETURNING after SET */
        if (has_ret && result)
            emit_returning_row(t, &t->rows.items[i], u->returning_columns, return_all, result);
    }
    /* rebuild indexes after cell mutation */
    if (updated > 0 && t->indexes.count > 0)
        rebuild_indexes(t);

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

static int query_insert_exec(struct table *t, struct query_insert *ins, struct rows *result)
{
    int has_returning = (ins->returning_columns.len > 0);
    int return_all = has_returning && sv_eq_cstr(ins->returning_columns, "*");

    for (size_t r = 0; r < ins->insert_rows.count; r++) {
        struct row *src = &ins->insert_rows.items[r];
        struct row copy = {0};
        da_init(&copy.cells);
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

int query_exec(struct table *t, struct query *q, struct rows *result, struct database *db)
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
            return -1;
        case QUERY_TYPE_SELECT:
            return query_select_exec(t, &q->select, result, db);
        case QUERY_TYPE_INSERT:
            return query_insert_exec(t, &q->insert, result);
        case QUERY_TYPE_DELETE:
            return query_delete_exec(t, &q->del, result);
        case QUERY_TYPE_UPDATE:
            return query_update_exec(t, &q->update, result);
    }
    return -1;
}
