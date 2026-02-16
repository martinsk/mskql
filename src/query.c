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
#include <math.h>

/* forward declarations for cell helpers used by legacy eval functions */
static void cell_release(struct cell *c);
static void cell_release_rb(struct cell *c, struct bump_alloc *rb);
static struct cell cell_deep_copy(const struct cell *src);
static struct cell cell_deep_copy_rb(const struct cell *src, struct bump_alloc *rb);
static int cell_is_null(const struct cell *c);

/* SQL LIKE pattern matching: % = any sequence, _ = any single char.
 * Iterative algorithm: tracks a single backtrack point for the last '%'
 * seen, giving O(n*m) worst case instead of exponential. */
int like_match(const char *pattern, const char *text, int case_insensitive)
{
    const char *star_p = NULL; /* pattern position after last '%' */
    const char *star_t = NULL; /* text position at last '%' match */
    while (*text) {
        if (*pattern == '\\' && (pattern[1] == '%' || pattern[1] == '_' || pattern[1] == '\\')) {
            /* escaped literal character */
            char pc = pattern[1], tc = *text;
            if (case_insensitive) { pc = tolower((unsigned char)pc); tc = tolower((unsigned char)tc); }
            if (pc == tc) {
                pattern += 2; text++;
                continue;
            }
            if (star_p) { pattern = star_p; text = ++star_t; continue; }
            return 0;
        }
        if (*pattern == '%') {
            pattern++;
            star_p = pattern;
            star_t = text;
            continue;
        }
        if (*pattern == '_') {
            pattern++; text++;
            continue;
        }
        char pc = *pattern, tc = *text;
        if (case_insensitive) { pc = tolower((unsigned char)pc); tc = tolower((unsigned char)tc); }
        if (pc == tc) {
            pattern++; text++;
            continue;
        }
        /* mismatch — backtrack to last '%' if possible */
        if (star_p) {
            pattern = star_p;
            text = ++star_t;
            continue;
        }
        return 0;
    }
    /* consume trailing '%' wildcards and escaped chars */
    while (*pattern == '%') pattern++;
    return *pattern == '\0';
}

/* condition_free, query_free, and all query_*_free functions live in parser.c
 * (the allocating module) per JPL ownership rules. */

/* cell_cmp → use shared cell_compare from row.h (returns -2 for incompatible types) */

static int row_matches(struct table *t, struct where_clause *w, struct query_arena *arena, struct row *row, struct database *db);
static double cell_to_double(const struct cell *c);

/* Replace all occurrences of `ref` in sql_buf with `lit`, respecting word
 * boundaries (the character after the match must not be alnum or '_'). */
static void subst_ref(char *sql_buf, size_t bufsize,
                       const char *ref, size_t rlen,
                       const char *lit, size_t llen)
{
    char *pos;
    while ((pos = strstr(sql_buf, ref)) != NULL) {
        /* check word boundary after match */
        char after = pos[rlen];
        if (isalnum((unsigned char)after) || after == '_') break;
        /* also check word boundary before match (must be start or non-ident) */
        if (pos > sql_buf) {
            char before = pos[-1];
            if (isalnum((unsigned char)before) || before == '_') {
                break;
            }
        }
        size_t cur_len = strlen(sql_buf);
        if (cur_len - rlen + llen >= bufsize - 1) break;
        memmove(pos + llen, pos + rlen,
                cur_len - (size_t)(pos - sql_buf) - rlen + 1);
        memcpy(pos, lit, llen);
    }
}

/* Format a cell value as a SQL literal string into buf. Returns 0 on success. */
static int cell_to_sql_literal(const struct cell *cv, char *buf, size_t bufsize)
{
    if (cell_is_null(cv)) {
        snprintf(buf, bufsize, "NULL");
        return 0;
    }
    switch (cv->type) {
    case COLUMN_TYPE_INT:
        snprintf(buf, bufsize, "%d", cv->value.as_int);
        return 0;
    case COLUMN_TYPE_BIGINT:
        snprintf(buf, bufsize, "%lld", cv->value.as_bigint);
        return 0;
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_NUMERIC:
        snprintf(buf, bufsize, "%g", cv->value.as_float);
        return 0;
    case COLUMN_TYPE_BOOLEAN:
        snprintf(buf, bufsize, "%s", cv->value.as_bool ? "TRUE" : "FALSE");
        return 0;
    case COLUMN_TYPE_SMALLINT:
        snprintf(buf, bufsize, "%d", (int)cv->value.as_smallint);
        return 0;
    case COLUMN_TYPE_DATE: {
        char tmp[32];
        date_to_str(cv->value.as_date, tmp, sizeof(tmp));
        snprintf(buf, bufsize, "'%s'", tmp);
        return 0;
    }
    case COLUMN_TYPE_TIME: {
        char tmp[32];
        time_to_str(cv->value.as_time, tmp, sizeof(tmp));
        snprintf(buf, bufsize, "'%s'", tmp);
        return 0;
    }
    case COLUMN_TYPE_TIMESTAMP: {
        char tmp[32];
        timestamp_to_str(cv->value.as_timestamp, tmp, sizeof(tmp));
        snprintf(buf, bufsize, "'%s'", tmp);
        return 0;
    }
    case COLUMN_TYPE_TIMESTAMPTZ: {
        char tmp[40];
        timestamptz_to_str(cv->value.as_timestamp, tmp, sizeof(tmp));
        snprintf(buf, bufsize, "'%s'", tmp);
        return 0;
    }
    case COLUMN_TYPE_INTERVAL: {
        char tmp[128];
        interval_to_str(cv->value.as_interval, tmp, sizeof(tmp));
        snprintf(buf, bufsize, "'%s'", tmp);
        return 0;
    }
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_UUID:
        if (!cv->value.as_text) return -1;
        {
            size_t pos = 0;
            if (pos < bufsize - 1) buf[pos++] = '\'';
            for (const char *p = cv->value.as_text; *p && pos < bufsize - 2; p++) {
                if (*p == '\'') {
                    if (pos < bufsize - 2) buf[pos++] = '\'';
                }
                if (pos < bufsize - 2) buf[pos++] = *p;
            }
            if (pos < bufsize - 1) buf[pos++] = '\'';
            buf[pos] = '\0';
        }
        return 0;
    }
    return -1;
}

/* Check if identifier of length id_len at id_start appears as a table name
 * in the subquery SQL (after FROM or JOIN keywords). */
static int is_inner_table_name(const char *sql_buf, const char *id_start,
                               size_t id_len)
{
    /* scan for FROM/JOIN keywords and check the identifier that follows */
    const char *p = sql_buf;
    while (*p) {
        /* skip non-alpha */
        if (!isalpha((unsigned char)*p)) { p++; continue; }
        /* read a word */
        const char *ws = p;
        while (isalnum((unsigned char)*p) || *p == '_') p++;
        size_t wlen = (size_t)(p - ws);
        int is_from = (wlen == 4 && strncasecmp(ws, "FROM", 4) == 0);
        int is_join = (wlen == 4 && strncasecmp(ws, "JOIN", 4) == 0);
        if (!is_from && !is_join) continue;
        /* skip whitespace after FROM/JOIN */
        while (*p == ' ' || *p == '\t' || *p == '\n') p++;
        /* skip optional quote */
        char quote = 0;
        if (*p == '"') { quote = '"'; p++; }
        /* read the table name */
        const char *ts = p;
        while (isalnum((unsigned char)*p) || *p == '_') p++;
        size_t tlen = (size_t)(p - ts);
        if (quote && *p == '"') p++;
        /* check if this table name matches the identifier */
        if (tlen == id_len && strncasecmp(ts, id_start, id_len) == 0)
            return 1;
        /* also check if the table has an alias: FROM tablename alias */
        const char *after_tbl = p;
        while (*after_tbl == ' ' || *after_tbl == '\t') after_tbl++;
        /* skip optional AS */
        if (strncasecmp(after_tbl, "AS", 2) == 0 &&
            !isalnum((unsigned char)after_tbl[2]) && after_tbl[2] != '_') {
            after_tbl += 2;
            while (*after_tbl == ' ' || *after_tbl == '\t') after_tbl++;
        }
        /* read alias */
        if (isalpha((unsigned char)*after_tbl) || *after_tbl == '_') {
            const char *as = after_tbl;
            while (isalnum((unsigned char)*after_tbl) || *after_tbl == '_') after_tbl++;
            size_t alen = (size_t)(after_tbl - as);
            if (alen == id_len && strncasecmp(as, id_start, id_len) == 0)
                return 1;
        }
    }
    return 0;
}

/* Substitute correlated outer column references in a subquery SQL string.
 * Replaces both "tablename.col" and any "X.col" patterns where X is an
 * unknown prefix (table alias) and col matches an outer table column,
 * but only if X is not a table name or alias referenced inside the subquery. */
static void subst_correlated_refs(char *sql_buf, size_t bufsize,
                                  struct table *t, struct row *row)
{
    for (size_t ci = 0; ci < t->columns.count && ci < row->cells.count; ci++) {
        const char *cname = t->columns.items[ci].name;
        size_t cname_len = strlen(cname);
        char lit[256];
        if (cell_to_sql_literal(&row->cells.items[ci], lit, sizeof(lit)) != 0)
            continue;
        size_t llen = strlen(lit);

        /* 1) substitute "tablename.col" */
        char ref[256];
        snprintf(ref, sizeof(ref), "%s.%s", t->name, cname);
        subst_ref(sql_buf, bufsize, ref, strlen(ref), lit, llen);

        /* 2) substitute any "X.col" where X is an alias we don't know —
         * scan for ".colname" preceded by an identifier */
        char dot_col[256];
        snprintf(dot_col, sizeof(dot_col), ".%s", cname);
        size_t dc_len = 1 + cname_len;
        char *pos = sql_buf;
        while ((pos = strstr(pos, dot_col)) != NULL) {
            /* check word boundary after */
            char after = pos[dc_len];
            if (isalnum((unsigned char)after) || after == '_') { pos++; continue; }
            /* find start of the identifier before the dot */
            char *dot = pos;
            if (dot <= sql_buf) { pos++; continue; }
            char *id_start = dot - 1;
            while (id_start > sql_buf && (isalnum((unsigned char)id_start[-1]) || id_start[-1] == '_'))
                id_start--;
            if (id_start == dot) { pos++; continue; } /* no identifier before dot */
            size_t id_len = (size_t)(dot - id_start);
            /* skip if it's the outer table name (already handled above) */
            if (id_len == strlen(t->name) && strncasecmp(id_start, t->name, id_len) == 0) { pos++; continue; }
            /* skip if the identifier is a table name or alias inside the subquery */
            if (is_inner_table_name(sql_buf, id_start, id_len)) { pos++; continue; }
            /* check word boundary before the identifier */
            if (id_start > sql_buf) {
                char before = id_start[-1];
                if (isalnum((unsigned char)before) || before == '_') { pos++; continue; }
            }
            /* replace "alias.col" with literal */
            size_t full_ref_len = id_len + dc_len;
            size_t cur_len = strlen(sql_buf);
            if (cur_len - full_ref_len + llen >= bufsize - 1) { pos++; continue; }
            memmove(id_start + llen, id_start + full_ref_len,
                    cur_len - (size_t)(id_start - sql_buf) - full_ref_len + 1);
            memcpy(id_start, lit, llen);
            pos = id_start + llen; /* continue after replacement */
        }
    }
}

static inline int cmp_result_matches_op(int r, enum cmp_op op)
{
    switch (op) {
    case CMP_EQ: return (r == 0);
    case CMP_NE: return (r != 0);
    case CMP_LT: return (r < 0);
    case CMP_GT: return (r > 0);
    case CMP_LE: return (r <= 0);
    case CMP_GE: return (r >= 0);
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
    case CMP_REGEX_MATCH:
    case CMP_REGEX_NOT_MATCH:
        return 0;
    }
    return 0;
}

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
                    char sql_buf[4096];
                    strncpy(sql_buf, sql_tmpl, sizeof(sql_buf) - 1);
                    sql_buf[sizeof(sql_buf) - 1] = '\0';
                    if (t && row)
                        subst_correlated_refs(sql_buf, sizeof(sql_buf), t, row);
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
            int has_lhs_expr = 0;
            if (cond->column.len > 0) {
                /* Try column lookup first (needed for HAVING with aggregate virtual columns) */
                int col_idx = table_find_column_sv(t, cond->column);
                if (col_idx >= 0) {
                    c = &row->cells.items[col_idx];
                } else if (cond->lhs_expr != IDX_NONE) {
                    /* Column not found — fall back to expression evaluation
                     * (needed for CASE WHEN SUM(x) > 50 etc.) */
                    has_lhs_expr = 1;
                    lhs_tmp = eval_expr(cond->lhs_expr, arena, t, row, db, NULL);
                    c = &lhs_tmp;
                } else {
                    return 0;
                }
            } else if (cond->lhs_expr != IDX_NONE) {
                has_lhs_expr = 1;
                lhs_tmp = eval_expr(cond->lhs_expr, arena, t, row, db, NULL);
                c = &lhs_tmp;
            } else {
                int col_idx = table_find_column_sv(t, cond->column);
                if (col_idx < 0) return 0;
                c = &row->cells.items[col_idx];
            }
            int cond_result = 0;
            if (cond->op == CMP_IS_NULL) {
                cond_result = c->is_null || (column_type_is_text(c->type)
                       ? (c->value.as_text == NULL) : 0);
                goto cond_cleanup;
            }
            if (cond->op == CMP_IS_NOT_NULL) {
                cond_result = !c->is_null && (column_type_is_text(c->type)
                       ? (c->value.as_text != NULL) : 1);
                goto cond_cleanup;
            }
            /* IN / NOT IN */
            if (cond->op == CMP_IN || cond->op == CMP_NOT_IN) {
                /* SQL standard: NULL IN (...) → UNKNOWN (false);
                 * NULL NOT IN (...) → UNKNOWN (false) */
                if (c->is_null || (column_type_is_text(c->type) && !c->value.as_text))
                    goto cond_cleanup;
                int found = 0;
                for (uint32_t i = 0; i < cond->in_values_count; i++) {
                    struct cell *iv = &ACELL(arena, cond->in_values_start + i);
                    /* skip NULL values in the IN list */
                    if (iv->is_null) continue;
                    if (cell_compare(c, iv) == 0) { found = 1; break; }
                }
                cond_result = cond->op == CMP_IN ? found : !found;
                goto cond_cleanup;
            }
            /* BETWEEN */
            if (cond->op == CMP_BETWEEN) {
                int lo = cell_compare(c, &cond->value);
                int hi = cell_compare(c, &cond->between_high);
                if (lo == -2 || hi == -2) goto cond_cleanup;
                cond_result = lo >= 0 && hi <= 0;
                goto cond_cleanup;
            }
            /* IS DISTINCT FROM / IS NOT DISTINCT FROM */
            if (cond->op == CMP_IS_DISTINCT || cond->op == CMP_IS_NOT_DISTINCT) {
                /* NULL-safe equality: NULL IS NOT DISTINCT FROM NULL → true */
                int a_null = c->is_null || (column_type_is_text(c->type)
                             && !c->value.as_text);
                struct cell *rhs_val = &cond->value;
                struct cell rhs_col_cell;
                if (cond->rhs_column.len > 0) {
                    int rhs_col = table_find_column_sv(t, cond->rhs_column);
                    if (rhs_col < 0) goto cond_cleanup;
                    rhs_col_cell = row->cells.items[rhs_col];
                    rhs_val = &rhs_col_cell;
                }
                int b_null = rhs_val->is_null || (column_type_is_text(rhs_val->type)
                             && !rhs_val->value.as_text);
                if (a_null && b_null) {
                    cond_result = (cond->op == CMP_IS_NOT_DISTINCT);
                    goto cond_cleanup;
                }
                if (a_null || b_null) {
                    cond_result = (cond->op == CMP_IS_DISTINCT);
                    goto cond_cleanup;
                }
                int eq = (cell_compare(c, rhs_val) == 0);
                cond_result = cond->op == CMP_IS_DISTINCT ? !eq : eq;
                goto cond_cleanup;
            }
            /* LIKE / ILIKE */
            if (cond->op == CMP_LIKE || cond->op == CMP_ILIKE) {
                if (!column_type_is_text(c->type) || !c->value.as_text)
                    goto cond_cleanup;
                if (!cond->value.value.as_text) goto cond_cleanup;
                cond_result = like_match(cond->value.value.as_text, c->value.as_text,
                                  cond->op == CMP_ILIKE);
                goto cond_cleanup;
            }
            /* ~ / !~ regex match (simple stub supporting ^, $, and () groups) */
            if (cond->op == CMP_REGEX_MATCH || cond->op == CMP_REGEX_NOT_MATCH) {
                if (!column_type_is_text(c->type) || !c->value.as_text)
                    goto cond_cleanup;
                if (!cond->value.value.as_text) goto cond_cleanup;
                const char *pat = cond->value.value.as_text;
                const char *str = c->value.as_text;
                /* strip anchors and parentheses to get a simplified pattern */
                char simpat[256];
                size_t si = 0;
                int anchored_start = 0, anchored_end = 0;
                for (const char *p = pat; *p && si < sizeof(simpat) - 1; p++) {
                    if (p == pat && *p == '^') { anchored_start = 1; continue; }
                    if (*p == '$' && *(p+1) == '\0') { anchored_end = 1; continue; }
                    if (*p == '(' || *p == ')') continue;
                    if (*p == '.' && *(p+1) == '*') { p++; continue; } /* .* = skip */
                    simpat[si++] = *p;
                }
                simpat[si] = '\0';
                int match = 0;
                if (anchored_start && anchored_end) {
                    match = (strcmp(str, simpat) == 0);
                } else if (anchored_start) {
                    match = (strncmp(str, simpat, si) == 0);
                } else if (anchored_end) {
                    size_t slen = strlen(str);
                    match = (slen >= si && strcmp(str + slen - si, simpat) == 0);
                } else {
                    match = (strstr(str, simpat) != NULL);
                }
                cond_result = (cond->op == CMP_REGEX_MATCH) ? match : !match;
                goto cond_cleanup;
            }
            /* ANY/ALL/SOME: col op ANY(SELECT ...) or col op ANY(ARRAY[...]) */
            if (cond->is_any || cond->is_all) {
                if (c->is_null || (column_type_is_text(c->type) && !c->value.as_text))
                    goto cond_cleanup;
                /* subquery variant: ANY/ALL (SELECT ...) */
                if (cond->subquery_sql != IDX_NONE && db) {
                    const char *sq_sql = ASTRING(arena, cond->subquery_sql);
                    char sql_buf[4096];
                    strncpy(sql_buf, sq_sql, sizeof(sql_buf) - 1);
                    sql_buf[sizeof(sql_buf) - 1] = '\0';
                    if (t && row)
                        subst_correlated_refs(sql_buf, sizeof(sql_buf), t, row);
                    struct query sq = {0};
                    if (query_parse(sql_buf, &sq) == 0) {
                        struct rows sq_result = {0};
                        if (db_exec(db, &sq, &sq_result, NULL) == 0) {
                            int all_m = 1;
                            for (size_t ri = 0; ri < sq_result.count; ri++) {
                                if (sq_result.data[ri].cells.count == 0) continue;
                                struct cell *sv = &sq_result.data[ri].cells.items[0];
                                int r = cell_compare(c, sv);
                                int match = (r != -2) ? cmp_result_matches_op(r, cond->op) : 0;
                                if (cond->is_any && match) { cond_result = 1; break; }
                                if (cond->is_all && !match) { all_m = 0; break; }
                            }
                            if (cond->is_all) cond_result = all_m;
                            for (size_t ri = 0; ri < sq_result.count; ri++)
                                row_free(&sq_result.data[ri]);
                            free(sq_result.data);
                        }
                    }
                    query_free(&sq);
                    goto cond_cleanup;
                }
                int all_matched = 1;
                for (uint32_t i = 0; i < cond->array_values_count; i++) {
                    struct cell *av = &ACELL(arena, cond->array_values_start + i);
                    int r = cell_compare(c, av);
                    int match = (r != -2) ? cmp_result_matches_op(r, cond->op) : 0;
                    if (cond->is_any && match) { cond_result = 1; break; }
                    if (cond->is_all && !match) { all_matched = 0; break; }
                }
                if (cond->is_all) cond_result = all_matched;
                goto cond_cleanup;
            }
            /* column-to-column comparison (JOIN ON conditions) */
            if (cond->rhs_column.len > 0) {
                int rhs_col = table_find_column_sv(t, cond->rhs_column);
                if (rhs_col < 0) goto cond_cleanup;
                struct cell *rhs = &row->cells.items[rhs_col];
                if (c->is_null || (column_type_is_text(c->type) && !c->value.as_text))
                    goto cond_cleanup;
                if (rhs->is_null || (column_type_is_text(rhs->type) && !rhs->value.as_text))
                    goto cond_cleanup;
                int r = cell_compare(c, rhs);
                if (r == -2) goto cond_cleanup;
                cond_result = cmp_result_matches_op(r, cond->op);
                goto cond_cleanup;
            }
            /* correlated scalar subquery: re-evaluate per row */
            if (cond->scalar_subquery_sql != IDX_NONE && db) {
                const char *sql_tmpl = ASTRING(arena, cond->scalar_subquery_sql);
                char sql_buf[4096];
                strncpy(sql_buf, sql_tmpl, sizeof(sql_buf) - 1);
                sql_buf[sizeof(sql_buf) - 1] = '\0';
                if (t && row)
                    subst_correlated_refs(sql_buf, sizeof(sql_buf), t, row);
                struct query sq = {0};
                if (query_parse(sql_buf, &sq) == 0) {
                    struct rows sq_result = {0};
                    if (db_exec(db, &sq, &sq_result, NULL) == 0) {
                        if (sq_result.count > 0 && sq_result.data[0].cells.count > 0) {
                            struct cell *src_cell = &sq_result.data[0].cells.items[0];
                            int r = cell_compare(c, src_cell);
                            for (size_t i = 0; i < sq_result.count; i++)
                                row_free(&sq_result.data[i]);
                            free(sq_result.data);
                            query_free(&sq);
                            if (r != -2)
                                cond_result = cmp_result_matches_op(r, cond->op);
                            goto cond_cleanup;
                        }
                        for (size_t i = 0; i < sq_result.count; i++)
                            row_free(&sq_result.data[i]);
                        free(sq_result.data);
                    }
                }
                query_free(&sq);
                goto cond_cleanup; /* subquery returned no rows or failed */
            }
            /* SQL three-valued logic: any comparison with NULL → UNKNOWN (false) */
            if (c->is_null || (column_type_is_text(c->type) && !c->value.as_text))
                goto cond_cleanup;
            {
                int r = cell_compare(c, &cond->value);
                if (r != -2)
                    cond_result = cmp_result_matches_op(r, cond->op);
            }
            cond_cleanup:
            if (has_lhs_expr) cell_release(&lhs_tmp);
            return cond_result;
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
    #define MAX_ARITH_OPERANDS 32
    double vals[MAX_ARITH_OPERANDS];
    char ops[MAX_ARITH_OPERANDS];
    int nvals = 0, nops = 0;
    int arith_has_null = 0;

    size_t start = 0;
    for (size_t i = 0; i <= expr.len && nvals < MAX_ARITH_OPERANDS; i++) {
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
            if (is_op && nops < MAX_ARITH_OPERANDS) ops[nops++] = c;
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

/* When rb is non-NULL, text lives in the bump — just zero the pointer.
 * Temporal types have no heap text — nothing to release. */
static void cell_release_rb(struct cell *c, struct bump_alloc *rb)
{
    if (column_type_is_temporal(c->type)) return;
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

static struct cell cell_make_bool(int val)
{
    struct cell c = {0};
    c.type = COLUMN_TYPE_BOOLEAN;
    c.value.as_bool = val ? 1 : 0;
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
    switch (c->type) {
    case COLUMN_TYPE_SMALLINT: return (double)c->value.as_smallint;
    case COLUMN_TYPE_INT:     return (double)c->value.as_int;
    case COLUMN_TYPE_FLOAT:   return c->value.as_float;
    case COLUMN_TYPE_BIGINT:  return (double)c->value.as_bigint;
    case COLUMN_TYPE_NUMERIC: return c->value.as_float;
    case COLUMN_TYPE_DATE:    return (double)c->value.as_date;
    case COLUMN_TYPE_TIME:
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_TIMESTAMPTZ:
        return (double)c->value.as_timestamp;
    case COLUMN_TYPE_INTERVAL:
        return (double)interval_to_usec_approx(c->value.as_interval);
    case COLUMN_TYPE_BOOLEAN:
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_UUID:
        break;
    }
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
    switch (c->type) {
    case COLUMN_TYPE_SMALLINT: snprintf(buf, sizeof(buf), "%d", (int)c->value.as_smallint); break;
    case COLUMN_TYPE_INT:      snprintf(buf, sizeof(buf), "%d", c->value.as_int); break;
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_NUMERIC:  snprintf(buf, sizeof(buf), "%g", c->value.as_float); break;
    case COLUMN_TYPE_BIGINT:   snprintf(buf, sizeof(buf), "%lld", c->value.as_bigint); break;
    case COLUMN_TYPE_BOOLEAN:  snprintf(buf, sizeof(buf), "%s", c->value.as_bool ? "true" : "false"); break;
    case COLUMN_TYPE_DATE:     date_to_str(c->value.as_date, buf, sizeof(buf)); break;
    case COLUMN_TYPE_TIME:     time_to_str(c->value.as_time, buf, sizeof(buf)); break;
    case COLUMN_TYPE_TIMESTAMP: timestamp_to_str(c->value.as_timestamp, buf, sizeof(buf)); break;
    case COLUMN_TYPE_TIMESTAMPTZ: timestamptz_to_str(c->value.as_timestamp, buf, sizeof(buf)); break;
    case COLUMN_TYPE_INTERVAL: interval_to_str(c->value.as_interval, buf, sizeof(buf)); break;
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_UUID:
        buf[0] = '\0'; break;
    }
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
    switch (c->type) {
    case COLUMN_TYPE_SMALLINT: snprintf(buf, sizeof(buf), "%d", (int)c->value.as_smallint); break;
    case COLUMN_TYPE_INT:      snprintf(buf, sizeof(buf), "%d", c->value.as_int); break;
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_NUMERIC:  snprintf(buf, sizeof(buf), "%g", c->value.as_float); break;
    case COLUMN_TYPE_BIGINT:   snprintf(buf, sizeof(buf), "%lld", c->value.as_bigint); break;
    case COLUMN_TYPE_BOOLEAN:  snprintf(buf, sizeof(buf), "%s", c->value.as_bool ? "true" : "false"); break;
    case COLUMN_TYPE_DATE:     date_to_str(c->value.as_date, buf, sizeof(buf)); break;
    case COLUMN_TYPE_TIME:     time_to_str(c->value.as_time, buf, sizeof(buf)); break;
    case COLUMN_TYPE_TIMESTAMP: timestamp_to_str(c->value.as_timestamp, buf, sizeof(buf)); break;
    case COLUMN_TYPE_TIMESTAMPTZ: timestamptz_to_str(c->value.as_timestamp, buf, sizeof(buf)); break;
    case COLUMN_TYPE_INTERVAL: interval_to_str(c->value.as_interval, buf, sizeof(buf)); break;
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_UUID:
        buf[0] = '\0'; break;
    }
    return bump_strdup(rb, buf);
}

/* ---- Extracted sub-functions for eval_expr ---- */

static struct cell eval_binary_op(struct expr *e, struct query_arena *arena,
                                  struct table *t, struct row *row,
                                  struct database *db, struct bump_alloc *rb)
{
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
            struct interval iv = rhs.value.as_interval;
            if (e->binary.op == OP_SUB) iv = interval_negate(iv);
            cell_release_rb(&lhs, rb); cell_release_rb(&rhs, rb);
            struct cell r = {0};
            r.type = lhs.type;
            if (lhs.type == COLUMN_TYPE_DATE) {
                r.value.as_date = date_add_interval(lhs.value.as_date, iv);
            } else {
                r.value.as_timestamp = timestamp_add_interval(lhs.value.as_timestamp, iv);
            }
            return r;
        }

        /* interval + date/timestamp */
        if (lhs_is_interval && rhs_is_dt && e->binary.op == OP_ADD) {
            struct interval iv = lhs.value.as_interval;
            cell_release_rb(&lhs, rb); cell_release_rb(&rhs, rb);
            struct cell r = {0};
            r.type = rhs.type;
            if (rhs.type == COLUMN_TYPE_DATE) {
                r.value.as_date = date_add_interval(rhs.value.as_date, iv);
            } else {
                r.value.as_timestamp = timestamp_add_interval(rhs.value.as_timestamp, iv);
            }
            return r;
        }

        /* interval +/- interval */
        if (lhs_is_interval && rhs_is_interval &&
            (e->binary.op == OP_ADD || e->binary.op == OP_SUB)) {
            struct cell r = {0};
            r.type = COLUMN_TYPE_INTERVAL;
            if (e->binary.op == OP_ADD)
                r.value.as_interval = interval_add(lhs.value.as_interval, rhs.value.as_interval);
            else
                r.value.as_interval = interval_sub(lhs.value.as_interval, rhs.value.as_interval);
            return r;
        }

        /* timestamp - timestamp = interval */
        if (lhs_is_dt && rhs_is_dt && e->binary.op == OP_SUB) {
            int64_t a_usec = (lhs.type == COLUMN_TYPE_DATE)
                ? (int64_t)lhs.value.as_date * USEC_PER_DAY : lhs.value.as_timestamp;
            int64_t b_usec = (rhs.type == COLUMN_TYPE_DATE)
                ? (int64_t)rhs.value.as_date * USEC_PER_DAY : rhs.value.as_timestamp;
            int64_t diff = a_usec - b_usec;
            cell_release_rb(&lhs, rb);
            cell_release_rb(&rhs, rb);
            struct cell r = {0};
            r.type = COLUMN_TYPE_INTERVAL;
            /* decompose into days + sub-day usec */
            r.value.as_interval.months = 0;
            r.value.as_interval.days = (int32_t)(diff / USEC_PER_DAY);
            r.value.as_interval.usec = diff % USEC_PER_DAY;
            return r;
        }

        /* date/timestamp +/- integer days */
        if (lhs_is_dt && (rhs.type == COLUMN_TYPE_INT || rhs.type == COLUMN_TYPE_BIGINT) &&
            (e->binary.op == OP_ADD || e->binary.op == OP_SUB)) {
            int32_t d = (rhs.type == COLUMN_TYPE_INT) ? rhs.value.as_int : (int32_t)rhs.value.as_bigint;
            if (e->binary.op == OP_SUB) d = -d;
            cell_release_rb(&lhs, rb); cell_release_rb(&rhs, rb);
            struct cell r = {0};
            r.type = lhs.type;
            if (lhs.type == COLUMN_TYPE_DATE) {
                r.value.as_date = lhs.value.as_date + d;
            } else {
                r.value.as_timestamp = lhs.value.as_timestamp + (int64_t)d * USEC_PER_DAY;
            }
            return r;
        }
    }

    int use_float = (lhs.type == COLUMN_TYPE_FLOAT || rhs.type == COLUMN_TYPE_FLOAT ||
                     lhs.type == COLUMN_TYPE_NUMERIC || rhs.type == COLUMN_TYPE_NUMERIC);
    double lv = cell_to_double_val(&lhs);
    double rv = cell_to_double_val(&rhs);
    double result_v = 0.0;
    switch (e->binary.op) {
        case OP_ADD: result_v = lv + rv; break;
        case OP_SUB: result_v = lv - rv; break;
        case OP_MUL: result_v = lv * rv; break;
        case OP_DIV:
            if (rv != 0.0) {
                if (!use_float)
                    result_v = (double)((long long)lv / (long long)rv);
                else
                    result_v = lv / rv;
            }
            break;
        case OP_MOD: result_v = (rv != 0.0) ? (double)((long long)lv % (long long)rv) : 0.0; break;
        case OP_EXP: result_v = pow(lv, rv); use_float = 1; break;
        case OP_CONCAT: break; /* handled above */
        case OP_NEG: break;    /* not a binary op */
        case OP_EQ: { cell_release_rb(&lhs, rb); cell_release_rb(&rhs, rb); return cell_make_bool(lv == rv); }
        case OP_NE: { cell_release_rb(&lhs, rb); cell_release_rb(&rhs, rb); return cell_make_bool(lv != rv); }
        case OP_LT: { cell_release_rb(&lhs, rb); cell_release_rb(&rhs, rb); return cell_make_bool(lv < rv); }
        case OP_GT: { cell_release_rb(&lhs, rb); cell_release_rb(&rhs, rb); return cell_make_bool(lv > rv); }
        case OP_LE: { cell_release_rb(&lhs, rb); cell_release_rb(&rhs, rb); return cell_make_bool(lv <= rv); }
        case OP_GE: { cell_release_rb(&lhs, rb); cell_release_rb(&rhs, rb); return cell_make_bool(lv >= rv); }
    }
    cell_release_rb(&lhs, rb);
    cell_release_rb(&rhs, rb);

    if (use_float || result_v != (double)(int)result_v)
        return cell_make_float(result_v);
    return cell_make_int((int)result_v);
}

static struct cell eval_func_call(enum expr_func fn, uint32_t nargs, uint32_t args_start,
                                  struct query_arena *arena, struct table *t,
                                  struct row *row, struct database *db,
                                  struct bump_alloc *rb)
{
    switch (fn) {
    case FUNC_COALESCE: {
        for (uint32_t i = 0; i < nargs; i++) {
            struct cell c = eval_expr(FUNC_ARG(arena, args_start, i), arena, t, row, db, rb);
            if (!cell_is_null(&c)) return c; /* ownership transfers to caller */
            cell_release_rb(&c, rb);
        }
        return cell_make_null();
    }

    case FUNC_NULLIF: {
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

    case FUNC_GREATEST: case FUNC_LEAST: {
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

    case FUNC_UPPER: case FUNC_LOWER: {
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

    case FUNC_LENGTH: {
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

    case FUNC_TRIM: {
        if (nargs == 0) return cell_make_null();
        struct cell arg = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        if (cell_is_null(&arg)) return arg;
        if (column_type_is_text(arg.type) && arg.value.as_text) {
            int mode = 0; /* 0=both, 1=leading, 2=trailing */
            const char *trim_chars = " \t";
            struct cell mode_cell = {0};
            struct cell chars_cell = {0};
            if (nargs >= 2) {
                mode_cell = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
                mode = mode_cell.value.as_int;
            }
            if (nargs >= 3) {
                chars_cell = eval_expr(FUNC_ARG(arena, args_start, 2), arena, t, row, db, rb);
                if (column_type_is_text(chars_cell.type) && chars_cell.value.as_text)
                    trim_chars = chars_cell.value.as_text;
            }
            char *s = arg.value.as_text;
            char *end = s + strlen(s);
            if (mode == 0 || mode == 1) {
                while (*s && strchr(trim_chars, *s)) s++;
            }
            if (mode == 0 || mode == 2) {
                while (end > s && strchr(trim_chars, end[-1])) end--;
            }
            size_t tlen = (size_t)(end - s);
            char *trimmed = rb ? (char *)bump_alloc(rb, tlen + 1)
                               : malloc(tlen + 1);
            memcpy(trimmed, s, tlen);
            trimmed[tlen] = '\0';
            if (!rb) free(arg.value.as_text);
            arg.value.as_text = trimmed;
            if (nargs >= 3) cell_release_rb(&chars_cell, rb);
        }
        return arg; /* ownership transfers to caller */
    }

    case FUNC_NEXTVAL: case FUNC_CURRVAL: {
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
            arena_set_error(arena, "42P01", "sequence '%s' not found", seq_name);
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
                arena_set_error(arena, "55000", "currval: sequence '%s' not yet called", seq_name);
                return cell_make_null();
            }
            struct cell r = {0};
            r.type = COLUMN_TYPE_BIGINT;
            r.value.as_bigint = seq->current_value;
            return r;
        }
    }

    case FUNC_GEN_RANDOM_UUID: {
        /* generate a v4 UUID: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx */
        static int uuid_seeded = 0;
        if (!uuid_seeded) { srand((unsigned)time(NULL)); uuid_seeded = 1; }
        #define UUID_STR_LEN 36
        char buf[UUID_STR_LEN + 1];
        const char *hex = "0123456789abcdef";
        for (int i = 0; i < UUID_STR_LEN; i++) {
            if (i == 8 || i == 13 || i == 18 || i == 23) buf[i] = '-';
            else if (i == 14) buf[i] = '4'; /* version 4 */
            else if (i == 19) buf[i] = hex[8 + (rand() & 3)]; /* variant 10xx */
            else buf[i] = hex[rand() & 15];
        }
        buf[UUID_STR_LEN] = '\0';
        struct cell r = {0};
        r.type = COLUMN_TYPE_UUID;
        r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
        return r;
    }

    /* ---- date/time functions ---- */

    case FUNC_NOW: case FUNC_CURRENT_TIMESTAMP: case FUNC_CURRENT_DATE: {
        time_t now = time(NULL);
        int64_t now_usec = ((int64_t)now - PG_EPOCH_UNIX) * USEC_PER_SEC;
        if (fn == FUNC_CURRENT_DATE) {
            struct cell r = {0};
            r.type = COLUMN_TYPE_DATE;
            r.value.as_date = (int32_t)(now_usec / USEC_PER_DAY);
            return r;
        } else {
            struct cell r = {0};
            r.type = COLUMN_TYPE_TIMESTAMP;
            r.value.as_timestamp = now_usec;
            return r;
        }
    }

    case FUNC_EXTRACT: case FUNC_DATE_PART: {
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
        double result_v = 0.0;
        if (src_c.type == COLUMN_TYPE_INTERVAL) {
            struct interval iv = src_c.value.as_interval;
            if (strcasecmp(field, "epoch") == 0)
                result_v = (double)interval_to_usec_approx(iv) / (double)USEC_PER_SEC;
            else if (strcasecmp(field, "year") == 0)
                result_v = iv.months / 12;
            else if (strcasecmp(field, "month") == 0)
                result_v = iv.months % 12;
            else if (strcasecmp(field, "day") == 0)
                result_v = iv.days;
            else if (strcasecmp(field, "hour") == 0)
                result_v = (double)(iv.usec / USEC_PER_HOUR);
            else if (strcasecmp(field, "minute") == 0)
                result_v = (double)((iv.usec % USEC_PER_HOUR) / USEC_PER_MIN);
            else if (strcasecmp(field, "second") == 0)
                result_v = (double)((iv.usec % USEC_PER_MIN) / USEC_PER_SEC);
        } else if (src_c.type == COLUMN_TYPE_DATE) {
            result_v = date_extract(src_c.value.as_date, field);
        } else if (src_c.type == COLUMN_TYPE_TIMESTAMP || src_c.type == COLUMN_TYPE_TIMESTAMPTZ) {
            result_v = timestamp_extract(src_c.value.as_timestamp, field);
        } else if (src_c.type == COLUMN_TYPE_TIME) {
            result_v = timestamp_extract(src_c.value.as_time, field);
        }
        cell_release_rb(&field_c, rb);
        cell_release_rb(&src_c, rb);
        struct cell r = {0};
        r.type = COLUMN_TYPE_FLOAT;
        r.value.as_float = result_v;
        return r;
    }

    case FUNC_DATE_TRUNC: {
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
        struct cell r = {0};
        if (src_c.type == COLUMN_TYPE_DATE) {
            r.type = COLUMN_TYPE_TIMESTAMP;
            r.value.as_timestamp = (int64_t)date_trunc_days(src_c.value.as_date, field) * USEC_PER_DAY;
        } else if (src_c.type == COLUMN_TYPE_TIMESTAMP || src_c.type == COLUMN_TYPE_TIMESTAMPTZ) {
            r.type = src_c.type;
            r.value.as_timestamp = timestamp_trunc_usec(src_c.value.as_timestamp, field);
        } else {
            r = cell_make_null();
        }
        cell_release_rb(&field_c, rb);
        cell_release_rb(&src_c, rb);
        return r;
    }

    case FUNC_AGE: {
        if (nargs < 1) return cell_make_null();
        struct cell a_c = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        struct cell b_c;
        if (nargs >= 2) {
            b_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
        } else {
            /* AGE(timestamp) = AGE(CURRENT_DATE, timestamp) */
            time_t now = time(NULL);
            int64_t now_usec = ((int64_t)now - PG_EPOCH_UNIX) * USEC_PER_SEC;
            b_c = a_c;
            a_c = (struct cell){0};
            a_c.type = COLUMN_TYPE_TIMESTAMP;
            a_c.value.as_timestamp = now_usec;
        }
        if (cell_is_null(&a_c) || cell_is_null(&b_c)) {
            cell_release_rb(&a_c, rb);
            cell_release_rb(&b_c, rb);
            struct cell r = {0}; r.type = COLUMN_TYPE_INTERVAL; r.is_null = 1;
            return r;
        }
        /* PostgreSQL-style AGE: compute difference as years/months/days */
        int32_t a_days, b_days;
        int64_t a_time_usec = 0, b_time_usec = 0;
        if (a_c.type == COLUMN_TYPE_DATE) {
            a_days = a_c.value.as_date;
        } else {
            int64_t au = a_c.value.as_timestamp;
            if (au >= 0) { a_days = (int32_t)(au / USEC_PER_DAY); a_time_usec = au % USEC_PER_DAY; }
            else { a_days = (int32_t)((au - USEC_PER_DAY + 1) / USEC_PER_DAY); a_time_usec = au - (int64_t)a_days * USEC_PER_DAY; }
        }
        if (b_c.type == COLUMN_TYPE_DATE) {
            b_days = b_c.value.as_date;
        } else {
            int64_t bu = b_c.value.as_timestamp;
            if (bu >= 0) { b_days = (int32_t)(bu / USEC_PER_DAY); b_time_usec = bu % USEC_PER_DAY; }
            else { b_days = (int32_t)((bu - USEC_PER_DAY + 1) / USEC_PER_DAY); b_time_usec = bu - (int64_t)b_days * USEC_PER_DAY; }
        }
        int ay, am, ad, by, bm, bd;
        days_to_ymd(a_days, &ay, &am, &ad);
        days_to_ymd(b_days, &by, &bm, &bd);
        int64_t time_diff = a_time_usec - b_time_usec;
        int result_months = (ay - by) * 12 + (am - bm);
        int result_days = ad - bd;
        if (time_diff < 0) { result_days--; time_diff += USEC_PER_DAY; }
        if (result_days < 0) { result_months--; /* borrow a month: use previous month's day count */
            int prev_m = am - 1; int prev_y = ay;
            if (prev_m < 1) { prev_m = 12; prev_y--; }
            int mdays[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
            int md = mdays[prev_m];
            if (prev_m == 2 && ((prev_y % 4 == 0 && prev_y % 100 != 0) || prev_y % 400 == 0)) md = 29;
            result_days += md;
        }
        cell_release_rb(&a_c, rb);
        cell_release_rb(&b_c, rb);
        struct cell r = {0};
        r.type = COLUMN_TYPE_INTERVAL;
        r.value.as_interval.months = result_months;
        r.value.as_interval.days = result_days;
        r.value.as_interval.usec = time_diff;
        return r;
    }

    case FUNC_TO_CHAR: {
        if (nargs < 2) return cell_make_null();
        struct cell src_c = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        struct cell fmt_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
        if (cell_is_null(&src_c)) {
            cell_release_rb(&fmt_c, rb);
            return src_c;
        }
        const char *fmt = (column_type_is_text(fmt_c.type) && fmt_c.value.as_text)
                          ? fmt_c.value.as_text : "";
        /* extract y/m/d/h/mi/s from the source cell */
        int y = 0, mo = 0, d = 0, hh = 0, mi = 0, ss = 0;
        if (src_c.type == COLUMN_TYPE_DATE) {
            days_to_ymd(src_c.value.as_date, &y, &mo, &d);
        } else if (src_c.type == COLUMN_TYPE_TIMESTAMP || src_c.type == COLUMN_TYPE_TIMESTAMPTZ) {
            int64_t usec = src_c.value.as_timestamp;
            int32_t days;
            int64_t time_part;
            if (usec >= 0) { days = (int32_t)(usec / USEC_PER_DAY); time_part = usec % USEC_PER_DAY; }
            else { days = (int32_t)((usec - USEC_PER_DAY + 1) / USEC_PER_DAY); time_part = usec - (int64_t)days * USEC_PER_DAY; }
            days_to_ymd(days, &y, &mo, &d);
            hh = (int)(time_part / USEC_PER_HOUR);
            mi = (int)((time_part % USEC_PER_HOUR) / USEC_PER_MIN);
            ss = (int)((time_part % USEC_PER_MIN) / USEC_PER_SEC);
        }
        /* simple PG format conversion: YYYY, MM, DD, HH24, MI, SS */
        char buf[256] = {0};
        char *out = buf;
        const char *fp = fmt;
        while (*fp && (size_t)(out - buf) < sizeof(buf) - 10) {
            if (strncasecmp(fp, "YYYY", 4) == 0) {
                out += sprintf(out, "%04d", y); fp += 4;
            } else if (strncasecmp(fp, "MM", 2) == 0) {
                out += sprintf(out, "%02d", mo); fp += 2;
            } else if (strncasecmp(fp, "DD", 2) == 0) {
                out += sprintf(out, "%02d", d); fp += 2;
            } else if (strncasecmp(fp, "HH24", 4) == 0) {
                out += sprintf(out, "%02d", hh); fp += 4;
            } else if (strncasecmp(fp, "HH", 2) == 0) {
                out += sprintf(out, "%02d", hh); fp += 2;
            } else if (strncasecmp(fp, "MI", 2) == 0) {
                out += sprintf(out, "%02d", mi); fp += 2;
            } else if (strncasecmp(fp, "SS", 2) == 0) {
                out += sprintf(out, "%02d", ss); fp += 2;
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

    case FUNC_SUBSTRING: {
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

    /* ---- math functions ---- */

    case FUNC_ABS: {
        if (nargs == 0) return cell_make_null();
        struct cell arg = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        if (cell_is_null(&arg)) return arg;
        if (arg.type == COLUMN_TYPE_INT) {
            if (arg.value.as_int < 0) arg.value.as_int = -arg.value.as_int;
            return arg;
        }
        if (arg.type == COLUMN_TYPE_BIGINT) {
            if (arg.value.as_bigint < 0) arg.value.as_bigint = -arg.value.as_bigint;
            return arg;
        }
        double v = cell_to_double_val(&arg);
        return cell_make_float(v < 0 ? -v : v);
    }

    case FUNC_CEIL: {
        if (nargs == 0) return cell_make_null();
        struct cell arg = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        if (cell_is_null(&arg)) { arg.type = COLUMN_TYPE_FLOAT; return arg; }
        double v = cell_to_double_val(&arg);
        return cell_make_float(ceil(v));
    }

    case FUNC_FLOOR: {
        if (nargs == 0) return cell_make_null();
        struct cell arg = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        if (cell_is_null(&arg)) { arg.type = COLUMN_TYPE_FLOAT; return arg; }
        double v = cell_to_double_val(&arg);
        return cell_make_float(floor(v));
    }

    case FUNC_ROUND: {
        if (nargs == 0) return cell_make_null();
        struct cell arg = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        if (cell_is_null(&arg)) { arg.type = COLUMN_TYPE_FLOAT; return arg; }
        double v = cell_to_double_val(&arg);
        int places = 0;
        if (nargs >= 2) {
            struct cell p = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
            places = (int)cell_to_double_val(&p);
            cell_release_rb(&p, rb);
        }
        if (places == 0) {
            return cell_make_float(round(v));
        } else {
            double factor = pow(10.0, (double)places);
            return cell_make_float(round(v * factor) / factor);
        }
    }

    case FUNC_POWER: {
        if (nargs < 2) return cell_make_null();
        struct cell a = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        struct cell b = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
        if (cell_is_null(&a) || cell_is_null(&b)) {
            cell_release_rb(&a, rb); cell_release_rb(&b, rb);
            struct cell r = {0}; r.type = COLUMN_TYPE_FLOAT; r.is_null = 1; return r;
        }
        double result_v = pow(cell_to_double_val(&a), cell_to_double_val(&b));
        cell_release_rb(&a, rb); cell_release_rb(&b, rb);
        return cell_make_float(result_v);
    }

    case FUNC_SQRT: {
        if (nargs == 0) return cell_make_null();
        struct cell arg = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        if (cell_is_null(&arg)) { arg.type = COLUMN_TYPE_FLOAT; return arg; }
        double v = cell_to_double_val(&arg);
        return cell_make_float(sqrt(v));
    }

    case FUNC_MOD: {
        if (nargs < 2) return cell_make_null();
        struct cell a = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        struct cell b = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
        if (cell_is_null(&a) || cell_is_null(&b)) {
            cell_release_rb(&a, rb); cell_release_rb(&b, rb);
            return cell_make_null();
        }
        if (a.type == COLUMN_TYPE_INT && b.type == COLUMN_TYPE_INT) {
            int bv = b.value.as_int;
            if (bv == 0) { return cell_make_null(); }
            return cell_make_int(a.value.as_int % bv);
        }
        double av = cell_to_double_val(&a);
        double bv = cell_to_double_val(&b);
        if (bv == 0.0) return cell_make_null();
        return cell_make_float(fmod(av, bv));
    }

    case FUNC_SIGN: {
        if (nargs == 0) return cell_make_null();
        struct cell arg = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        if (cell_is_null(&arg)) { arg.type = COLUMN_TYPE_INT; return arg; }
        double v = cell_to_double_val(&arg);
        return cell_make_int(v > 0 ? 1 : (v < 0 ? -1 : 0));
    }

    case FUNC_RANDOM: {
        static int rand_seeded = 0;
        if (!rand_seeded) { srand((unsigned)time(NULL)); rand_seeded = 1; }
        return cell_make_float((double)rand() / ((double)RAND_MAX + 1.0));
    }

    /* ---- string functions ---- */

    case FUNC_REPLACE: {
        if (nargs < 3) return cell_make_null();
        struct cell str = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        struct cell from_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
        struct cell to_c = eval_expr(FUNC_ARG(arena, args_start, 2), arena, t, row, db, rb);
        if (cell_is_null(&str)) {
            cell_release_rb(&from_c, rb); cell_release_rb(&to_c, rb);
            return str;
        }
        const char *s = (column_type_is_text(str.type) && str.value.as_text) ? str.value.as_text : "";
        const char *from = (column_type_is_text(from_c.type) && from_c.value.as_text) ? from_c.value.as_text : "";
        const char *to = (column_type_is_text(to_c.type) && to_c.value.as_text) ? to_c.value.as_text : "";
        size_t from_len = strlen(from);
        size_t to_len = strlen(to);
        if (from_len == 0) {
            cell_release_rb(&from_c, rb); cell_release_rb(&to_c, rb);
            return str;
        }
        /* count occurrences to size output */
        size_t count = 0;
        const char *p = s;
        while ((p = strstr(p, from)) != NULL) { count++; p += from_len; }
        size_t slen = strlen(s);
        size_t out_len = slen + count * (to_len - from_len);
        char *out = rb ? (char *)bump_alloc(rb, out_len + 1) : malloc(out_len + 1);
        char *wp = out;
        p = s;
        while (*p) {
            if (strncmp(p, from, from_len) == 0) {
                memcpy(wp, to, to_len); wp += to_len; p += from_len;
            } else {
                *wp++ = *p++;
            }
        }
        *wp = '\0';
        if (!rb) free(str.value.as_text);
        str.value.as_text = out;
        if (!column_type_is_text(str.type)) str.type = COLUMN_TYPE_TEXT;
        cell_release_rb(&from_c, rb); cell_release_rb(&to_c, rb);
        return str;
    }

    case FUNC_LPAD: case FUNC_RPAD: {
        if (nargs < 2) return cell_make_null();
        struct cell str = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        struct cell len_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
        if (cell_is_null(&str)) { cell_release_rb(&len_c, rb); return str; }
        int target_len = (int)cell_to_double_val(&len_c);
        cell_release_rb(&len_c, rb);
        const char *fill = " ";
        struct cell fill_c = {0};
        if (nargs >= 3) {
            fill_c = eval_expr(FUNC_ARG(arena, args_start, 2), arena, t, row, db, rb);
            if (column_type_is_text(fill_c.type) && fill_c.value.as_text)
                fill = fill_c.value.as_text;
        }
        const char *s = (column_type_is_text(str.type) && str.value.as_text) ? str.value.as_text : "";
        int slen = (int)strlen(s);
        size_t fill_len = strlen(fill);
        if (target_len <= 0 || fill_len == 0) {
            if (!rb) free(str.value.as_text);
            str.value.as_text = rb ? bump_strdup(rb, "") : strdup("");
            cell_release_rb(&fill_c, rb);
            return str;
        }
        if (slen >= target_len) {
            /* truncate to target_len */
            char *out = rb ? (char *)bump_alloc(rb, (size_t)target_len + 1)
                           : malloc((size_t)target_len + 1);
            memcpy(out, s, (size_t)target_len);
            out[target_len] = '\0';
            if (!rb) free(str.value.as_text);
            str.value.as_text = out;
            cell_release_rb(&fill_c, rb);
            return str;
        }
        int pad_needed = target_len - slen;
        char *out = rb ? (char *)bump_alloc(rb, (size_t)target_len + 1)
                       : malloc((size_t)target_len + 1);
        if (fn == FUNC_LPAD) {
            for (int i = 0; i < pad_needed; i++)
                out[i] = fill[i % fill_len];
            memcpy(out + pad_needed, s, (size_t)slen);
        } else {
            memcpy(out, s, (size_t)slen);
            for (int i = 0; i < pad_needed; i++)
                out[slen + i] = fill[i % fill_len];
        }
        out[target_len] = '\0';
        if (!rb) free(str.value.as_text);
        str.value.as_text = out;
        cell_release_rb(&fill_c, rb);
        return str;
    }

    case FUNC_CONCAT: {
        /* CONCAT(a, b, ...) — NULLs treated as empty string */
        size_t cap = 256, wp = 0;
        char *buf = malloc(cap);
        if (!buf) return cell_make_null();
        for (uint32_t i = 0; i < nargs; i++) {
            struct cell c = eval_expr(FUNC_ARG(arena, args_start, i), arena, t, row, db, rb);
            if (!cell_is_null(&c)) {
                char *txt = cell_to_text_rb(&c, rb);
                if (txt) {
                    size_t tlen = strlen(txt);
                    if (wp + tlen >= cap) {
                        while (wp + tlen >= cap) cap *= 2;
                        char *tmp = realloc(buf, cap);
                        if (!tmp) { free(buf); cell_release_rb(&c, rb); if (!rb) free(txt); return cell_make_null(); }
                        buf = tmp;
                    }
                    memcpy(buf + wp, txt, tlen);
                    wp += tlen;
                    if (!rb) free(txt);
                }
            }
            cell_release_rb(&c, rb);
        }
        buf[wp] = '\0';
        struct cell r = {0};
        r.type = COLUMN_TYPE_TEXT;
        r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
        free(buf);
        return r;
    }

    case FUNC_CONCAT_WS: {
        /* CONCAT_WS(sep, a, b, ...) — skip NULLs */
        if (nargs < 1) return cell_make_null();
        struct cell sep_c = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        if (cell_is_null(&sep_c)) return sep_c;
        const char *sep = (column_type_is_text(sep_c.type) && sep_c.value.as_text) ? sep_c.value.as_text : "";
        size_t sep_len = strlen(sep);
        size_t cap = 256, wp = 0;
        char *buf = malloc(cap);
        if (!buf) { cell_release_rb(&sep_c, rb); return cell_make_null(); }
        int first = 1;
        for (uint32_t i = 1; i < nargs; i++) {
            struct cell c = eval_expr(FUNC_ARG(arena, args_start, i), arena, t, row, db, rb);
            if (!cell_is_null(&c)) {
                char *txt = cell_to_text_rb(&c, rb);
                if (txt) {
                    size_t tlen = strlen(txt);
                    size_t need = wp + (first ? 0 : sep_len) + tlen;
                    if (need >= cap) {
                        while (need >= cap) cap *= 2;
                        char *tmp = realloc(buf, cap);
                        if (!tmp) { free(buf); cell_release_rb(&c, rb); if (!rb) free(txt); cell_release_rb(&sep_c, rb); return cell_make_null(); }
                        buf = tmp;
                    }
                    if (!first) { memcpy(buf + wp, sep, sep_len); wp += sep_len; }
                    memcpy(buf + wp, txt, tlen);
                    wp += tlen;
                    first = 0;
                    if (!rb) free(txt);
                }
            }
            cell_release_rb(&c, rb);
        }
        buf[wp] = '\0';
        cell_release_rb(&sep_c, rb);
        struct cell r = {0};
        r.type = COLUMN_TYPE_TEXT;
        r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
        free(buf);
        return r;
    }

    case FUNC_POSITION: {
        /* POSITION(sub IN str) — returns 1-based index, 0 if not found */
        if (nargs < 2) return cell_make_int(0);
        struct cell sub_c = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        struct cell str_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
        if (cell_is_null(&sub_c) || cell_is_null(&str_c)) {
            cell_release_rb(&sub_c, rb); cell_release_rb(&str_c, rb);
            struct cell r = {0}; r.type = COLUMN_TYPE_INT; r.is_null = 1; return r;
        }
        const char *sub = (column_type_is_text(sub_c.type) && sub_c.value.as_text) ? sub_c.value.as_text : "";
        const char *s = (column_type_is_text(str_c.type) && str_c.value.as_text) ? str_c.value.as_text : "";
        const char *found = strstr(s, sub);
        int pos = found ? (int)(found - s) + 1 : 0;
        cell_release_rb(&sub_c, rb); cell_release_rb(&str_c, rb);
        return cell_make_int(pos);
    }

    case FUNC_SPLIT_PART: {
        if (nargs < 3) return cell_make_null();
        struct cell str = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        struct cell delim_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
        struct cell part_c = eval_expr(FUNC_ARG(arena, args_start, 2), arena, t, row, db, rb);
        if (cell_is_null(&str)) {
            cell_release_rb(&delim_c, rb); cell_release_rb(&part_c, rb);
            return str;
        }
        const char *s = (column_type_is_text(str.type) && str.value.as_text) ? str.value.as_text : "";
        const char *delim = (column_type_is_text(delim_c.type) && delim_c.value.as_text) ? delim_c.value.as_text : "";
        int part = (int)cell_to_double_val(&part_c);
        cell_release_rb(&delim_c, rb); cell_release_rb(&part_c, rb);
        size_t dlen = strlen(delim);
        if (dlen == 0 || part < 1) {
            if (!rb) free(str.value.as_text);
            str.value.as_text = rb ? bump_strdup(rb, "") : strdup("");
            return str;
        }
        const char *start = s;
        int cur = 1;
        while (cur < part) {
            const char *f = strstr(start, delim);
            if (!f) { start = s + strlen(s); break; }
            start = f + dlen;
            cur++;
        }
        const char *end = strstr(start, delim);
        if (!end) end = s + strlen(s);
        size_t rlen = (size_t)(end - start);
        char *out = rb ? (char *)bump_alloc(rb, rlen + 1) : malloc(rlen + 1);
        memcpy(out, start, rlen);
        out[rlen] = '\0';
        if (!rb) free(str.value.as_text);
        str.value.as_text = out;
        return str;
    }

    case FUNC_LEFT: {
        if (nargs < 2) return cell_make_null();
        struct cell str = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        struct cell n_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
        if (cell_is_null(&str)) { cell_release_rb(&n_c, rb); return str; }
        int n = (int)cell_to_double_val(&n_c);
        cell_release_rb(&n_c, rb);
        const char *s = (column_type_is_text(str.type) && str.value.as_text) ? str.value.as_text : "";
        int slen = (int)strlen(s);
        if (n < 0) n = slen + n;
        if (n < 0) n = 0;
        if (n > slen) n = slen;
        char *out = rb ? (char *)bump_alloc(rb, (size_t)n + 1) : malloc((size_t)n + 1);
        memcpy(out, s, (size_t)n);
        out[n] = '\0';
        if (!rb) free(str.value.as_text);
        str.value.as_text = out;
        return str;
    }

    case FUNC_RIGHT: {
        if (nargs < 2) return cell_make_null();
        struct cell str = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        struct cell n_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
        if (cell_is_null(&str)) { cell_release_rb(&n_c, rb); return str; }
        int n = (int)cell_to_double_val(&n_c);
        cell_release_rb(&n_c, rb);
        const char *s = (column_type_is_text(str.type) && str.value.as_text) ? str.value.as_text : "";
        int slen = (int)strlen(s);
        if (n < 0) n = slen + n;
        if (n < 0) n = 0;
        if (n > slen) n = slen;
        int start = slen - n;
        char *out = rb ? (char *)bump_alloc(rb, (size_t)n + 1) : malloc((size_t)n + 1);
        memcpy(out, s + start, (size_t)n);
        out[n] = '\0';
        if (!rb) free(str.value.as_text);
        str.value.as_text = out;
        return str;
    }

    case FUNC_REPEAT: {
        if (nargs < 2) return cell_make_null();
        struct cell str = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        struct cell n_c = eval_expr(FUNC_ARG(arena, args_start, 1), arena, t, row, db, rb);
        if (cell_is_null(&str)) { cell_release_rb(&n_c, rb); return str; }
        int n = (int)cell_to_double_val(&n_c);
        cell_release_rb(&n_c, rb);
        if (n <= 0) {
            if (!rb) free(str.value.as_text);
            str.value.as_text = rb ? bump_strdup(rb, "") : strdup("");
            return str;
        }
        const char *s = (column_type_is_text(str.type) && str.value.as_text) ? str.value.as_text : "";
        size_t slen = strlen(s);
        size_t out_len = slen * (size_t)n;
        char *out = rb ? (char *)bump_alloc(rb, out_len + 1) : malloc(out_len + 1);
        for (int i = 0; i < n; i++)
            memcpy(out + i * slen, s, slen);
        out[out_len] = '\0';
        if (!rb) free(str.value.as_text);
        str.value.as_text = out;
        return str;
    }

    case FUNC_REVERSE: {
        if (nargs == 0) return cell_make_null();
        struct cell str = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        if (cell_is_null(&str)) return str;
        if (column_type_is_text(str.type) && str.value.as_text) {
            size_t slen = strlen(str.value.as_text);
            char *out = rb ? (char *)bump_alloc(rb, slen + 1) : malloc(slen + 1);
            for (size_t i = 0; i < slen; i++)
                out[i] = str.value.as_text[slen - 1 - i];
            out[slen] = '\0';
            if (!rb) free(str.value.as_text);
            str.value.as_text = out;
        }
        return str;
    }

    case FUNC_INITCAP: {
        if (nargs == 0) return cell_make_null();
        struct cell str = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        if (cell_is_null(&str)) return str;
        if (column_type_is_text(str.type) && str.value.as_text) {
            int at_start = 1;
            for (char *p = str.value.as_text; *p; p++) {
                if (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r' ||
                    *p == '-' || *p == '_') {
                    at_start = 1;
                } else if (at_start) {
                    *p = toupper((unsigned char)*p);
                    at_start = 0;
                } else {
                    *p = tolower((unsigned char)*p);
                }
            }
        }
        return str;
    }

    /* ---- pg_catalog stub functions ---- */

    case FUNC_PG_GET_USERBYID: {
        /* pg_get_userbyid(oid) -> role name; return db name as owner */
        struct cell r = {0};
        r.type = COLUMN_TYPE_TEXT;
        r.value.as_text = strdup(db ? db->name : "mskql");
        return r;
    }

    case FUNC_PG_TABLE_IS_VISIBLE: {
        /* pg_table_is_visible(oid) -> true for all public tables */
        struct cell r = {0};
        r.type = COLUMN_TYPE_BOOLEAN;
        r.value.as_bool = 1;
        return r;
    }

    case FUNC_FORMAT_TYPE: {
        /* format_type(type_oid, typemod) -> type name string */
        if (nargs < 1) return cell_make_null();
        struct cell oid_c = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, row, db, rb);
        int type_oid = 0;
        if (oid_c.type == COLUMN_TYPE_INT) type_oid = oid_c.value.as_int;
        else if (oid_c.type == COLUMN_TYPE_BIGINT) type_oid = (int)oid_c.value.as_bigint;
        else if (column_type_is_text(oid_c.type) && oid_c.value.as_text) {
            type_oid = atoi(oid_c.value.as_text);
            cell_release(&oid_c);
        }
        const char *name = "unknown";
        switch (type_oid) {
            case 16:   name = "boolean"; break;
            case 20:   name = "bigint"; break;
            case 21:   name = "smallint"; break;
            case 23:   name = "integer"; break;
            case 25:   name = "text"; break;
            case 701:  name = "double precision"; break;
            case 1043: name = "character varying"; break;
            case 1082: name = "date"; break;
            case 1083: name = "time without time zone"; break;
            case 1114: name = "timestamp without time zone"; break;
            case 1184: name = "timestamp with time zone"; break;
            case 1186: name = "interval"; break;
            case 1700: name = "numeric"; break;
            case 2950: name = "uuid"; break;
        }
        struct cell r = {0};
        r.type = COLUMN_TYPE_TEXT;
        r.value.as_text = strdup(name);
        return r;
    }

    case FUNC_PG_GET_EXPR:
    case FUNC_OBJ_DESCRIPTION:
    case FUNC_COL_DESCRIPTION:
    case FUNC_SHOBJ_DESCRIPTION:
        return cell_make_null();

    case FUNC_PG_ENCODING_TO_CHAR: {
        struct cell r = {0};
        r.type = COLUMN_TYPE_TEXT;
        r.value.as_text = strdup("UTF8");
        return r;
    }

    case FUNC_HAS_TABLE_PRIVILEGE:
    case FUNC_HAS_DATABASE_PRIVILEGE: {
        struct cell r = {0};
        r.type = COLUMN_TYPE_BOOLEAN;
        r.value.as_bool = 1;
        return r;
    }

    case FUNC_PG_GET_CONSTRAINTDEF:
    case FUNC_PG_GET_INDEXDEF: {
        struct cell r = {0};
        r.type = COLUMN_TYPE_TEXT;
        r.value.as_text = strdup("");
        return r;
    }

    case FUNC_ARRAY_TO_STRING: {
        struct cell r = {0};
        r.type = COLUMN_TYPE_TEXT;
        r.value.as_text = strdup("");
        return r;
    }

    case FUNC_CURRENT_SCHEMA: {
        struct cell r = {0};
        r.type = COLUMN_TYPE_TEXT;
        r.value.as_text = strdup("public");
        return r;
    }

    case FUNC_CURRENT_SCHEMAS: {
        struct cell r = {0};
        r.type = COLUMN_TYPE_TEXT;
        r.value.as_text = strdup("{pg_catalog,public}");
        return r;
    }

    case FUNC_PG_IS_IN_RECOVERY: {
        struct cell r = {0};
        r.type = COLUMN_TYPE_BOOLEAN;
        r.value.as_bool = 0;
        return r;
    }

    case FUNC_AGG_SUM:
    case FUNC_AGG_COUNT:
    case FUNC_AGG_AVG:
    case FUNC_AGG_MIN:
    case FUNC_AGG_MAX: {
        if (!t) break;
        double sum = 0;
        size_t nonnull = 0;
        struct cell min_c = {0}, max_c = {0};
        int minmax_init = 0;
        for (size_t i = 0; i < t->rows.count; i++) {
            struct cell v;
            if (nargs > 0)
                v = eval_expr(FUNC_ARG(arena, args_start, 0), arena, t, &t->rows.items[i], db, NULL);
            else
                v = cell_make_null();
            if (fn == FUNC_AGG_COUNT && nargs == 0) {
                nonnull++;
                continue;
            }
            if (cell_is_null(&v)) continue;
            nonnull++;
            double dv = cell_to_double(&v);
            sum += dv;
            if (!minmax_init) {
                min_c = v; max_c = v; minmax_init = 1;
            } else {
                if (cell_compare(&v, &min_c) < 0) min_c = v;
                if (cell_compare(&v, &max_c) > 0) max_c = v;
            }
        }
        if (fn == FUNC_AGG_SUM) {
            if (nonnull == 0) return cell_make_null();
            if (sum != (double)(int)sum || sum > 2147483647.0 || sum < -2147483648.0)
                return cell_make_float(sum);
            return cell_make_int((int)sum);
        } else if (fn == FUNC_AGG_COUNT) {
            return cell_make_int((int)nonnull);
        } else if (fn == FUNC_AGG_AVG) {
            if (nonnull == 0) return cell_make_null();
            return cell_make_float(sum / (double)nonnull);
        } else if (fn == FUNC_AGG_MIN) {
            if (!minmax_init) return cell_make_null();
            return min_c;
        } else if (fn == FUNC_AGG_MAX) {
            if (!minmax_init) return cell_make_null();
            return max_c;
        }
        break;
    }

    } /* end switch */
    return cell_make_null();
}

static struct cell eval_cast(struct expr *e, struct query_arena *arena,
                             struct table *t, struct row *row,
                             struct database *db, struct bump_alloc *rb)
{
    struct cell src = eval_expr(e->cast.operand, arena, t, row, db, rb);
    if (cell_is_null(&src)) {
        src.type = e->cast.target;
        return src;
    }
    enum column_type target = e->cast.target;

    /* already the right type? */
    if (src.type == target) return src;

    /* numeric → numeric conversions */
    if ((target == COLUMN_TYPE_SMALLINT || target == COLUMN_TYPE_INT || target == COLUMN_TYPE_BIGINT ||
         target == COLUMN_TYPE_FLOAT || target == COLUMN_TYPE_NUMERIC) &&
        (src.type == COLUMN_TYPE_SMALLINT || src.type == COLUMN_TYPE_INT || src.type == COLUMN_TYPE_BIGINT ||
         src.type == COLUMN_TYPE_FLOAT || src.type == COLUMN_TYPE_NUMERIC)) {
        double v = cell_to_double_val(&src);
        cell_release_rb(&src, rb);
        struct cell r = {0};
        r.type = target;
        switch (target) {
        case COLUMN_TYPE_SMALLINT: r.value.as_smallint = (int16_t)v; break;
        case COLUMN_TYPE_INT:      r.value.as_int = (int)v; break;
        case COLUMN_TYPE_BIGINT:   r.value.as_bigint = (long long)v; break;
        case COLUMN_TYPE_FLOAT:    r.value.as_float = v; break;
        case COLUMN_TYPE_NUMERIC: {
            if (e->cast.scale >= 0) {
                double factor = 1.0;
                for (int s = 0; s < e->cast.scale; s++) factor *= 10.0;
                v = round(v * factor) / factor;
            }
            r.value.as_float = v;
            break;
        }
        case COLUMN_TYPE_BOOLEAN:
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID:
            break;
        }
        return r;
    }

    /* numeric → text */
    if (column_type_is_text(target) &&
        (src.type == COLUMN_TYPE_SMALLINT || src.type == COLUMN_TYPE_INT || src.type == COLUMN_TYPE_BIGINT ||
         src.type == COLUMN_TYPE_FLOAT || src.type == COLUMN_TYPE_NUMERIC ||
         src.type == COLUMN_TYPE_BOOLEAN)) {
        char buf[128];
        switch (src.type) {
        case COLUMN_TYPE_SMALLINT: snprintf(buf, sizeof(buf), "%d", (int)src.value.as_smallint); break;
        case COLUMN_TYPE_INT:      snprintf(buf, sizeof(buf), "%d", src.value.as_int); break;
        case COLUMN_TYPE_BIGINT:   snprintf(buf, sizeof(buf), "%lld", src.value.as_bigint); break;
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:  snprintf(buf, sizeof(buf), "%g", src.value.as_float); break;
        case COLUMN_TYPE_BOOLEAN:  snprintf(buf, sizeof(buf), "%s", src.value.as_bool ? "true" : "false"); break;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID:
            buf[0] = '\0'; break;
        }
        cell_release_rb(&src, rb);
        struct cell r = {0};
        r.type = target;
        r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
        return r;
    }

    /* text → numeric */
    if ((target == COLUMN_TYPE_SMALLINT || target == COLUMN_TYPE_INT || target == COLUMN_TYPE_BIGINT ||
         target == COLUMN_TYPE_FLOAT || target == COLUMN_TYPE_NUMERIC) &&
        column_type_is_text(src.type) && src.value.as_text) {
        const char *s = src.value.as_text;
        struct cell r = {0};
        r.type = target;
        switch (target) {
        case COLUMN_TYPE_SMALLINT: r.value.as_smallint = (int16_t)atoi(s); break;
        case COLUMN_TYPE_INT:      r.value.as_int = atoi(s); break;
        case COLUMN_TYPE_BIGINT:   r.value.as_bigint = atoll(s); break;
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:  r.value.as_float = atof(s); break;
        case COLUMN_TYPE_BOOLEAN:
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID:
            break;
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

    /* text → temporal: parse string into integer/struct */
    if (column_type_is_temporal(target) && column_type_is_text(src.type) && src.value.as_text) {
        const char *s = src.value.as_text;
        struct cell r = {0};
        r.type = target;
        switch (target) {
        case COLUMN_TYPE_DATE:        r.value.as_date = date_from_str(s); break;
        case COLUMN_TYPE_TIME:        r.value.as_time = time_from_str(s); break;
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ: r.value.as_timestamp = timestamp_from_str(s); break;
        case COLUMN_TYPE_INTERVAL:    r.value.as_interval = interval_from_str(s); break;
        case COLUMN_TYPE_SMALLINT: case COLUMN_TYPE_INT: case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_TEXT: case COLUMN_TYPE_ENUM: case COLUMN_TYPE_BOOLEAN:
        case COLUMN_TYPE_BIGINT: case COLUMN_TYPE_NUMERIC: case COLUMN_TYPE_UUID:
            break;
        }
        cell_release_rb(&src, rb);
        return r;
    }

    /* temporal → text: format integer/struct to string */
    if (column_type_is_text(target) && column_type_is_temporal(src.type)) {
        char buf[64];
        switch (src.type) {
        case COLUMN_TYPE_DATE:        date_to_str(src.value.as_date, buf, sizeof(buf)); break;
        case COLUMN_TYPE_TIME:        time_to_str(src.value.as_time, buf, sizeof(buf)); break;
        case COLUMN_TYPE_TIMESTAMP:   timestamp_to_str(src.value.as_timestamp, buf, sizeof(buf)); break;
        case COLUMN_TYPE_TIMESTAMPTZ: timestamptz_to_str(src.value.as_timestamp, buf, sizeof(buf)); break;
        case COLUMN_TYPE_INTERVAL:    interval_to_str(src.value.as_interval, buf, sizeof(buf)); break;
        case COLUMN_TYPE_SMALLINT: case COLUMN_TYPE_INT: case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_TEXT: case COLUMN_TYPE_ENUM: case COLUMN_TYPE_BOOLEAN:
        case COLUMN_TYPE_BIGINT: case COLUMN_TYPE_NUMERIC: case COLUMN_TYPE_UUID:
            buf[0] = '\0'; break;
        }
        struct cell r = {0};
        r.type = target;
        r.value.as_text = rb ? bump_strdup(rb, buf) : strdup(buf);
        return r;
    }

    /* DATE ↔ TIMESTAMP promotion */
    if (target == COLUMN_TYPE_TIMESTAMP && src.type == COLUMN_TYPE_DATE) {
        struct cell r = {0};
        r.type = COLUMN_TYPE_TIMESTAMP;
        r.value.as_timestamp = (int64_t)src.value.as_date * USEC_PER_DAY;
        return r;
    }
    if (target == COLUMN_TYPE_DATE && (src.type == COLUMN_TYPE_TIMESTAMP || src.type == COLUMN_TYPE_TIMESTAMPTZ)) {
        struct cell r = {0};
        r.type = COLUMN_TYPE_DATE;
        if (src.value.as_timestamp >= 0)
            r.value.as_date = (int32_t)(src.value.as_timestamp / USEC_PER_DAY);
        else
            r.value.as_date = (int32_t)((src.value.as_timestamp - USEC_PER_DAY + 1) / USEC_PER_DAY);
        return r;
    }

    /* text → text-like (TEXT, ENUM, UUID) — just change the type tag */
    if (column_type_is_text(target) && column_type_is_text(src.type)) {
        src.type = target;
        return src;
    }

    /* fallback: convert to text first, then to target */
    char *txt = cell_to_text_rb(&src, rb);
    cell_release_rb(&src, rb);
    struct cell r = {0};
    r.type = target;
    switch (target) {
    case COLUMN_TYPE_SMALLINT:
        r.value.as_smallint = txt ? (int16_t)atoi(txt) : 0;
        if (!rb && txt) free(txt);
        break;
    case COLUMN_TYPE_INT:
        r.value.as_int = txt ? atoi(txt) : 0;
        if (!rb && txt) free(txt);
        break;
    case COLUMN_TYPE_BIGINT:
        r.value.as_bigint = txt ? atoll(txt) : 0;
        if (!rb && txt) free(txt);
        break;
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_NUMERIC:
        r.value.as_float = txt ? atof(txt) : 0.0;
        if (!rb && txt) free(txt);
        break;
    case COLUMN_TYPE_BOOLEAN:
        r.value.as_bool = (txt && (strcasecmp(txt, "true") == 0 || strcmp(txt, "1") == 0)) ? 1 : 0;
        if (!rb && txt) free(txt);
        break;
    case COLUMN_TYPE_DATE:
        r.value.as_date = txt ? date_from_str(txt) : 0;
        if (!rb && txt) free(txt);
        break;
    case COLUMN_TYPE_TIME:
        r.value.as_time = txt ? time_from_str(txt) : 0;
        if (!rb && txt) free(txt);
        break;
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_TIMESTAMPTZ:
        r.value.as_timestamp = txt ? timestamp_from_str(txt) : 0;
        if (!rb && txt) free(txt);
        break;
    case COLUMN_TYPE_INTERVAL:
        r.value.as_interval = txt ? interval_from_str(txt) : (struct interval){0,0,0};
        if (!rb && txt) free(txt);
        break;
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_UUID:
        r.value.as_text = txt;
        break;
    }
    return r;
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
        int idx = -1;
        if (e->column_ref.table.len > 0) {
            /* try table.column qualified lookup first (avoids ambiguity in merged join tables) */
            char qname[256];
            snprintf(qname, sizeof(qname), SV_FMT "." SV_FMT,
                     SV_ARG(e->column_ref.table), SV_ARG(col));
            sv qsv = sv_from(qname, strlen(qname));
            idx = table_find_column_sv(t, qsv);
        }
        if (idx < 0)
            idx = table_find_column_sv(t, col);
        if (idx < 0) return cell_make_null();
        return cell_deep_copy_rb(&row->cells.items[idx], rb);
    }

    case EXPR_UNARY_OP: {
        struct cell operand = eval_expr(e->unary.operand, arena, t, row, db, rb);
        if (cell_is_null(&operand)) return operand;
        switch (e->unary.op) {
        case OP_NEG:
            switch (operand.type) {
            case COLUMN_TYPE_SMALLINT: operand.value.as_smallint = -operand.value.as_smallint; break;
            case COLUMN_TYPE_INT:      operand.value.as_int = -operand.value.as_int; break;
            case COLUMN_TYPE_FLOAT:    operand.value.as_float = -operand.value.as_float; break;
            case COLUMN_TYPE_BIGINT:   operand.value.as_bigint = -operand.value.as_bigint; break;
            case COLUMN_TYPE_NUMERIC:  operand.value.as_float = -operand.value.as_float; break;
            case COLUMN_TYPE_BOOLEAN:
            case COLUMN_TYPE_TEXT:
            case COLUMN_TYPE_ENUM:
            case COLUMN_TYPE_DATE:
            case COLUMN_TYPE_TIME:
            case COLUMN_TYPE_TIMESTAMP:
            case COLUMN_TYPE_TIMESTAMPTZ:
            case COLUMN_TYPE_INTERVAL:
            case COLUMN_TYPE_UUID:
                break;
            }
            break;
        case OP_ADD:
        case OP_SUB:
        case OP_MUL:
        case OP_DIV:
        case OP_MOD:
        case OP_CONCAT:
        case OP_EXP:
        case OP_EQ:
        case OP_NE:
        case OP_LT:
        case OP_GT:
        case OP_LE:
        case OP_GE:
            break; /* not unary ops */
        }
        return operand;
    }

    case EXPR_BINARY_OP:
        return eval_binary_op(e, arena, t, row, db, rb);

    case EXPR_FUNC_CALL:
        return eval_func_call(e->func_call.func, e->func_call.args_count,
                              e->func_call.args_start, arena, t, row, db, rb);


    case EXPR_CASE_WHEN: {
        for (uint32_t i = 0; i < e->case_when.branches_count; i++) {
            struct case_when_branch *b = &ABRANCH(arena, e->case_when.branches_start + i);
            if (eval_condition(b->cond_idx, arena, row, t, db))
                return eval_expr(b->then_expr_idx, arena, t, row, db, rb);
        }
        if (e->case_when.else_expr != IDX_NONE)
            return eval_expr(e->case_when.else_expr, arena, t, row, db, rb);
        return cell_make_null();
    }

    case EXPR_SUBQUERY: {
        if (!db || e->subquery.sql_idx == IDX_NONE) return cell_make_null();
        const char *orig_sql = ASTRING(arena, e->subquery.sql_idx);
        if (!orig_sql || !*orig_sql) return cell_make_null();
        /* correlated subquery: substitute outer column refs with literals */
        char sql_buf[2048];
        size_t sql_len = strlen(orig_sql);
        if (sql_len >= sizeof(sql_buf)) sql_len = sizeof(sql_buf) - 1;
        memcpy(sql_buf, orig_sql, sql_len);
        sql_buf[sql_len] = '\0';

        if (t && row)
            subst_correlated_refs(sql_buf, sizeof(sql_buf), t, row);

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

    case EXPR_CAST:
        return eval_cast(e, arena, t, row, db, rb);

    case EXPR_IS_NULL: {
        struct cell val = eval_expr(e->is_null.operand_is, arena, t, row, db, rb);
        int is_null = cell_is_null(&val);
        cell_release_rb(&val, rb);
        struct cell r = {0};
        r.type = COLUMN_TYPE_BOOLEAN;
        r.value.as_bool = e->is_null.negate ? !is_null : is_null;
        return r;
    }

    case EXPR_EXISTS: {
        if (!db || e->exists.sql_idx == IDX_NONE) {
            struct cell r = {0};
            r.type = COLUMN_TYPE_BOOLEAN;
            r.value.as_bool = e->exists.negate ? 1 : 0;
            return r;
        }
        const char *orig_sql = ASTRING(arena, e->exists.sql_idx);
        char sql_buf[2048];
        size_t sql_len = strlen(orig_sql);
        if (sql_len >= sizeof(sql_buf)) sql_len = sizeof(sql_buf) - 1;
        memcpy(sql_buf, orig_sql, sql_len);
        sql_buf[sql_len] = '\0';

        if (t && row)
            subst_correlated_refs(sql_buf, sizeof(sql_buf), t, row);

        int has_rows = 0;
        struct query sub_q = {0};
        if (query_parse(sql_buf, &sub_q) == 0) {
            struct rows sub_rows = {0};
            if (db_exec(db, &sub_q, &sub_rows, NULL) == 0 && sub_rows.count > 0)
                has_rows = 1;
            for (size_t ri = 0; ri < sub_rows.count; ri++)
                row_free(&sub_rows.data[ri]);
            free(sub_rows.data);
        }
        query_free(&sub_q);
        struct cell r = {0};
        r.type = COLUMN_TYPE_BOOLEAN;
        r.value.as_bool = e->exists.negate ? !has_rows : has_rows;
        return r;
    }

    }
    __builtin_unreachable();
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
            /* handle table.* expansion */
            if (sc->expr_idx != IDX_NONE) {
                struct expr *e = &EXPR(arena, sc->expr_idx);
                if (e->type == EXPR_COLUMN_REF && e->column_ref.column.len == 1 &&
                    e->column_ref.column.data[0] == '*') {
                    /* expand all columns from the table */
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
                    continue;
                }
            }
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
                    subst_correlated_refs(sql_buf, sizeof(sql_buf), t, src);
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

/* Shared aggregate helpers used by both query_aggregate and query_group_by */

static inline void agg_accumulate_cell(const struct cell *c, size_t a,
                                        double *sums, size_t *nonnull,
                                        int *minmax_init,
                                        struct cell *min_cells,
                                        struct cell *max_cells)
{
    if (c->is_null || (column_type_is_text(c->type) && !c->value.as_text))
        return;
    nonnull[a]++;
    sums[a] += cell_to_double(c);
    if (!minmax_init[a]) {
        min_cells[a] = *c;
        max_cells[a] = *c;
        minmax_init[a] = 1;
    } else {
        if (cell_compare(c, &min_cells[a]) < 0) min_cells[a] = *c;
        if (cell_compare(c, &max_cells[a]) > 0) max_cells[a] = *c;
    }
}

static struct cell agg_build_result_cell(const struct agg_expr *ae, size_t a,
                                          const double *sums, const size_t *nonnull,
                                          const int *minmax_init,
                                          const struct cell *min_cells,
                                          const struct cell *max_cells,
                                          int col_is_float, int col_is_bigint,
                                          int col_is_text, struct bump_alloc *rb)
{
    (void)minmax_init;
    struct cell c = {0};
    switch (ae->func) {
    case AGG_SUM:
        if (nonnull[a] == 0) {
            c.type = col_is_float ? COLUMN_TYPE_FLOAT : COLUMN_TYPE_INT;
            c.is_null = 1;
        } else if (col_is_float) {
            c.type = COLUMN_TYPE_FLOAT;
            c.value.as_float = sums[a];
        } else if (col_is_bigint || sums[a] > 2147483647.0 || sums[a] < -2147483648.0) {
            c.type = COLUMN_TYPE_BIGINT;
            c.value.as_bigint = (long long)sums[a];
        } else {
            c.type = COLUMN_TYPE_INT;
            c.value.as_int = (int)sums[a];
        }
        break;
    case AGG_AVG:
        c.type = COLUMN_TYPE_FLOAT;
        if (nonnull[a] == 0)
            c.is_null = 1;
        else
            c.value.as_float = sums[a] / (double)nonnull[a];
        break;
    case AGG_MIN:
    case AGG_MAX:
        if (nonnull[a] == 0) {
            c.type = col_is_text ? COLUMN_TYPE_TEXT :
                     (col_is_float ? COLUMN_TYPE_FLOAT : COLUMN_TYPE_INT);
            c.is_null = 1;
        } else {
            const struct cell *src = (ae->func == AGG_MIN) ? &min_cells[a] : &max_cells[a];
            if (rb) cell_copy_bump(&c, src, rb);
            else    cell_copy(&c, src);
        }
        break;
    case AGG_NONE:
    case AGG_COUNT:
    case AGG_STRING_AGG:
    case AGG_ARRAY_AGG:
        break;
    }
    return c;
}

static const char *cell_to_text_buf(const struct cell *cv, char *tmp, size_t tmp_size)
{
    if (cv->is_null || (column_type_is_text(cv->type) && !cv->value.as_text))
        return NULL;
    switch (cv->type) {
    case COLUMN_TYPE_INT:
        snprintf(tmp, tmp_size, "%d", cv->value.as_int);
        return tmp;
    case COLUMN_TYPE_BIGINT:
        snprintf(tmp, tmp_size, "%lld", cv->value.as_bigint);
        return tmp;
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_NUMERIC:
        snprintf(tmp, tmp_size, "%g", cv->value.as_float);
        return tmp;
    case COLUMN_TYPE_BOOLEAN:
        return cv->value.as_bool ? "true" : "false";
    case COLUMN_TYPE_SMALLINT:
        snprintf(tmp, tmp_size, "%d", (int)cv->value.as_smallint);
        return tmp;
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_DATE:
    case COLUMN_TYPE_TIME:
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_TIMESTAMPTZ:
    case COLUMN_TYPE_INTERVAL:
    case COLUMN_TYPE_UUID:
        return cv->value.as_text;
    }
    return NULL;
}

int query_aggregate(struct table *t, struct query_select *s, struct query_arena *arena, struct rows *result, struct bump_alloc *rb)
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
            arena_set_error(arena, "42703", "column '%.*s' not found", (int)s->where.where_column.len, s->where.where_column.data);
            return -1;
        }
    }

    uint32_t naggs = s->aggregates_count;
    /* resolve column index for each aggregate (expr-based aggs use agg_col=-2) */
    int *agg_col = bump_calloc(&arena->scratch, naggs, sizeof(int));
    for (uint32_t a = 0; a < naggs; a++) {
        struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
        if (ae->expr_idx != IDX_NONE) {
            agg_col[a] = -2; /* expression-based aggregate */
        } else if (sv_eq_cstr(ae->column, "*")) {
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
                arena_set_error(arena, "42703", "column '%.*s' not found", (int)ae->column.len, ae->column.data);
                return -1;
            }
        }
    }

    /* accumulate — single allocation for all aggregate arrays
     * layout: double[N] | size_t[N] | int[N] | struct cell[2*N] (min_cells, max_cells) */
    size_t _nagg = naggs;
    size_t _agg_alloc = _nagg * (sizeof(double) + sizeof(size_t) + sizeof(int)) + 2 * _nagg * sizeof(struct cell);
    char *_agg_buf = bump_calloc(&arena->scratch, 1, _agg_alloc ? _agg_alloc : 1);
    double *sums = (double *)_agg_buf;
    size_t *nonnull_count = (size_t *)(sums + _nagg);
    int *minmax_init = (int *)(nonnull_count + _nagg);
    struct cell *min_cells = (struct cell *)(minmax_init + _nagg);
    struct cell *max_cells = min_cells + _nagg;
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
            struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
            /* FILTER (WHERE ...) — skip this row for this aggregate if filter doesn't match */
            if (ae->filter_cond != IDX_NONE) {
                if (!eval_condition(ae->filter_cond, arena, &t->rows.items[i], t, NULL))
                    continue;
            }
            struct cell cv;
            struct cell *c;
            if (agg_col[a] == -2) {
                cv = eval_expr(ae->expr_idx, arena, t, &t->rows.items[i], NULL, NULL);
                c = &cv;
            } else if (agg_col[a] >= 0) {
                c = &t->rows.items[i].cells.items[agg_col[a]];
            } else {
                /* COUNT(*) with FILTER — count this row */
                nonnull_count[a]++;
                continue;
            }
            agg_accumulate_cell(c, a, sums, nonnull_count, minmax_init, min_cells, max_cells);
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
        int col_is_bigint = (agg_col[a] >= 0 &&
                             t->columns.items[agg_col[a]].type == COLUMN_TYPE_BIGINT);
        int col_is_text = (agg_col[a] >= 0 &&
                           column_type_is_text(t->columns.items[agg_col[a]].type));
        /* expression-based aggregates: infer type from accumulated min_cells */
        if (agg_col[a] == -2 && minmax_init[a]) {
            if (min_cells[a].type == COLUMN_TYPE_FLOAT || min_cells[a].type == COLUMN_TYPE_NUMERIC)
                col_is_float = 1;
            else if (min_cells[a].type == COLUMN_TYPE_BIGINT)
                col_is_bigint = 1;
            else if (column_type_is_text(min_cells[a].type))
                col_is_text = 1;
        }
        switch (ae->func) {
            case AGG_COUNT:
                c.type = COLUMN_TYPE_INT;
                if (ae->has_distinct && (agg_col[a] >= 0 || agg_col[a] == -2)) {
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
                        struct cell ev;
                        struct cell *cv;
                        if (agg_col[a] == -2) {
                            ev = eval_expr(ae->expr_idx, arena, t, &t->rows.items[i], NULL, NULL);
                            cv = &ev;
                        } else {
                            cv = &t->rows.items[i].cells.items[agg_col[a]];
                        }
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
                    /* COUNT(*) counts all rows; COUNT(col/expr) counts non-NULL */
                    /* When FILTER is present, COUNT(*) uses nonnull_count (filtered) */
                    if (agg_col[a] == -1 && ae->filter_cond == IDX_NONE)
                        c.value.as_int = (int)row_count;
                    else
                        c.value.as_int = (int)nonnull_count[a];
                }
                break;
            case AGG_SUM:
            case AGG_AVG:
                if (ae->has_distinct && (agg_col[a] >= 0 || agg_col[a] == -2)) {
                    /* SUM/AVG(DISTINCT col): re-scan rows, accumulate only unique values */
                    double dsum = 0.0; size_t dcount = 0;
                    struct cell *seen_d = nonnull_count[a] > 0
                        ? bump_calloc(&arena->scratch, nonnull_count[a], sizeof(struct cell)) : NULL;
                    int nseen_d = 0;
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
                        if (ae->filter_cond != IDX_NONE) {
                            if (!eval_condition(ae->filter_cond, arena, &t->rows.items[i], t, NULL))
                                continue;
                        }
                        struct cell ev;
                        struct cell *cv;
                        if (agg_col[a] == -2) {
                            ev = eval_expr(ae->expr_idx, arena, t, &t->rows.items[i], NULL, NULL);
                            cv = &ev;
                        } else {
                            cv = &t->rows.items[i].cells.items[agg_col[a]];
                        }
                        if (cv->is_null || (column_type_is_text(cv->type) && !cv->value.as_text))
                            continue;
                        int dup = 0;
                        for (int d = 0; d < nseen_d; d++) {
                            if (cell_compare(cv, &seen_d[d]) == 0) { dup = 1; break; }
                        }
                        if (dup) continue;
                        if (seen_d) seen_d[nseen_d++] = *cv;
                        switch (cv->type) {
                            case COLUMN_TYPE_SMALLINT: dsum += (double)cv->value.as_smallint; break;
                            case COLUMN_TYPE_INT:      dsum += (double)cv->value.as_int; break;
                            case COLUMN_TYPE_BIGINT:   dsum += (double)cv->value.as_bigint; break;
                            case COLUMN_TYPE_FLOAT:
                            case COLUMN_TYPE_NUMERIC:  dsum += cv->value.as_float; break;
                            case COLUMN_TYPE_BOOLEAN:
                            case COLUMN_TYPE_TEXT:
                            case COLUMN_TYPE_ENUM:
                            case COLUMN_TYPE_UUID:
                            case COLUMN_TYPE_DATE:
                            case COLUMN_TYPE_TIME:
                            case COLUMN_TYPE_TIMESTAMP:
                            case COLUMN_TYPE_TIMESTAMPTZ:
                            case COLUMN_TYPE_INTERVAL:  break;
                        }
                        dcount++;
                    }
                    if (dcount == 0) {
                        c.type = col_is_float ? COLUMN_TYPE_FLOAT : COLUMN_TYPE_BIGINT;
                        c.is_null = 1;
                    } else if (ae->func == AGG_AVG) {
                        c.type = COLUMN_TYPE_FLOAT;
                        c.value.as_float = dsum / (double)dcount;
                    } else if (col_is_float) {
                        c.type = COLUMN_TYPE_FLOAT;
                        c.value.as_float = dsum;
                    } else if (dsum == (double)(int)dsum && dsum >= -2147483648.0 && dsum <= 2147483647.0) {
                        c.type = COLUMN_TYPE_INT;
                        c.value.as_int = (int)dsum;
                    } else {
                        c.type = COLUMN_TYPE_BIGINT;
                        c.value.as_bigint = (long long)dsum;
                    }
                } else {
                    c = agg_build_result_cell(ae, a, sums, nonnull_count, minmax_init,
                                              min_cells, max_cells, col_is_float,
                                              col_is_bigint, col_is_text, rb);
                }
                break;
            case AGG_MIN:
            case AGG_MAX:
                c = agg_build_result_cell(ae, a, sums, nonnull_count, minmax_init,
                                          min_cells, max_cells, col_is_float,
                                          col_is_bigint, col_is_text, rb);
                break;
            case AGG_STRING_AGG:
            case AGG_ARRAY_AGG: {
                c.type = COLUMN_TYPE_TEXT;
                if (nonnull_count[a] == 0 && ae->func == AGG_STRING_AGG) {
                    c.is_null = 1;
                    c.value.as_text = NULL;
                } else {
                    /* Build concatenated string by re-scanning rows */
                    char *buf = bump_alloc(&arena->scratch, 4096);
                    size_t buf_len = 0, buf_cap = 4096;
                    int is_array = (ae->func == AGG_ARRAY_AGG);
                    if (is_array) { buf[buf_len++] = '{'; }
                    int first = 1;
                    /* Track seen values for DISTINCT */
                    struct cell *seen_vals = NULL;
                    int seen_count = 0;
                    if (ae->has_distinct && nonnull_count[a] > 0)
                        seen_vals = bump_calloc(&arena->scratch, nonnull_count[a], sizeof(struct cell));
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
                        struct cell ev;
                        struct cell *cv;
                        if (agg_col[a] == -2) {
                            ev = eval_expr(ae->expr_idx, arena, t, &t->rows.items[i], NULL, NULL);
                            cv = &ev;
                        } else if (agg_col[a] >= 0) {
                            cv = &t->rows.items[i].cells.items[agg_col[a]];
                        } else {
                            continue;
                        }
                        int is_null_val = cv->is_null || (column_type_is_text(cv->type) && !cv->value.as_text);
                        /* STRING_AGG skips NULLs; ARRAY_AGG includes them */
                        if (is_null_val && !is_array) continue;
                        /* DISTINCT check */
                        if (ae->has_distinct && !is_null_val && seen_vals) {
                            int dup = 0;
                            for (int d = 0; d < seen_count; d++) {
                                if (cell_compare(cv, &seen_vals[d]) == 0) { dup = 1; break; }
                            }
                            if (dup) continue;
                            seen_vals[seen_count++] = *cv;
                        }
                        char tmp[128];
                        const char *val_str = is_null_val ? "NULL" : cell_to_text_buf(cv, tmp, sizeof(tmp));
                        if (!val_str) continue;
                        size_t val_len = strlen(val_str);
                        /* Add separator */
                        const char *sep = NULL;
                        size_t sep_len = 0;
                        if (!first) {
                            if (is_array) {
                                sep = ","; sep_len = 1;
                            } else if (ae->separator.len > 0) {
                                sep = ae->separator.data; sep_len = ae->separator.len;
                            }
                        }
                        /* Ensure buffer capacity */
                        size_t needed = buf_len + (sep ? sep_len : 0) + val_len + 2;
                        while (needed > buf_cap) {
                            buf_cap *= 2;
                            char *nb = bump_alloc(&arena->scratch, buf_cap);
                            memcpy(nb, buf, buf_len);
                            buf = nb;
                        }
                        if (sep) { memcpy(buf + buf_len, sep, sep_len); buf_len += sep_len; }
                        memcpy(buf + buf_len, val_str, val_len); buf_len += val_len;
                        first = 0;
                    }
                    if (is_array) { buf[buf_len++] = '}'; }
                    buf[buf_len] = '\0';
                    if (is_array && buf_len == 2) {
                        /* empty array_agg — return NULL */
                        c.is_null = 1;
                        c.value.as_text = NULL;
                    } else {
                        if (rb) c.value.as_text = bump_strdup(rb, buf);
                        else    c.value.as_text = strdup(buf);
                    }
                }
                break;
            }
            case AGG_NONE:
                break;
        }
        da_push(&dst.cells, c);
    }
    /* HAVING filter (without GROUP BY): evaluate condition against result row */
    if (s->has_having && s->having_cond != IDX_NONE) {
        struct table having_t = {0};
        da_init(&having_t.columns);
        da_init(&having_t.rows);
        da_init(&having_t.indexes);
        for (uint32_t a = 0; a < naggs; a++) {
            struct agg_expr *ae_h = &arena->aggregates.items[s->aggregates_start + a];
            char agg_name[256];
            const char *fn = "?";
            switch (ae_h->func) {
                case AGG_COUNT: fn = "count"; break;
                case AGG_SUM:   fn = "sum"; break;
                case AGG_AVG:   fn = "avg"; break;
                case AGG_MIN:   fn = "min"; break;
                case AGG_MAX:   fn = "max"; break;
                case AGG_STRING_AGG: fn = "string_agg"; break;
                case AGG_ARRAY_AGG:  fn = "array_agg"; break;
                case AGG_NONE: fn = "none"; break;
            }
            if (sv_eq_cstr(ae_h->column, "*"))
                snprintf(agg_name, sizeof(agg_name), "%s", fn);
            else
                snprintf(agg_name, sizeof(agg_name), "%s", fn);
            struct column col_h = { .name = strdup(agg_name),
                                    .type = dst.cells.items[a].type,
                                    .enum_type_name = NULL };
            da_push(&having_t.columns, col_h);
        }
        int passes = eval_condition(s->having_cond, arena, &dst, &having_t, NULL);
        for (size_t i = 0; i < having_t.columns.count; i++)
            column_free(&having_t.columns.items[i]);
        da_free(&having_t.columns);
        if (!passes) {
            da_free(&dst.cells);
            return 0;
        }
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
    int *nulls_first;      /* per-column: -1=default, 0=NULLS LAST, 1=NULLS FIRST */
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
        const struct cell *ca = &ra->cells.items[ci];
        const struct cell *cb = &rb->cells.items[ci];
        int a_null = ca->is_null || (column_type_is_text(ca->type) && !ca->value.as_text);
        int b_null = cb->is_null || (column_type_is_text(cb->type) && !cb->value.as_text);
        if (a_null || b_null) {
            if (a_null && b_null) continue;
            int nf = _sort_ctx.nulls_first ? _sort_ctx.nulls_first[k] : -1;
            /* default: ASC → NULLS LAST, DESC → NULLS FIRST */
            int nulls_go_first = (nf == 1) || (nf == -1 && _sort_ctx.descs[k]);
            if (a_null) return nulls_go_first ? -1 : 1;
            else        return nulls_go_first ? 1 : -1;
        }
        int cmp = cell_compare(ca, cb);
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
        const struct cell *ca = &t->rows.items[ia].cells.items[ci];
        const struct cell *cb = &t->rows.items[ib].cells.items[ci];
        int a_null = ca->is_null || (column_type_is_text(ca->type) && !ca->value.as_text);
        int b_null = cb->is_null || (column_type_is_text(cb->type) && !cb->value.as_text);
        if (a_null || b_null) {
            if (a_null && b_null) continue;
            int nf = _sort_ctx.nulls_first ? _sort_ctx.nulls_first[k] : -1;
            int nulls_go_first = (nf == 1) || (nf == -1 && _sort_ctx.descs[k]);
            if (a_null) return nulls_go_first ? -1 : 1;
            else        return nulls_go_first ? 1 : -1;
        }
        int cmp = cell_compare(ca, cb);
        if (_sort_ctx.descs[k]) cmp = -cmp;
        if (cmp != 0) return cmp;
    }
    return 0;
}

static double cell_to_double(const struct cell *c)
{
    switch (c->type) {
        case COLUMN_TYPE_SMALLINT: return (double)c->value.as_smallint;
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

static int query_window(struct table *t, struct query_select *s, struct query_arena *arena, struct rows *result, struct bump_alloc *rb)
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
                arena_set_error(arena, "42703", "column '%.*s' not found", (int)se->column.len, se->column.data);
                return -1;
            }
        } else {
            if (se->win.has_partition) {
                /* try single column first */
                part_idx[e] = table_find_column_sv(t, se->win.partition_col);
                /* if not found, it might be multi-column — resolve first column only for part_idx */
                if (part_idx[e] < 0) {
                    sv pcol = se->win.partition_col;
                    /* find first comma */
                    size_t comma = 0;
                    while (comma < pcol.len && pcol.data[comma] != ',') comma++;
                    if (comma < pcol.len) {
                        sv first_col = sv_from(pcol.data, comma);
                        while (first_col.len > 0 && first_col.data[first_col.len-1] == ' ') first_col.len--;
                        part_idx[e] = table_find_column_sv(t, first_col);
                    }
                    if (part_idx[e] < 0) {
                        arena_set_error(arena, "42703", "partition column '%.*s' not found", (int)se->win.partition_col.len, se->win.partition_col.data);
                        return -1;
                    }
                }
            }
            if (se->win.has_order) {
                ord_idx[e] = table_find_column_sv(t, se->win.order_col);
                if (ord_idx[e] < 0) {
                    arena_set_error(arena, "42703", "order column '%.*s' not found", (int)se->win.order_col.len, se->win.order_col.data);
                    return -1;
                }
            }
            if (se->win.arg_column.len > 0 && !sv_eq_cstr(se->win.arg_column, "*")) {
                arg_idx[e] = table_find_column_sv(t, se->win.arg_column);
                if (arg_idx[e] < 0) {
                    arena_set_error(arena, "42703", "window arg column '%.*s' not found", (int)se->win.arg_column.len, se->win.arg_column.data);
                    return -1;
                }
            }
        }
    }

    /* collect rows matching WHERE (or all rows if no WHERE) */
    size_t *sorted_idx = (size_t *)bump_alloc(&arena->scratch,
                               (nrows ? nrows : 1) * sizeof(size_t));
    size_t nmatch = 0;
    for (size_t i = 0; i < nrows; i++) {
        if (!row_matches(t, &s->where, arena, &t->rows.items[i], NULL))
            continue;
        sorted_idx[nmatch++] = i;
    }

    if (nmatch == 0) return 0;

    /* resolve multi-column partition indices for the first window expression */
    int multi_part_cols[16];
    int multi_part_ncols = 0;

    /* find the first window expression's partition and order columns */
    int global_part = -1, global_ord = -1, global_ord_desc = 0;
    for (size_t e = 0; e < nexprs; e++) {
        if (arena->select_exprs.items[s->select_exprs_start + e].kind == SEL_WINDOW) {
            if (part_idx[e] >= 0 && global_part < 0) {
                global_part = part_idx[e];
                /* resolve all partition columns from the comma-separated sv */
                struct select_expr *se = &arena->select_exprs.items[s->select_exprs_start + e];
                sv pcol = se->win.partition_col;
                while (pcol.len > 0 && multi_part_ncols < 16) {
                    /* trim leading whitespace */
                    while (pcol.len > 0 && pcol.data[0] == ' ') { pcol.data++; pcol.len--; }
                    size_t comma = 0;
                    while (comma < pcol.len && pcol.data[comma] != ',') comma++;
                    sv one = sv_from(pcol.data, comma);
                    while (one.len > 0 && one.data[one.len-1] == ' ') one.len--;
                    if (one.len > 0) {
                        int ci = table_find_column_sv(t, one);
                        if (ci >= 0) multi_part_cols[multi_part_ncols++] = ci;
                    }
                    if (comma < pcol.len) { pcol.data += comma + 1; pcol.len -= comma + 1; }
                    else break;
                }
            }
            if (ord_idx[e] >= 0 && global_ord < 0) {
                global_ord = ord_idx[e];
                global_ord_desc = arena->select_exprs.items[s->select_exprs_start + e].win.order_desc;
            }
        }
    }

    /* sort by (partition_cols..., order_col) using cmp_indices_multi */
    int sort_cols[18];
    int sort_descs[18];
    size_t sort_ncols = 0;
    for (int pc = 0; pc < multi_part_ncols && sort_ncols < 17; pc++) {
        sort_cols[sort_ncols] = multi_part_cols[pc];
        sort_descs[sort_ncols] = 0; /* partition always ASC for grouping */
        sort_ncols++;
    }
    if (global_ord >= 0) {
        sort_cols[sort_ncols] = global_ord;
        sort_descs[sort_ncols] = global_ord_desc;
        sort_ncols++;
    }
    if (sort_ncols > 0) {
        _sort_ctx.table = t;
        _sort_ctx.cols = sort_cols;
        _sort_ctx.descs = sort_descs;
        _sort_ctx.ncols = sort_ncols;
        qsort(sorted_idx, nmatch, sizeof(size_t), cmp_indices_multi);
    }

    /* build partition boundaries — O(N) linear scan */
    size_t *part_starts = (size_t *)bump_alloc(&arena->scratch,
                               (nmatch + 1) * sizeof(size_t));
    size_t nparts = 0;
    part_starts[nparts++] = 0;
    for (size_t i = 1; i < nmatch; i++) {
        if (multi_part_ncols > 0) {
            int differ = 0;
            for (int pc = 0; pc < multi_part_ncols; pc++) {
                struct cell *ca = &t->rows.items[sorted_idx[i - 1]].cells.items[multi_part_cols[pc]];
                struct cell *cb = &t->rows.items[sorted_idx[i]].cells.items[multi_part_cols[pc]];
                if (!cell_equal_nullsafe(ca, cb)) { differ = 1; break; }
            }
            if (differ) part_starts[nparts++] = i;
        }
    }
    part_starts[nparts] = nmatch; /* sentinel */

    /* pre-compute per-partition aggregate totals for SUM/COUNT/AVG without frames */
    /* We store results indexed by ORIGINAL ROW INDEX (sorted_idx[i]) so that
     * re-sorting for different partition columns doesn't invalidate earlier results */
    int *win_int_vals = (int *)bump_calloc(&arena->scratch, nrows * nexprs, sizeof(int));
    double *win_dbl_vals = (double *)bump_calloc(&arena->scratch, nrows * nexprs, sizeof(double));
    int *win_is_null = (int *)bump_calloc(&arena->scratch, nrows * nexprs, sizeof(int));
    int *win_is_dbl = (int *)bump_calloc(&arena->scratch, nexprs, sizeof(int));

    /* process each window expression across all partitions */
    for (size_t e = 0; e < nexprs; e++) {
        struct select_expr *se = &arena->select_exprs.items[s->select_exprs_start + e];
        if (se->kind == SEL_COLUMN) continue;

        /* Per-expression partition boundaries: if this expression's partition
         * column differs from the global one, re-sort and rebuild boundaries */
        size_t *expr_part_starts = part_starts;
        size_t expr_nparts = nparts;
        if (part_idx[e] >= 0 && part_idx[e] != global_part) {
            /* Re-sort sorted_idx by this expression's partition + order columns */
            int e_sort_cols[2];
            int e_sort_descs[2];
            size_t e_sort_ncols = 0;
            e_sort_cols[e_sort_ncols] = part_idx[e];
            e_sort_descs[e_sort_ncols] = 0;
            e_sort_ncols++;
            if (ord_idx[e] >= 0) {
                e_sort_cols[e_sort_ncols] = ord_idx[e];
                e_sort_descs[e_sort_ncols] = se->win.order_desc;
                e_sort_ncols++;
            }
            _sort_ctx.table = t;
            _sort_ctx.cols = e_sort_cols;
            _sort_ctx.descs = e_sort_descs;
            _sort_ctx.ncols = e_sort_ncols;
            qsort(sorted_idx, nmatch, sizeof(size_t), cmp_indices_multi);

            expr_part_starts = (size_t *)bump_alloc(&arena->scratch,
                                     (nmatch + 1) * sizeof(size_t));
            expr_nparts = 0;
            expr_part_starts[expr_nparts++] = 0;
            for (size_t i = 1; i < nmatch; i++) {
                struct cell *ca = &t->rows.items[sorted_idx[i - 1]].cells.items[part_idx[e]];
                struct cell *cb_c = &t->rows.items[sorted_idx[i]].cells.items[part_idx[e]];
                if (!cell_equal_nullsafe(ca, cb_c))
                    expr_part_starts[expr_nparts++] = i;
            }
            expr_part_starts[expr_nparts] = nmatch;
        } else if (part_idx[e] < 0 && global_part >= 0) {
            /* No partition for this expr — treat entire result as one partition */
            expr_part_starts = (size_t *)bump_alloc(&arena->scratch, 2 * sizeof(size_t));
            expr_part_starts[0] = 0;
            expr_part_starts[1] = nmatch;
            expr_nparts = 1;
        }

        for (size_t p = 0; p < expr_nparts; p++) {
            size_t ps = expr_part_starts[p];
            size_t pe = expr_part_starts[p + 1];
            size_t psize = pe - ps;

            switch (se->win.func) {
                case WIN_ROW_NUMBER:
                    for (size_t i = ps; i < pe; i++)
                        win_int_vals[sorted_idx[i] * nexprs + e] = (int)(i - ps + 1);
                    break;

                case WIN_RANK: {
                    int rank = 1;
                    for (size_t i = ps; i < pe; i++) {
                        if (i > ps && ord_idx[e] >= 0) {
                            int cmp = cell_compare(
                                &t->rows.items[sorted_idx[i]].cells.items[ord_idx[e]],
                                &t->rows.items[sorted_idx[i - 1]].cells.items[ord_idx[e]]);
                            if (cmp != 0) rank = (int)(i - ps + 1);
                        }
                        win_int_vals[sorted_idx[i] * nexprs + e] = rank;
                    }
                    break;
                }

                case WIN_DENSE_RANK: {
                    int rank = 1;
                    for (size_t i = ps; i < pe; i++) {
                        if (i > ps && ord_idx[e] >= 0) {
                            int cmp = cell_compare(
                                &t->rows.items[sorted_idx[i]].cells.items[ord_idx[e]],
                                &t->rows.items[sorted_idx[i - 1]].cells.items[ord_idx[e]]);
                            if (cmp != 0) rank++;
                        }
                        win_int_vals[sorted_idx[i] * nexprs + e] = rank;
                    }
                    break;
                }

                case WIN_NTILE: {
                    int nbuckets = se->win.offset > 0 ? se->win.offset : 1;
                    for (size_t i = ps; i < pe; i++) {
                        size_t pos = i - ps; /* 0-based */
                        int bucket = (int)((pos * (size_t)nbuckets) / psize) + 1;
                        win_int_vals[sorted_idx[i] * nexprs + e] = bucket;
                    }
                    break;
                }

                case WIN_PERCENT_RANK: {
                    win_is_dbl[e] = 1;
                    if (psize <= 1) {
                        for (size_t i = ps; i < pe; i++)
                            win_dbl_vals[sorted_idx[i] * nexprs + e] = 0.0;
                    } else {
                        int rank = 1;
                        for (size_t i = ps; i < pe; i++) {
                            if (i > ps && ord_idx[e] >= 0) {
                                int cmp = cell_compare(
                                    &t->rows.items[sorted_idx[i]].cells.items[ord_idx[e]],
                                    &t->rows.items[sorted_idx[i - 1]].cells.items[ord_idx[e]]);
                                if (cmp != 0) rank = (int)(i - ps + 1);
                            }
                            win_dbl_vals[sorted_idx[i] * nexprs + e] = (double)(rank - 1) / (double)(psize - 1);
                        }
                    }
                    break;
                }

                case WIN_CUME_DIST: {
                    win_is_dbl[e] = 1;
                    if (ord_idx[e] >= 0) {
                        /* walk partition; for each group of tied values,
                         * cume_dist = (last position of tie group + 1) / psize */
                        size_t i = ps;
                        while (i < pe) {
                            /* find end of tie group */
                            size_t j = i + 1;
                            while (j < pe && cell_compare(
                                &t->rows.items[sorted_idx[j]].cells.items[ord_idx[e]],
                                &t->rows.items[sorted_idx[i]].cells.items[ord_idx[e]]) == 0)
                                j++;
                            double cd = (double)(j - ps) / (double)psize;
                            for (size_t k = i; k < j; k++)
                                win_dbl_vals[sorted_idx[k] * nexprs + e] = cd;
                            i = j;
                        }
                    } else {
                        for (size_t i = ps; i < pe; i++)
                            win_dbl_vals[sorted_idx[i] * nexprs + e] = 1.0;
                    }
                    break;
                }

                case WIN_LAG:
                case WIN_LEAD: {
                    int offset = se->win.offset;
                    for (size_t i = ps; i < pe; i++) {
                        size_t pos = i - ps;
                        size_t target;
                        int in_range = 0;
                        if (se->win.func == WIN_LAG) {
                            if (pos >= (size_t)offset) { target = i - (size_t)offset; in_range = 1; }
                        } else {
                            target = i + (size_t)offset;
                            if (target < pe) in_range = 1;
                        }
                        if (in_range && arg_idx[e] >= 0) {
                            struct cell *ac = &t->rows.items[sorted_idx[target]].cells.items[arg_idx[e]];
                            if (ac->is_null || (column_type_is_text(ac->type) && !ac->value.as_text)) {
                                win_is_null[sorted_idx[i] * nexprs + e] = 1;
                            } else {
                                win_dbl_vals[sorted_idx[i] * nexprs + e] = cell_to_double(ac);
                                win_is_dbl[e] = 1;
                            }
                        } else {
                            win_is_null[sorted_idx[i] * nexprs + e] = 1;
                        }
                    }
                    break;
                }

                case WIN_FIRST_VALUE:
                case WIN_LAST_VALUE: {
                    for (size_t i = ps; i < pe; i++) {
                        size_t target = (se->win.func == WIN_FIRST_VALUE) ? ps : (pe - 1);
                        if (arg_idx[e] >= 0) {
                            struct cell *ac = &t->rows.items[sorted_idx[target]].cells.items[arg_idx[e]];
                            if (ac->is_null || (column_type_is_text(ac->type) && !ac->value.as_text)) {
                                win_is_null[sorted_idx[i] * nexprs + e] = 1;
                            } else {
                                win_dbl_vals[sorted_idx[i] * nexprs + e] = cell_to_double(ac);
                                win_is_dbl[e] = 1;
                            }
                        } else {
                            win_is_null[sorted_idx[i] * nexprs + e] = 1;
                        }
                    }
                    break;
                }

                case WIN_NTH_VALUE: {
                    int nth = se->win.offset; /* 1-based */
                    for (size_t i = ps; i < pe; i++) {
                        if (nth >= 1 && (size_t)nth <= psize && arg_idx[e] >= 0) {
                            size_t target = ps + (size_t)(nth - 1);
                            struct cell *ac = &t->rows.items[sorted_idx[target]].cells.items[arg_idx[e]];
                            if (ac->is_null || (column_type_is_text(ac->type) && !ac->value.as_text)) {
                                win_is_null[sorted_idx[i] * nexprs + e] = 1;
                            } else {
                                win_dbl_vals[sorted_idx[i] * nexprs + e] = cell_to_double(ac);
                                win_is_dbl[e] = 1;
                            }
                        } else {
                            win_is_null[sorted_idx[i] * nexprs + e] = 1;
                        }
                    }
                    break;
                }

                case WIN_SUM:
                case WIN_COUNT:
                case WIN_AVG: {
                    if (!se->win.has_frame && !se->win.has_order) {
                        /* no frame, no ORDER BY: compute partition total in one pass */
                        double part_sum = 0.0;
                        int part_nn = 0;
                        int part_count = (int)psize;
                        for (size_t i = ps; i < pe; i++) {
                            if (arg_idx[e] >= 0) {
                                struct cell *ac = &t->rows.items[sorted_idx[i]].cells.items[arg_idx[e]];
                                if (!ac->is_null && !(column_type_is_text(ac->type) && !ac->value.as_text)) {
                                    part_sum += cell_to_double(ac);
                                    part_nn++;
                                }
                            }
                        }
                        for (size_t i = ps; i < pe; i++) {
                            size_t oi = sorted_idx[i];
                            if (se->win.func == WIN_SUM) {
                                if (arg_idx[e] >= 0 &&
                                    t->columns.items[arg_idx[e]].type == COLUMN_TYPE_FLOAT) {
                                    win_is_dbl[e] = 1;
                                    win_dbl_vals[oi * nexprs + e] = part_sum;
                                } else {
                                    win_int_vals[oi * nexprs + e] = (int)part_sum;
                                }
                            } else if (se->win.func == WIN_COUNT) {
                                win_int_vals[oi * nexprs + e] = (arg_idx[e] >= 0) ? part_nn : part_count;
                            } else { /* WIN_AVG */
                                win_is_dbl[e] = 1;
                                if (part_nn > 0) {
                                    win_dbl_vals[oi * nexprs + e] = part_sum / (double)part_nn;
                                } else {
                                    win_is_null[oi * nexprs + e] = 1;
                                }
                            }
                        }
                    } else if (!se->win.has_frame && se->win.has_order) {
                        /* ORDER BY without explicit frame: implicit UNBOUNDED PRECEDING TO CURRENT ROW
                         * — compute cumulative running values */
                        double running_sum = 0.0;
                        int running_nn = 0;
                        int running_count = 0;
                        for (size_t i = ps; i < pe; i++) {
                            if (arg_idx[e] >= 0) {
                                struct cell *ac = &t->rows.items[sorted_idx[i]].cells.items[arg_idx[e]];
                                if (!ac->is_null && !(column_type_is_text(ac->type) && !ac->value.as_text)) {
                                    running_sum += cell_to_double(ac);
                                    running_nn++;
                                }
                            }
                            running_count++;
                            size_t oi = sorted_idx[i];
                            if (se->win.func == WIN_SUM) {
                                if (arg_idx[e] >= 0 &&
                                    t->columns.items[arg_idx[e]].type == COLUMN_TYPE_FLOAT) {
                                    win_is_dbl[e] = 1;
                                    win_dbl_vals[oi * nexprs + e] = running_sum;
                                } else {
                                    win_int_vals[oi * nexprs + e] = (int)running_sum;
                                }
                            } else if (se->win.func == WIN_COUNT) {
                                win_int_vals[oi * nexprs + e] = (arg_idx[e] >= 0) ? running_nn : running_count;
                            } else { /* WIN_AVG */
                                win_is_dbl[e] = 1;
                                if (running_nn > 0) {
                                    win_dbl_vals[oi * nexprs + e] = running_sum / (double)running_nn;
                                } else {
                                    win_is_null[oi * nexprs + e] = 1;
                                }
                            }
                        }
                    } else {
                        /* with frame: compute per-row */
                        for (size_t i = ps; i < pe; i++) {
                            size_t my_pos = i - ps;
                            size_t fs = 0, fe = psize;
                            switch (se->win.frame_start) {
                                case FRAME_UNBOUNDED_PRECEDING: fs = 0; break;
                                case FRAME_CURRENT_ROW: fs = my_pos; break;
                                case FRAME_N_PRECEDING: fs = (my_pos >= (size_t)se->win.frame_start_n) ? my_pos - (size_t)se->win.frame_start_n : 0; break;
                                case FRAME_N_FOLLOWING: fs = my_pos + (size_t)se->win.frame_start_n; break;
                                case FRAME_UNBOUNDED_FOLLOWING: fs = psize; break;
                            }
                            switch (se->win.frame_end) {
                                case FRAME_UNBOUNDED_FOLLOWING: fe = psize; break;
                                case FRAME_CURRENT_ROW: fe = my_pos + 1; break;
                                case FRAME_N_FOLLOWING: fe = my_pos + (size_t)se->win.frame_end_n + 1; if (fe > psize) fe = psize; break;
                                case FRAME_N_PRECEDING: fe = (my_pos >= (size_t)se->win.frame_end_n) ? my_pos - (size_t)se->win.frame_end_n + 1 : 0; break;
                                case FRAME_UNBOUNDED_PRECEDING: fe = 0; break;
                            }
                            if (fs > psize) fs = psize;
                            double frame_sum = 0.0;
                            int frame_nn = 0;
                            int frame_count = 0;
                            for (size_t fi = fs; fi < fe; fi++) {
                                size_t j = sorted_idx[ps + fi];
                                frame_count++;
                                if (arg_idx[e] >= 0) {
                                    struct cell *ac = &t->rows.items[j].cells.items[arg_idx[e]];
                                    if (!ac->is_null && !(column_type_is_text(ac->type) && !ac->value.as_text)) {
                                        frame_sum += cell_to_double(ac);
                                        frame_nn++;
                                    }
                                }
                            }
                            size_t oi = sorted_idx[i];
                            if (se->win.func == WIN_SUM) {
                                if (arg_idx[e] >= 0 &&
                                    t->columns.items[arg_idx[e]].type == COLUMN_TYPE_FLOAT) {
                                    win_is_dbl[e] = 1;
                                    win_dbl_vals[oi * nexprs + e] = frame_sum;
                                } else {
                                    win_int_vals[oi * nexprs + e] = (int)frame_sum;
                                }
                            } else if (se->win.func == WIN_COUNT) {
                                win_int_vals[oi * nexprs + e] = (arg_idx[e] >= 0) ? frame_nn : frame_count;
                            } else { /* WIN_AVG */
                                win_is_dbl[e] = 1;
                                if (frame_nn > 0) {
                                    win_dbl_vals[oi * nexprs + e] = frame_sum / (double)frame_nn;
                                } else {
                                    win_is_null[oi * nexprs + e] = 1;
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }
    }

    /* Re-sort sorted_idx back to the original global sort order for emit.
     * This is needed because per-expression partition handling may have
     * re-sorted sorted_idx for a different partition column. */
    if (global_part >= 0 || global_ord >= 0) {
        int e_sort_cols[2];
        int e_sort_descs[2];
        size_t e_sort_ncols = 0;
        if (global_part >= 0) {
            e_sort_cols[e_sort_ncols] = global_part;
            e_sort_descs[e_sort_ncols] = 0;
            e_sort_ncols++;
        }
        if (global_ord >= 0) {
            e_sort_cols[e_sort_ncols] = global_ord;
            e_sort_descs[e_sort_ncols] = 0; /* use original desc from first window expr */
            e_sort_ncols++;
        }
        _sort_ctx.table = t;
        _sort_ctx.cols = e_sort_cols;
        _sort_ctx.descs = e_sort_descs;
        _sort_ctx.ncols = e_sort_ncols;
        qsort(sorted_idx, nmatch, sizeof(size_t), cmp_indices_multi);
    }

    /* emit result rows in sorted order */
    for (size_t ri = 0; ri < nmatch; ri++) {
        size_t row_i = sorted_idx[ri];
        struct row *src = &t->rows.items[row_i];
        struct row dst = {0};
        da_init(&dst.cells);

        for (size_t e = 0; e < nexprs; e++) {
            struct select_expr *se = &arena->select_exprs.items[s->select_exprs_start + e];
            struct cell c = {0};

            if (se->kind == SEL_COLUMN) {
                if (rb) cell_copy_bump(&c, &src->cells.items[col_idx[e]], rb);
                else    cell_copy(&c, &src->cells.items[col_idx[e]]);
            } else if (win_is_null[row_i * nexprs + e]) {
                c.type = (win_is_dbl[e]) ? COLUMN_TYPE_FLOAT : COLUMN_TYPE_INT;
                c.is_null = 1;
            } else if (win_is_dbl[e]) {
                c.type = COLUMN_TYPE_FLOAT;
                c.value.as_float = win_dbl_vals[row_i * nexprs + e];
            } else {
                c.type = COLUMN_TYPE_INT;
                c.value.as_int = win_int_vals[row_i * nexprs + e];
            }

            /* LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE: copy actual cell for text types */
            if (se->kind == SEL_WINDOW && !win_is_null[row_i * nexprs + e] &&
                (se->win.func == WIN_LAG || se->win.func == WIN_LEAD ||
                 se->win.func == WIN_FIRST_VALUE || se->win.func == WIN_LAST_VALUE ||
                 se->win.func == WIN_NTH_VALUE) && arg_idx[e] >= 0) {
                /* re-derive the target row for text cell copy */
                /* find which partition this row belongs to */
                size_t p = 0;
                for (size_t pp = 0; pp < nparts; pp++) {
                    if (ri >= part_starts[pp] && ri < part_starts[pp + 1]) { p = pp; break; }
                }
                size_t ps = part_starts[p];
                size_t pe = part_starts[p + 1];
                size_t psize = pe - ps;
                size_t target = ri; /* default */
                size_t pos = ri - ps;
                if (se->win.func == WIN_LAG) {
                    if (pos >= (size_t)se->win.offset) target = ri - (size_t)se->win.offset;
                } else if (se->win.func == WIN_LEAD) {
                    target = ri + (size_t)se->win.offset;
                } else if (se->win.func == WIN_FIRST_VALUE) {
                    target = ps;
                } else if (se->win.func == WIN_LAST_VALUE) {
                    target = pe - 1;
                } else if (se->win.func == WIN_NTH_VALUE) {
                    int nth = se->win.offset;
                    target = (nth >= 1 && (size_t)nth <= psize) ? ps + (size_t)(nth - 1) : ri;
                }
                struct cell *ac = &t->rows.items[sorted_idx[target]].cells.items[arg_idx[e]];
                if (column_type_is_text(ac->type)) {
                    c.type = COLUMN_TYPE_INT; /* reset */
                    c.is_null = 0;
                    if (rb) cell_copy_bump(&c, ac, rb);
                    else    cell_copy(&c, ac);
                }
            }

            da_push(&dst.cells, c);
        }
        rows_push(result, dst);
    }

    /* apply outer ORDER BY if present — use qsort */
    if (s->has_order_by && result->count > 1) {
        /* resolve ORDER BY columns in result */
        int ob_cols[32];
        int ob_descs[32];
        size_t ob_n = 0;
        for (uint32_t oi = 0; oi < s->order_by_count && ob_n < 32; oi++) {
            struct order_by_item *ob = &arena->order_items.items[s->order_by_start + oi];
            sv ord_name = ob->column;
            int res_col = -1;
            for (size_t e = 0; e < nexprs; e++) {
                struct select_expr *se2 = &arena->select_exprs.items[s->select_exprs_start + e];
                if (se2->kind == SEL_COLUMN) {
                    sv col = se2->column;
                    sv bare_col = col, bare_ord = ord_name;
                    for (size_t k = 0; k < col.len; k++)
                        if (col.data[k] == '.') { bare_col = sv_from(col.data+k+1, col.len-k-1); break; }
                    for (size_t k = 0; k < ord_name.len; k++)
                        if (ord_name.data[k] == '.') { bare_ord = sv_from(ord_name.data+k+1, ord_name.len-k-1); break; }
                    if (sv_eq_ignorecase(bare_col, bare_ord)) { res_col = (int)e; break; }
                }
                /* also match by alias */
                if (se2->alias.len > 0 && sv_eq_ignorecase(se2->alias, ord_name)) {
                    res_col = (int)e; break;
                }
            }
            if (res_col >= 0) {
                ob_cols[ob_n] = res_col;
                ob_descs[ob_n] = ob->desc;
                ob_n++;
            }
        }
        if (ob_n > 0) {
            _sort_ctx.cols = ob_cols;
            _sort_ctx.descs = ob_descs;
            _sort_ctx.ncols = ob_n;
            _sort_ctx.rows = result;
            qsort(result->data, result->count, sizeof(struct row), cmp_rows_multi);
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
        if (grp_cols[k] == -2) {
            /* expression-based group key — try matching by SELECT alias */
            for (uint32_t pc = 0; pc < s->parsed_columns_count; pc++) {
                struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + pc];
                if (sc->alias.len > 0 && sv_eq_ignorecase(sc->alias, name) &&
                    sc->expr_idx != IDX_NONE) {
                    return (int)(grp_offset + k);
                }
            }
            /* fallback: scan raw s->columns text for " AS <name>" patterns
             * to match aliases when parsed_columns are not available */
            if (s->parsed_columns_count == 0 && s->columns.len > 0) {
                const char *p = s->columns.data;
                const char *end = s->columns.data + s->columns.len;
                while (p < end - 3) {
                    if ((p[0] == ' ' || p[0] == ')') &&
                        (p[1] == 'A' || p[1] == 'a') &&
                        (p[2] == 'S' || p[2] == 's') &&
                        p[3] == ' ') {
                        const char *alias_start = p + 4;
                        while (alias_start < end && *alias_start == ' ') alias_start++;
                        const char *alias_end = alias_start;
                        while (alias_end < end && *alias_end != ',' && *alias_end != ' ') alias_end++;
                        sv alias_sv = sv_from(alias_start, (size_t)(alias_end - alias_start));
                        if (sv_eq_ignorecase(alias_sv, name))
                            return (int)(grp_offset + k);
                    }
                    p++;
                }
            }
        }
    }
    /* check aggregate aliases and names */
    for (size_t a = 0; a < agg_n; a++) {
        struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
        /* check alias first (e.g. SUM(val) AS total → ORDER BY total) */
        if (ae->alias.len > 0 && sv_eq_ignorecase(ae->alias, name))
            return (int)(agg_offset + a);
        const char *agg_name = "?";
        switch (ae->func) {
            case AGG_SUM:        agg_name = "sum";        break;
            case AGG_COUNT:      agg_name = "count";      break;
            case AGG_AVG:        agg_name = "avg";        break;
            case AGG_MIN:        agg_name = "min";        break;
            case AGG_MAX:        agg_name = "max";        break;
            case AGG_STRING_AGG: agg_name = "string_agg"; break;
            case AGG_ARRAY_AGG:  agg_name = "array_agg";  break;
            case AGG_NONE: break;
        }
        if (sv_eq_cstr(name, agg_name))
            return (int)(agg_offset + a);
    }
    return -1;
}

int query_group_by(struct table *t, struct query_select *s, struct query_arena *arena, struct rows *result, struct bump_alloc *rb)
{
    /* resolve GROUP BY column indices (grp_cols[k] = -2 means expression-based) */
    size_t ngrp = s->group_by_count;
    if (ngrp == 0) ngrp = 1; /* backward compat: single group_by_col */
    int grp_cols[32];
    uint32_t grp_expr[32]; /* expr indices for expression-based GROUP BY keys */
    for (size_t k = 0; k < 32; k++) grp_expr[k] = IDX_NONE;
    if (s->group_by_count > 0) {
        for (size_t k = 0; k < ngrp && k < 32; k++) {
            /* check if this GROUP BY item has an expression index */
            uint32_t expr_idx = IDX_NONE;
            if (s->group_by_exprs_start > 0 || arena->arg_indices.count > 0) {
                uint32_t ai = s->group_by_exprs_start + (uint32_t)k;
                if (ai < (uint32_t)arena->arg_indices.count)
                    expr_idx = arena->arg_indices.items[ai];
            }
            if (expr_idx != IDX_NONE) {
                grp_cols[k] = -2; /* expression-based */
                grp_expr[k] = expr_idx;
            } else {
                sv gbcol = ASV(arena, s->group_by_start + (uint32_t)k);
                grp_cols[k] = table_find_column_sv(t, gbcol);
                if (grp_cols[k] < 0) {
                    /* try matching by SELECT alias (parsed_columns path) */
                    for (uint32_t pc = 0; pc < s->parsed_columns_count; pc++) {
                        struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + pc];
                        if (sc->alias.len > 0 && sv_eq_ignorecase(sc->alias, gbcol) && sc->expr_idx != IDX_NONE) {
                            struct expr *e = &EXPR(arena, sc->expr_idx);
                            if (e->type == EXPR_COLUMN_REF) {
                                grp_cols[k] = table_find_column_sv(t, e->column_ref.column);
                                if (grp_cols[k] >= 0) break;
                            }
                        }
                    }
                    /* fallback: scan raw s->columns text for "colname AS alias" */
                    if (grp_cols[k] < 0 && s->columns.len > 0) {
                        const char *p = s->columns.data;
                        const char *end = s->columns.data + s->columns.len;
                        while (p < end) {
                            /* find start of this column item (skip leading whitespace) */
                            while (p < end && (*p == ' ' || *p == '\t')) p++;
                            const char *col_start = p;
                            /* scan to comma or end, tracking " AS " position */
                            const char *as_pos = NULL;
                            int depth = 0;
                            while (p < end && (depth > 0 || *p != ',')) {
                                if (*p == '(') depth++;
                                else if (*p == ')') depth--;
                                else if (depth == 0 && p + 3 <= end &&
                                         p > col_start &&
                                         (p[-1] == ' ' || p[-1] == ')') &&
                                         (p[0] == 'A' || p[0] == 'a') &&
                                         (p[1] == 'S' || p[1] == 's') &&
                                         (p[2] == ' ' || p[2] == ',')) {
                                    as_pos = p;
                                }
                                p++;
                            }
                            if (as_pos) {
                                const char *alias_start = as_pos + 3;
                                while (alias_start < p && *alias_start == ' ') alias_start++;
                                const char *alias_end = alias_start;
                                while (alias_end < p && *alias_end != ' ' && *alias_end != ',') alias_end++;
                                sv alias_sv = sv_from(alias_start, (size_t)(alias_end - alias_start));
                                if (sv_eq_ignorecase(alias_sv, gbcol)) {
                                    /* extract the column name before AS */
                                    const char *cn_start = col_start;
                                    const char *cn_end = as_pos;
                                    while (cn_end > cn_start && cn_end[-1] == ' ') cn_end--;
                                    sv cn = sv_from(cn_start, (size_t)(cn_end - cn_start));
                                    grp_cols[k] = table_find_column_sv(t, cn);
                                }
                            }
                            if (*p == ',') p++;
                        }
                    }
                    if (grp_cols[k] < 0) {
                        arena_set_error(arena, "42703", "GROUP BY column '%.*s' not found", (int)gbcol.len, gbcol.data);
                        return -1;
                    }
                }
            }
        }
    } else {
        grp_cols[0] = table_find_column_sv(t, s->group_by_col);
        if (grp_cols[0] < 0) {
            arena_set_error(arena, "42703", "GROUP BY column '%.*s' not found", (int)s->group_by_col.len, s->group_by_col.data);
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

    /* Pre-compute expression-based group keys for all matching rows */
    int has_expr_grp = 0;
    for (size_t k = 0; k < ngrp; k++)
        if (grp_cols[k] == -2) { has_expr_grp = 1; break; }

    /* grp_vals[row_idx * ngrp + k] holds the evaluated group key for expression-based keys */
    struct cell *grp_vals = NULL;
    if (has_expr_grp && matching_count > 0) {
        grp_vals = (struct cell *)bump_calloc(&arena->scratch, matching_count * ngrp, sizeof(struct cell));
        for (size_t m = 0; m < matching_count; m++) {
            size_t ri = matching[m];
            for (size_t k = 0; k < ngrp; k++) {
                if (grp_cols[k] == -2) {
                    grp_vals[m * ngrp + k] = eval_expr(grp_expr[k], arena, t, &t->rows.items[ri], NULL, rb);
                }
            }
        }
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
                if (grp_cols[k] == -2) {
                    if (!cell_equal_nullsafe(&grp_vals[m * ngrp + k],
                                    &grp_vals[group_starts[g] * ngrp + k])) {
                        eq = 0; break;
                    }
                } else {
                    if (!cell_equal_nullsafe(&t->rows.items[ri].cells.items[grp_cols[k]],
                                    &t->rows.items[gi].cells.items[grp_cols[k]])) {
                        eq = 0; break;
                    }
                }
            }
            if (eq) { found = 1; break; }
        }
        if (!found) group_starts[group_starts_count++] = m;
    }

    /* pre-allocate aggregate accumulators in a single allocation */
    size_t agg_n = s->aggregates_count;
    void *_grp_buf = NULL;
    double *sums = NULL;
    int *gminmax_init = NULL, *gagg_cols = NULL;
    size_t *gnonnull = NULL;
    struct cell *gmin_cells = NULL, *gmax_cells = NULL;
    if (agg_n > 0) {
        /* layout: double[N] | size_t[N] | int[2*N] | struct cell[2*N] */
        size_t _grp_alloc = agg_n * sizeof(double) + agg_n * sizeof(size_t) + 2 * agg_n * sizeof(int) + 2 * agg_n * sizeof(struct cell);
        _grp_buf = bump_alloc(&arena->scratch, _grp_alloc);
        memset(_grp_buf, 0, _grp_alloc);
        sums          = (double *)_grp_buf;
        gnonnull      = (size_t *)(sums + agg_n);
        gminmax_init  = (int *)(gnonnull + agg_n);
        gagg_cols     = gminmax_init + agg_n;
        gmin_cells    = (struct cell *)(gagg_cols + agg_n);
        gmax_cells    = gmin_cells + agg_n;
    }

    /* resolve aggregate column indices once (expr-based aggs use -2) */
    for (size_t a = 0; a < agg_n; a++) {
        struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
        if (ae->expr_idx != IDX_NONE)
            gagg_cols[a] = -2;
        else if (sv_eq_cstr(ae->column, "*"))
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
                struct column col_grp;
                if (grp_cols[k] == -2) {
                    col_grp = (struct column){ .name = strdup("?"), .type = COLUMN_TYPE_FLOAT, .enum_type_name = NULL };
                } else {
                    col_grp = (struct column){ .name = strdup(t->columns.items[grp_cols[k]].name),
                                              .type = t->columns.items[grp_cols[k]].type,
                                              .enum_type_name = NULL };
                }
                da_push(&having_t.columns, col_grp);
            }
        }
        for (size_t a = 0; a < agg_n; a++) {
            struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
            const char *agg_name = "?";
            switch (ae->func) {
                case AGG_SUM:        agg_name = "sum";        break;
                case AGG_COUNT:      agg_name = "count";      break;
                case AGG_AVG:        agg_name = "avg";        break;
                case AGG_MIN:        agg_name = "min";        break;
                case AGG_MAX:        agg_name = "max";        break;
                case AGG_STRING_AGG: agg_name = "string_agg"; break;
                case AGG_ARRAY_AGG:  agg_name = "array_agg";  break;
                case AGG_NONE: break;
            }
            int ac_idx = gagg_cols[a];
            enum column_type ctype = COLUMN_TYPE_INT;
            if (ac_idx >= 0)
                ctype = t->columns.items[ac_idx].type;
            if (ae->func == AGG_SUM) {
                if (ctype == COLUMN_TYPE_FLOAT || ctype == COLUMN_TYPE_NUMERIC)
                    ctype = COLUMN_TYPE_FLOAT;
                else if (ctype == COLUMN_TYPE_BIGINT)
                    ctype = COLUMN_TYPE_BIGINT;
                else
                    ctype = COLUMN_TYPE_INT;
            }
            if (ae->func == AGG_AVG)
                ctype = COLUMN_TYPE_FLOAT;
            if (ae->func == AGG_COUNT)
                ctype = COLUMN_TYPE_INT;
            if (ae->func == AGG_STRING_AGG || ae->func == AGG_ARRAY_AGG)
                ctype = COLUMN_TYPE_TEXT;
            struct column col_a = { .name = strdup(agg_name),
                                    .type = ctype,
                                    .enum_type_name = NULL };
            da_push(&having_t.columns, col_a);
        }
        if (s->agg_before_cols) {
            for (size_t k = 0; k < ngrp; k++) {
                struct column col_grp;
                if (grp_cols[k] == -2) {
                    col_grp = (struct column){ .name = strdup("?"), .type = COLUMN_TYPE_FLOAT, .enum_type_name = NULL };
                } else {
                    col_grp = (struct column){ .name = strdup(t->columns.items[grp_cols[k]].name),
                                              .type = t->columns.items[grp_cols[k]].type,
                                              .enum_type_name = NULL };
                }
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
            memset(gmin_cells, 0, agg_n * sizeof(struct cell));
            memset(gmax_cells, 0, agg_n * sizeof(struct cell));
        }

        for (size_t m = 0; m < matching_count; m++) {
            size_t ri = matching[m];
            /* check if this row belongs to the current group */
            int eq = 1;
            for (size_t k = 0; k < ngrp; k++) {
                if (grp_cols[k] == -2) {
                    if (!cell_equal_nullsafe(&grp_vals[m * ngrp + k],
                                    &grp_vals[group_starts[g] * ngrp + k])) {
                        eq = 0; break;
                    }
                } else {
                    if (!cell_equal_nullsafe(&t->rows.items[ri].cells.items[grp_cols[k]],
                                    &t->rows.items[first_ri].cells.items[grp_cols[k]])) {
                        eq = 0; break;
                    }
                }
            }
            if (!eq) continue;
            grp_count++;
            for (size_t a = 0; a < agg_n; a++) {
                struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
                int ac = gagg_cols[a];
                struct cell ev;
                struct cell *gc;
                if (ac == -2) {
                    ev = eval_expr(ae->expr_idx, arena, t, &t->rows.items[ri], NULL, NULL);
                    gc = &ev;
                } else if (ac >= 0) {
                    gc = &t->rows.items[ri].cells.items[ac];
                } else {
                    continue; /* COUNT(*) — no column value needed */
                }
                agg_accumulate_cell(gc, a, sums, gnonnull, gminmax_init, gmin_cells, gmax_cells);
            }
        }

        /* build result row: group key columns + aggregates (or reversed if agg_before_cols) */
        struct row dst = {0};
        da_init(&dst.cells);

        if (!s->agg_before_cols) {
            /* default: group key columns first */
            for (size_t k = 0; k < ngrp; k++) {
                struct cell gc;
                if (grp_cols[k] == -2) {
                    if (rb) cell_copy_bump(&gc, &grp_vals[group_starts[g] * ngrp + k], rb);
                    else    cell_copy(&gc, &grp_vals[group_starts[g] * ngrp + k]);
                } else {
                    if (rb) cell_copy_bump(&gc, &t->rows.items[first_ri].cells.items[grp_cols[k]], rb);
                    else    cell_copy(&gc, &t->rows.items[first_ri].cells.items[grp_cols[k]]);
                }
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
            int col_is_bigint = (ac_idx >= 0 &&
                                 t->columns.items[ac_idx].type == COLUMN_TYPE_BIGINT);
            int col_is_text = (ac_idx >= 0 &&
                               column_type_is_text(t->columns.items[ac_idx].type));
            /* expression-based aggregates: infer type from accumulated min_cells */
            if (ac_idx == -2 && gminmax_init[a]) {
                if (gmin_cells[a].type == COLUMN_TYPE_FLOAT || gmin_cells[a].type == COLUMN_TYPE_NUMERIC)
                    col_is_float = 1;
                else if (gmin_cells[a].type == COLUMN_TYPE_BIGINT)
                    col_is_bigint = 1;
                else if (column_type_is_text(gmin_cells[a].type))
                    col_is_text = 1;
            }
            switch (ae->func) {
                case AGG_COUNT:
                    c.type = COLUMN_TYPE_INT;
                    if (ae->has_distinct && gagg_cols[a] != -1) {
                        /* COUNT(DISTINCT col): count unique non-NULL values in this group */
                        int dc = 0;
                        struct cell *seen_d = gnonnull[a] > 0 ?
                            (struct cell *)bump_calloc(&arena->scratch, gnonnull[a], sizeof(struct cell)) : NULL;
                        for (size_t m2 = 0; m2 < matching_count; m2++) {
                            size_t ri2 = matching[m2];
                            int eq2 = 1;
                            for (size_t k2 = 0; k2 < ngrp; k2++) {
                                if (grp_cols[k2] == -2) {
                                    if (!cell_equal_nullsafe(&grp_vals[m2 * ngrp + k2],
                                                    &grp_vals[group_starts[g] * ngrp + k2])) { eq2 = 0; break; }
                                } else {
                                    if (!cell_equal_nullsafe(&t->rows.items[ri2].cells.items[grp_cols[k2]],
                                                    &t->rows.items[first_ri].cells.items[grp_cols[k2]])) { eq2 = 0; break; }
                                }
                            }
                            if (!eq2) continue;
                            struct cell *cv2;
                            struct cell ev2;
                            int ac2 = gagg_cols[a];
                            if (ac2 == -2) { ev2 = eval_expr(ae->expr_idx, arena, t, &t->rows.items[ri2], NULL, NULL); cv2 = &ev2; }
                            else { cv2 = &t->rows.items[ri2].cells.items[ac2]; }
                            if (cv2->is_null || (column_type_is_text(cv2->type) && !cv2->value.as_text)) continue;
                            int dup2 = 0;
                            for (int d2 = 0; d2 < dc; d2++) { if (cell_compare(cv2, &seen_d[d2]) == 0) { dup2 = 1; break; } }
                            if (!dup2 && seen_d) { seen_d[dc++] = *cv2; }
                        }
                        c.value.as_int = dc;
                    } else {
                        /* COUNT(*) counts all rows; COUNT(col/expr) counts non-NULL */
                        c.value.as_int = (gagg_cols[a] == -1) ? (int)grp_count : (int)gnonnull[a];
                    }
                    break;
                case AGG_SUM:
                case AGG_AVG:
                case AGG_MIN:
                case AGG_MAX:
                    c = agg_build_result_cell(ae, a, sums, gnonnull, gminmax_init,
                                              gmin_cells, gmax_cells, col_is_float,
                                              col_is_bigint, col_is_text, rb);
                    break;
                case AGG_STRING_AGG:
                case AGG_ARRAY_AGG: {
                    c.type = COLUMN_TYPE_TEXT;
                    if (gnonnull[a] == 0 && ae->func == AGG_STRING_AGG) {
                        c.is_null = 1;
                        c.value.as_text = NULL;
                    } else {
                        char *buf = bump_alloc(&arena->scratch, 4096);
                        size_t buf_len = 0, buf_cap = 4096;
                        int is_array = (ae->func == AGG_ARRAY_AGG);
                        if (is_array) { buf[buf_len++] = '{'; }
                        int first = 1;
                        for (size_t m = 0; m < matching_count; m++) {
                            size_t ri = matching[m];
                            int eq = 1;
                            for (size_t k = 0; k < ngrp; k++) {
                                if (!cell_equal_nullsafe(&t->rows.items[ri].cells.items[grp_cols[k]],
                                                &t->rows.items[first_ri].cells.items[grp_cols[k]])) {
                                    eq = 0; break;
                                }
                            }
                            if (!eq) continue;
                            struct cell ev;
                            struct cell *cv;
                            if (ac_idx == -2) {
                                ev = eval_expr(ae->expr_idx, arena, t, &t->rows.items[ri], NULL, NULL);
                                cv = &ev;
                            } else if (ac_idx >= 0) {
                                cv = &t->rows.items[ri].cells.items[ac_idx];
                            } else {
                                continue;
                            }
                            int is_null_val = cv->is_null || (column_type_is_text(cv->type) && !cv->value.as_text);
                            if (is_null_val && !is_array) continue;
                            char tmp[128];
                            const char *val_str = is_null_val ? "NULL" : cell_to_text_buf(cv, tmp, sizeof(tmp));
                            if (!val_str) continue;
                            size_t val_len = strlen(val_str);
                            const char *sep = NULL;
                            size_t sep_len = 0;
                            if (!first) {
                                if (is_array) { sep = ","; sep_len = 1; }
                                else if (ae->separator.len > 0) { sep = ae->separator.data; sep_len = ae->separator.len; }
                            }
                            size_t needed = buf_len + (sep ? sep_len : 0) + val_len + 2;
                            while (needed > buf_cap) {
                                buf_cap *= 2;
                                char *nb = bump_alloc(&arena->scratch, buf_cap);
                                memcpy(nb, buf, buf_len);
                                buf = nb;
                            }
                            if (sep) { memcpy(buf + buf_len, sep, sep_len); buf_len += sep_len; }
                            memcpy(buf + buf_len, val_str, val_len); buf_len += val_len;
                            first = 0;
                        }
                        if (is_array) { buf[buf_len++] = '}'; }
                        buf[buf_len] = '\0';
                        if (is_array && buf_len == 2) {
                            c.is_null = 1;
                            c.value.as_text = NULL;
                        } else {
                            if (rb) c.value.as_text = bump_strdup(rb, buf);
                            else    c.value.as_text = strdup(buf);
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
                if (grp_cols[k] == -2) {
                    if (rb) cell_copy_bump(&gc, &grp_vals[group_starts[g] * ngrp + k], rb);
                    else    cell_copy(&gc, &grp_vals[group_starts[g] * ngrp + k]);
                } else {
                    if (rb) cell_copy_bump(&gc, &t->rows.items[first_ri].cells.items[grp_cols[k]], rb);
                    else    cell_copy(&gc, &t->rows.items[first_ri].cells.items[grp_cols[k]]);
                }
                da_push(&dst.cells, gc);
            }
        }

        /* If parsed_columns with expr_idx exist (e.g. CASE WHEN SUM(x)>50 ...),
         * rebuild the row by evaluating each expression against a temp table
         * containing only this group's rows so inline aggregates compute correctly.
         * Only enter this path when a parsed column has a complex expression
         * (CASE WHEN, BINARY_OP, etc.) — simple column refs and direct aggregate
         * calls are already represented in the raw group-key + aggregate row. */
        int need_expr_rebuild = 0;
        if (s->parsed_columns_count > 0) {
            for (uint32_t pc = 0; pc < s->parsed_columns_count; pc++) {
                struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + pc];
                if (sc->expr_idx != IDX_NONE) {
                    enum expr_type et = EXPR(arena, sc->expr_idx).type;
                    if (et == EXPR_CASE_WHEN) {
                        need_expr_rebuild = 1;
                        break;
                    }
                }
            }
        }
        if (need_expr_rebuild) {
            /* Build temp table with only this group's rows */
            struct table grp_t = *t;
            size_t grp_row_count = 0;
            for (size_t m2 = 0; m2 < matching_count; m2++) {
                size_t ri2 = matching[m2];
                int eq2 = 1;
                for (size_t k2 = 0; k2 < ngrp; k2++) {
                    if (grp_cols[k2] == -2) {
                        if (!cell_equal_nullsafe(&grp_vals[m2 * ngrp + k2],
                                        &grp_vals[group_starts[g] * ngrp + k2])) { eq2 = 0; break; }
                    } else {
                        if (!cell_equal_nullsafe(&t->rows.items[ri2].cells.items[grp_cols[k2]],
                                        &t->rows.items[first_ri].cells.items[grp_cols[k2]])) { eq2 = 0; break; }
                    }
                }
                if (eq2) grp_row_count++;
            }
            struct row *grp_row_arr = bump_alloc(&arena->scratch, grp_row_count * sizeof(struct row));
            size_t gi = 0;
            for (size_t m2 = 0; m2 < matching_count; m2++) {
                size_t ri2 = matching[m2];
                int eq2 = 1;
                for (size_t k2 = 0; k2 < ngrp; k2++) {
                    if (grp_cols[k2] == -2) {
                        if (!cell_equal_nullsafe(&grp_vals[m2 * ngrp + k2],
                                        &grp_vals[group_starts[g] * ngrp + k2])) { eq2 = 0; break; }
                    } else {
                        if (!cell_equal_nullsafe(&t->rows.items[ri2].cells.items[grp_cols[k2]],
                                        &t->rows.items[first_ri].cells.items[grp_cols[k2]])) { eq2 = 0; break; }
                    }
                }
                if (eq2) grp_row_arr[gi++] = t->rows.items[ri2];
            }
            grp_t.rows.items = grp_row_arr;
            grp_t.rows.count = grp_row_count;
            /* Save aggregate cells from original dst before rebuilding.
             * Layout of dst: [grp0..grpN, agg0..aggN] (or reversed if agg_before_cols).
             * Aggregate placeholders in parsed_columns (expr_idx==IDX_NONE) map
             * sequentially to the aggregate cells. */
            size_t agg_base = s->agg_before_cols ? 0 : ngrp;
            struct cell *saved_agg = NULL;
            if (agg_n > 0) {
                saved_agg = (struct cell *)bump_alloc(&arena->scratch, agg_n * sizeof(struct cell));
                for (size_t a = 0; a < agg_n; a++)
                    saved_agg[a] = dst.cells.items[agg_base + a];
            }
            /* Rebuild dst from parsed_columns */
            if (rb) da_free(&dst.cells);
            else    row_free(&dst);
            dst = (struct row){0};
            da_init(&dst.cells);
            size_t agg_placeholder = 0;
            for (uint32_t pc = 0; pc < s->parsed_columns_count; pc++) {
                struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + pc];
                if (sc->expr_idx != IDX_NONE) {
                    struct cell c = eval_expr(sc->expr_idx, arena, &grp_t, &grp_t.rows.items[0], NULL, rb);
                    da_push(&dst.cells, c);
                } else if (saved_agg && agg_placeholder < agg_n) {
                    /* Aggregate placeholder — restore saved aggregate value */
                    da_push(&dst.cells, saved_agg[agg_placeholder++]);
                } else {
                    struct cell nc = { .type = COLUMN_TYPE_TEXT, .is_null = 1 };
                    da_push(&dst.cells, nc);
                }
            }
        }

        /* HAVING filter */
        if (has_having_t) {
            int passes = eval_condition(s->having_cond, arena, &dst, &having_t, NULL);
            if (!passes) {
                if (rb) da_free(&dst.cells);
                else    row_free(&dst);
                continue;
            }
        }

        rows_push(result, dst);
    }

    /* matching and group_starts are scratch-allocated — no free needed */
    /* free strdup'd text inside grp_vals cells (array itself is bump-allocated).
     * Only needed when rb is NULL (heap-allocated text); when rb is set,
     * text lives in the bump arena and is freed by bump_reset. */
    if (grp_vals && !rb) {
        for (size_t m = 0; m < matching_count; m++) {
            for (size_t k = 0; k < ngrp; k++) {
                struct cell *gv = &grp_vals[m * ngrp + k];
                if (column_type_is_text(gv->type) && gv->value.as_text)
                    free((char *)gv->value.as_text);
            }
        }
    }
    if (has_having_t) {
        for (size_t i = 0; i < having_t.columns.count; i++)
            column_free(&having_t.columns.items[i]);
        da_free(&having_t.columns);
    }

    /* ORDER BY on grouped results (multi-column) */
    if (s->has_order_by && s->order_by_count > 0 && result->count > 1) {
        int ord_res[32];
        int ord_descs[32];
        int ord_nf[32];
        size_t nord = s->order_by_count < 32 ? s->order_by_count : 32;
        for (size_t k = 0; k < nord; k++) {
            struct order_by_item *obi = &arena->order_items.items[s->order_by_start + k];
            ord_res[k] = grp_find_result_col(t, grp_cols, ngrp, s, arena,
                                             obi->column);
            ord_descs[k] = obi->desc;
            ord_nf[k] = obi->nulls_first;
        }
        _sort_ctx = (struct sort_ctx){ .cols = ord_res, .descs = ord_descs, .nulls_first = ord_nf, .ncols = nord };
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
        /* free discarded rows: bump text must not be free'd */
        int owns = result->arena_owns_text;
        if (rb) {
            for (size_t i = 0; i < result->count; i++)
                da_free(&result->data[i].cells);
            free(result->data);
            result->data = NULL;
            result->count = 0;
            result->capacity = 0;
        } else {
            rows_free(result);
        }
        *result = trimmed;
        result->arena_owns_text = owns;
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
        return query_window(t, s, arena, result, rb);

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
                    query_aggregate(t, s, arena, &sub, NULL);
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
                                if (rb) cell_copy_bump(&dup, &sub.data[r].cells.items[ci], rb);
                                else    cell_copy(&dup, &sub.data[r].cells.items[ci]);
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
                    query_group_by(t, s, arena, &sub, NULL);
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
                                        if (rb) cell_copy_bump(&dup, &sub.data[r].cells.items[sub_grp_i], rb);
                                        else    cell_copy(&dup, &sub.data[r].cells.items[sub_grp_i]);
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
                                if (rb) cell_copy_bump(&dup, &sub.data[r].cells.items[ci], rb);
                                else    cell_copy(&dup, &sub.data[r].cells.items[ci]);
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
        return query_group_by(t, s, arena, result, rb);
    }

    /* dispatch to aggregate path if aggregates are present */
    if (s->aggregates_count > 0)
        return query_aggregate(t, s, arena, result, rb);

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

    /* try block-oriented plan executor for simple queries
     * (skip if has_set_op — db_exec handles set operations separately) */
    if (!s->has_set_op) {
        struct plan_result pr = plan_build_select(t, s, arena, db);
        if (pr.status == PLAN_OK) {
            struct plan_exec_ctx ctx;
            plan_exec_init(&ctx, arena, db, pr.node);
            return plan_exec_to_rows(&ctx, pr.node, result, rb);
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
    int result_ord_nf[32];
    size_t result_nord = 0;
    if (s->has_order_by && s->order_by_count > 0) {
        /* resolve column indices for all ORDER BY items */
        int ord_cols[32];
        int ord_descs[32];
        int ord_nf[32];
        size_t nord = s->order_by_count < 32 ? s->order_by_count : 32;
        int all_resolved = 1;
        for (size_t k = 0; k < nord; k++) {
            struct order_by_item *obi = &arena->order_items.items[s->order_by_start + k];
            ord_nf[k] = obi->nulls_first;
            /* expression-based ORDER BY: find matching output column */
            if (obi->expr_idx != IDX_NONE) {
                ord_cols[k] = -1;
                /* try to find a SELECT column with matching alias or expression */
                if (s->parsed_columns_count > 0) {
                    for (uint32_t pc = 0; pc < s->parsed_columns_count; pc++) {
                        struct select_column *scp = &arena->select_cols.items[s->parsed_columns_start + pc];
                        if (scp->expr_idx != IDX_NONE &&
                            scp->expr_idx == obi->expr_idx) {
                            result_ord_cols[k] = (int)pc;
                            result_ord_descs[k] = obi->desc;
                            result_ord_nf[k] = obi->nulls_first;
                            order_on_result = 1;
                            ord_cols[k] = -2;
                            break;
                        }
                    }
                }
                /* if not found by expr match, evaluate expression per row */
                if (ord_cols[k] == -1) {
                    /* compute expression values for all matching rows and add as temp column */
                    order_on_result = 1;
                    ord_cols[k] = -3; /* mark as expr-eval needed */
                    result_ord_descs[k] = obi->desc;
                    result_ord_nf[k] = obi->nulls_first;
                }
                ord_descs[k] = obi->desc;
                continue;
            }
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
                        result_ord_nf[k] = obi->nulls_first;
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
                if (ord_cols[k] == -2) continue; /* already resolved */
                if (ord_cols[k] == -3) {
                    /* expression-based ORDER BY — find matching output column by alias */
                    result_ord_cols[k] = -1;
                    result_ord_descs[k] = ord_descs[k];
                    result_ord_nf[k] = ord_nf[k];
                    continue;
                }
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
                result_ord_nf[k] = ord_nf[k];
            }
        } else if (all_resolved) {
            _sort_ctx = (struct sort_ctx){ .cols = ord_cols, .descs = ord_descs,
                                           .nulls_first = ord_nf, .ncols = nord, .table = t };
            qsort(match_items, match_count, sizeof(size_t), cmp_indices_multi);
        }
    }

    /* project into result rows */
    struct rows tmp = {0};
    for (size_t i = 0; i < match_count; i++) {
        emit_row(t, s, arena, &t->rows.items[match_items[i]], &tmp, select_all, db, rb);
    }

    /* sort projected result rows if ORDER BY references a SELECT alias or expression */
    if (order_on_result && result_nord > 0 && tmp.count > 1) {
        /* for expression-based ORDER BY with unresolved columns, evaluate and append temp col */
        int added_temp_cols = 0;
        for (size_t k = 0; k < result_nord; k++) {
            if (result_ord_cols[k] == -1) {
                struct order_by_item *obi = &arena->order_items.items[s->order_by_start + k];
                if (obi->expr_idx != IDX_NONE) {
                    /* append expression value as temp column to each result row */
                    int temp_col = (int)(tmp.count > 0 ? tmp.data[0].cells.count : 0);
                    for (size_t ri = 0; ri < match_count && ri < tmp.count; ri++) {
                        struct cell val = eval_expr(obi->expr_idx, arena, t,
                                                    &t->rows.items[match_items[ri]], db, rb);
                        da_push(&tmp.data[ri].cells, val);
                    }
                    result_ord_cols[k] = temp_col;
                    added_temp_cols++;
                }
            }
        }
        _sort_ctx = (struct sort_ctx){ .cols = result_ord_cols, .descs = result_ord_descs,
                                       .nulls_first = result_ord_nf, .ncols = result_nord };
        qsort(tmp.data, tmp.count, sizeof(struct row), cmp_rows_multi);
        /* remove temp columns */
        if (added_temp_cols > 0) {
            for (size_t ri = 0; ri < tmp.count; ri++) {
                for (int tc = 0; tc < added_temp_cols; tc++) {
                    if (tmp.data[ri].cells.count > 0)
                        tmp.data[ri].cells.count--;
                }
            }
        }
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
                               struct rows *result, struct bump_alloc *rb)
{
    struct row ret = {0};
    da_init(&ret.cells);
    if (return_all) {
        for (size_t c = 0; c < src->cells.count; c++) {
            struct cell cp;
            if (rb) cell_copy_bump(&cp, &src->cells.items[c], rb);
            else    cell_copy(&cp, &src->cells.items[c]);
            da_push(&ret.cells, cp);
        }
    } else {
        sv cols = returning_columns;
        while (cols.len > 0) {
            size_t end = 0;
            int depth = 0;
            while (end < cols.len) {
                if (cols.data[end] == '(') depth++;
                else if (cols.data[end] == ')') depth--;
                else if (cols.data[end] == ',' && depth == 0) break;
                end++;
            }
            sv one = sv_trim(sv_from(cols.data, end));
            /* strip optional AS alias */
            sv expr_sv = one;
            for (size_t p = 0; p + 2 < one.len; p++) {
                if ((one.data[p] == ' ' || one.data[p] == '\t') &&
                    (one.data[p+1] == 'a' || one.data[p+1] == 'A') &&
                    (one.data[p+2] == 's' || one.data[p+2] == 'S') &&
                    (p + 3 >= one.len || one.data[p+3] == ' ' || one.data[p+3] == '\t')) {
                    expr_sv = sv_trim(sv_from(one.data, p));
                    break;
                }
            }
            /* try column name match first */
            int found = 0;
            for (size_t j = 0; j < t->columns.count; j++) {
                if (sv_eq_cstr(expr_sv, t->columns.items[j].name)) {
                    struct cell cp;
                    if (rb) cell_copy_bump(&cp, &src->cells.items[j], rb);
                    else    cell_copy(&cp, &src->cells.items[j]);
                    da_push(&ret.cells, cp);
                    found = 1;
                    break;
                }
            }
            if (!found) {
                /* try parsing as expression and evaluating against the row */
                char ebuf[512];
                size_t elen = expr_sv.len < sizeof(ebuf)-1 ? expr_sv.len : sizeof(ebuf)-1;
                memcpy(ebuf, expr_sv.data, elen);
                ebuf[elen] = '\0';
                char wrap[600];
                snprintf(wrap, sizeof(wrap), "SELECT %s FROM x", ebuf);
                struct query eq = {0};
                if (query_parse(wrap, &eq) == 0 && eq.select.parsed_columns_count > 0) {
                    struct select_column *sc = &eq.arena.select_cols.items[eq.select.parsed_columns_start];
                    if (sc->expr_idx != IDX_NONE) {
                        struct cell c = eval_expr(sc->expr_idx, &eq.arena, t, src, NULL, rb);
                        da_push(&ret.cells, c);
                        found = 1;
                    }
                }
                if (!found) {
                    struct cell null_cell = { .type = COLUMN_TYPE_TEXT, .is_null = 1 };
                    da_push(&ret.cells, null_cell);
                }
                query_free(&eq);
            }
            if (end < cols.len) end++;
            cols = sv_from(cols.data + end, cols.len - end);
        }
    }
    rows_push(result, ret);
}

static int fk_check_value_exists(struct database *db, const char *ref_table_name,
                                  const char *ref_col_name, const struct cell *value);
static int fk_enforce_delete(struct database *db, struct table *parent_t,
                              struct row *deleted_row, struct query_arena *arena);
static int fk_enforce_update(struct database *db, struct table *parent_t,
                              const struct cell *old_val, const struct cell *new_val,
                              const char *parent_col_name, struct query_arena *arena);

static int query_delete_exec(struct table *t, struct query_delete *d, struct query_arena *arena, struct rows *result, struct database *db, struct bump_alloc *rb)
{
    int has_ret = (d->has_returning && d->returning_columns.len > 0);
    int return_all = has_ret && sv_eq_cstr(d->returning_columns, "*");
    size_t deleted = 0;
    for (size_t i = 0; i < t->rows.count; ) {
        if (row_matches(t, &d->where, arena, &t->rows.items[i], NULL)) {
            /* enforce FK constraints before deleting */
            if (fk_enforce_delete(db, t, &t->rows.items[i], arena) != 0)
                return -1;
            /* capture row for RETURNING before freeing */
            if (has_ret && result)
                emit_returning_row(t, &t->rows.items[i], d->returning_columns, return_all, result, rb);
            row_free(&t->rows.items[i]);
            for (size_t j = i; j + 1 < t->rows.count; j++)
                t->rows.items[j] = t->rows.items[j + 1];
            t->rows.count--;
            deleted++;
            t->generation++;
            db->total_generation++;
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

static int check_constraints_ok(struct table *t, struct row *row, struct query_arena *arena, struct database *db);

static int query_update_exec(struct table *t, struct query_update *u, struct query_arena *arena, struct rows *result, struct database *db, struct bump_alloc *rb)
{
    int has_ret = (u->has_returning && u->returning_columns.len > 0);
    int return_all = has_ret && sv_eq_cstr(u->returning_columns, "*");
    size_t updated = 0;

    /* ---- Try index-accelerated WHERE lookup ---- */
    size_t *idx_row_ids = NULL;
    size_t idx_row_count = 0;
    int use_index_scan = 0;

    if (u->where.has_where) {
        sv where_col = {0};
        struct cell where_val = {0};
        int have_simple_eq = 0;

        if (u->where.where_cond == IDX_NONE && u->where.where_column.len > 0) {
            where_col = u->where.where_column;
            where_val = u->where.where_value;
            have_simple_eq = 1;
        } else if (u->where.where_cond != IDX_NONE) {
            struct condition *cond = &arena->conditions.items[u->where.where_cond];
            if (cond->type == COND_COMPARE && cond->op == CMP_EQ &&
                cond->column.len > 0 && cond->lhs_expr == IDX_NONE &&
                cond->rhs_column.len == 0 && cond->scalar_subquery_sql == IDX_NONE) {
                where_col = cond->column;
                where_val = cond->value;
                have_simple_eq = 1;
            }
        }

        if (have_simple_eq) {
            int wcol = table_find_column_sv(t, where_col);
            if (wcol >= 0) {
                for (size_t ix = 0; ix < t->indexes.count; ix++) {
                    if (t->indexes.items[ix].column_idx == wcol) {
                        index_lookup(&t->indexes.items[ix], &where_val,
                                     &idx_row_ids, &idx_row_count);
                        use_index_scan = 1;
                        break;
                    }
                }
            }
        }
    }

    size_t scan_count = use_index_scan ? idx_row_count : t->rows.count;
    for (size_t si = 0; si < scan_count; si++) {
        size_t i = use_index_scan ? idx_row_ids[si] : si;
        if (i >= t->rows.count) continue;
        if (!use_index_scan && !row_matches(t, &u->where, arena, &t->rows.items[i], db))
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
        /* enforce FK constraints before applying new values */
        for (size_t sc = 0; sc < nsc; sc++) {
            if (col_idxs[sc] < 0) continue;
            int ci = col_idxs[sc];
            /* referencing side: if this column has an FK, validate new value exists */
            if (t->columns.items[ci].fk_table) {
                if (!fk_check_value_exists(db, t->columns.items[ci].fk_table,
                                            t->columns.items[ci].fk_column, &new_vals[sc])) {
                    arena_set_error(arena, "23503",
                        "insert or update on table '%s' violates foreign key constraint on column '%s'",
                        t->name, t->columns.items[ci].name);
                    for (size_t k = 0; k < nsc; k++) {
                        if (column_type_is_text(new_vals[k].type) && new_vals[k].value.as_text)
                            free(new_vals[k].value.as_text);
                    }
                    return -1;
                }
            }
            /* referenced side: if another table references this column, enforce FK action */
            struct cell *old_val = &t->rows.items[i].cells.items[ci];
            if (fk_enforce_update(db, t, old_val, &new_vals[sc],
                                   t->columns.items[ci].name, arena) != 0) {
                for (size_t k = 0; k < nsc; k++) {
                    if (column_type_is_text(new_vals[k].type) && new_vals[k].value.as_text)
                        free(new_vals[k].value.as_text);
                }
                return -1;
            }
        }
        /* incremental index update: remove old keys, insert new keys */
        for (size_t ix = 0; ix < t->indexes.count; ix++) {
            struct index *idx = &t->indexes.items[ix];
            int ic = idx->column_idx;
            /* check if this indexed column is being updated */
            for (uint32_t sc = 0; sc < nsc; sc++) {
                if (col_idxs[sc] == ic) {
                    index_remove(idx, &t->rows.items[i].cells.items[ic], i);
                    index_insert(idx, &new_vals[sc], i);
                    break;
                }
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
        /* enforce CHECK constraints on the updated row */
        if (check_constraints_ok(t, &t->rows.items[i], arena, db) != 0)
            return -1;
        /* capture row for RETURNING after SET */
        if (has_ret && result)
            emit_returning_row(t, &t->rows.items[i], u->returning_columns, return_all, result, rb);
    }
    /* bump generation (no full index rebuild needed — indexes updated incrementally) */
    if (updated > 0) {
        t->generation++;
        db->total_generation++;
        /* patch scan cache in-place for each updated row (avoids full rebuild) */
        if (t->scan_cache.col_data && t->scan_cache.nrows == t->rows.count) {
            size_t scan2_count = use_index_scan ? idx_row_count : t->rows.count;
            for (size_t si2 = 0; si2 < scan2_count; si2++) {
                size_t ri = use_index_scan ? idx_row_ids[si2] : si2;
                if (ri < t->rows.count)
                    scan_cache_update_row(t, ri);
            }
        }
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

/* --- Foreign key helpers --- */

/* Check if a value exists in the referenced table's column. Returns 1 if found. */
static int fk_check_value_exists(struct database *db, const char *ref_table_name,
                                  const char *ref_col_name, const struct cell *value)
{
    if (!ref_table_name || !ref_col_name) return 1; /* no FK → ok */
    if (value->is_null) return 1; /* NULLs bypass FK check */
    struct table *ref_t = db_find_table(db, ref_table_name);
    if (!ref_t) return 0;
    int col_idx = -1;
    for (size_t c = 0; c < ref_t->columns.count; c++) {
        if (strcmp(ref_t->columns.items[c].name, ref_col_name) == 0) {
            col_idx = (int)c;
            break;
        }
    }
    if (col_idx < 0) return 0;
    for (size_t r = 0; r < ref_t->rows.count; r++) {
        if (cell_compare(value, &ref_t->rows.items[r].cells.items[col_idx]) == 0)
            return 1;
    }
    return 0;
}

/* Apply FK action on DELETE: scan all tables for columns referencing this table.
 * For each matching referencing row, apply the fk_on_delete action.
 * Returns 0 on success, -1 on error (sets arena error). */
static int fk_enforce_delete(struct database *db, struct table *parent_t,
                              struct row *deleted_row, struct query_arena *arena)
{
    for (size_t ti = 0; ti < db->tables.count; ti++) {
        struct table *child_t = &db->tables.items[ti];
        for (size_t ci = 0; ci < child_t->columns.count; ci++) {
            struct column *col = &child_t->columns.items[ci];
            if (!col->fk_table || strcmp(col->fk_table, parent_t->name) != 0)
                continue;
            /* Find the referenced column index in parent */
            int parent_col = -1;
            for (size_t pc = 0; pc < parent_t->columns.count; pc++) {
                if (col->fk_column && strcmp(parent_t->columns.items[pc].name, col->fk_column) == 0) {
                    parent_col = (int)pc;
                    break;
                }
            }
            if (parent_col < 0) continue;
            struct cell *parent_val = &deleted_row->cells.items[parent_col];
            if (parent_val->is_null) continue;

            enum fk_action action = col->fk_on_delete;

            /* Scan child rows for matches */
            for (size_t r = 0; r < child_t->rows.count; ) {
                struct cell *child_val = &child_t->rows.items[r].cells.items[ci];
                if (child_val->is_null || cell_compare(parent_val, child_val) != 0) {
                    r++;
                    continue;
                }
                /* Match found — apply action */
                switch (action) {
                    case FK_NO_ACTION:
                    case FK_RESTRICT:
                        arena_set_error(arena, "23503",
                            "update or delete on table '%s' violates foreign key constraint on table '%s'",
                            parent_t->name, child_t->name);
                        return -1;
                    case FK_CASCADE:
                        row_free(&child_t->rows.items[r]);
                        memmove(&child_t->rows.items[r],
                                &child_t->rows.items[r + 1],
                                (child_t->rows.count - r - 1) * sizeof(struct row));
                        child_t->rows.count--;
                        child_t->generation++;
                        db->total_generation++;
                        continue; /* don't increment r */
                    case FK_SET_NULL:
                        cell_free_text(child_val);
                        memset(&child_val->value, 0, sizeof(child_val->value));
                        child_val->is_null = 1;
                        child_t->generation++;
                        db->total_generation++;
                        r++;
                        break;
                    case FK_SET_DEFAULT:
                        cell_free_text(child_val);
                        if (col->has_default && col->default_value) {
                            cell_copy(child_val, col->default_value);
                        } else {
                            memset(&child_val->value, 0, sizeof(child_val->value));
                            child_val->is_null = 1;
                        }
                        child_t->generation++;
                        db->total_generation++;
                        r++;
                        break;
                }
            }
        }
    }
    return 0;
}

/* Apply FK action on UPDATE: scan all tables for columns referencing this table.
 * old_val is the value being changed, new_val is what it's changing to.
 * Returns 0 on success, -1 on error. */
static int fk_enforce_update(struct database *db, struct table *parent_t,
                              const struct cell *old_val, const struct cell *new_val,
                              const char *parent_col_name, struct query_arena *arena)
{
    if (old_val->is_null) return 0;
    if (cell_compare(old_val, new_val) == 0) return 0; /* value unchanged */

    for (size_t ti = 0; ti < db->tables.count; ti++) {
        struct table *child_t = &db->tables.items[ti];
        for (size_t ci = 0; ci < child_t->columns.count; ci++) {
            struct column *col = &child_t->columns.items[ci];
            if (!col->fk_table || strcmp(col->fk_table, parent_t->name) != 0)
                continue;
            if (!col->fk_column || strcmp(col->fk_column, parent_col_name) != 0)
                continue;

            enum fk_action action = col->fk_on_update;

            for (size_t r = 0; r < child_t->rows.count; r++) {
                struct cell *child_val = &child_t->rows.items[r].cells.items[ci];
                if (child_val->is_null || cell_compare(old_val, child_val) != 0)
                    continue;
                /* Match found — apply action */
                switch (action) {
                    case FK_NO_ACTION:
                    case FK_RESTRICT:
                        arena_set_error(arena, "23503",
                            "update or delete on table '%s' violates foreign key constraint on table '%s'",
                            parent_t->name, child_t->name);
                        return -1;
                    case FK_CASCADE:
                        cell_free_text(child_val);
                        cell_copy(child_val, new_val);
                        child_t->generation++;
                        db->total_generation++;
                        break;
                    case FK_SET_NULL:
                        cell_free_text(child_val);
                        memset(&child_val->value, 0, sizeof(child_val->value));
                        child_val->is_null = 1;
                        child_t->generation++;
                        db->total_generation++;
                        break;
                    case FK_SET_DEFAULT:
                        cell_free_text(child_val);
                        if (col->has_default && col->default_value) {
                            cell_copy(child_val, col->default_value);
                        } else {
                            memset(&child_val->value, 0, sizeof(child_val->value));
                            child_val->is_null = 1;
                        }
                        child_t->generation++;
                        db->total_generation++;
                        break;
                }
            }
        }
    }
    return 0;
}

/* Evaluate CHECK constraints for a row. Returns 0 if all pass, -1 on violation. */
static int check_constraints_ok(struct table *t, struct row *row, struct query_arena *arena, struct database *db)
{
    for (size_t i = 0; i < t->columns.count; i++) {
        if (!t->columns.items[i].check_expr_sql)
            continue;
        /* Parse the check expression as a WHERE condition:
         * "SELECT 1 FROM _dummy WHERE <check_expr>" */
        char sql_buf[512];
        snprintf(sql_buf, sizeof(sql_buf), "SELECT 1 FROM _dummy WHERE %s",
                 t->columns.items[i].check_expr_sql);
        struct query chk = {0};
        if (query_parse(sql_buf, &chk) != 0 || chk.query_type != QUERY_TYPE_SELECT) {
            query_free(&chk);
            continue; /* unparseable check — skip */
        }
        /* SQL standard: CHECK is satisfied if expression is TRUE or NULL.
         * For NULL columns, eval_condition returns false for comparisons,
         * but SQL says CHECK(x > 0) should pass when x IS NULL.
         * Check if any referenced column is NULL — if so, skip. */
        int any_null = 0;
        for (size_t ci = 0; ci < row->cells.count && ci < t->columns.count; ci++) {
            if (t->columns.items[ci].check_expr_sql == t->columns.items[i].check_expr_sql) {
                /* This is the column with the CHECK — check if its value is NULL */
                struct cell *c = &row->cells.items[ci];
                if (c->is_null || (column_type_is_text(c->type) && !c->value.as_text))
                    any_null = 1;
            }
        }
        if (any_null) {
            query_free(&chk);
            continue; /* NULL satisfies CHECK per SQL standard */
        }
        /* Evaluate the WHERE condition against the row */
        if (chk.select.where.has_where && chk.select.where.where_cond != IDX_NONE) {
            int passes = eval_condition(chk.select.where.where_cond, &chk.arena, row, t, db);
            if (!passes) {
                arena_set_error(arena, "23514", "CHECK constraint violated for column '%s'",
                                t->columns.items[i].name);
                query_free(&chk);
                return -1;
            }
        }
        query_free(&chk);
    }
    return 0;
}

static int query_insert_exec(struct table *t, struct query_insert *ins, struct query_arena *arena, struct rows *result, struct database *db, struct bump_alloc *rb)
{
    int has_returning = (ins->returning_columns.len > 0);
    int return_all = has_returning && sv_eq_cstr(ins->returning_columns, "*");

    for (uint32_t r = 0; r < ins->insert_rows_count; r++) {
        struct row *src = &arena->rows.items[ins->insert_rows_start + r];

        /* resolve expression sentinel cells (is_null==2) in source row */
        for (size_t ci = 0; ci < src->cells.count; ci++) {
            if (src->cells.items[ci].is_null == 2) {
                uint32_t ei = (uint32_t)src->cells.items[ci].value.as_int;
                struct cell val = eval_expr(ei, arena, t, NULL, db, &arena->bump);
                src->cells.items[ci] = val;
            }
        }

        struct row copy = {0};
        da_init(&copy.cells);

        if (ins->is_default_values) {
            /* DEFAULT VALUES: build full-width row from column defaults */
            for (size_t ci = 0; ci < t->columns.count; ci++) {
                struct column *col = &t->columns.items[ci];
                if (col->is_serial) {
                    struct cell sc = {0};
                    sc.type = COLUMN_TYPE_INT;
                    sc.value.as_int = col->serial_next++;
                    da_push(&copy.cells, sc);
                } else if (col->has_default && col->default_value) {
                    struct cell dup;
                    cell_copy(&dup, col->default_value);
                    da_push(&copy.cells, dup);
                } else {
                    struct cell null_cell = {0};
                    null_cell.type = col->type;
                    null_cell.is_null = 1;
                    da_push(&copy.cells, null_cell);
                }
            }
        } else if (ins->insert_columns_count > 0) {
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
        /* type coercion: convert TEXT cells to the column's temporal/UUID type */
        for (size_t i = 0; i < t->columns.count && i < copy.cells.count; i++) {
            struct cell *c = &copy.cells.items[i];
            enum column_type ct = t->columns.items[i].type;
            if (c->type == COLUMN_TYPE_TEXT && !c->is_null && c->value.as_text) {
                const char *s = c->value.as_text;
                switch (ct) {
                case COLUMN_TYPE_DATE: {
                    int32_t v = date_from_str(s);
                    free((char *)s);
                    c->type = ct;
                    c->value.as_date = v;
                    break;
                }
                case COLUMN_TYPE_TIME: {
                    int64_t v = time_from_str(s);
                    free((char *)s);
                    c->type = ct;
                    c->value.as_time = v;
                    break;
                }
                case COLUMN_TYPE_TIMESTAMP:
                case COLUMN_TYPE_TIMESTAMPTZ: {
                    int64_t v = timestamp_from_str(s);
                    free((char *)s);
                    c->type = ct;
                    c->value.as_timestamp = v;
                    break;
                }
                case COLUMN_TYPE_INTERVAL: {
                    struct interval v = interval_from_str(s);
                    free((char *)s);
                    c->type = ct;
                    c->value.as_interval = v;
                    break;
                }
                case COLUMN_TYPE_UUID:
                    c->type = ct;
                    break;
                case COLUMN_TYPE_SMALLINT: case COLUMN_TYPE_INT: case COLUMN_TYPE_BIGINT:
                case COLUMN_TYPE_FLOAT: case COLUMN_TYPE_NUMERIC: case COLUMN_TYPE_BOOLEAN:
                case COLUMN_TYPE_TEXT: case COLUMN_TYPE_ENUM:
                    break;
                }
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
                    } else if (t->columns.items[i].type == COLUMN_TYPE_SMALLINT) {
                        c->type = COLUMN_TYPE_SMALLINT;
                        c->value.as_smallint = (int16_t)val;
                    } else {
                        c->type = COLUMN_TYPE_INT;
                        c->value.as_int = (int)val;
                    }
                } else {
                    /* user provided a value — update serial_next if needed */
                    long long v = (c->type == COLUMN_TYPE_BIGINT) ? c->value.as_bigint :
                                  (c->type == COLUMN_TYPE_SMALLINT) ? (long long)c->value.as_smallint : c->value.as_int;
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
                    arena_set_error(arena, "23502", "NOT NULL constraint violated for column '%s'", t->columns.items[i].name);
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
                        arena_set_error(arena, "23505", "UNIQUE constraint violated for column '%s'", t->columns.items[i].name);
                        row_free(&copy);
                        return -1;
                    }
                }
            }
        }
        /* enforce SMALLINT range and coerce INT literals to SMALLINT */
        for (size_t i = 0; i < t->columns.count && i < copy.cells.count; i++) {
            if (t->columns.items[i].type == COLUMN_TYPE_SMALLINT) {
                struct cell *c = &copy.cells.items[i];
                if (c->is_null) continue;
                long long v;
                if (c->type == COLUMN_TYPE_SMALLINT) {
                    v = c->value.as_smallint;
                } else if (c->type == COLUMN_TYPE_INT) {
                    v = c->value.as_int;
                } else if (c->type == COLUMN_TYPE_BIGINT) {
                    v = c->value.as_bigint;
                } else if (c->type == COLUMN_TYPE_FLOAT || c->type == COLUMN_TYPE_NUMERIC) {
                    v = (long long)c->value.as_float;
                } else {
                    continue;
                }
                if (v < -32768 || v > 32767) {
                    arena_set_error(arena, "22003", "smallint out of range for column '%s'", t->columns.items[i].name);
                    row_free(&copy);
                    return -1;
                }
                c->type = COLUMN_TYPE_SMALLINT;
                c->value.as_smallint = (int16_t)v;
            }
        }
        /* enforce FK constraints on INSERT */
        for (size_t i = 0; i < t->columns.count && i < copy.cells.count; i++) {
            if (t->columns.items[i].fk_table) {
                struct cell *c = &copy.cells.items[i];
                if (!fk_check_value_exists(db, t->columns.items[i].fk_table,
                                            t->columns.items[i].fk_column, c)) {
                    arena_set_error(arena, "23503",
                        "insert or update on table '%s' violates foreign key constraint on column '%s'",
                        t->name, t->columns.items[i].name);
                    row_free(&copy);
                    return -1;
                }
            }
        }
        /* enforce CHECK constraints */
        if (check_constraints_ok(t, &copy, arena, db) != 0) {
            row_free(&copy);
            return -1;
        }
        da_push(&t->rows, copy);
        t->generation++;
        db->total_generation++;

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
            emit_returning_row(t, &t->rows.items[t->rows.count - 1], ins->returning_columns, return_all, result, rb);
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
        case QUERY_TYPE_TRUNCATE:
        case QUERY_TYPE_EXPLAIN:
        case QUERY_TYPE_COPY:
        case QUERY_TYPE_SET:
        case QUERY_TYPE_SHOW:
            return -1;
        case QUERY_TYPE_SELECT:
            return query_select_exec(t, &q->select, &q->arena, result, db, rb);
        case QUERY_TYPE_INSERT:
            return query_insert_exec(t, &q->insert, &q->arena, result, db, rb);
        case QUERY_TYPE_DELETE:
            return query_delete_exec(t, &q->del, &q->arena, result, db, rb);
        case QUERY_TYPE_UPDATE:
            return query_update_exec(t, &q->update, &q->arena, result, db, rb);
    }
    __builtin_unreachable();
}
