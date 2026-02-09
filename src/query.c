#include "query.h"
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

void condition_free(struct condition *c)
{
    if (!c) return;
    if (c->type == COND_AND || c->type == COND_OR) {
        condition_free(c->left);
        condition_free(c->right);
    } else if (c->type == COND_NOT) {
        condition_free(c->left);
    } else if (c->type == COND_COMPARE) {
        if (column_type_is_text(c->value.type) && c->value.value.as_text)
            free(c->value.value.as_text);
        /* free IN / NOT IN value list */
        for (size_t i = 0; i < c->in_values.count; i++) {
            if (column_type_is_text(c->in_values.items[i].type) &&
                c->in_values.items[i].value.as_text)
                free(c->in_values.items[i].value.as_text);
        }
        da_free(&c->in_values);
        /* free BETWEEN high value */
        if (column_type_is_text(c->between_high.type) && c->between_high.value.as_text)
            free(c->between_high.value.as_text);
        /* free unresolved subquery SQL */
        free(c->subquery_sql);
        free(c->scalar_subquery_sql);
    }
    free(c);
}

/* free_cell_text → use shared cell_free_text from row.h */

void query_free(struct query *q)
{
    /* where / having conditions */
    condition_free(q->where_cond);
    q->where_cond = NULL;
    condition_free(q->having_cond);
    q->having_cond = NULL;

    /* where_value is a shallow copy of where_cond->value — already freed above, do not double-free */

    /* insert_rows — insert_row is always an alias into this array */
    for (size_t i = 0; i < q->insert_rows.count; i++) {
        for (size_t j = 0; j < q->insert_rows.items[i].cells.count; j++)
            cell_free_text(&q->insert_rows.items[i].cells.items[j]);
        da_free(&q->insert_rows.items[i].cells);
    }
    da_free(&q->insert_rows);
    q->insert_row = NULL;

    /* create_columns (strdup'd names + default values) */
    for (size_t i = 0; i < q->create_columns.count; i++) {
        free(q->create_columns.items[i].name);
        free(q->create_columns.items[i].enum_type_name);
        if (q->create_columns.items[i].default_value) {
            cell_free_text(q->create_columns.items[i].default_value);
            free(q->create_columns.items[i].default_value);
        }
    }
    da_free(&q->create_columns);

    /* set_clauses (UPDATE SET) */
    for (size_t i = 0; i < q->set_clauses.count; i++)
        cell_free_text(&q->set_clauses.items[i].value);
    da_free(&q->set_clauses);

    /* enum_values */
    for (size_t i = 0; i < q->enum_values.count; i++)
        free(q->enum_values.items[i]);
    da_free(&q->enum_values);

    /* aggregates, select_exprs, joins, order_by_items, group_by_cols — no heap pointers inside */
    da_free(&q->aggregates);
    da_free(&q->select_exprs);
    da_free(&q->joins);
    da_free(&q->order_by_items);
    da_free(&q->group_by_cols);

    /* alter_new_col (ADD COLUMN) */
    free(q->alter_new_col.name);
    free(q->alter_new_col.enum_type_name);
    if (q->alter_new_col.default_value) {
        cell_free_text(q->alter_new_col.default_value);
        free(q->alter_new_col.default_value);
    }

    free(q->set_rhs_sql);
    free(q->set_order_by);
    free(q->cte_name);
    free(q->cte_sql);
    free(q->insert_select_sql);
    free(q->from_subquery_sql);

    /* multiple CTEs */
    for (size_t i = 0; i < q->ctes.count; i++) {
        free(q->ctes.items[i].name);
        free(q->ctes.items[i].sql);
    }
    da_free(&q->ctes);

    /* lateral subquery SQL in joins */
    for (size_t i = 0; i < q->joins.count; i++) {
        free(q->joins.items[i].lateral_subquery_sql);
    }
}

/* cell_cmp → use shared cell_compare from row.h (returns -2 for incompatible types) */

static int row_matches(struct table *t, struct query *q, struct row *row);
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
                int found = 0;
                for (size_t i = 0; i < cond->in_values.count; i++) {
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

// TODO: eval_coalesce does manual comma-splitting and type detection on raw sv;
// could reuse the lexer for tokenization and parse_literal_value for type inference
/* evaluate COALESCE(arg1, arg2, ...) — returns first non-NULL argument */
static struct cell eval_coalesce(sv expr, struct table *t, struct row *src)
{
    /* skip "COALESCE(" and find matching ")" */
    size_t start = 0;
    while (start < expr.len && expr.data[start] != '(') start++;
    start++; /* skip '(' */
    size_t end = expr.len;
    if (end > 0 && expr.data[end - 1] == ')') end--;

    sv args = sv_from(expr.data + start, end - start);

    /* split on commas (respecting parentheses) */
    int depth = 0;
    size_t arg_start = 0;
    for (size_t i = 0; i <= args.len; i++) {
        char c = (i < args.len) ? args.data[i] : '\0';
        if (c == '(') depth++;
        else if (c == ')') depth--;
        else if ((c == ',' && depth == 0) || i == args.len) {
            sv arg = sv_from(args.data + arg_start, i - arg_start);
            /* trim */
            while (arg.len > 0 && (arg.data[0] == ' ' || arg.data[0] == '\t'))
                { arg.data++; arg.len--; }
            while (arg.len > 0 && (arg.data[arg.len-1] == ' ' || arg.data[arg.len-1] == '\t'))
                arg.len--;

            /* check if it's a string literal */
            if (arg.len >= 2 && arg.data[0] == '\'') {
                struct cell c2 = {0};
                c2.type = COLUMN_TYPE_TEXT;
                c2.value.as_text = malloc(arg.len - 1);
                memcpy(c2.value.as_text, arg.data + 1, arg.len - 2);
                c2.value.as_text[arg.len - 2] = '\0';
                return c2;
            }

            /* check if it's a column reference */
            sv col = arg;
            for (size_t k = 0; k < col.len; k++) {
                if (col.data[k] == '.') { col = sv_from(col.data + k + 1, col.len - k - 1); break; }
            }
            for (size_t j = 0; j < t->columns.count; j++) {
                if (sv_eq_cstr(col, t->columns.items[j].name)) {
                    struct cell *sc = &src->cells.items[j];
                    if (!sc->is_null && !(column_type_is_text(sc->type) && !sc->value.as_text)) {
                        struct cell copy = { .type = sc->type };
                        if (column_type_is_text(sc->type) && sc->value.as_text)
                            copy.value.as_text = strdup(sc->value.as_text);
                        else
                            copy.value = sc->value;
                        return copy;
                    }
                    break; /* column found but is NULL, try next arg */
                }
            }

            /* try as number literal */
            if (arg.len > 0 && (isdigit((unsigned char)arg.data[0]) || arg.data[0] == '-')) {
                char buf[64];
                size_t n = arg.len < 63 ? arg.len : 63;
                memcpy(buf, arg.data, n); buf[n] = '\0';
                struct cell c2 = {0};
                if (strchr(buf, '.')) {
                    c2.type = COLUMN_TYPE_FLOAT;
                    c2.value.as_float = atof(buf);
                } else {
                    c2.type = COLUMN_TYPE_INT;
                    c2.value.as_int = atoi(buf);
                }
                return c2;
            }

            arg_start = i + 1;
        }
    }

    /* all NULL */
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

            /* evaluate condition: simple "col op value" */
            int cond_match = 0;
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

            if (cond_match) {
                /* return the THEN value */
                if (val_sv.len >= 2 && val_sv.data[0] == '\'') {
                    struct cell c2 = { .type = COLUMN_TYPE_TEXT };
                    c2.value.as_text = malloc(val_sv.len - 1);
                    memcpy(c2.value.as_text, val_sv.data + 1, val_sv.len - 2);
                    c2.value.as_text[val_sv.len - 2] = '\0';
                    return c2;
                }
                double v = resolve_operand(val_sv, t, src);
                struct cell c2 = {0};
                if (v == (double)(int)v) { c2.type = COLUMN_TYPE_INT; c2.value.as_int = (int)v; }
                else { c2.type = COLUMN_TYPE_FLOAT; c2.value.as_float = v; }
                return c2;
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

            if (else_val.len >= 2 && else_val.data[0] == '\'') {
                struct cell c2 = { .type = COLUMN_TYPE_TEXT };
                c2.value.as_text = malloc(else_val.len - 1);
                memcpy(c2.value.as_text, else_val.data + 1, else_val.len - 2);
                c2.value.as_text[else_val.len - 2] = '\0';
                return c2;
            }
            double v = resolve_operand(else_val, t, src);
            struct cell c2 = {0};
            if (v == (double)(int)v) { c2.type = COLUMN_TYPE_INT; c2.value.as_int = (int)v; }
            else { c2.type = COLUMN_TYPE_FLOAT; c2.value.as_float = v; }
            return c2;
        }

        break;
    }

    struct cell null_cell = { .type = COLUMN_TYPE_TEXT, .is_null = 1 };
    return null_cell;
}

/* check if an sv segment contains an arithmetic operator */
static int has_arith_op(sv s)
{
    for (size_t i = 0; i < s.len; i++) {
        char c = s.data[i];
        if (c == '+' || c == '/' || c == '-') return 1;
        if (c == '*') {
            /* distinguish SELECT * from multiplication: * is arith only if not alone */
            if (s.len > 1) return 1;
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

    /* tokenize: split on +, -, *, / keeping operators */
    double vals[32];
    char ops[32];
    int nvals = 0, nops = 0;

    size_t start = 0;
    for (size_t i = 0; i <= expr.len && nvals < 32; i++) {
        char c = (i < expr.len) ? expr.data[i] : '\0';
        int is_op = (c == '+' || c == '-' || c == '/' || c == '*');
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
            vals[nvals++] = resolve_operand(operand, t, src);
            if (is_op && nops < 32) ops[nops++] = c;
            start = i + 1;
        }
    }

    /* evaluate: first pass for * and / */
    for (int i = 0; i < nops; i++) {
        if (ops[i] == '*' || ops[i] == '/') {
            if (ops[i] == '*') vals[i] = vals[i] * vals[i+1];
            else vals[i] = (vals[i+1] != 0.0) ? vals[i] / vals[i+1] : 0.0;
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

static void emit_row(struct table *t, struct query *q, struct row *src,
                     struct rows *result, int select_all)
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
        sv cols = q->columns;
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

            /* strip table prefix (e.g. t1.col -> col) */
            for (size_t k = 0; k < one.len; k++) {
                if (one.data[k] == '.') {
                    one = sv_from(one.data + k + 1, one.len - k - 1);
                    break;
                }
            }

            if (sv_starts_with_ci(one, "COALESCE")) {
                struct cell c = eval_coalesce(one, t, src);
                struct cell copy = { .type = c.type, .is_null = c.is_null };
                if (column_type_is_text(c.type) && c.value.as_text)
                    copy.value.as_text = strdup(c.value.as_text);
                else
                    copy.value = c.value;
                da_push(&dst.cells, copy);
            } else if (sv_starts_with_ci(one, "CASE")) {
                struct cell c = eval_case_when(one, t, src);
                struct cell copy = { .type = c.type, .is_null = c.is_null };
                if (column_type_is_text(c.type) && c.value.as_text)
                    copy.value.as_text = strdup(c.value.as_text);
                else
                    copy.value = c.value;
                da_push(&dst.cells, copy);
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

            if (end < cols.len) end++; /* skip comma */
            cols = sv_from(cols.data + end, cols.len - end);
        }
    }

    rows_push(result, dst);
}

static int query_aggregate(struct table *t, struct query *q, struct rows *result)
{
    /* find WHERE column index if applicable (legacy path) */
    int where_col = -1;
    if (q->has_where && !q->where_cond) {
        for (size_t j = 0; j < t->columns.count; j++) {
            if (sv_eq_cstr(q->where_column, t->columns.items[j].name)) {
                where_col = (int)j;
                break;
            }
        }
        if (where_col < 0) {
            fprintf(stderr, "WHERE column '" SV_FMT "' not found\n",
                    SV_ARG(q->where_column));
            return -1;
        }
    }

    /* resolve column index for each aggregate */
    int *agg_col = calloc(q->aggregates.count, sizeof(int));
    for (size_t a = 0; a < q->aggregates.count; a++) {
        if (sv_eq_cstr(q->aggregates.items[a].column, "*")) {
            agg_col[a] = -1; /* COUNT(*) doesn't need a column */
        } else {
            agg_col[a] = -1;
            for (size_t j = 0; j < t->columns.count; j++) {
                if (sv_eq_cstr(q->aggregates.items[a].column, t->columns.items[j].name)) {
                    agg_col[a] = (int)j;
                    break;
                }
            }
            if (agg_col[a] < 0) {
                fprintf(stderr, "aggregate column '" SV_FMT "' not found\n",
                        SV_ARG(q->aggregates.items[a].column));
                free(agg_col);
                return -1;
            }
        }
    }

    /* accumulate — single allocation for all aggregate arrays */
    size_t _nagg = q->aggregates.count;
    size_t _agg_alloc = _nagg * (3 * sizeof(double) + sizeof(int));
    char *_agg_buf = calloc(1, _agg_alloc ? _agg_alloc : 1);
    double *sums = (double *)_agg_buf;
    double *mins = sums + _nagg;
    double *maxs = mins + _nagg;
    int *minmax_init = (int *)(maxs + _nagg);
    size_t row_count = 0;

    for (size_t i = 0; i < t->rows.count; i++) {
        if (q->has_where) {
            if (q->where_cond) {
                if (!eval_condition(q->where_cond, &t->rows.items[i], t))
                    continue;
            } else if (where_col >= 0) {
                if (!cell_equal(&t->rows.items[i].cells.items[where_col],
                                &q->where_value))
                    continue;
            }
        }
        row_count++;
        for (size_t a = 0; a < q->aggregates.count; a++) {
            if (agg_col[a] < 0) continue;
            struct cell *c = &t->rows.items[i].cells.items[agg_col[a]];
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
    for (size_t a = 0; a < q->aggregates.count; a++) {
        struct cell c = {0};
        int col_is_float = (agg_col[a] >= 0 &&
                            t->columns.items[agg_col[a]].type == COLUMN_TYPE_FLOAT);
        switch (q->aggregates.items[a].func) {
            case AGG_COUNT:
                c.type = COLUMN_TYPE_INT;
                c.value.as_int = (int)row_count;
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
                c.value.as_float = row_count > 0 ? sums[a] / (double)row_count : 0.0;
                break;
            case AGG_MIN:
            case AGG_MAX: {
                double val = (q->aggregates.items[a].func == AGG_MIN) ? mins[a] : maxs[a];
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

static int query_window(struct table *t, struct query *q, struct rows *result)
{
    size_t nrows = t->rows.count;
    size_t nexprs = q->select_exprs.count;

    /* resolve column indices for plain columns and window args — single allocation */
    int *_win_buf = calloc(4 * nexprs + 1, sizeof(int));
    int *col_idx = _win_buf;
    int *part_idx = _win_buf + nexprs;
    int *ord_idx = _win_buf + 2 * nexprs;
    int *arg_idx = _win_buf + 3 * nexprs;

    for (size_t e = 0; e < nexprs; e++) {
        struct select_expr *se = &q->select_exprs.items[e];
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
        if (!row_matches(t, q, &t->rows.items[i]))
            continue;
        da_push(&match_idx, i);
    }
    size_t nmatch = match_idx.count;

    /* build sorted row index (by first ORDER BY we find, or original order) */
    struct sort_entry *sorted = calloc(nmatch, sizeof(struct sort_entry));
    int global_ord = -1;
    for (size_t e = 0; e < nexprs; e++) {
        if (q->select_exprs.items[e].kind == SEL_WINDOW && ord_idx[e] >= 0) {
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
            struct select_expr *se = &q->select_exprs.items[e];
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
                        if (!cell_equal(&src->cells.items[part_idx[e]],
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
                        /* RANK: count peers with strictly smaller order key + 1 */
                        if (se->win.has_order && ord_idx[e] >= 0) {
                            int rank = 1;
                            for (size_t rj = 0; rj < nmatch; rj++) {
                                size_t j = sorted[rj].idx;
                                if (se->win.has_partition && part_idx[e] >= 0) {
                                    if (!cell_equal(&src->cells.items[part_idx[e]],
                                                    &t->rows.items[j].cells.items[part_idx[e]]))
                                        continue;
                                }
                                if (cell_compare(&t->rows.items[j].cells.items[ord_idx[e]],
                                                 &src->cells.items[ord_idx[e]]) < 0)
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
    return 0;
}

/* helper: resolve a result column index by name in grouped output */
static int grp_find_result_col(struct table *t, int *grp_cols, size_t ngrp,
                               struct query *q, sv name)
{
    /* check group columns first */
    for (size_t k = 0; k < ngrp; k++) {
        if (grp_cols[k] >= 0 && sv_eq_cstr(name, t->columns.items[grp_cols[k]].name))
            return (int)k;
    }
    /* check aggregate names */
    for (size_t a = 0; a < q->aggregates.count; a++) {
        const char *agg_name = "?";
        switch (q->aggregates.items[a].func) {
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

static int query_group_by(struct table *t, struct query *q, struct rows *result)
{
    /* resolve GROUP BY column indices */
    size_t ngrp = q->group_by_cols.count;
    if (ngrp == 0) ngrp = 1; /* backward compat: single group_by_col */
    int grp_cols[32];
    if (q->group_by_cols.count > 0) {
        for (size_t k = 0; k < ngrp && k < 32; k++) {
            grp_cols[k] = table_find_column_sv(t, q->group_by_cols.items[k]);
            if (grp_cols[k] < 0) {
                fprintf(stderr, "GROUP BY column '" SV_FMT "' not found\n",
                        SV_ARG(q->group_by_cols.items[k]));
                return -1;
            }
        }
    } else {
        grp_cols[0] = table_find_column_sv(t, q->group_by_col);
        if (grp_cols[0] < 0) {
            fprintf(stderr, "GROUP BY column '" SV_FMT "' not found\n",
                    SV_ARG(q->group_by_col));
            return -1;
        }
    }
    if (ngrp > 32) ngrp = 32;

    /* collect matching rows */
    DYNAMIC_ARRAY(size_t) matching;
    da_init(&matching);
    for (size_t i = 0; i < t->rows.count; i++) {
        if (q->has_where && q->where_cond) {
            if (!eval_condition(q->where_cond, &t->rows.items[i], t))
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
                if (!cell_equal(&t->rows.items[ri].cells.items[grp_cols[k]],
                                &t->rows.items[gi].cells.items[grp_cols[k]])) {
                    eq = 0; break;
                }
            }
            if (eq) { found = 1; break; }
        }
        if (!found) da_push(&group_starts, m);
    }

    /* pre-allocate aggregate accumulators in a single allocation */
    size_t agg_n = q->aggregates.count;
    void *_grp_buf = NULL;
    double *sums = NULL, *gmins = NULL, *gmaxs = NULL;
    int *gminmax_init = NULL, *gagg_cols = NULL;
    if (agg_n > 0) {
        _grp_buf = malloc(3 * agg_n * sizeof(double) + 2 * agg_n * sizeof(int));
        sums          = (double *)_grp_buf;
        gmins         = sums + agg_n;
        gmaxs         = gmins + agg_n;
        gminmax_init  = (int *)(gmaxs + agg_n);
        gagg_cols     = gminmax_init + agg_n;
    }

    /* resolve aggregate column indices once */
    for (size_t a = 0; a < agg_n; a++) {
        if (sv_eq_cstr(q->aggregates.items[a].column, "*"))
            gagg_cols[a] = -1;
        else
            gagg_cols[a] = table_find_column_sv(t, q->aggregates.items[a].column);
    }

    /* build HAVING tmp_t once (columns don't change between groups) */
    struct table having_t = {0};
    int has_having_t = 0;
    if (q->has_having && q->having_cond) {
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
        for (size_t a = 0; a < q->aggregates.count; a++) {
            const char *agg_name = "?";
            switch (q->aggregates.items[a].func) {
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
            if (q->aggregates.items[a].func == AGG_AVG)
                ctype = COLUMN_TYPE_FLOAT;
            if (q->aggregates.items[a].func == AGG_COUNT)
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
        }

        for (size_t m = 0; m < matching.count; m++) {
            size_t ri = matching.items[m];
            /* check if this row belongs to the current group */
            int eq = 1;
            for (size_t k = 0; k < ngrp; k++) {
                if (!cell_equal(&t->rows.items[ri].cells.items[grp_cols[k]],
                                &t->rows.items[first_ri].cells.items[grp_cols[k]])) {
                    eq = 0; break;
                }
            }
            if (!eq) continue;
            grp_count++;
            for (size_t a = 0; a < q->aggregates.count; a++) {
                int ac = gagg_cols[a];
                if (ac < 0) continue;
                {
                    double v = cell_to_double(&t->rows.items[ri].cells.items[ac]);
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
        for (size_t a = 0; a < q->aggregates.count; a++) {
            struct cell c = {0};
            int ac_idx = gagg_cols[a];
            int col_is_float = (ac_idx >= 0 &&
                                t->columns.items[ac_idx].type == COLUMN_TYPE_FLOAT);
            switch (q->aggregates.items[a].func) {
                case AGG_COUNT:
                    c.type = COLUMN_TYPE_INT;
                    c.value.as_int = (int)grp_count;
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
                    c.value.as_float = grp_count > 0 ? sums[a] / (double)grp_count : 0.0;
                    break;
                case AGG_MIN:
                case AGG_MAX: {
                    double val = (q->aggregates.items[a].func == AGG_MIN) ? gmins[a] : gmaxs[a];
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
            int passes = eval_condition(q->having_cond, &dst, &having_t);
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
            free(having_t.columns.items[i].name);
        da_free(&having_t.columns);
    }

    /* ORDER BY on grouped results (multi-column) */
    if (q->has_order_by && q->order_by_items.count > 0 && result->count > 1) {
        int ord_res[32];
        int ord_descs[32];
        size_t nord = q->order_by_items.count < 32 ? q->order_by_items.count : 32;
        for (size_t k = 0; k < nord; k++) {
            ord_res[k] = grp_find_result_col(t, grp_cols, ngrp, q,
                                             q->order_by_items.items[k].column);
            ord_descs[k] = q->order_by_items.items[k].desc;
        }
        _sort_ctx = (struct sort_ctx){ .cols = ord_res, .descs = ord_descs, .ncols = nord };
        qsort(result->data, result->count, sizeof(struct row), cmp_rows_multi);
    }

    /* LIMIT / OFFSET on grouped results */
    if (q->has_offset || q->has_limit) {
        size_t start = q->has_offset ? (size_t)q->offset_count : 0;
        if (start > result->count) start = result->count;
        size_t end = result->count;
        if (q->has_limit) {
            size_t lim = (size_t)q->limit_count;
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

static int row_matches(struct table *t, struct query *q, struct row *row)
{
    if (!q->has_where) return 1;
    if (q->where_cond)
        return eval_condition(q->where_cond, row, t);
    /* legacy single-column = value */
    int where_col = -1;
    for (size_t j = 0; j < t->columns.count; j++) {
        if (sv_eq_cstr(q->where_column, t->columns.items[j].name)) {
            where_col = (int)j; break;
        }
    }
    if (where_col < 0) return 0;
    return cell_equal(&row->cells.items[where_col], &q->where_value);
}

static int query_select(struct table *t, struct query *q, struct rows *result)
{
    /* dispatch to window path if select_exprs are present */
    if (q->select_exprs.count > 0)
        return query_window(t, q, result);

    /* dispatch to GROUP BY path */
    if (q->has_group_by)
        return query_group_by(t, q, result);

    /* dispatch to aggregate path if aggregates are present */
    if (q->aggregates.count > 0)
        return query_aggregate(t, q, result);

    int select_all = sv_eq_cstr(q->columns, "*");

    /* try index lookup for simple equality WHERE on indexed column */
    if (q->has_where && q->where_cond && q->where_cond->type == COND_COMPARE
        && q->where_cond->op == CMP_EQ && !q->has_order_by) {
        int where_col = table_find_column_sv(t, q->where_cond->column);
        if (where_col >= 0) {
            for (size_t idx = 0; idx < t->indexes.count; idx++) {
                if (strcmp(t->indexes.items[idx].column_name,
                           t->columns.items[where_col].name) == 0) {
                    size_t *ids = NULL;
                    size_t id_count = 0;
                    index_lookup(&t->indexes.items[idx], &q->where_value,
                                 &ids, &id_count);
                    for (size_t k = 0; k < id_count; k++) {
                        if (ids[k] < t->rows.count)
                            emit_row(t, q, &t->rows.items[ids[k]], result, select_all);
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
        if (!row_matches(t, q, &t->rows.items[i]))
            continue;
        da_push(&match_idx, i);
    }

    /* ORDER BY — sort indices using the original table data (multi-column) */
    if (q->has_order_by && q->order_by_items.count > 0) {
        /* resolve column indices for all ORDER BY items */
        int ord_cols[32];
        int ord_descs[32];
        size_t nord = q->order_by_items.count < 32 ? q->order_by_items.count : 32;
        for (size_t k = 0; k < nord; k++) {
            ord_cols[k] = table_find_column_sv(t, q->order_by_items.items[k].column);
            /* if not found, try resolving as a SELECT alias */
            if (ord_cols[k] < 0 && q->columns.len > 0) {
                ord_cols[k] = resolve_alias_to_column(t, q->columns,
                                                       q->order_by_items.items[k].column);
            }
            ord_descs[k] = q->order_by_items.items[k].desc;
        }
        _sort_ctx = (struct sort_ctx){ .cols = ord_cols, .descs = ord_descs,
                                       .ncols = nord, .table = t };
        qsort(match_idx.items, match_idx.count, sizeof(size_t), cmp_indices_multi);
    }

    /* project into result rows */
    struct rows tmp = {0};
    for (size_t i = 0; i < match_idx.count; i++) {
        emit_row(t, q, &t->rows.items[match_idx.items[i]], &tmp, select_all);
    }
    da_free(&match_idx);

    // TODO: DISTINCT dedup is O(n^2); could sort rows first then deduplicate
    // in a single linear pass for O(n log n) performance
    // TODO: CONTAINER REUSE: the row-equality loop here is the same pattern used in
    // UNION/INTERSECT/EXCEPT in database.c; extract a shared rows_equal helper into row.c
    /* DISTINCT: deduplicate before LIMIT (SQL semantics) */
    if (q->has_distinct && tmp.count > 1) {
        struct rows deduped = {0};
        for (size_t i = 0; i < tmp.count; i++) {
            int dup = 0;
            for (size_t j = 0; j < deduped.count; j++) {
                if (row_equal(&tmp.data[i], &deduped.data[j])) { dup = 1; break; }
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
    if (q->has_offset) {
        start = (size_t)q->offset_count;
        if (start > tmp.count) start = tmp.count;
    }
    if (q->has_limit) {
        size_t lim = (size_t)q->limit_count;
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

static int query_delete(struct table *t, struct query *q, struct rows *result)
{
    int has_returning = (q->has_returning && q->returning_columns.len > 0);
    int return_all = has_returning && sv_eq_cstr(q->returning_columns, "*");
    size_t deleted = 0;
    for (size_t i = 0; i < t->rows.count; ) {
        if (row_matches(t, q, &t->rows.items[i])) {
            /* capture row for RETURNING before freeing */
            if (has_returning && result)
                emit_returning_row(t, &t->rows.items[i], q->returning_columns, return_all, result);
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
    if (!has_returning && result) {
        struct row r = {0};
        da_init(&r.cells);
        struct cell c = { .type = COLUMN_TYPE_INT };
        c.value.as_int = (int)deleted;
        da_push(&r.cells, c);
        rows_push(result, r);
    }
    return 0;
}

static int query_update(struct table *t, struct query *q, struct rows *result)
{
    int has_returning = (q->has_returning && q->returning_columns.len > 0);
    int return_all = has_returning && sv_eq_cstr(q->returning_columns, "*");
    size_t updated = 0;
    for (size_t i = 0; i < t->rows.count; i++) {
        if (!row_matches(t, q, &t->rows.items[i]))
            continue;
        updated++;
        for (size_t s = 0; s < q->set_clauses.count; s++) {
            int col_idx = table_find_column_sv(t, q->set_clauses.items[s].column);
            if (col_idx < 0) continue;
            struct cell *dst = &t->rows.items[i].cells.items[col_idx];
            /* free old text */
            if (column_type_is_text(dst->type) && dst->value.as_text)
                free(dst->value.as_text);
            /* copy new value */
            cell_copy(dst, &q->set_clauses.items[s].value);
        }
        /* capture row for RETURNING after SET */
        if (has_returning && result)
            emit_returning_row(t, &t->rows.items[i], q->returning_columns, return_all, result);
    }
    /* rebuild indexes after cell mutation */
    if (updated > 0 && t->indexes.count > 0)
        rebuild_indexes(t);

    if (!has_returning && result) {
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

static int query_insert(struct table *t, struct query *q, struct rows *result)
{
    int has_returning = (q->returning_columns.len > 0);
    int return_all = has_returning && sv_eq_cstr(q->returning_columns, "*");

    for (size_t r = 0; r < q->insert_rows.count; r++) {
        struct row *src = &q->insert_rows.items[r];
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
            emit_returning_row(t, &t->rows.items[t->rows.count - 1], q->returning_columns, return_all, result);
    }

    return 0;
}

int query_exec(struct table *t, struct query *q, struct rows *result)
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
            return query_select(t, q, result);
        case QUERY_TYPE_INSERT:
            return query_insert(t, q, result);
        case QUERY_TYPE_DELETE:
            return query_delete(t, q, result);
        case QUERY_TYPE_UPDATE:
            return query_update(t, q, result);
    }
    return -1;
}
