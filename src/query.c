#include "query.h"
#include <string.h>
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
    } else if (c->type == COND_COMPARE) {
        if ((c->value.type == COLUMN_TYPE_TEXT || c->value.type == COLUMN_TYPE_ENUM)
            && c->value.value.as_text)
            free(c->value.value.as_text);
    }
    free(c);
}

static int cell_cmp(const struct cell *a, const struct cell *b)
{
    /* promote INT <-> FLOAT for numeric comparison */
    if ((a->type == COLUMN_TYPE_INT && b->type == COLUMN_TYPE_FLOAT) ||
        (a->type == COLUMN_TYPE_FLOAT && b->type == COLUMN_TYPE_INT)) {
        double da = (a->type == COLUMN_TYPE_FLOAT) ? a->value.as_float : (double)a->value.as_int;
        double db = (b->type == COLUMN_TYPE_FLOAT) ? b->value.as_float : (double)b->value.as_int;
        if (da < db) return -1;
        if (da > db) return  1;
        return 0;
    }
    if (a->type != b->type) return -2; /* incompatible */
    switch (a->type) {
        case COLUMN_TYPE_INT:
            if (a->value.as_int < b->value.as_int) return -1;
            if (a->value.as_int > b->value.as_int) return  1;
            return 0;
        case COLUMN_TYPE_FLOAT:
            if (a->value.as_float < b->value.as_float) return -1;
            if (a->value.as_float > b->value.as_float) return  1;
            return 0;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
            if (!a->value.as_text && !b->value.as_text) return 0;
            if (!a->value.as_text) return -1;
            if (!b->value.as_text) return  1;
            return strcmp(a->value.as_text, b->value.as_text);
    }
    return -2;
}

static int find_col_idx(struct table *t, sv name);
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
        case COND_COMPARE: {
            int col_idx = find_col_idx(t, cond->column);
            if (col_idx < 0) return 0;
            struct cell *c = &row->cells.items[col_idx];
            if (cond->op == CMP_IS_NULL)
                return c->is_null || ((c->type == COLUMN_TYPE_TEXT || c->type == COLUMN_TYPE_ENUM)
                       ? (c->value.as_text == NULL) : 0);
            if (cond->op == CMP_IS_NOT_NULL)
                return !c->is_null && ((c->type == COLUMN_TYPE_TEXT || c->type == COLUMN_TYPE_ENUM)
                       ? (c->value.as_text != NULL) : 1);
            /* IN / NOT IN */
            if (cond->op == CMP_IN || cond->op == CMP_NOT_IN) {
                int found = 0;
                for (size_t i = 0; i < cond->in_values.count; i++) {
                    if (cell_cmp(c, &cond->in_values.items[i]) == 0) { found = 1; break; }
                }
                return cond->op == CMP_IN ? found : !found;
            }
            /* BETWEEN */
            if (cond->op == CMP_BETWEEN) {
                int lo = cell_cmp(c, &cond->value);
                int hi = cell_cmp(c, &cond->between_high);
                if (lo == -2 || hi == -2) return 0;
                return lo >= 0 && hi <= 0;
            }
            /* LIKE / ILIKE */
            if (cond->op == CMP_LIKE || cond->op == CMP_ILIKE) {
                if ((c->type != COLUMN_TYPE_TEXT && c->type != COLUMN_TYPE_ENUM) || !c->value.as_text)
                    return 0;
                if (!cond->value.value.as_text) return 0;
                return like_match(cond->value.value.as_text, c->value.as_text,
                                  cond->op == CMP_ILIKE);
            }
            int r = cell_cmp(c, &cond->value);
            if (r == -2) return 0;
            switch (cond->op) {
                case CMP_EQ: return r == 0;
                case CMP_NE: return r != 0;
                case CMP_LT: return r < 0;
                case CMP_GT: return r > 0;
                case CMP_LE: return r <= 0;
                case CMP_GE: return r >= 0;
                default: return 0;
            }
        }
    }
    return 0;
}

static int cell_match(const struct cell *a, const struct cell *b)
{
    /* promote INT <-> FLOAT */
    if ((a->type == COLUMN_TYPE_INT && b->type == COLUMN_TYPE_FLOAT) ||
        (a->type == COLUMN_TYPE_FLOAT && b->type == COLUMN_TYPE_INT)) {
        double da = (a->type == COLUMN_TYPE_FLOAT) ? a->value.as_float : (double)a->value.as_int;
        double db = (b->type == COLUMN_TYPE_FLOAT) ? b->value.as_float : (double)b->value.as_int;
        return da == db;
    }
    if (a->type != b->type) return 0;
    switch (a->type) {
        case COLUMN_TYPE_INT:   return a->value.as_int == b->value.as_int;
        case COLUMN_TYPE_FLOAT: return a->value.as_float == b->value.as_float;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
            if (!a->value.as_text || !b->value.as_text) return a->value.as_text == b->value.as_text;
            return strcmp(a->value.as_text, b->value.as_text) == 0;
    }
    return 0;
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
            if ((c.type == COLUMN_TYPE_TEXT || c.type == COLUMN_TYPE_ENUM)
                && c.value.as_text)
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
            /* find end of this column name (comma or end) */
            size_t end = 0;
            while (end < cols.len && cols.data[end] != ',') end++;
            sv one = sv_from(cols.data, end);
            /* trim trailing whitespace */
            while (one.len > 0 && (one.data[one.len - 1] == ' ' || one.data[one.len - 1] == '\t'))
                one.len--;

            /* strip column alias: "col AS alias" -> "col" */
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

            /* strip table prefix (e.g. t1.col -> col) */
            for (size_t k = 0; k < one.len; k++) {
                if (one.data[k] == '.') {
                    one = sv_from(one.data + k + 1, one.len - k - 1);
                    break;
                }
            }

            for (size_t j = 0; j < t->columns.count; j++) {
                if (sv_eq_cstr(one, t->columns.items[j].name)) {
                    struct cell c = src->cells.items[j];
                    struct cell copy = { .type = c.type, .is_null = c.is_null };
                    if ((c.type == COLUMN_TYPE_TEXT || c.type == COLUMN_TYPE_ENUM)
                        && c.value.as_text)
                        copy.value.as_text = strdup(c.value.as_text);
                    else
                        copy.value = c.value;
                    da_push(&dst.cells, copy);
                    break;
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

    /* accumulate */
    double *sums = calloc(q->aggregates.count, sizeof(double));
    double *mins = malloc(q->aggregates.count * sizeof(double));
    double *maxs = malloc(q->aggregates.count * sizeof(double));
    int *minmax_init = calloc(q->aggregates.count, sizeof(int));
    size_t row_count = 0;

    for (size_t i = 0; i < t->rows.count; i++) {
        if (q->has_where) {
            if (q->where_cond) {
                if (!eval_condition(q->where_cond, &t->rows.items[i], t))
                    continue;
            } else if (where_col >= 0) {
                if (!cell_match(&t->rows.items[i].cells.items[where_col],
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
            default:
                break;
        }
        da_push(&dst.cells, c);
    }
    rows_push(result, dst);

    free(sums);
    free(mins);
    free(maxs);
    free(minmax_init);
    free(agg_col);
    return 0;
}

static void copy_cell_into(struct cell *dst, const struct cell *src);

static int find_col_idx(struct table *t, sv name)
{
    /* strip table prefix if present (e.g. t1.id -> id) */
    sv col = name;
    for (size_t k = 0; k < name.len; k++) {
        if (name.data[k] == '.') {
            col = sv_from(name.data + k + 1, name.len - k - 1);
            break;
        }
    }
    for (size_t j = 0; j < t->columns.count; j++) {
        if (sv_eq_cstr(col, t->columns.items[j].name))
            return (int)j;
    }
    return -1;
}

static int cell_compare(const struct cell *a, const struct cell *b)
{
    if (a->type != b->type) return (int)a->type - (int)b->type;
    switch (a->type) {
        case COLUMN_TYPE_INT:
            if (a->value.as_int < b->value.as_int) return -1;
            if (a->value.as_int > b->value.as_int) return  1;
            return 0;
        case COLUMN_TYPE_FLOAT:
            if (a->value.as_float < b->value.as_float) return -1;
            if (a->value.as_float > b->value.as_float) return  1;
            return 0;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
            if (!a->value.as_text && !b->value.as_text) return 0;
            if (!a->value.as_text) return -1;
            if (!b->value.as_text) return  1;
            return strcmp(a->value.as_text, b->value.as_text);
    }
    return 0;
}

static double cell_to_double(const struct cell *c)
{
    switch (c->type) {
        case COLUMN_TYPE_INT:   return (double)c->value.as_int;
        case COLUMN_TYPE_FLOAT: return c->value.as_float;
        default: return 0.0;
    }
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

    /* resolve column indices for plain columns and window args */
    int *col_idx = calloc(nexprs, sizeof(int));
    int *part_idx = calloc(nexprs, sizeof(int));
    int *ord_idx = calloc(nexprs, sizeof(int));
    int *arg_idx = calloc(nexprs, sizeof(int));

    for (size_t e = 0; e < nexprs; e++) {
        struct select_expr *se = &q->select_exprs.items[e];
        col_idx[e] = -1;
        part_idx[e] = -1;
        ord_idx[e] = -1;
        arg_idx[e] = -1;

        if (se->kind == SEL_COLUMN) {
            col_idx[e] = find_col_idx(t, se->column);
            if (col_idx[e] < 0) {
                fprintf(stderr, "column '" SV_FMT "' not found\n", SV_ARG(se->column));
                free(col_idx); free(part_idx); free(ord_idx); free(arg_idx);
                return -1;
            }
        } else {
            if (se->win.has_partition) {
                part_idx[e] = find_col_idx(t, se->win.partition_col);
                if (part_idx[e] < 0) {
                    fprintf(stderr, "partition column '" SV_FMT "' not found\n",
                            SV_ARG(se->win.partition_col));
                    free(col_idx); free(part_idx); free(ord_idx); free(arg_idx);
                    return -1;
                }
            }
            if (se->win.has_order) {
                ord_idx[e] = find_col_idx(t, se->win.order_col);
                if (ord_idx[e] < 0) {
                    fprintf(stderr, "order column '" SV_FMT "' not found\n",
                            SV_ARG(se->win.order_col));
                    free(col_idx); free(part_idx); free(ord_idx); free(arg_idx);
                    return -1;
                }
            }
            if (se->win.arg_column.len > 0 && !sv_eq_cstr(se->win.arg_column, "*")) {
                arg_idx[e] = find_col_idx(t, se->win.arg_column);
                if (arg_idx[e] < 0) {
                    fprintf(stderr, "window arg column '" SV_FMT "' not found\n",
                            SV_ARG(se->win.arg_column));
                    free(col_idx); free(part_idx); free(ord_idx); free(arg_idx);
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
                copy_cell_into(&c, &src->cells.items[col_idx[e]]);
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
                        if (!cell_match(&src->cells.items[part_idx[e]],
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
                                    if (!cell_match(&src->cells.items[part_idx[e]],
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
    free(col_idx);
    free(part_idx);
    free(ord_idx);
    free(arg_idx);
    return 0;
}

static int query_group_by(struct table *t, struct query *q, struct rows *result)
{
    int grp_col = find_col_idx(t, q->group_by_col);
    if (grp_col < 0) {
        fprintf(stderr, "GROUP BY column '" SV_FMT "' not found\n",
                SV_ARG(q->group_by_col));
        return -1;
    }

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

    /* find distinct group keys */
    DYNAMIC_ARRAY(size_t) group_starts; /* index into matching for first of each group */
    da_init(&group_starts);

    for (size_t m = 0; m < matching.count; m++) {
        size_t ri = matching.items[m];
        struct cell *key = &t->rows.items[ri].cells.items[grp_col];
        int found = 0;
        for (size_t g = 0; g < group_starts.count; g++) {
            size_t gi = matching.items[group_starts.items[g]];
            if (cell_match(key, &t->rows.items[gi].cells.items[grp_col])) {
                found = 1; break;
            }
        }
        if (!found) da_push(&group_starts, m);
    }

    /* for each group, compute aggregates */
    for (size_t g = 0; g < group_starts.count; g++) {
        size_t first_ri = matching.items[group_starts.items[g]];
        struct cell *group_key = &t->rows.items[first_ri].cells.items[grp_col];

        /* count and accumulate for this group */
        size_t grp_count = 0;
        double *sums = NULL;
        double *gmins = NULL, *gmaxs = NULL;
        int *gminmax_init = NULL;
        if (q->aggregates.count > 0) {
            sums = calloc(q->aggregates.count, sizeof(double));
            gmins = malloc(q->aggregates.count * sizeof(double));
            gmaxs = malloc(q->aggregates.count * sizeof(double));
            gminmax_init = calloc(q->aggregates.count, sizeof(int));
        }

        for (size_t m = 0; m < matching.count; m++) {
            size_t ri = matching.items[m];
            if (!cell_match(group_key, &t->rows.items[ri].cells.items[grp_col]))
                continue;
            grp_count++;
            for (size_t a = 0; a < q->aggregates.count; a++) {
                if (sv_eq_cstr(q->aggregates.items[a].column, "*")) continue;
                int ac = find_col_idx(t, q->aggregates.items[a].column);
                if (ac >= 0) {
                    double v = cell_to_double(&t->rows.items[ri].cells.items[ac]);
                    sums[a] += v;
                    if (!gminmax_init[a] || v < gmins[a]) gmins[a] = v;
                    if (!gminmax_init[a] || v > gmaxs[a]) gmaxs[a] = v;
                    gminmax_init[a] = 1;
                }
            }
        }

        /* build result row: group key column + aggregates */
        struct row dst = {0};
        da_init(&dst.cells);

        /* add group key */
        struct cell gc;
        copy_cell_into(&gc, group_key);
        da_push(&dst.cells, gc);

        /* add aggregate values */
        for (size_t a = 0; a < q->aggregates.count; a++) {
            struct cell c = {0};
            int ac_idx = find_col_idx(t, q->aggregates.items[a].column);
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
                default: break;
            }
            da_push(&dst.cells, c);
        }
        free(sums);
        free(gmins);
        free(gmaxs);
        free(gminmax_init);

        /* HAVING filter: evaluate against the result row using a temporary table */
        if (q->has_having && q->having_cond) {
            /* build a temp table with column names matching result columns */
            struct table tmp_t = {0};
            da_init(&tmp_t.columns);
            da_init(&tmp_t.rows);
            da_init(&tmp_t.indexes);

            /* group column */
            struct column col_grp = { .name = t->columns.items[grp_col].name,
                                      .type = t->columns.items[grp_col].type,
                                      .enum_type_name = NULL };
            da_push(&tmp_t.columns, col_grp);

            /* aggregate columns named by function */
            for (size_t a = 0; a < q->aggregates.count; a++) {
                const char *agg_name = "?";
                switch (q->aggregates.items[a].func) {
                    case AGG_SUM:   agg_name = "sum";   break;
                    case AGG_COUNT: agg_name = "count"; break;
                    case AGG_AVG:   agg_name = "avg";   break;
                    default: break;
                }
                struct column col_a = { .name = (char *)agg_name,
                                        .type = dst.cells.items[1 + a].type,
                                        .enum_type_name = NULL };
                da_push(&tmp_t.columns, col_a);
            }

            int passes = eval_condition(q->having_cond, &dst, &tmp_t);
            da_free(&tmp_t.columns);

            if (!passes) {
                row_free(&dst);
                continue;
            }
        }

        rows_push(result, dst);
    }

    da_free(&matching);
    da_free(&group_starts);

    /* ORDER BY on grouped results */
    if (q->has_order_by && result->count > 1) {
        /* find column index in result: 0 = group col, 1+ = aggregates */
        int ord_res = -1;
        if (sv_eq_cstr(q->order_by_col, t->columns.items[grp_col].name)) {
            ord_res = 0;
        } else {
            for (size_t a = 0; a < q->aggregates.count; a++) {
                const char *agg_name = "?";
                switch (q->aggregates.items[a].func) {
                    case AGG_SUM:   agg_name = "sum";   break;
                    case AGG_COUNT: agg_name = "count"; break;
                    case AGG_AVG:   agg_name = "avg";   break;
                    default: break;
                }
                if (sv_eq_cstr(q->order_by_col, agg_name)) {
                    ord_res = (int)(1 + a);
                    break;
                }
            }
        }
        if (ord_res >= 0) {
            for (size_t i = 0; i < result->count; i++) {
                for (size_t j = i + 1; j < result->count; j++) {
                    int cmp = cell_compare(&result->data[i].cells.items[ord_res],
                                           &result->data[j].cells.items[ord_res]);
                    if (q->order_desc ? (cmp < 0) : (cmp > 0)) {
                        struct row swap = result->data[i];
                        result->data[i] = result->data[j];
                        result->data[j] = swap;
                    }
                }
            }
        }
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
            result->data[i] = (struct row){0}; /* prevent double free */
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
    return cell_match(&row->cells.items[where_col], &q->where_value);
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
        int where_col = find_col_idx(t, q->where_cond->column);
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

    /* ORDER BY — sort indices using the original table data */
    if (q->has_order_by) {
        int ord_col = find_col_idx(t, q->order_by_col);
        if (ord_col >= 0) {
            for (size_t i = 0; i < match_idx.count; i++) {
                for (size_t j = i + 1; j < match_idx.count; j++) {
                    struct cell *a = &t->rows.items[match_idx.items[i]].cells.items[ord_col];
                    struct cell *b = &t->rows.items[match_idx.items[j]].cells.items[ord_col];
                    int cmp = cell_compare(a, b);
                    if (q->order_desc ? (cmp < 0) : (cmp > 0)) {
                        size_t swap = match_idx.items[i];
                        match_idx.items[i] = match_idx.items[j];
                        match_idx.items[j] = swap;
                    }
                }
            }
        }
    }

    /* project into result rows */
    struct rows tmp = {0};
    for (size_t i = 0; i < match_idx.count; i++) {
        emit_row(t, q, &t->rows.items[match_idx.items[i]], &tmp, select_all);
    }
    da_free(&match_idx);

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

    /* DISTINCT: deduplicate result rows */
    if (q->has_distinct && result->count > 1) {
        struct rows deduped = {0};
        for (size_t i = 0; i < result->count; i++) {
            int dup = 0;
            for (size_t j = 0; j < deduped.count; j++) {
                if (deduped.data[j].cells.count != result->data[i].cells.count) continue;
                int eq = 1;
                for (size_t k = 0; k < result->data[i].cells.count; k++) {
                    if (cell_cmp(&result->data[i].cells.items[k],
                                 &deduped.data[j].cells.items[k]) != 0) { eq = 0; break; }
                }
                if (eq) { dup = 1; break; }
            }
            if (!dup) {
                rows_push(&deduped, result->data[i]);
                result->data[i] = (struct row){0};
            }
        }
        rows_free(result);
        *result = deduped;
    }

    return 0;
}

static void rebuild_indexes(struct table *t)
{
    for (size_t idx = 0; idx < t->indexes.count; idx++) {
        struct index *ix = &t->indexes.items[idx];
        /* save metadata */
        char *name = strdup(ix->name);
        char *col_name = strdup(ix->column_name);
        int col_idx = ix->column_idx;
        /* free and reinit */
        index_free(ix);
        index_init(ix, name, col_name, col_idx);
        free(name);
        free(col_name);
        /* re-insert all rows */
        for (size_t r = 0; r < t->rows.count; r++) {
            if ((size_t)col_idx < t->rows.items[r].cells.count)
                index_insert(ix, &t->rows.items[r].cells.items[col_idx], r);
        }
    }
}

static int query_delete(struct table *t, struct query *q, struct rows *result)
{
    (void)result;
    size_t deleted = 0;
    for (size_t i = 0; i < t->rows.count; ) {
        if (row_matches(t, q, &t->rows.items[i])) {
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

    /* store deleted count for command tag */
    if (result) {
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
    (void)result;
    size_t updated = 0;
    for (size_t i = 0; i < t->rows.count; i++) {
        if (!row_matches(t, q, &t->rows.items[i]))
            continue;
        updated++;
        for (size_t s = 0; s < q->set_clauses.count; s++) {
            int col_idx = find_col_idx(t, q->set_clauses.items[s].column);
            if (col_idx < 0) continue;
            struct cell *dst = &t->rows.items[i].cells.items[col_idx];
            /* free old text */
            if ((dst->type == COLUMN_TYPE_TEXT || dst->type == COLUMN_TYPE_ENUM)
                && dst->value.as_text)
                free(dst->value.as_text);
            /* copy new value */
            copy_cell_into(dst, &q->set_clauses.items[s].value);
        }
    }
    /* rebuild indexes after cell mutation */
    if (updated > 0 && t->indexes.count > 0)
        rebuild_indexes(t);

    if (result) {
        struct row r = {0};
        da_init(&r.cells);
        struct cell c = { .type = COLUMN_TYPE_INT };
        c.value.as_int = (int)updated;
        da_push(&r.cells, c);
        rows_push(result, r);
    }
    return 0;
}

static void copy_cell_into(struct cell *dst, const struct cell *src)
{
    dst->type = src->type;
    dst->is_null = src->is_null;
    if ((src->type == COLUMN_TYPE_TEXT || src->type == COLUMN_TYPE_ENUM)
        && src->value.as_text) {
        dst->value.as_text = strdup(src->value.as_text);
    } else {
        dst->value = src->value;
    }
}

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
            copy_cell_into(&dup, &src->cells.items[i]);
            da_push(&copy.cells, dup);
        }
        /* pad with NULL if fewer values than columns */
        while (copy.cells.count < t->columns.count) {
            struct cell null_cell = {0};
            null_cell.type = t->columns.items[copy.cells.count].type;
            null_cell.is_null = 1;
            da_push(&copy.cells, null_cell);
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

        if (has_returning && result) {
            struct row *inserted = &t->rows.items[t->rows.count - 1];
            struct row ret = {0};
            da_init(&ret.cells);

            if (return_all) {
                for (size_t i = 0; i < inserted->cells.count; i++) {
                    struct cell c;
                    copy_cell_into(&c, &inserted->cells.items[i]);
                    da_push(&ret.cells, c);
                }
            } else {
                /* walk comma-separated returning column list */
                sv cols = q->returning_columns;
                while (cols.len > 0) {
                    size_t end = 0;
                    while (end < cols.len && cols.data[end] != ',') end++;
                    sv one = sv_trim(sv_from(cols.data, end));

                    for (size_t j = 0; j < t->columns.count; j++) {
                        if (sv_eq_cstr(one, t->columns.items[j].name)) {
                            struct cell c;
                            copy_cell_into(&c, &inserted->cells.items[j]);
                            da_push(&ret.cells, c);
                            break;
                        }
                    }

                    if (end < cols.len) end++;
                    cols = sv_from(cols.data + end, cols.len - end);
                }
            }

            rows_push(result, ret);
        }
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
