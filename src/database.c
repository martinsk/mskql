#include "database.h"
#include "parser.h"
#include "query.h"
#include "stringview.h"
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

void db_init(struct database *db, const char *name)
{
    db->name = strdup(name);
    da_init(&db->tables);
    da_init(&db->types);
}

struct enum_type *db_find_type(struct database *db, const char *name)
{
    for (size_t i = 0; i < db->types.count; i++) {
        if (strcmp(db->types.items[i].name, name) == 0)
            return &db->types.items[i];
    }
    return NULL;
}

static struct enum_type *db_find_type_sv(struct database *db, sv name)
{
    for (size_t i = 0; i < db->types.count; i++) {
        if (sv_eq_cstr(name, db->types.items[i].name))
            return &db->types.items[i];
    }
    return NULL;
}

int db_create_table(struct database *db, const char *name, struct column *cols)
{
    struct table t;
    table_init(&t, name);
    table_add_column(&t, cols);
    da_push(&db->tables, t);
    return 0;
}

struct table *db_find_table(struct database *db, const char *name)
{
    for (size_t i = 0; i < db->tables.count; i++) {
        if (strcmp(db->tables.items[i].name, name) == 0) {
            return &db->tables.items[i];
        }
    }
    return NULL;
}

struct table *db_find_table_sv(struct database *db, sv name)
{
    for (size_t i = 0; i < db->tables.count; i++) {
        if (sv_eq_cstr(name, db->tables.items[i].name)) {
            return &db->tables.items[i];
        }
    }
    return NULL;
}

int db_table_exec_query(struct database *db, sv table_name,
                        struct query *q, struct rows *result)
{
    struct table *t = db_find_table_sv(db, table_name);
    if (!t) {
        fprintf(stderr, "table '" SV_FMT "' not found\n", SV_ARG(table_name));
        return -1;
    }
    return query_exec(t, q, result);
}

/* find_column_index / find_column_index_sv → use shared table_find_column / table_find_column_sv from table.h */

// TODO: STRINGVIEW OPPORTUNITY: extract_col_name copies into a stack buffer then returns
// const char*; callers could use an sv-returning variant to avoid the copy entirely,
// since the result is only used for strcmp/find_column_index lookups
/* extract the column part from "table.column" or just "column" */
static const char *extract_col_name(sv ref, char *buf, size_t bufsz)
{
    for (size_t i = 0; i < ref.len; i++) {
        if (ref.data[i] == '.') {
            sv col = sv_from(ref.data + i + 1, ref.len - i - 1);
            if (col.len >= bufsz) col.len = bufsz - 1;
            memcpy(buf, col.data, col.len);
            buf[col.len] = '\0';
            return buf;
        }
    }
    if (ref.len >= bufsz) ref.len = bufsz - 1;
    memcpy(buf, ref.data, ref.len);
    buf[ref.len] = '\0';
    return buf;
}

/* free_cell_text_ext → use shared cell_free_text from row.h */

/* copy_cell → use shared cell_copy from row.h */

/* cell_compare_join → use shared cell_compare from row.h */

/* qsort context for multi-column ORDER BY (single-threaded, so static is fine) */
struct join_sort_ctx {
    int *cols;
    int *descs;
    size_t ncols;
};
static struct join_sort_ctx _jsort_ctx;

static int cmp_rows_join(const void *a, const void *b)
{
    const struct row *ra = (const struct row *)a;
    const struct row *rb = (const struct row *)b;
    for (size_t k = 0; k < _jsort_ctx.ncols; k++) {
        int ci = _jsort_ctx.cols[k];
        if (ci < 0) continue;
        int cmp = cell_compare(&ra->cells.items[ci], &rb->cells.items[ci]);
        if (_jsort_ctx.descs[k]) cmp = -cmp;
        if (cmp != 0) return cmp;
    }
    return 0;
}

/* cells_equal → use shared cell_equal from row.h */

static void emit_merged_row(struct row *r1, size_t ncols1,
                            struct row *r2, size_t ncols2,
                            struct rows *out)
{
    struct row full = {0};
    da_init(&full.cells);
    for (size_t c = 0; c < ncols1; c++) {
        struct cell cp; cell_copy(&cp, &r1->cells.items[c]);
        da_push(&full.cells, cp);
    }
    for (size_t c = 0; c < ncols2; c++) {
        struct cell cp; cell_copy(&cp, &r2->cells.items[c]);
        da_push(&full.cells, cp);
    }
    rows_push(out, full);
}

static void emit_null_right(struct row *r1, size_t ncols1,
                             struct table *t2, struct rows *out)
{
    struct row full = {0};
    da_init(&full.cells);
    for (size_t c = 0; c < ncols1; c++) {
        struct cell cp; cell_copy(&cp, &r1->cells.items[c]);
        da_push(&full.cells, cp);
    }
    for (size_t c = 0; c < t2->columns.count; c++) {
        struct cell cp = { .type = t2->columns.items[c].type, .is_null = 1 };
        da_push(&full.cells, cp);
    }
    rows_push(out, full);
}

static void emit_null_left(struct table *t1, struct row *r2,
                            size_t ncols2, struct rows *out)
{
    struct row full = {0};
    da_init(&full.cells);
    for (size_t c = 0; c < t1->columns.count; c++) {
        struct cell cp = { .type = t1->columns.items[c].type, .is_null = 1 };
        da_push(&full.cells, cp);
    }
    for (size_t c = 0; c < ncols2; c++) {
        struct cell cp; cell_copy(&cp, &r2->cells.items[c]);
        da_push(&full.cells, cp);
    }
    rows_push(out, full);
}

// TODO: STRINGVIEW OPPORTUNITY: make_aliased_name heap-allocates "alias.col" strings
// that are only used for column name lookups; could use a stack buffer or sv concatenation
static char *make_aliased_name(const char *alias, const char *col_name)
{
    if (!alias || !alias[0]) return strdup(col_name);
    size_t alen = strlen(alias);
    size_t clen = strlen(col_name);
    char *buf = malloc(alen + 1 + clen + 1);
    memcpy(buf, alias, alen);
    buf[alen] = '.';
    memcpy(buf + alen + 1, col_name, clen);
    buf[alen + 1 + clen] = '\0';
    return buf;
}

static void build_merged_columns_ex(struct table *t1, const char *alias1,
                                     struct table *t2, const char *alias2,
                                     struct table *out_meta)
{
    for (size_t c = 0; c < t1->columns.count; c++) {
        struct column col = t1->columns.items[c];
        col.name = make_aliased_name(alias1, col.name);
        col.enum_type_name = col.enum_type_name ? strdup(col.enum_type_name) : NULL;
        col.default_value = NULL;
        da_push(&out_meta->columns, col);
    }
    for (size_t c = 0; c < t2->columns.count; c++) {
        struct column col = t2->columns.items[c];
        col.name = make_aliased_name(alias2, col.name);
        col.enum_type_name = col.enum_type_name ? strdup(col.enum_type_name) : NULL;
        col.default_value = NULL;
        da_push(&out_meta->columns, col);
    }
}

/* perform a single join between two table descriptors, producing merged rows and columns */
static int do_single_join(struct table *t1, const char *alias1,
                          struct table *t2, const char *alias2,
                          sv left_col_sv, sv right_col_sv, int join_type,
                          struct rows *out_rows,
                          struct table *out_meta)
{
    size_t ncols1 = t1->columns.count;
    size_t ncols2 = t2->columns.count;

    /* CROSS JOIN (type 4): cartesian product, no join columns */
    if (join_type == 4) {
        for (size_t i = 0; i < t1->rows.count; i++) {
            for (size_t j = 0; j < t2->rows.count; j++) {
                emit_merged_row(&t1->rows.items[i], ncols1,
                                &t2->rows.items[j], ncols2, out_rows);
            }
        }
        build_merged_columns_ex(t1, alias1, t2, alias2, out_meta);
        return 0;
    }

    char buf[256], buf2[256];
    const char *left_col = extract_col_name(left_col_sv, buf, sizeof(buf));
    const char *right_col = extract_col_name(right_col_sv, buf2, sizeof(buf2));

    int left_in_t1 = (table_find_column(t1, left_col) >= 0);
    int t1_join_col = left_in_t1 ? table_find_column(t1, left_col) : table_find_column(t1, right_col);
    int t2_join_col = left_in_t1 ? table_find_column(t2, right_col) : table_find_column(t2, left_col);

    if (t1_join_col < 0 || t2_join_col < 0) {
        fprintf(stderr, "join error: could not resolve ON columns\n");
        return -1;
    }

    int *t1_matched = calloc(t1->rows.count, sizeof(int));
    int *t2_matched = calloc(t2->rows.count, sizeof(int));

    for (size_t i = 0; i < t1->rows.count; i++) {
        struct row *r1 = &t1->rows.items[i];
        for (size_t j = 0; j < t2->rows.count; j++) {
            struct row *r2 = &t2->rows.items[j];
            if (!cell_equal(&r1->cells.items[t1_join_col], &r2->cells.items[t2_join_col]))
                continue;
            t1_matched[i] = 1;
            t2_matched[j] = 1;
            emit_merged_row(r1, ncols1, r2, ncols2, out_rows);
        }
    }

    /* LEFT or FULL: unmatched left rows */
    if (join_type == 1 || join_type == 3) {
        for (size_t i = 0; i < t1->rows.count; i++) {
            if (t1_matched[i]) continue;
            emit_null_right(&t1->rows.items[i], ncols1, t2, out_rows);
        }
    }

    /* RIGHT or FULL: unmatched right rows */
    if (join_type == 2 || join_type == 3) {
        for (size_t j = 0; j < t2->rows.count; j++) {
            if (t2_matched[j]) continue;
            emit_null_left(t1, &t2->rows.items[j], ncols2, out_rows);
        }
    }

    free(t1_matched);
    free(t2_matched);
    build_merged_columns_ex(t1, alias1, t2, alias2, out_meta);
    return 0;
}

/* free strdup'd column names in a merged table descriptor */
static void free_merged_columns(struct table *mt)
{
    for (size_t c = 0; c < mt->columns.count; c++) {
        free(mt->columns.items[c].name);
        free(mt->columns.items[c].enum_type_name);
    }
    da_free(&mt->columns);
}

static void free_merged_rows(struct rows *mr)
{
    for (size_t i = 0; i < mr->count; i++)
        row_free(&mr->data[i]);
    free(mr->data);
}

/* resolve USING and NATURAL join columns; returns the join column sv for both sides */
static void resolve_join_cols(struct join_info *ji, struct table *left, struct table *right)
{
    if (ji->has_using) {
        /* USING(col) — same column name on both sides */
        ji->join_left_col = ji->using_col;
        ji->join_right_col = ji->using_col;
    } else if (ji->is_natural) {
        /* NATURAL JOIN — find first column with same name in both tables */
        for (size_t i = 0; i < left->columns.count; i++) {
            for (size_t j = 0; j < right->columns.count; j++) {
                if (strcmp(left->columns.items[i].name, right->columns.items[j].name) == 0) {
                    ji->join_left_col = sv_from_cstr(left->columns.items[i].name);
                    ji->join_right_col = sv_from_cstr(right->columns.items[j].name);
                    return;
                }
            }
        }
        /* no matching column found — degrade to cross join */
        ji->join_type = 4;
    }
}

static int exec_join(struct database *db, struct query *q, struct rows *result)
{
    struct query_select *s = &q->select;
    struct table *t1 = db_find_table_sv(db, s->table);
    if (!t1) { fprintf(stderr, "table '" SV_FMT "' not found\n", SV_ARG(s->table)); return -1; }

    /* build merged table through successive joins */
    struct table merged_t = {0};
    da_init(&merged_t.columns);
    da_init(&merged_t.rows);
    da_init(&merged_t.indexes);
    struct rows merged = {0};

    /* first join */
    struct join_info *ji = &s->joins.items[0];
    char t1_alias_buf[128] = {0};
    if (s->table_alias.len > 0)
        snprintf(t1_alias_buf, sizeof(t1_alias_buf), "%.*s", (int)s->table_alias.len, s->table_alias.data);
    const char *a1 = t1_alias_buf[0] ? t1_alias_buf : NULL;

    if (ji->is_lateral && ji->lateral_subquery_sql) {
        /* LATERAL JOIN: for each row of t1, execute the subquery and cross-join */
        /* build merged column metadata: t1 columns + lateral result columns */
        for (size_t c = 0; c < t1->columns.count; c++) {
            struct column col = {0};
            if (a1) {
                char buf[256];
                snprintf(buf, sizeof(buf), "%s.%s", a1, t1->columns.items[c].name);
                col.name = strdup(buf);
            } else {
                col.name = strdup(t1->columns.items[c].name);
            }
            col.type = t1->columns.items[c].type;
            da_push(&merged_t.columns, col);
        }
        int lat_cols_added = 0;
        char lat_alias_buf[128] = {0};
        if (ji->join_alias.len > 0)
            snprintf(lat_alias_buf, sizeof(lat_alias_buf), "%.*s", (int)ji->join_alias.len, ji->join_alias.data);

        for (size_t ri = 0; ri < t1->rows.count; ri++) {
            /* Build a rewritten SQL where outer_table.col references are
             * replaced with literal values from the current outer row */
            const char *outer_prefix = a1 ? a1 : t1->name;
            size_t pfx_len = strlen(outer_prefix);
            char rewritten[4096];
            const char *src = ji->lateral_subquery_sql;
            size_t wp = 0;
            while (*src && wp < sizeof(rewritten) - 128) {
                /* check for outer_prefix.col_name */
                if (strncasecmp(src, outer_prefix, pfx_len) == 0 && src[pfx_len] == '.') {
                    const char *col_start = src + pfx_len + 1;
                    const char *col_end = col_start;
                    while (*col_end && (isalnum((unsigned char)*col_end) || *col_end == '_')) col_end++;
                    size_t col_len = (size_t)(col_end - col_start);
                    /* find column index in t1 */
                    int ci = -1;
                    for (size_t c = 0; c < t1->columns.count; c++) {
                        if (strlen(t1->columns.items[c].name) == col_len &&
                            strncasecmp(t1->columns.items[c].name, col_start, col_len) == 0) {
                            ci = (int)c;
                            break;
                        }
                    }
                    if (ci >= 0 && ci < (int)t1->rows.items[ri].cells.count) {
                        struct cell *cv = &t1->rows.items[ri].cells.items[ci];
                        if (cv->is_null) {
                            wp += (size_t)snprintf(rewritten + wp, sizeof(rewritten) - wp, "NULL");
                        } else if (cv->type == COLUMN_TYPE_INT) {
                            wp += (size_t)snprintf(rewritten + wp, sizeof(rewritten) - wp, "%lld", cv->value.as_int);
                        } else if (cv->type == COLUMN_TYPE_FLOAT) {
                            wp += (size_t)snprintf(rewritten + wp, sizeof(rewritten) - wp, "%g", cv->value.as_float);
                        } else if (column_type_is_text(cv->type) && cv->value.as_text) {
                            wp += (size_t)snprintf(rewritten + wp, sizeof(rewritten) - wp, "'%s'", cv->value.as_text);
                        } else {
                            wp += (size_t)snprintf(rewritten + wp, sizeof(rewritten) - wp, "NULL");
                        }
                        src = col_end;
                        continue;
                    }
                }
                rewritten[wp++] = *src++;
            }
            rewritten[wp] = '\0';

            struct rows lat_rows = {0};
            db_exec_sql(db, rewritten, &lat_rows);

            /* add lateral result columns on first iteration */
            if (!lat_cols_added && lat_rows.count > 0) {
                /* infer column names from the lateral subquery's column list */
                struct query lat_q = {0};
                if (query_parse(ji->lateral_subquery_sql, &lat_q) == 0) {
                    /* parse the SELECT column list to get names */
                    sv cols = lat_q.select.columns;
                    size_t ci = 0;
                    while (cols.len > 0 && ci < lat_rows.data[0].cells.count) {
                        while (cols.len > 0 && (cols.data[0] == ' ' || cols.data[0] == '\t'))
                            { cols.data++; cols.len--; }
                        size_t end = 0;
                        while (end < cols.len && cols.data[end] != ',') end++;
                        sv col_sv = sv_from(cols.data, end);
                        while (col_sv.len > 0 && (col_sv.data[col_sv.len-1] == ' ' || col_sv.data[col_sv.len-1] == '\t'))
                            col_sv.len--;
                        /* strip table prefix */
                        sv col_name = col_sv;
                        for (size_t k = 0; k < col_name.len; k++) {
                            if (col_name.data[k] == '.') {
                                col_name = sv_from(col_name.data + k + 1, col_name.len - k - 1);
                                break;
                            }
                        }
                        struct column col = {0};
                        char buf[256];
                        if (lat_alias_buf[0])
                            snprintf(buf, sizeof(buf), "%s.%.*s", lat_alias_buf, (int)col_name.len, col_name.data);
                        else
                            snprintf(buf, sizeof(buf), "%.*s", (int)col_name.len, col_name.data);
                        col.name = strdup(buf);
                        col.type = lat_rows.data[0].cells.items[ci].type;
                        da_push(&merged_t.columns, col);
                        ci++;
                        if (end < cols.len) { cols.data += end + 1; cols.len -= end + 1; }
                        else break;
                    }
                    /* fallback for remaining columns */
                    for (; ci < lat_rows.data[0].cells.count; ci++) {
                        struct column col = {0};
                        char buf[32];
                        snprintf(buf, sizeof(buf), "col%zu", ci + 1);
                        col.name = strdup(buf);
                        col.type = lat_rows.data[0].cells.items[ci].type;
                        da_push(&merged_t.columns, col);
                    }
                    query_free(&lat_q);
                }
                lat_cols_added = 1;
            }

            /* cross-join: each lateral row with the current t1 row */
            for (size_t li = 0; li < lat_rows.count; li++) {
                struct row dst = {0};
                da_init(&dst.cells);
                for (size_t c = 0; c < t1->rows.items[ri].cells.count; c++) {
                    struct cell cp;
                    cell_copy(&cp, &t1->rows.items[ri].cells.items[c]);
                    da_push(&dst.cells, cp);
                }
                for (size_t c = 0; c < lat_rows.data[li].cells.count; c++) {
                    struct cell cp;
                    cell_copy(&cp, &lat_rows.data[li].cells.items[c]);
                    da_push(&dst.cells, cp);
                }
                rows_push(&merged, dst);
            }
            for (size_t li = 0; li < lat_rows.count; li++)
                row_free(&lat_rows.data[li]);
            free(lat_rows.data);
        }
    } else {
        struct table *t2 = db_find_table_sv(db, ji->join_table);
        if (!t2) {
            fprintf(stderr, "table '" SV_FMT "' not found\n", SV_ARG(ji->join_table));
            free_merged_rows(&merged);
            free_merged_columns(&merged_t);
            return -1;
        }
        resolve_join_cols(ji, t1, t2);
        char t2_alias_buf[128] = {0};
        if (ji->join_alias.len > 0)
            snprintf(t2_alias_buf, sizeof(t2_alias_buf), "%.*s", (int)ji->join_alias.len, ji->join_alias.data);
        const char *a2 = t2_alias_buf[0] ? t2_alias_buf : NULL;
        if (do_single_join(t1, a1, t2, a2, ji->join_left_col, ji->join_right_col, ji->join_type,
                           &merged, &merged_t) != 0) {
            free_merged_rows(&merged);
            free_merged_columns(&merged_t);
            return -1;
        }
    }

    /* subsequent joins: build a temp table from merged results, join with next */
    for (size_t jn = 1; jn < s->joins.count; jn++) {
        ji = &s->joins.items[jn];
        struct table *tn = db_find_table_sv(db, ji->join_table);
        if (!tn) {
            fprintf(stderr, "table '" SV_FMT "' not found\n", SV_ARG(ji->join_table));
            free_merged_rows(&merged);
            free_merged_columns(&merged_t);
            return -1;
        }

        /* use merged_t as the left table descriptor */
        merged_t.rows.items = merged.data;
        merged_t.rows.count = merged.count;
        merged_t.rows.capacity = merged.count;

        resolve_join_cols(ji, &merged_t, tn);

        struct rows next_merged = {0};
        struct table next_meta = {0};
        da_init(&next_meta.columns);
        da_init(&next_meta.rows);
        da_init(&next_meta.indexes);
        char jn_alias_buf[128] = {0};
        if (ji->join_alias.len > 0)
            snprintf(jn_alias_buf, sizeof(jn_alias_buf), "%.*s", (int)ji->join_alias.len, ji->join_alias.data);
        if (do_single_join(&merged_t, NULL, tn, jn_alias_buf[0] ? jn_alias_buf : NULL,
                           ji->join_left_col, ji->join_right_col, ji->join_type,
                           &next_merged, &next_meta) != 0) {
            free_merged_rows(&merged);
            free_merged_columns(&merged_t);
            return -1;
        }

        /* free old merged rows (do_single_join copied cells) */
        for (size_t i = 0; i < merged.count; i++)
            row_free(&merged.data[i]);
        free(merged.data);
        free_merged_columns(&merged_t);

        merged = next_merged;
        merged_t = next_meta;
    }

    /* WHERE filter — compact in-place */
    if (s->where.has_where && s->where.where_cond) {
        size_t write = 0;
        for (size_t i = 0; i < merged.count; i++) {
            merged_t.rows.items = merged.data;
            merged_t.rows.count = merged.count;
            if (eval_condition(s->where.where_cond, &merged.data[i], &merged_t)) {
                if (write != i)
                    merged.data[write] = merged.data[i];
                write++;
            } else {
                row_free(&merged.data[i]);
            }
        }
        merged.count = write;
    }

    /* ORDER BY (multi-column) */
    if (s->has_order_by && s->order_by_items.count > 0 && merged.count > 1) {
        int ord_cols[32];
        int ord_descs[32];
        size_t nord = s->order_by_items.count < 32 ? s->order_by_items.count : 32;
        for (size_t k = 0; k < nord; k++) {
            sv ordcol = s->order_by_items.items[k].column;
            ord_cols[k] = -1;
            /* try exact match first (for aliased columns like b.name) */
            for (size_t c = 0; c < merged_t.columns.count; c++) {
                if (sv_eq_cstr(ordcol, merged_t.columns.items[c].name)) {
                    ord_cols[k] = (int)c; break;
                }
            }
            /* fallback: strip prefix and match base name */
            if (ord_cols[k] < 0) {
                /* extract base column name from sv (after last '.') */
                sv base_sv = ordcol;
                for (size_t p = 0; p < ordcol.len; p++) {
                    if (ordcol.data[p] == '.') {
                        base_sv = sv_from(ordcol.data + p + 1, ordcol.len - p - 1);
                    }
                }
                for (size_t c = 0; c < merged_t.columns.count; c++) {
                    const char *dot = strrchr(merged_t.columns.items[c].name, '.');
                    const char *base = dot ? dot + 1 : merged_t.columns.items[c].name;
                    if (sv_eq_cstr(base_sv, base)) {
                        ord_cols[k] = (int)c; break;
                    }
                }
            }
            ord_descs[k] = s->order_by_items.items[k].desc;
        }
        _jsort_ctx = (struct join_sort_ctx){ .cols = ord_cols, .descs = ord_descs, .ncols = nord };
        qsort(merged.data, merged.count, sizeof(struct row), cmp_rows_join);
    }

    /* OFFSET / LIMIT */
    size_t start = 0, end = merged.count;
    if (s->has_offset) {
        start = (size_t)s->offset_count;
        if (start > merged.count) start = merged.count;
    }
    if (s->has_limit) {
        size_t lim = (size_t)s->limit_count;
        if (start + lim < end) end = start + lim;
    }

    /* project selected columns from merged rows */
    int select_all = sv_eq_cstr(s->columns, "*");
    for (size_t i = start; i < end; i++) {
        struct row dst = {0};
        da_init(&dst.cells);

        if (select_all) {
            for (size_t c = 0; c < merged.data[i].cells.count; c++) {
                struct cell cp; cell_copy(&cp, &merged.data[i].cells.items[c]);
                da_push(&dst.cells, cp);
            }
        } else {
            sv cols = s->columns;
            while (cols.len > 0) {
                size_t cend = 0;
                while (cend < cols.len && cols.data[cend] != ',') cend++;
                sv one = sv_trim(sv_from(cols.data, cend));

                /* get full column reference (e.g. "a.name") */
                char fullbuf[256];
                snprintf(fullbuf, sizeof(fullbuf), "%.*s", (int)one.len, one.data);
                /* strip trailing whitespace */
                size_t fl = strlen(fullbuf);
                while (fl > 0 && fullbuf[fl-1] == ' ') fullbuf[--fl] = '\0';
                /* strip " AS alias" */
                char *as_pos = strstr(fullbuf, " AS ");
                if (!as_pos) as_pos = strstr(fullbuf, " as ");
                if (as_pos) *as_pos = '\0';
                fl = strlen(fullbuf);
                while (fl > 0 && fullbuf[fl-1] == ' ') fullbuf[--fl] = '\0';

                /* try exact match first (handles aliased columns like "a.name") */
                int idx = -1;
                for (size_t c = 0; c < merged_t.columns.count; c++) {
                    if (strcmp(merged_t.columns.items[c].name, fullbuf) == 0) {
                        idx = (int)c; break;
                    }
                }
                /* fallback: strip table/alias prefix and match base name */
                if (idx < 0) {
                    char cbuf[256];
                    const char *cname = extract_col_name(one, cbuf, sizeof(cbuf));
                    char clean[256];
                    strncpy(clean, cname, sizeof(clean) - 1);
                    clean[sizeof(clean) - 1] = '\0';
                    as_pos = strstr(clean, " AS ");
                    if (!as_pos) as_pos = strstr(clean, " as ");
                    if (as_pos) *as_pos = '\0';
                    size_t cl = strlen(clean);
                    while (cl > 0 && clean[cl-1] == ' ') clean[--cl] = '\0';
                    for (size_t c = 0; c < merged_t.columns.count; c++) {
                        /* match against base part of qualified column name */
                        const char *dot = strrchr(merged_t.columns.items[c].name, '.');
                        const char *base = dot ? dot + 1 : merged_t.columns.items[c].name;
                        if (strcmp(base, clean) == 0) {
                            idx = (int)c; break;
                        }
                    }
                }
                if (idx >= 0) {
                    struct cell cp; cell_copy(&cp, &merged.data[i].cells.items[idx]);
                    da_push(&dst.cells, cp);
                }

                if (cend < cols.len) cend++;
                cols = sv_from(cols.data + cend, cols.len - cend);
            }
        }

        rows_push(result, dst);
    }

    /* free merged rows */
    free_merged_rows(&merged);
    free_merged_columns(&merged_t);

    return 0;
}

/* recursively resolve subqueries in condition tree */
static void resolve_subqueries(struct database *db, struct condition *c)
{
    if (!c) return;
    if (c->type == COND_AND || c->type == COND_OR) {
        resolve_subqueries(db, c->left);
        resolve_subqueries(db, c->right);
        return;
    }
    if (c->type == COND_NOT) {
        resolve_subqueries(db, c->left);
        return;
    }
    /* EXISTS / NOT EXISTS subquery */
    if (c->type == COND_COMPARE && c->subquery_sql &&
        (c->op == CMP_EXISTS || c->op == CMP_NOT_EXISTS)) {
        struct query sq = {0};
        if (query_parse(c->subquery_sql, &sq) == 0) {
            struct rows sq_result = {0};
            if (db_exec(db, &sq, &sq_result) == 0) {
                c->value.type = COLUMN_TYPE_INT;
                c->value.value.as_int = (sq_result.count > 0) ? 1 : 0;
                for (size_t i = 0; i < sq_result.count; i++)
                    row_free(&sq_result.data[i]);
                free(sq_result.data);
            }
        }
        query_free(&sq);
        free(c->subquery_sql);
        c->subquery_sql = NULL;
        return;
    }
    if (c->type == COND_COMPARE && c->subquery_sql &&
        (c->op == CMP_IN || c->op == CMP_NOT_IN)) {
        struct query sq = {0};
        if (query_parse(c->subquery_sql, &sq) == 0) {
            struct rows sq_result = {0};
            if (db_exec(db, &sq, &sq_result) == 0) {
                /* free any pre-existing in_values from the parser */
                for (size_t i = 0; i < c->in_values.count; i++)
                    cell_free_text(&c->in_values.items[i]);
                da_free(&c->in_values);
                da_init(&c->in_values);
                for (size_t i = 0; i < sq_result.count; i++) {
                    if (sq_result.data[i].cells.count > 0) {
                        struct cell v = {0};
                        struct cell *src = &sq_result.data[i].cells.items[0];
                        v.type = src->type;
                        v.is_null = src->is_null;
                        if (column_type_is_text(src->type) && src->value.as_text)
                            v.value.as_text = strdup(src->value.as_text);
                        else
                            v.value = src->value;
                        da_push(&c->in_values, v);
                    }
                }
                for (size_t i = 0; i < sq_result.count; i++)
                    row_free(&sq_result.data[i]);
                free(sq_result.data);
            }
        }
        query_free(&sq);
        free(c->subquery_sql);
        c->subquery_sql = NULL;
    }
    /* scalar subquery: WHERE col > (SELECT ...) */
    if (c->type == COND_COMPARE && c->scalar_subquery_sql) {
        struct query sq = {0};
        if (query_parse(c->scalar_subquery_sql, &sq) == 0) {
            struct rows sq_result = {0};
            if (db_exec(db, &sq, &sq_result) == 0) {
                if (sq_result.count > 0 && sq_result.data[0].cells.count > 0) {
                    /* free old value before overwriting */
                    cell_free_text(&c->value);
                    struct cell *src = &sq_result.data[0].cells.items[0];
                    c->value.type = src->type;
                    c->value.is_null = src->is_null;
                    if (column_type_is_text(src->type) && src->value.as_text)
                        c->value.value.as_text = strdup(src->value.as_text);
                    else
                        c->value.value = src->value;
                }
                for (size_t i = 0; i < sq_result.count; i++)
                    row_free(&sq_result.data[i]);
                free(sq_result.data);
            }
        }
        query_free(&sq);
        free(c->scalar_subquery_sql);
        c->scalar_subquery_sql = NULL;
    }
}

static void snapshot_free(struct db_snapshot *snap)
{
    for (size_t i = 0; i < snap->tables.count; i++)
        table_free(&snap->tables.items[i]);
    da_free(&snap->tables);
    for (size_t i = 0; i < snap->types.count; i++)
        enum_type_free(&snap->types.items[i]);
    da_free(&snap->types);
    free(snap);
}

static struct db_snapshot *snapshot_create(struct database *db)
{
    struct db_snapshot *snap = calloc(1, sizeof(*snap));
    da_init(&snap->tables);
    da_init(&snap->types);
    for (size_t i = 0; i < db->tables.count; i++) {
        struct table t;
        table_deep_copy(&t, &db->tables.items[i]);
        da_push(&snap->tables, t);
    }
    for (size_t i = 0; i < db->types.count; i++) {
        struct enum_type et;
        et.name = strdup(db->types.items[i].name);
        da_init(&et.values);
        for (size_t j = 0; j < db->types.items[i].values.count; j++)
            da_push(&et.values, strdup(db->types.items[i].values.items[j]));
        da_push(&snap->types, et);
    }
    return snap;
}

static void snapshot_restore(struct database *db, struct db_snapshot *snap)
{
    /* free current state */
    for (size_t i = 0; i < db->tables.count; i++)
        table_free(&db->tables.items[i]);
    da_free(&db->tables);
    for (size_t i = 0; i < db->types.count; i++)
        enum_type_free(&db->types.items[i]);
    da_free(&db->types);

    /* move snapshot data into db (memcpy because DYNAMIC_ARRAY creates anonymous struct types) */
    memcpy(&db->tables, &snap->tables, sizeof(db->tables));
    memcpy(&db->types, &snap->types, sizeof(db->types));
    /* zero out snap arrays so snapshot_free won't double-free */
    memset(&snap->tables, 0, sizeof(snap->tables));
    memset(&snap->types, 0, sizeof(snap->types));
}

/* Materialize a subquery into a temporary table added to db->tables.
 * Returns a pointer to the new table, or NULL on failure.
 * The caller is responsible for removing the table when done. */
static struct table *materialize_subquery(struct database *db, const char *sql,
                                          const char *table_name)
{
    struct query sq = {0};
    if (query_parse(sql, &sq) != 0) {
        query_free(&sq);
        return NULL;
    }
    struct rows sq_rows = {0};
    if (db_exec(db, &sq, &sq_rows) != 0) {
        query_free(&sq);
        return NULL;
    }
    struct table ct = {0};
    ct.name = strdup(table_name);
    da_init(&ct.columns);
    da_init(&ct.rows);
    da_init(&ct.indexes);
    /* infer column names */
    if (sq.query_type == QUERY_TYPE_SELECT && sq_rows.count > 0) {
        size_t ncells = sq_rows.data[0].cells.count;
        struct table *src_t = db_find_table_sv(db, sq.select.table);
        if (src_t && sv_eq_cstr(sq.select.columns, "*")) {
            for (size_t c = 0; c < src_t->columns.count; c++) {
                struct column col = {0};
                col.name = strdup(src_t->columns.items[c].name);
                col.type = src_t->columns.items[c].type;
                da_push(&ct.columns, col);
            }
        } else if (src_t) {
            sv cols = sq.select.columns;
            size_t ci = 0;
            while (cols.len > 0 && ci < ncells) {
                while (cols.len > 0 && (cols.data[0] == ' ' || cols.data[0] == '\t'))
                    { cols.data++; cols.len--; }
                size_t end = 0;
                while (end < cols.len && cols.data[end] != ',') end++;
                sv col_sv = sv_from(cols.data, end);
                while (col_sv.len > 0 && (col_sv.data[col_sv.len-1] == ' ' || col_sv.data[col_sv.len-1] == '\t'))
                    col_sv.len--;
                sv col_name = col_sv;
                for (size_t k = 0; k + 2 < col_sv.len; k++) {
                    if ((col_sv.data[k] == ' ' || col_sv.data[k] == '\t') &&
                        (col_sv.data[k+1] == 'A' || col_sv.data[k+1] == 'a') &&
                        (col_sv.data[k+2] == 'S' || col_sv.data[k+2] == 's') &&
                        (k + 3 >= col_sv.len || col_sv.data[k+3] == ' ' || col_sv.data[k+3] == '\t')) {
                        size_t alias_start = k + 3;
                        while (alias_start < col_sv.len && (col_sv.data[alias_start] == ' ' || col_sv.data[alias_start] == '\t'))
                            alias_start++;
                        col_name = sv_from(col_sv.data + alias_start, col_sv.len - alias_start);
                        break;
                    }
                }
                for (size_t k = 0; k < col_name.len; k++) {
                    if (col_name.data[k] == '.') {
                        col_name = sv_from(col_name.data + k + 1, col_name.len - k - 1);
                        break;
                    }
                }
                struct column col = {0};
                col.name = sv_to_cstr(col_name);
                col.type = sq_rows.data[0].cells.items[ci].type;
                da_push(&ct.columns, col);
                ci++;
                if (end < cols.len) { cols.data += end + 1; cols.len -= end + 1; }
                else break;
            }
            for (size_t a = 0; a < sq.select.aggregates.count && ci < ncells; a++, ci++) {
                struct column col = {0};
                if (sq.select.aggregates.items[a].alias.len > 0)
                    col.name = sv_to_cstr(sq.select.aggregates.items[a].alias);
                else
                    col.name = sv_to_cstr(sq.select.aggregates.items[a].column);
                col.type = sq_rows.data[0].cells.items[ci].type;
                da_push(&ct.columns, col);
            }
        }
        /* fallback */
        if (ct.columns.count == 0) {
            for (size_t c = 0; c < ncells; c++) {
                struct column col = {0};
                char buf[32];
                snprintf(buf, sizeof(buf), "column%zu", c + 1);
                col.name = strdup(buf);
                col.type = sq_rows.data[0].cells.items[c].type;
                da_push(&ct.columns, col);
            }
        }
    }
    /* copy rows */
    for (size_t i = 0; i < sq_rows.count; i++) {
        struct row r = {0};
        da_init(&r.cells);
        for (size_t c = 0; c < sq_rows.data[i].cells.count; c++) {
            struct cell cp;
            cell_copy(&cp, &sq_rows.data[i].cells.items[c]);
            da_push(&r.cells, cp);
        }
        da_push(&ct.rows, r);
    }
    for (size_t i = 0; i < sq_rows.count; i++)
        row_free(&sq_rows.data[i]);
    free(sq_rows.data);
    query_free(&sq);
    da_push(&db->tables, ct);
    return &db->tables.items[db->tables.count - 1];
}

/* Remove a temporary table from the database by pointer */
static void remove_temp_table(struct database *db, struct table *t)
{
    if (!t) return;
    for (size_t i = 0; i < db->tables.count; i++) {
        if (&db->tables.items[i] == t) {
            table_free(&db->tables.items[i]);
            for (size_t j = i; j + 1 < db->tables.count; j++)
                db->tables.items[j] = db->tables.items[j + 1];
            db->tables.count--;
            return;
        }
    }
}

int db_exec(struct database *db, struct query *q, struct rows *result)
{
    /* handle transaction statements first */
    if (q->query_type == QUERY_TYPE_BEGIN) {
        if (db->in_transaction) {
            fprintf(stderr, "WARNING: already in a transaction\n");
            return 0;
        }
        db->in_transaction = 1;
        db->snapshot = snapshot_create(db);
        return 0;
    }
    if (q->query_type == QUERY_TYPE_COMMIT) {
        if (!db->in_transaction) {
            fprintf(stderr, "WARNING: no transaction in progress\n");
            return 0;
        }
        db->in_transaction = 0;
        snapshot_free(db->snapshot);
        db->snapshot = NULL;
        return 0;
    }
    if (q->query_type == QUERY_TYPE_ROLLBACK) {
        if (!db->in_transaction) {
            fprintf(stderr, "WARNING: no transaction in progress\n");
            return 0;
        }
        snapshot_restore(db, db->snapshot);
        snapshot_free(db->snapshot);
        db->snapshot = NULL;
        db->in_transaction = 0;
        return 0;
    }

    switch (q->query_type) {
        case QUERY_TYPE_CREATE: {
            struct query_create_table *crt = &q->create_table;
            struct table t;
            table_init_own(&t, sv_to_cstr(crt->table));
            for (size_t i = 0; i < crt->create_columns.count; i++) {
                table_add_column(&t, &crt->create_columns.items[i]);
            }
            da_push(&db->tables, t);
            return 0;
        }
        case QUERY_TYPE_DROP: {
            sv drop_tbl = q->drop_table.table;
            for (size_t i = 0; i < db->tables.count; i++) {
                if (sv_eq_cstr(drop_tbl, db->tables.items[i].name)) {
                    table_free(&db->tables.items[i]);
                    /* shift remaining tables down */
                    for (size_t j = i; j + 1 < db->tables.count; j++) {
                        db->tables.items[j] = db->tables.items[j + 1];
                    }
                    db->tables.count--;
                    return 0;
                }
            }
            fprintf(stderr, "drop error: table '" SV_FMT "' not found\n", SV_ARG(drop_tbl));
            return -1;
        }
        case QUERY_TYPE_CREATE_TYPE: {
            struct query_create_type *ct = &q->create_type;
            if (db_find_type_sv(db, ct->type_name)) {
                fprintf(stderr, "type '" SV_FMT "' already exists\n", SV_ARG(ct->type_name));
                return -1;
            }
            struct enum_type et;
            et.name = sv_to_cstr(ct->type_name);
            da_init(&et.values);
            for (size_t i = 0; i < ct->enum_values.count; i++) {
                da_push(&et.values, ct->enum_values.items[i]);
                ct->enum_values.items[i] = NULL; /* transfer ownership */
            }
            da_push(&db->types, et);
            return 0;
        }
        case QUERY_TYPE_DROP_TYPE: {
            sv dtn = q->drop_type.type_name;
            for (size_t i = 0; i < db->types.count; i++) {
                if (sv_eq_cstr(dtn, db->types.items[i].name)) {
                    enum_type_free(&db->types.items[i]);
                    for (size_t j = i; j + 1 < db->types.count; j++)
                        db->types.items[j] = db->types.items[j + 1];
                    db->types.count--;
                    return 0;
                }
            }
            fprintf(stderr, "type '" SV_FMT "' not found\n", SV_ARG(dtn));
            return -1;
        }
        case QUERY_TYPE_CREATE_INDEX: {
            struct query_create_index *ci = &q->create_index;
            struct table *t = db_find_table_sv(db, ci->table);
            if (!t) {
                fprintf(stderr, "table '" SV_FMT "' not found\n", SV_ARG(ci->table));
                return -1;
            }
            int col_idx = table_find_column_sv(t, ci->index_column);
            if (col_idx < 0) {
                fprintf(stderr, "column '" SV_FMT "' not found in table '" SV_FMT "'\n",
                        SV_ARG(ci->index_column), SV_ARG(ci->table));
                return -1;
            }
            struct index idx;
            index_init_sv(&idx, ci->index_name, ci->index_column, col_idx);
            /* backfill existing rows */
            for (size_t i = 0; i < t->rows.count; i++) {
                if ((size_t)col_idx < t->rows.items[i].cells.count) {
                    index_insert(&idx, &t->rows.items[i].cells.items[col_idx], i);
                }
            }
            da_push(&t->indexes, idx);
            return 0;
        }
        case QUERY_TYPE_DROP_INDEX: {
            /* search all tables for the named index */
            for (size_t ti = 0; ti < db->tables.count; ti++) {
                struct table *t = &db->tables.items[ti];
                for (size_t ii = 0; ii < t->indexes.count; ii++) {
                    if (sv_eq_cstr(q->drop_index.index_name, t->indexes.items[ii].name)) {
                        index_free(&t->indexes.items[ii]);
                        for (size_t j = ii; j + 1 < t->indexes.count; j++)
                            t->indexes.items[j] = t->indexes.items[j + 1];
                        t->indexes.count--;
                        return 0;
                    }
                }
            }
            fprintf(stderr, "index '" SV_FMT "' not found\n", SV_ARG(q->drop_index.index_name));
            return -1;
        }
        case QUERY_TYPE_SELECT: {
            struct query_select *s = &q->select;
            /* CTE: create temporary tables from CTE definitions */
            struct table *cte_temps[32] = {0};
            size_t n_cte_temps = 0;

            /* multiple CTEs (new path) */
            if (s->ctes.count > 0) {
                for (size_t ci = 0; ci < s->ctes.count && n_cte_temps < 32; ci++) {
                    struct cte_def *cd = &s->ctes.items[ci];
                    if (cd->is_recursive) {
                        /* Recursive CTE: the SQL should be "base UNION ALL recursive"
                         * We execute the base case, create the temp table, then
                         * repeatedly execute the recursive part until no new rows. */
                        /* find UNION ALL separator */
                        const char *union_pos = NULL;
                        const char *p = cd->sql;
                        int depth = 0;
                        while (*p) {
                            if (*p == '(') depth++;
                            else if (*p == ')') depth--;
                            else if (depth == 0 && (p[0] == 'U' || p[0] == 'u')) {
                                if (strncasecmp(p, "UNION", 5) == 0 &&
                                    (p[5] == ' ' || p[5] == '\t' || p[5] == '\n')) {
                                    union_pos = p;
                                    break;
                                }
                            }
                            p++;
                        }
                        if (!union_pos) {
                            /* no UNION — just materialize normally */
                            cte_temps[n_cte_temps] = materialize_subquery(db, cd->sql, cd->name);
                            n_cte_temps++;
                            continue;
                        }
                        /* extract base SQL (before UNION) */
                        size_t base_len = (size_t)(union_pos - cd->sql);
                        char *base_sql = malloc(base_len + 1);
                        memcpy(base_sql, cd->sql, base_len);
                        base_sql[base_len] = '\0';
                        /* skip "UNION ALL " */
                        const char *rec_start = union_pos + 5;
                        while (*rec_start == ' ' || *rec_start == '\t' || *rec_start == '\n') rec_start++;
                        if (strncasecmp(rec_start, "ALL", 3) == 0) {
                            rec_start += 3;
                            while (*rec_start == ' ' || *rec_start == '\t' || *rec_start == '\n') rec_start++;
                        }
                        char *rec_sql = strdup(rec_start);

                        /* execute base case */
                        struct table *ct = materialize_subquery(db, base_sql, cd->name);
                        free(base_sql);
                        if (!ct) { free(rec_sql); continue; }
                        cte_temps[n_cte_temps++] = ct;

                        /* Collect all rows in an accumulator; the CTE table acts as
                         * the "working table" containing only the newest rows so the
                         * recursive query doesn't re-process old rows. */
                        /* save base rows into accumulator */
                        struct rows accum = {0};
                        for (size_t ri = 0; ri < ct->rows.count; ri++) {
                            struct row r = {0};
                            da_init(&r.cells);
                            for (size_t c = 0; c < ct->rows.items[ri].cells.count; c++) {
                                struct cell cp;
                                cell_copy(&cp, &ct->rows.items[ri].cells.items[c]);
                                da_push(&r.cells, cp);
                            }
                            rows_push(&accum, r);
                        }

                        for (int iter = 0; iter < 1000; iter++) {
                            /* ct currently holds only the working set (new rows) */
                            ct = db_find_table(db, cd->name);
                            if (!ct) break;
                            struct rows rec_rows = {0};
                            if (db_exec_sql(db, rec_sql, &rec_rows) != 0) break;
                            if (rec_rows.count == 0) { free(rec_rows.data); break; }

                            /* replace CTE table rows with only the new rows */
                            for (size_t ri = 0; ri < ct->rows.count; ri++)
                                row_free(&ct->rows.items[ri]);
                            ct->rows.count = 0;

                            for (size_t ri = 0; ri < rec_rows.count; ri++) {
                                /* add to accumulator */
                                struct row ar = {0};
                                da_init(&ar.cells);
                                for (size_t c = 0; c < rec_rows.data[ri].cells.count; c++) {
                                    struct cell cp;
                                    cell_copy(&cp, &rec_rows.data[ri].cells.items[c]);
                                    da_push(&ar.cells, cp);
                                }
                                rows_push(&accum, ar);
                                /* add to working table */
                                struct row wr = {0};
                                da_init(&wr.cells);
                                for (size_t c = 0; c < rec_rows.data[ri].cells.count; c++) {
                                    struct cell cp;
                                    cell_copy(&cp, &rec_rows.data[ri].cells.items[c]);
                                    da_push(&wr.cells, cp);
                                }
                                da_push(&ct->rows, wr);
                            }
                            for (size_t ri = 0; ri < rec_rows.count; ri++)
                                row_free(&rec_rows.data[ri]);
                            free(rec_rows.data);
                        }

                        /* replace CTE table rows with the full accumulator */
                        ct = db_find_table(db, cd->name);
                        if (ct) {
                            for (size_t ri = 0; ri < ct->rows.count; ri++)
                                row_free(&ct->rows.items[ri]);
                            ct->rows.count = 0;
                            for (size_t ri = 0; ri < accum.count; ri++)
                                da_push(&ct->rows, accum.data[ri]);
                            free(accum.data);
                        } else {
                            // TODO: MEMORY LEAK: if ct is NULL (table was removed between
                            // iterations), the accum rows are freed here, but any rows that
                            // were pushed into ct->rows during the loop iterations are now
                            // orphaned because ct was removed. This is an edge case but
                            // indicates fragile ownership of the working table rows.
                            for (size_t ri = 0; ri < accum.count; ri++)
                                row_free(&accum.data[ri]);
                            free(accum.data);
                        }
                        free(rec_sql);
                    } else {
                        cte_temps[n_cte_temps] = materialize_subquery(db, cd->sql, cd->name);
                        n_cte_temps++;
                    }
                }
            } else if (s->cte_name && s->cte_sql) {
                /* legacy single CTE */
                cte_temps[0] = materialize_subquery(db, s->cte_sql, s->cte_name);
                n_cte_temps = 1;
            }

            /* FROM subquery: create temp table */
            struct table *from_sub_table = NULL;
            if (s->from_subquery_sql) {
                char alias_buf[256];
                if (s->from_subquery_alias.len > 0) {
                    snprintf(alias_buf, sizeof(alias_buf), "%.*s",
                             (int)s->from_subquery_alias.len, s->from_subquery_alias.data);
                } else {
                    snprintf(alias_buf, sizeof(alias_buf), "_from_sub");
                }
                from_sub_table = materialize_subquery(db, s->from_subquery_sql, alias_buf);
            }

            /* resolve any IN (SELECT ...) / EXISTS subqueries */
            if (s->where.has_where && s->where.where_cond)
                resolve_subqueries(db, s->where.where_cond);
            int sel_rc;
            if (s->table.len == 0 && s->insert_row && result) {
                /* SELECT <literal> — no table, return literal values */
                struct row dst = {0};
                da_init(&dst.cells);
                for (size_t i = 0; i < s->insert_row->cells.count; i++) {
                    struct cell c = {0};
                    c.is_null = s->insert_row->cells.items[i].is_null;
                    c.type = s->insert_row->cells.items[i].type;
                    if (column_type_is_text(c.type)
                        && s->insert_row->cells.items[i].value.as_text)
                        c.value.as_text = strdup(s->insert_row->cells.items[i].value.as_text);
                    else
                        c.value = s->insert_row->cells.items[i].value;
                    da_push(&dst.cells, c);
                }
                rows_push(result, dst);
                sel_rc = 0;
            } else if (s->has_join) {
                sel_rc = exec_join(db, q, result);
            } else {
                sel_rc = db_table_exec_query(db, s->table, q, result);
            }

            /* UNION / INTERSECT / EXCEPT */
            // TODO: CONTAINER REUSE: the row-equality comparison loop (iterate cells,
            // call cell_equal) is duplicated three times below for UNION, INTERSECT, and
            // EXCEPT. Extract a rows_equal(row*, row*) helper into row.c.
            if (sel_rc == 0 && s->has_set_op && s->set_rhs_sql && result) {
                struct query rhs_q = {0};
                if (query_parse(s->set_rhs_sql, &rhs_q) == 0) {
                    struct rows rhs_rows = {0};
                    if (db_exec(db, &rhs_q, &rhs_rows) == 0) {
                        if (s->set_op == 0) {
                            // TODO: UNION duplicate check is O(n*m); could hash or sort
                            // for better performance on large result sets
                            /* UNION [ALL] — append RHS rows */
                            for (size_t i = 0; i < rhs_rows.count; i++) {
                                if (!s->set_all) {
                                    /* check for duplicates */
                                    int dup = 0;
                                    for (size_t j = 0; j < result->count; j++) {
                                        if (row_equal(&result->data[j], &rhs_rows.data[i])) { dup = 1; break; }
                                    }
                                    if (dup) { row_free(&rhs_rows.data[i]); continue; }
                                }
                                rows_push(result, rhs_rows.data[i]);
                                rhs_rows.data[i].cells.items = NULL;
                                rhs_rows.data[i].cells.count = 0;
                            }
                        } else if (s->set_op == 1) {
                            /* INTERSECT — keep only rows in both */
                            size_t w = 0;
                            for (size_t i = 0; i < result->count; i++) {
                                int found = 0;
                                for (size_t j = 0; j < rhs_rows.count; j++) {
                                    if (row_equal(&result->data[i], &rhs_rows.data[j])) { found = 1; break; }
                                }
                                if (found) {
                                    if (w != i) result->data[w] = result->data[i];
                                    w++;
                                } else {
                                    row_free(&result->data[i]);
                                }
                            }
                            result->count = w;
                        } else if (s->set_op == 2) {
                            /* EXCEPT — keep LHS rows not in RHS */
                            size_t w = 0;
                            for (size_t i = 0; i < result->count; i++) {
                                int found = 0;
                                for (size_t j = 0; j < rhs_rows.count; j++) {
                                    if (row_equal(&result->data[i], &rhs_rows.data[j])) { found = 1; break; }
                                }
                                if (!found) {
                                    if (w != i) result->data[w] = result->data[i];
                                    w++;
                                } else {
                                    row_free(&result->data[i]);
                                }
                            }
                            result->count = w;
                        }
                        for (size_t i = 0; i < rhs_rows.count; i++)
                            row_free(&rhs_rows.data[i]);
                        free(rhs_rows.data);
                    }
                    query_free(&rhs_q);
                }
            }

            /* ORDER BY on combined set operation result */
            if (sel_rc == 0 && s->has_set_op && s->set_order_by &&
                result && result->count > 1) {
                /* parse the ORDER BY clause stored as text */
                char ob_wrap[512];
                snprintf(ob_wrap, sizeof(ob_wrap), "SELECT x FROM t %s", s->set_order_by);
                struct query ob_q = {0};
                if (query_parse(ob_wrap, &ob_q) == 0 && ob_q.select.has_order_by &&
                    ob_q.select.order_by_items.count > 0) {
                    /* resolve column indices against the source table */
                    struct table *src_t = db_find_table_sv(db, s->table);
                    int ord_cols[32];
                    int ord_descs[32];
                    size_t nord = ob_q.select.order_by_items.count < 32 ? ob_q.select.order_by_items.count : 32;
                    for (size_t k = 0; k < nord; k++) {
                        ord_cols[k] = -1;
                        ord_descs[k] = ob_q.select.order_by_items.items[k].desc;
                        if (src_t) {
                            char obuf[256];
                            const char *ord_name = extract_col_name(
                                ob_q.select.order_by_items.items[k].column, obuf, sizeof(obuf));
                            ord_cols[k] = table_find_column(src_t, ord_name);
                            /* if not found, try resolving as a SELECT alias */
                            if (ord_cols[k] < 0 && s->columns.len > 0) {
                                ord_cols[k] = resolve_alias_to_column(src_t, s->columns,
                                    ob_q.select.order_by_items.items[k].column);
                            }
                        }
                    }
                    _jsort_ctx = (struct join_sort_ctx){ .cols = ord_cols, .descs = ord_descs, .ncols = nord };
                    qsort(result->data, result->count, sizeof(struct row), cmp_rows_join);
                }
                query_free(&ob_q);
            }

            /* clean up FROM subquery temp table */
            if (from_sub_table)
                remove_temp_table(db, from_sub_table);

            /* clean up CTE temp tables (reverse order to keep indices valid) */
            for (size_t ci = n_cte_temps; ci > 0; ci--) {
                if (cte_temps[ci - 1])
                    remove_temp_table(db, cte_temps[ci - 1]);
            }

            return sel_rc;
        }
        case QUERY_TYPE_INSERT: {
            struct query_insert *ins = &q->insert;
            /* INSERT ... SELECT */
            if (ins->insert_select_sql) {
                struct query sel_q = {0};
                if (query_parse(ins->insert_select_sql, &sel_q) != 0) return -1;
                struct rows sel_rows = {0};
                if (db_exec(db, &sel_q, &sel_rows) != 0) {
                    query_free(&sel_q);
                    return -1;
                }
                /* insert each result row into the target table */
                struct table *t = db_find_table_sv(db, ins->table);
                if (!t) {
                    fprintf(stderr, "table '" SV_FMT "' not found\n", SV_ARG(ins->table));
                    for (size_t i = 0; i < sel_rows.count; i++) row_free(&sel_rows.data[i]);
                    free(sel_rows.data);
                    query_free(&sel_q);
                    return -1;
                }
                for (size_t i = 0; i < sel_rows.count; i++) {
                    struct row r = {0};
                    da_init(&r.cells);
                    for (size_t c = 0; c < sel_rows.data[i].cells.count; c++) {
                        struct cell cp;
                        cell_copy(&cp, &sel_rows.data[i].cells.items[c]);
                        da_push(&r.cells, cp);
                    }
                    da_push(&t->rows, r);
                }
                int cnt = (int)sel_rows.count;
                for (size_t i = 0; i < sel_rows.count; i++) row_free(&sel_rows.data[i]);
                free(sel_rows.data);
                query_free(&sel_q);
                return cnt;
            }
            /* ON CONFLICT DO NOTHING — check for duplicate before insert */
            if (ins->has_on_conflict && ins->on_conflict_do_nothing) {
                struct table *t = db_find_table_sv(db, ins->table);
                if (t) {
                    int conflict_col = -1;
                    if (ins->conflict_column.len > 0)
                        conflict_col = table_find_column_sv(t, ins->conflict_column);
                    else {
                        /* find first UNIQUE or PRIMARY KEY column */
                        for (size_t c = 0; c < t->columns.count; c++) {
                            if (t->columns.items[c].is_unique || t->columns.items[c].is_primary_key) {
                                conflict_col = (int)c;
                                break;
                            }
                        }
                    }
                    if (conflict_col >= 0) {
                        /* filter out rows that conflict */
                        size_t orig_count = ins->insert_rows.count;
                        for (size_t ri = 0; ri < ins->insert_rows.count; ) {
                            struct cell *new_cell = &ins->insert_rows.items[ri].cells.items[conflict_col];
                            int conflict = 0;
                            for (size_t ei = 0; ei < t->rows.count; ei++) {
                                if (cell_equal(new_cell, &t->rows.items[ei].cells.items[conflict_col])) {
                                    conflict = 1;
                                    break;
                                }
                            }
                            if (conflict) {
                                /* skip this row — shift remaining */
                                row_free(&ins->insert_rows.items[ri]);
                                for (size_t j = ri; j + 1 < ins->insert_rows.count; j++)
                                    ins->insert_rows.items[j] = ins->insert_rows.items[j + 1];
                                ins->insert_rows.count--;
                                if (ins->insert_rows.count > 0)
                                    ins->insert_row = &ins->insert_rows.items[0];
                                else
                                    ins->insert_row = NULL;
                            } else {
                                ri++;
                            }
                        }
                        (void)orig_count;
                    }
                }
                if (ins->insert_rows.count == 0) return 0;
            }
            return db_table_exec_query(db, ins->table, q, result);
        }
        case QUERY_TYPE_DELETE:
            /* resolve subqueries in WHERE */
            if (q->del.where.has_where && q->del.where.where_cond)
                resolve_subqueries(db, q->del.where.where_cond);
            return db_table_exec_query(db, q->del.table, q, result);
        case QUERY_TYPE_UPDATE: {
            struct query_update *u = &q->update;
            /* resolve subqueries in WHERE */
            if (u->where.has_where && u->where.where_cond)
                resolve_subqueries(db, u->where.where_cond);
            /* UPDATE ... FROM — use parsed join columns */
            if (u->has_update_from) {
                struct table *t = db_find_table_sv(db, u->table);
                struct table *ft = db_find_table_sv(db, u->update_from_table);
                if (!t || !ft) {
                    fprintf(stderr, "update from: table not found\n");
                    return -1;
                }
                /* resolve join columns from the parsed WHERE t1.col = t2.col */
                int t_join_col = -1, ft_join_col = -1;
                if (u->update_from_join_left.len > 0 && u->update_from_join_right.len > 0) {
                    char lbuf[256], rbuf[256];
                    const char *lcol = extract_col_name(u->update_from_join_left, lbuf, sizeof(lbuf));
                    const char *rcol = extract_col_name(u->update_from_join_right, rbuf, sizeof(rbuf));
                    int l_in_t = table_find_column(t, lcol);
                    int l_in_ft = table_find_column(ft, lcol);
                    int r_in_t = table_find_column(t, rcol);
                    int r_in_ft = table_find_column(ft, rcol);
                    if (l_in_t >= 0 && r_in_ft >= 0) {
                        t_join_col = l_in_t; ft_join_col = r_in_ft;
                    } else if (l_in_ft >= 0 && r_in_t >= 0) {
                        t_join_col = r_in_t; ft_join_col = l_in_ft;
                    }
                }
                size_t updated = 0;
                for (size_t i = 0; i < t->rows.count; i++) {
                    int matched = 0;
                    for (size_t j = 0; j < ft->rows.count; j++) {
                        if (t_join_col >= 0 && ft_join_col >= 0) {
                            if (cell_equal(&t->rows.items[i].cells.items[t_join_col],
                                            &ft->rows.items[j].cells.items[ft_join_col])) {
                                matched = 1;
                                break;
                            }
                        }
                    }
                    if (!matched) continue;
                    updated++;
                    for (size_t sc = 0; sc < u->set_clauses.count; sc++) {
                        int ci = table_find_column_sv(t, u->set_clauses.items[sc].column);
                        if (ci < 0) continue;
                        if (column_type_is_text(t->rows.items[i].cells.items[ci].type)
                            && t->rows.items[i].cells.items[ci].value.as_text)
                            free(t->rows.items[i].cells.items[ci].value.as_text);
                        t->rows.items[i].cells.items[ci].type = u->set_clauses.items[sc].value.type;
                        if (column_type_is_text(u->set_clauses.items[sc].value.type)
                            && u->set_clauses.items[sc].value.value.as_text)
                            t->rows.items[i].cells.items[ci].value.as_text =
                                strdup(u->set_clauses.items[sc].value.value.as_text);
                        else
                            t->rows.items[i].cells.items[ci].value = u->set_clauses.items[sc].value.value;
                        t->rows.items[i].cells.items[ci].is_null = 0;
                    }
                }
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
            return db_table_exec_query(db, u->table, q, result);
        }
        case QUERY_TYPE_ALTER: {
            struct query_alter *a = &q->alter;
            struct table *t = db_find_table_sv(db, a->table);
            if (!t) {
                fprintf(stderr, "alter error: table '" SV_FMT "' not found\n", SV_ARG(a->table));
                return -1;
            }
            switch (a->alter_action) {
                case ALTER_ADD_COLUMN: {
                    table_add_column(t, &a->alter_new_col);
                    /* pad existing rows with NULL for the new column */
                    size_t new_idx = t->columns.count - 1;
                    for (size_t i = 0; i < t->rows.count; i++) {
                        struct cell null_cell = {0};
                        null_cell.type = t->columns.items[new_idx].type;
                        null_cell.is_null = 1;
                        da_push(&t->rows.items[i].cells, null_cell);
                    }
                    return 0;
                }
                case ALTER_DROP_COLUMN: {
                    int col_idx = table_find_column_sv(t, a->alter_column);
                    if (col_idx < 0) {
                        fprintf(stderr, "alter error: column '" SV_FMT "' not found\n", SV_ARG(a->alter_column));
                        return -1;
                    }
                    /* remove column from schema */
                    free(t->columns.items[col_idx].name);
                    free(t->columns.items[col_idx].enum_type_name);
                    if (t->columns.items[col_idx].default_value) {
                        if (column_type_is_text(t->columns.items[col_idx].default_value->type)
                            && t->columns.items[col_idx].default_value->value.as_text)
                            free(t->columns.items[col_idx].default_value->value.as_text);
                        free(t->columns.items[col_idx].default_value);
                    }
                    for (size_t j = (size_t)col_idx; j + 1 < t->columns.count; j++)
                        t->columns.items[j] = t->columns.items[j + 1];
                    t->columns.count--;
                    /* remove cell from each row */
                    for (size_t i = 0; i < t->rows.count; i++) {
                        struct row *r = &t->rows.items[i];
                        if ((size_t)col_idx < r->cells.count) {
                            cell_free_text(&r->cells.items[col_idx]);
                            for (size_t j = (size_t)col_idx; j + 1 < r->cells.count; j++)
                                r->cells.items[j] = r->cells.items[j + 1];
                            r->cells.count--;
                        }
                    }
                    return 0;
                }
                case ALTER_RENAME_COLUMN: {
                    int col_idx = table_find_column_sv(t, a->alter_column);
                    if (col_idx < 0) {
                        fprintf(stderr, "alter error: column '" SV_FMT "' not found\n", SV_ARG(a->alter_column));
                        return -1;
                    }
                    free(t->columns.items[col_idx].name);
                    t->columns.items[col_idx].name = sv_to_cstr(a->alter_new_name);
                    return 0;
                }
                case ALTER_COLUMN_TYPE: {
                    int col_idx = table_find_column_sv(t, a->alter_column);
                    if (col_idx < 0) {
                        fprintf(stderr, "alter error: column '" SV_FMT "' not found\n", SV_ARG(a->alter_column));
                        return -1;
                    }
                    t->columns.items[col_idx].type = a->alter_new_col.type;
                    return 0;
                }
            }
            return -1;
        }
        case QUERY_TYPE_BEGIN:
        case QUERY_TYPE_COMMIT:
        case QUERY_TYPE_ROLLBACK:
            return 0; /* handled above */
    }
    return -1;
}

int db_exec_sql(struct database *db, const char *sql, struct rows *result)
{
    struct query q = {0};
    if (query_parse(sql, &q) != 0) return -1;
    int rc = db_exec(db, &q, result);
    query_free(&q);
    return rc;
}

void db_free(struct database *db)
{
    free(db->name);
    for (size_t i = 0; i < db->tables.count; i++) {
        table_free(&db->tables.items[i]);
    }
    da_free(&db->tables);
    for (size_t i = 0; i < db->types.count; i++) {
        enum_type_free(&db->types.items[i]);
    }
    da_free(&db->types);
}
