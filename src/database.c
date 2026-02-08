#include "database.h"
#include "parser.h"
#include "query.h"
#include "stringview.h"
#include <string.h>
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

static int find_column_index(struct table *t, const char *name)
{
    for (size_t i = 0; i < t->columns.count; i++) {
        if (strcmp(t->columns.items[i].name, name) == 0)
            return (int)i;
    }
    return -1;
}

static int find_column_index_sv(struct table *t, sv name)
{
    for (size_t i = 0; i < t->columns.count; i++) {
        if (sv_eq_cstr(name, t->columns.items[i].name))
            return (int)i;
    }
    return -1;
}

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

static void free_cell_text_ext(struct cell *c)
{
    if (column_type_is_text(c->type) && c->value.as_text)
        free(c->value.as_text);
}

static void copy_cell(struct cell *dst, const struct cell *src)
{
    dst->type = src->type;
    dst->is_null = src->is_null;
    if (column_type_is_text(src->type) && src->value.as_text) {
        dst->value.as_text = strdup(src->value.as_text);
    } else {
        dst->value = src->value;
    }
}

static int cell_compare_join(const struct cell *a, const struct cell *b)
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
        case COLUMN_TYPE_BOOLEAN:
            if (a->value.as_bool < b->value.as_bool) return -1;
            if (a->value.as_bool > b->value.as_bool) return  1;
            return 0;
        case COLUMN_TYPE_BIGINT:
            if (a->value.as_bigint < b->value.as_bigint) return -1;
            if (a->value.as_bigint > b->value.as_bigint) return  1;
            return 0;
        case COLUMN_TYPE_NUMERIC:
            if (a->value.as_numeric < b->value.as_numeric) return -1;
            if (a->value.as_numeric > b->value.as_numeric) return  1;
            return 0;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_UUID:
            if (!a->value.as_text && !b->value.as_text) return 0;
            if (!a->value.as_text) return -1;
            if (!b->value.as_text) return  1;
            return strcmp(a->value.as_text, b->value.as_text);
    }
    return 0;
}

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
        int cmp = cell_compare_join(&ra->cells.items[ci], &rb->cells.items[ci]);
        if (_jsort_ctx.descs[k]) cmp = -cmp;
        if (cmp != 0) return cmp;
    }
    return 0;
}

static int cells_equal(const struct cell *a, const struct cell *b)
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
        case COLUMN_TYPE_INT:     return a->value.as_int == b->value.as_int;
        case COLUMN_TYPE_FLOAT:   return a->value.as_float == b->value.as_float;
        case COLUMN_TYPE_BOOLEAN: return a->value.as_bool == b->value.as_bool;
        case COLUMN_TYPE_BIGINT:  return a->value.as_bigint == b->value.as_bigint;
        case COLUMN_TYPE_NUMERIC: return a->value.as_numeric == b->value.as_numeric;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_UUID:
            if (!a->value.as_text || !b->value.as_text) return a->value.as_text == b->value.as_text;
            return strcmp(a->value.as_text, b->value.as_text) == 0;
    }
    return 0;
}

static void emit_merged_row(struct row *r1, size_t ncols1,
                            struct row *r2, size_t ncols2,
                            struct rows *out)
{
    struct row full = {0};
    da_init(&full.cells);
    for (size_t c = 0; c < ncols1; c++) {
        struct cell cp; copy_cell(&cp, &r1->cells.items[c]);
        da_push(&full.cells, cp);
    }
    for (size_t c = 0; c < ncols2; c++) {
        struct cell cp; copy_cell(&cp, &r2->cells.items[c]);
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
        struct cell cp; copy_cell(&cp, &r1->cells.items[c]);
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
        struct cell cp; copy_cell(&cp, &r2->cells.items[c]);
        da_push(&full.cells, cp);
    }
    rows_push(out, full);
}

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

    int left_in_t1 = (find_column_index(t1, left_col) >= 0);
    int t1_join_col = left_in_t1 ? find_column_index(t1, left_col) : find_column_index(t1, right_col);
    int t2_join_col = left_in_t1 ? find_column_index(t2, right_col) : find_column_index(t2, left_col);

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
            if (!cells_equal(&r1->cells.items[t1_join_col], &r2->cells.items[t2_join_col]))
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
    struct table *t1 = db_find_table_sv(db, q->table);
    if (!t1) { fprintf(stderr, "table '" SV_FMT "' not found\n", SV_ARG(q->table)); return -1; }

    /* build merged table through successive joins */
    struct table merged_t = {0};
    da_init(&merged_t.columns);
    da_init(&merged_t.rows);
    da_init(&merged_t.indexes);
    struct rows merged = {0};

    /* first join */
    struct join_info *ji = &q->joins.items[0];
    struct table *t2 = db_find_table_sv(db, ji->join_table);
    if (!t2) { fprintf(stderr, "table '" SV_FMT "' not found\n", SV_ARG(ji->join_table)); return -1; }
    resolve_join_cols(ji, t1, t2);
    /* convert aliases to C strings for merged column naming */
    char t1_alias_buf[128] = {0};
    if (q->table_alias.len > 0)
        snprintf(t1_alias_buf, sizeof(t1_alias_buf), "%.*s", (int)q->table_alias.len, q->table_alias.data);
    char t2_alias_buf[128] = {0};
    if (ji->join_alias.len > 0)
        snprintf(t2_alias_buf, sizeof(t2_alias_buf), "%.*s", (int)ji->join_alias.len, ji->join_alias.data);
    const char *a1 = t1_alias_buf[0] ? t1_alias_buf : NULL;
    const char *a2 = t2_alias_buf[0] ? t2_alias_buf : NULL;
    if (do_single_join(t1, a1, t2, a2, ji->join_left_col, ji->join_right_col, ji->join_type,
                       &merged, &merged_t) != 0) return -1;

    /* subsequent joins: build a temp table from merged results, join with next */
    for (size_t jn = 1; jn < q->joins.count; jn++) {
        ji = &q->joins.items[jn];
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
    if (q->has_where && q->where_cond) {
        size_t write = 0;
        for (size_t i = 0; i < merged.count; i++) {
            merged_t.rows.items = merged.data;
            merged_t.rows.count = merged.count;
            if (eval_condition(q->where_cond, &merged.data[i], &merged_t)) {
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
    if (q->has_order_by && q->order_by_items.count > 0 && merged.count > 1) {
        int ord_cols[32];
        int ord_descs[32];
        size_t nord = q->order_by_items.count < 32 ? q->order_by_items.count : 32;
        for (size_t k = 0; k < nord; k++) {
            sv ordcol = q->order_by_items.items[k].column;
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
            ord_descs[k] = q->order_by_items.items[k].desc;
        }
        _jsort_ctx = (struct join_sort_ctx){ .cols = ord_cols, .descs = ord_descs, .ncols = nord };
        qsort(merged.data, merged.count, sizeof(struct row), cmp_rows_join);
    }

    /* OFFSET / LIMIT */
    size_t start = 0, end = merged.count;
    if (q->has_offset) {
        start = (size_t)q->offset_count;
        if (start > merged.count) start = merged.count;
    }
    if (q->has_limit) {
        size_t lim = (size_t)q->limit_count;
        if (start + lim < end) end = start + lim;
    }

    /* project selected columns from merged rows */
    int select_all = sv_eq_cstr(q->columns, "*");
    for (size_t i = start; i < end; i++) {
        struct row dst = {0};
        da_init(&dst.cells);

        if (select_all) {
            for (size_t c = 0; c < merged.data[i].cells.count; c++) {
                struct cell cp; copy_cell(&cp, &merged.data[i].cells.items[c]);
                da_push(&dst.cells, cp);
            }
        } else {
            sv cols = q->columns;
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
                    struct cell cp; copy_cell(&cp, &merged.data[i].cells.items[idx]);
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
            query_free(&sq);
        }
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
            query_free(&sq);
        }
        free(c->subquery_sql);
        c->subquery_sql = NULL;
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
            struct table t;
            table_init_own(&t, sv_to_cstr(q->table));
            for (size_t i = 0; i < q->create_columns.count; i++) {
                table_add_column(&t, &q->create_columns.items[i]);
            }
            da_push(&db->tables, t);
            return 0;
        }
        case QUERY_TYPE_DROP: {
            for (size_t i = 0; i < db->tables.count; i++) {
                if (sv_eq_cstr(q->table, db->tables.items[i].name)) {
                    table_free(&db->tables.items[i]);
                    /* shift remaining tables down */
                    for (size_t j = i; j + 1 < db->tables.count; j++) {
                        db->tables.items[j] = db->tables.items[j + 1];
                    }
                    db->tables.count--;
                    return 0;
                }
            }
            fprintf(stderr, "drop error: table '" SV_FMT "' not found\n", SV_ARG(q->table));
            return -1;
        }
        case QUERY_TYPE_CREATE_TYPE: {
            if (db_find_type_sv(db, q->type_name)) {
                fprintf(stderr, "type '" SV_FMT "' already exists\n", SV_ARG(q->type_name));
                return -1;
            }
            struct enum_type et;
            et.name = sv_to_cstr(q->type_name);
            da_init(&et.values);
            for (size_t i = 0; i < q->enum_values.count; i++) {
                da_push(&et.values, q->enum_values.items[i]);
                q->enum_values.items[i] = NULL; /* transfer ownership */
            }
            da_push(&db->types, et);
            return 0;
        }
        case QUERY_TYPE_DROP_TYPE: {
            for (size_t i = 0; i < db->types.count; i++) {
                if (sv_eq_cstr(q->type_name, db->types.items[i].name)) {
                    enum_type_free(&db->types.items[i]);
                    for (size_t j = i; j + 1 < db->types.count; j++)
                        db->types.items[j] = db->types.items[j + 1];
                    db->types.count--;
                    return 0;
                }
            }
            fprintf(stderr, "type '" SV_FMT "' not found\n", SV_ARG(q->type_name));
            return -1;
        }
        case QUERY_TYPE_CREATE_INDEX: {
            struct table *t = db_find_table_sv(db, q->table);
            if (!t) {
                fprintf(stderr, "table '" SV_FMT "' not found\n", SV_ARG(q->table));
                return -1;
            }
            int col_idx = find_column_index_sv(t, q->index_column);
            if (col_idx < 0) {
                fprintf(stderr, "column '" SV_FMT "' not found in table '" SV_FMT "'\n",
                        SV_ARG(q->index_column), SV_ARG(q->table));
                return -1;
            }
            struct index idx;
            index_init_sv(&idx, q->index_name, q->index_column, col_idx);
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
                    if (sv_eq_cstr(q->index_name, t->indexes.items[ii].name)) {
                        index_free(&t->indexes.items[ii]);
                        for (size_t j = ii; j + 1 < t->indexes.count; j++)
                            t->indexes.items[j] = t->indexes.items[j + 1];
                        t->indexes.count--;
                        return 0;
                    }
                }
            }
            fprintf(stderr, "index '" SV_FMT "' not found\n", SV_ARG(q->index_name));
            return -1;
        }
        case QUERY_TYPE_SELECT: {
            /* CTE: create a temporary table from the CTE query */
            struct table *cte_table = NULL;
            if (q->cte_name && q->cte_sql) {
                struct query cte_q = {0};
                if (query_parse(q->cte_sql, &cte_q) != 0) return -1;
                struct rows cte_rows = {0};
                if (db_exec(db, &cte_q, &cte_rows) != 0) {
                    query_free(&cte_q);
                    return -1;
                }
                /* create a temp table with the CTE name */
                struct table ct = {0};
                ct.name = strdup(q->cte_name);
                da_init(&ct.columns);
                da_init(&ct.rows);
                da_init(&ct.indexes);
                /* infer column names from the CTE query's column list and source table */
                if (cte_q.query_type == QUERY_TYPE_SELECT) {
                    struct table *src_t = db_find_table_sv(db, cte_q.table);
                    if (src_t && sv_eq_cstr(cte_q.columns, "*")) {
                        /* SELECT * — copy all column names from source table */
                        for (size_t c = 0; c < src_t->columns.count; c++) {
                            struct column col = {0};
                            col.name = strdup(src_t->columns.items[c].name);
                            col.type = src_t->columns.items[c].type;
                            da_push(&ct.columns, col);
                        }
                    } else if (src_t) {
                        // TODO: this manual sv tokenization duplicates what the lexer already does;
                // could reuse the lexer to parse the column list instead
                /* parse the select column list to get names */
                        sv cols = cte_q.columns;
                        size_t ci = 0;
                        while (cols.len > 0 && ci < (cte_rows.count > 0 ? cte_rows.data[0].cells.count : 0)) {
                            /* skip whitespace */
                            while (cols.len > 0 && (cols.data[0] == ' ' || cols.data[0] == '\t'))
                                { cols.data++; cols.len--; }
                            /* find end of this column name (comma or end) */
                            size_t end = 0;
                            while (end < cols.len && cols.data[end] != ',') end++;
                            sv col_sv = sv_from(cols.data, end);
                            /* trim trailing whitespace */
                            while (col_sv.len > 0 && (col_sv.data[col_sv.len-1] == ' ' || col_sv.data[col_sv.len-1] == '\t'))
                                col_sv.len--;
                            /* strip table prefix (e.g. "t.col" -> "col") */
                            for (size_t k = 0; k < col_sv.len; k++) {
                                if (col_sv.data[k] == '.') {
                                    col_sv = sv_from(col_sv.data + k + 1, col_sv.len - k - 1);
                                    break;
                                }
                            }
                            struct column col = {0};
                            col.name = sv_to_cstr(col_sv);
                            col.type = cte_rows.data[0].cells.items[ci].type;
                            da_push(&ct.columns, col);
                            ci++;
                            if (end < cols.len) { cols.data += end + 1; cols.len -= end + 1; }
                            else break;
                        }
                    }
                }
                /* fallback: if no columns were resolved, use "?" */
                if (ct.columns.count == 0 && cte_rows.count > 0) {
                    for (size_t c = 0; c < cte_rows.data[0].cells.count; c++) {
                        struct column col = {0};
                        char buf[32];
                        snprintf(buf, sizeof(buf), "column%zu", c + 1);
                        col.name = strdup(buf);
                        col.type = cte_rows.data[0].cells.items[c].type;
                        da_push(&ct.columns, col);
                    }
                }
                for (size_t i = 0; i < cte_rows.count; i++) {
                    struct row r = {0};
                    da_init(&r.cells);
                    for (size_t c = 0; c < cte_rows.data[i].cells.count; c++) {
                        struct cell cp;
                        copy_cell(&cp, &cte_rows.data[i].cells.items[c]);
                        da_push(&r.cells, cp);
                    }
                    da_push(&ct.rows, r);
                }
                for (size_t i = 0; i < cte_rows.count; i++)
                    row_free(&cte_rows.data[i]);
                free(cte_rows.data);
                query_free(&cte_q);
                da_push(&db->tables, ct);
                cte_table = &db->tables.items[db->tables.count - 1];
            }

            /* resolve any IN (SELECT ...) / EXISTS subqueries */
            if (q->has_where && q->where_cond)
                resolve_subqueries(db, q->where_cond);
            int sel_rc;
            if (q->table.len == 0 && q->insert_row && result) {
                /* SELECT <literal> — no table, return literal values */
                struct row dst = {0};
                da_init(&dst.cells);
                for (size_t i = 0; i < q->insert_row->cells.count; i++) {
                    struct cell c = {0};
                    c.is_null = q->insert_row->cells.items[i].is_null;
                    c.type = q->insert_row->cells.items[i].type;
                    if (column_type_is_text(c.type)
                        && q->insert_row->cells.items[i].value.as_text)
                        c.value.as_text = strdup(q->insert_row->cells.items[i].value.as_text);
                    else
                        c.value = q->insert_row->cells.items[i].value;
                    da_push(&dst.cells, c);
                }
                rows_push(result, dst);
                sel_rc = 0;
            } else if (q->has_join) {
                sel_rc = exec_join(db, q, result);
            } else {
                sel_rc = db_table_exec_query(db, q->table, q, result);
            }

            /* UNION / INTERSECT / EXCEPT */
            if (sel_rc == 0 && q->has_set_op && q->set_rhs_sql && result) {
                struct query rhs_q = {0};
                if (query_parse(q->set_rhs_sql, &rhs_q) == 0) {
                    struct rows rhs_rows = {0};
                    if (db_exec(db, &rhs_q, &rhs_rows) == 0) {
                        if (q->set_op == 0) {
                            // TODO: UNION duplicate check is O(n*m); could hash or sort
                            // for better performance on large result sets
                            /* UNION [ALL] — append RHS rows */
                            for (size_t i = 0; i < rhs_rows.count; i++) {
                                if (!q->set_all) {
                                    /* check for duplicates */
                                    int dup = 0;
                                    for (size_t j = 0; j < result->count; j++) {
                                        if (result->data[j].cells.count != rhs_rows.data[i].cells.count) continue;
                                        int eq = 1;
                                        for (size_t c = 0; c < rhs_rows.data[i].cells.count; c++) {
                                            if (!cells_equal(&result->data[j].cells.items[c],
                                                             &rhs_rows.data[i].cells.items[c])) { eq = 0; break; }
                                        }
                                        if (eq) { dup = 1; break; }
                                    }
                                    if (dup) { row_free(&rhs_rows.data[i]); continue; }
                                }
                                rows_push(result, rhs_rows.data[i]);
                                rhs_rows.data[i].cells.items = NULL;
                                rhs_rows.data[i].cells.count = 0;
                            }
                        } else if (q->set_op == 1) {
                            /* INTERSECT — keep only rows in both */
                            size_t w = 0;
                            for (size_t i = 0; i < result->count; i++) {
                                int found = 0;
                                for (size_t j = 0; j < rhs_rows.count; j++) {
                                    if (result->data[i].cells.count != rhs_rows.data[j].cells.count) continue;
                                    int eq = 1;
                                    for (size_t c = 0; c < result->data[i].cells.count; c++) {
                                        if (!cells_equal(&result->data[i].cells.items[c],
                                                         &rhs_rows.data[j].cells.items[c])) { eq = 0; break; }
                                    }
                                    if (eq) { found = 1; break; }
                                }
                                if (found) {
                                    if (w != i) result->data[w] = result->data[i];
                                    w++;
                                } else {
                                    row_free(&result->data[i]);
                                }
                            }
                            result->count = w;
                        } else if (q->set_op == 2) {
                            /* EXCEPT — keep LHS rows not in RHS */
                            size_t w = 0;
                            for (size_t i = 0; i < result->count; i++) {
                                int found = 0;
                                for (size_t j = 0; j < rhs_rows.count; j++) {
                                    if (result->data[i].cells.count != rhs_rows.data[j].cells.count) continue;
                                    int eq = 1;
                                    for (size_t c = 0; c < result->data[i].cells.count; c++) {
                                        if (!cells_equal(&result->data[i].cells.items[c],
                                                         &rhs_rows.data[j].cells.items[c])) { eq = 0; break; }
                                    }
                                    if (eq) { found = 1; break; }
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
            if (sel_rc == 0 && q->has_set_op && q->set_order_by &&
                result && result->count > 1) {
                /* parse the ORDER BY clause stored as text */
                char ob_wrap[512];
                snprintf(ob_wrap, sizeof(ob_wrap), "SELECT x FROM t %s", q->set_order_by);
                struct query ob_q = {0};
                if (query_parse(ob_wrap, &ob_q) == 0 && ob_q.has_order_by &&
                    ob_q.order_by_items.count > 0) {
                    /* resolve column indices against the source table */
                    struct table *src_t = db_find_table_sv(db, q->table);
                    int ord_cols[32];
                    int ord_descs[32];
                    size_t nord = ob_q.order_by_items.count < 32 ? ob_q.order_by_items.count : 32;
                    for (size_t k = 0; k < nord; k++) {
                        ord_cols[k] = -1;
                        ord_descs[k] = ob_q.order_by_items.items[k].desc;
                        if (src_t) {
                            char obuf[256];
                            const char *ord_name = extract_col_name(
                                ob_q.order_by_items.items[k].column, obuf, sizeof(obuf));
                            ord_cols[k] = find_column_index(src_t, ord_name);
                        }
                    }
                    _jsort_ctx = (struct join_sort_ctx){ .cols = ord_cols, .descs = ord_descs, .ncols = nord };
                    qsort(result->data, result->count, sizeof(struct row), cmp_rows_join);
                }
                query_free(&ob_q);
            }

            /* clean up CTE temp table */
            if (cte_table) {
                table_free(cte_table);
                /* remove from tables array */
                size_t idx = db->tables.count - 1;
                for (size_t j = idx; j + 1 < db->tables.count; j++)
                    db->tables.items[j] = db->tables.items[j + 1];
                db->tables.count--;
            }

            return sel_rc;
        }
        case QUERY_TYPE_INSERT: {
            /* INSERT ... SELECT */
            if (q->insert_select_sql) {
                struct query sel_q = {0};
                if (query_parse(q->insert_select_sql, &sel_q) != 0) return -1;
                struct rows sel_rows = {0};
                if (db_exec(db, &sel_q, &sel_rows) != 0) {
                    query_free(&sel_q);
                    return -1;
                }
                /* insert each result row into the target table */
                struct table *t = db_find_table_sv(db, q->table);
                if (!t) {
                    fprintf(stderr, "table '" SV_FMT "' not found\n", SV_ARG(q->table));
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
                        copy_cell(&cp, &sel_rows.data[i].cells.items[c]);
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
            if (q->has_on_conflict && q->on_conflict_do_nothing) {
                struct table *t = db_find_table_sv(db, q->table);
                if (t) {
                    int conflict_col = -1;
                    if (q->conflict_column.len > 0)
                        conflict_col = find_column_index_sv(t, q->conflict_column);
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
                        size_t orig_count = q->insert_rows.count;
                        for (size_t ri = 0; ri < q->insert_rows.count; ) {
                            struct cell *new_cell = &q->insert_rows.items[ri].cells.items[conflict_col];
                            int conflict = 0;
                            for (size_t ei = 0; ei < t->rows.count; ei++) {
                                if (cells_equal(new_cell, &t->rows.items[ei].cells.items[conflict_col])) {
                                    conflict = 1;
                                    break;
                                }
                            }
                            if (conflict) {
                                /* skip this row — shift remaining */
                                row_free(&q->insert_rows.items[ri]);
                                for (size_t j = ri; j + 1 < q->insert_rows.count; j++)
                                    q->insert_rows.items[j] = q->insert_rows.items[j + 1];
                                q->insert_rows.count--;
                                if (q->insert_rows.count > 0)
                                    q->insert_row = &q->insert_rows.items[0];
                                else
                                    q->insert_row = NULL;
                            } else {
                                ri++;
                            }
                        }
                        (void)orig_count;
                    }
                }
                if (q->insert_rows.count == 0) return 0;
            }
            return db_table_exec_query(db, q->table, q, result);
        }
        case QUERY_TYPE_DELETE:
            /* resolve subqueries in WHERE */
            if (q->has_where && q->where_cond)
                resolve_subqueries(db, q->where_cond);
            return db_table_exec_query(db, q->table, q, result);
        case QUERY_TYPE_UPDATE:
            /* resolve subqueries in WHERE */
            if (q->has_where && q->where_cond)
                resolve_subqueries(db, q->where_cond);
            /* UPDATE ... FROM — use parsed join columns */
            if (q->has_update_from) {
                struct table *t = db_find_table_sv(db, q->table);
                struct table *ft = db_find_table_sv(db, q->update_from_table);
                if (!t || !ft) {
                    fprintf(stderr, "update from: table not found\n");
                    return -1;
                }
                /* resolve join columns from the parsed WHERE t1.col = t2.col */
                int t_join_col = -1, ft_join_col = -1;
                if (q->update_from_join_left.len > 0 && q->update_from_join_right.len > 0) {
                    char lbuf[256], rbuf[256];
                    const char *lcol = extract_col_name(q->update_from_join_left, lbuf, sizeof(lbuf));
                    const char *rcol = extract_col_name(q->update_from_join_right, rbuf, sizeof(rbuf));
                    int l_in_t = find_column_index(t, lcol);
                    int l_in_ft = find_column_index(ft, lcol);
                    int r_in_t = find_column_index(t, rcol);
                    int r_in_ft = find_column_index(ft, rcol);
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
                            if (cells_equal(&t->rows.items[i].cells.items[t_join_col],
                                            &ft->rows.items[j].cells.items[ft_join_col])) {
                                matched = 1;
                                break;
                            }
                        }
                    }
                    if (!matched) continue;
                    updated++;
                    for (size_t s = 0; s < q->set_clauses.count; s++) {
                        int ci = find_column_index_sv(t, q->set_clauses.items[s].column);
                        if (ci < 0) continue;
                        if (column_type_is_text(t->rows.items[i].cells.items[ci].type)
                            && t->rows.items[i].cells.items[ci].value.as_text)
                            free(t->rows.items[i].cells.items[ci].value.as_text);
                        t->rows.items[i].cells.items[ci].type = q->set_clauses.items[s].value.type;
                        if (column_type_is_text(q->set_clauses.items[s].value.type)
                            && q->set_clauses.items[s].value.value.as_text)
                            t->rows.items[i].cells.items[ci].value.as_text =
                                strdup(q->set_clauses.items[s].value.value.as_text);
                        else
                            t->rows.items[i].cells.items[ci].value = q->set_clauses.items[s].value.value;
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
            return db_table_exec_query(db, q->table, q, result);
        case QUERY_TYPE_ALTER: {
            struct table *t = db_find_table_sv(db, q->table);
            if (!t) {
                fprintf(stderr, "alter error: table '" SV_FMT "' not found\n", SV_ARG(q->table));
                return -1;
            }
            switch (q->alter_action) {
                case ALTER_ADD_COLUMN: {
                    table_add_column(t, &q->alter_new_col);
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
                    int col_idx = find_column_index_sv(t, q->alter_column);
                    if (col_idx < 0) {
                        fprintf(stderr, "alter error: column '" SV_FMT "' not found\n", SV_ARG(q->alter_column));
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
                            free_cell_text_ext(&r->cells.items[col_idx]);
                            for (size_t j = (size_t)col_idx; j + 1 < r->cells.count; j++)
                                r->cells.items[j] = r->cells.items[j + 1];
                            r->cells.count--;
                        }
                    }
                    return 0;
                }
                case ALTER_RENAME_COLUMN: {
                    int col_idx = find_column_index_sv(t, q->alter_column);
                    if (col_idx < 0) {
                        fprintf(stderr, "alter error: column '" SV_FMT "' not found\n", SV_ARG(q->alter_column));
                        return -1;
                    }
                    free(t->columns.items[col_idx].name);
                    t->columns.items[col_idx].name = sv_to_cstr(q->alter_new_name);
                    return 0;
                }
                case ALTER_COLUMN_TYPE: {
                    int col_idx = find_column_index_sv(t, q->alter_column);
                    if (col_idx < 0) {
                        fprintf(stderr, "alter error: column '" SV_FMT "' not found\n", SV_ARG(q->alter_column));
                        return -1;
                    }
                    t->columns.items[col_idx].type = q->alter_new_col.type;
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
