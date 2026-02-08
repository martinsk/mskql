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

/* perform a single join between two table descriptors, producing merged rows and columns */
static int do_single_join(struct table *t1, struct table *t2,
                          sv left_col_sv, sv right_col_sv, int join_type,
                          struct rows *out_rows,
                          struct table *out_meta)
{
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

    size_t ncols1 = t1->columns.count;
    size_t ncols2 = t2->columns.count;
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
            struct row full = {0};
            da_init(&full.cells);
            for (size_t c = 0; c < r1->cells.count; c++) {
                struct cell cp; copy_cell(&cp, &r1->cells.items[c]);
                da_push(&full.cells, cp);
            }
            for (size_t c = 0; c < r2->cells.count; c++) {
                struct cell cp; copy_cell(&cp, &r2->cells.items[c]);
                da_push(&full.cells, cp);
            }
            rows_push(out_rows, full);
        }
    }

    if (join_type == 1 || join_type == 3) {
        for (size_t i = 0; i < t1->rows.count; i++) {
            if (t1_matched[i]) continue;
            struct row full = {0};
            da_init(&full.cells);
            for (size_t c = 0; c < ncols1; c++) {
                struct cell cp; copy_cell(&cp, &t1->rows.items[i].cells.items[c]);
                da_push(&full.cells, cp);
            }
            for (size_t c = 0; c < ncols2; c++) {
                struct cell cp = { .type = t2->columns.items[c].type, .is_null = 1 };
                da_push(&full.cells, cp);
            }
            rows_push(out_rows, full);
        }
    }

    if (join_type == 2 || join_type == 3) {
        for (size_t j = 0; j < t2->rows.count; j++) {
            if (t2_matched[j]) continue;
            struct row full = {0};
            da_init(&full.cells);
            for (size_t c = 0; c < ncols1; c++) {
                struct cell cp = { .type = t1->columns.items[c].type, .is_null = 1 };
                da_push(&full.cells, cp);
            }
            for (size_t c = 0; c < ncols2; c++) {
                struct cell cp; copy_cell(&cp, &t2->rows.items[j].cells.items[c]);
                da_push(&full.cells, cp);
            }
            rows_push(out_rows, full);
        }
    }

    free(t1_matched);
    free(t2_matched);

    /* build merged column list in out_meta (deep-copy names to avoid dangling pointers) */
    for (size_t c = 0; c < t1->columns.count; c++) {
        struct column col = t1->columns.items[c];
        col.name = strdup(col.name);
        col.enum_type_name = col.enum_type_name ? strdup(col.enum_type_name) : NULL;
        da_push(&out_meta->columns, col);
    }
    for (size_t c = 0; c < t2->columns.count; c++) {
        struct column col = t2->columns.items[c];
        col.name = strdup(col.name);
        col.enum_type_name = col.enum_type_name ? strdup(col.enum_type_name) : NULL;
        da_push(&out_meta->columns, col);
    }

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
    if (do_single_join(t1, t2, ji->join_left_col, ji->join_right_col, ji->join_type,
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

        struct rows next_merged = {0};
        struct table next_meta = {0};
        da_init(&next_meta.columns);
        da_init(&next_meta.rows);
        da_init(&next_meta.indexes);
        if (do_single_join(&merged_t, tn, ji->join_left_col, ji->join_right_col, ji->join_type,
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
            char obuf[256];
            const char *ord_name = extract_col_name(q->order_by_items.items[k].column, obuf, sizeof(obuf));
            ord_cols[k] = -1;
            for (size_t c = 0; c < merged_t.columns.count; c++) {
                if (strcmp(merged_t.columns.items[c].name, ord_name) == 0) {
                    ord_cols[k] = (int)c; break;
                }
            }
            ord_descs[k] = q->order_by_items.items[k].desc;
        }
        for (size_t i = 0; i < merged.count; i++) {
            for (size_t j = i + 1; j < merged.count; j++) {
                int cmp = 0;
                for (size_t k = 0; k < nord && cmp == 0; k++) {
                    if (ord_cols[k] < 0) continue;
                    cmp = cell_compare_join(
                        &merged.data[i].cells.items[ord_cols[k]],
                        &merged.data[j].cells.items[ord_cols[k]]);
                    if (ord_descs[k]) cmp = -cmp;
                }
                if (cmp > 0) {
                    struct row swap = merged.data[i];
                    merged.data[i] = merged.data[j];
                    merged.data[j] = swap;
                }
            }
        }
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

                char cbuf[256];
                const char *cname = extract_col_name(one, cbuf, sizeof(cbuf));

                /* strip alias: "col AS alias" -> "col" */
                char clean[256];
                strncpy(clean, cname, sizeof(clean) - 1);
                clean[sizeof(clean) - 1] = '\0';
                char *as_pos = strstr(clean, " AS ");
                if (!as_pos) as_pos = strstr(clean, " as ");
                if (as_pos) *as_pos = '\0';
                size_t cl = strlen(clean);
                while (cl > 0 && clean[cl-1] == ' ') clean[--cl] = '\0';

                int idx = -1;
                for (size_t c = 0; c < merged_t.columns.count; c++) {
                    if (strcmp(merged_t.columns.items[c].name, clean) == 0) {
                        idx = (int)c; break;
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
                        if ((src->type == COLUMN_TYPE_TEXT || src->type == COLUMN_TYPE_ENUM)
                            && src->value.as_text)
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
            char *tname = sv_to_cstr(q->table);
            struct table t;
            table_init(&t, tname);
            free(tname);
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
                da_push(&et.values, strdup(q->enum_values.items[i]));
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
        case QUERY_TYPE_SELECT:
            /* resolve any IN (SELECT ...) subqueries */
            if (q->has_where && q->where_cond)
                resolve_subqueries(db, q->where_cond);
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
                return 0;
            }
            if (q->has_join)
                return exec_join(db, q, result);
            return db_table_exec_query(db, q->table, q, result);
        case QUERY_TYPE_INSERT:
        case QUERY_TYPE_DELETE:
        case QUERY_TYPE_UPDATE:
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
