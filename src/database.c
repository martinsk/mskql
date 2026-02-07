#include "database.h"
#include "parser.h"
#include "query.h"
#include "stringview.h"
#include <string.h>
#include <stdio.h>

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

static void copy_cell(struct cell *dst, const struct cell *src)
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
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
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
        case COLUMN_TYPE_INT:   return a->value.as_int == b->value.as_int;
        case COLUMN_TYPE_FLOAT: return a->value.as_float == b->value.as_float;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
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

    /* WHERE filter */
    if (q->has_where && q->where_cond) {
        struct rows filtered = {0};
        for (size_t i = 0; i < merged.count; i++) {
            merged_t.rows.items = merged.data;
            merged_t.rows.count = merged.count;
            if (eval_condition(q->where_cond, &merged.data[i], &merged_t)) {
                rows_push(&filtered, merged.data[i]);
                merged.data[i] = (struct row){0};
            } else {
                row_free(&merged.data[i]);
            }
        }
        free(merged.data);
        merged = filtered;
    }

    /* ORDER BY */
    if (q->has_order_by && merged.count > 1) {
        char obuf[256];
        const char *ord_name = extract_col_name(q->order_by_col, obuf, sizeof(obuf));
        int ord_col = -1;
        for (size_t c = 0; c < merged_t.columns.count; c++) {
            if (strcmp(merged_t.columns.items[c].name, ord_name) == 0) {
                ord_col = (int)c; break;
            }
        }
        if (ord_col >= 0) {
            for (size_t i = 0; i < merged.count; i++) {
                for (size_t j = i + 1; j < merged.count; j++) {
                    int cmp = cell_compare_join(
                        &merged.data[i].cells.items[ord_col],
                        &merged.data[j].cells.items[ord_col]);
                    if (q->order_desc ? (cmp < 0) : (cmp > 0)) {
                        struct row swap = merged.data[i];
                        merged.data[i] = merged.data[j];
                        merged.data[j] = swap;
                    }
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
    if (c->type == COND_COMPARE && c->subquery_sql &&
        (c->op == CMP_IN || c->op == CMP_NOT_IN)) {
        // TODO: MEMORY LEAK: The parsed subquery struct sq contains heap-allocated
        // members that are never freed after use. A query_free() function should be
        // called after db_exec completes.
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

int db_exec(struct database *db, struct query *q, struct rows *result)
{
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
            char *tname = sv_to_cstr(q->type_name);
            if (db_find_type(db, tname)) {
                fprintf(stderr, "type '%s' already exists\n", tname);
                free(tname);
                return -1;
            }
            struct enum_type et;
            et.name = tname;
            da_init(&et.values);
            for (size_t i = 0; i < q->enum_values.count; i++) {
                da_push(&et.values, strdup(q->enum_values.items[i]));
            }
            da_push(&db->types, et);
            return 0;
        }
        case QUERY_TYPE_DROP_TYPE: {
            char *tname = sv_to_cstr(q->type_name);
            for (size_t i = 0; i < db->types.count; i++) {
                if (strcmp(db->types.items[i].name, tname) == 0) {
                    enum_type_free(&db->types.items[i]);
                    for (size_t j = i; j + 1 < db->types.count; j++)
                        db->types.items[j] = db->types.items[j + 1];
                    db->types.count--;
                    free(tname);
                    return 0;
                }
            }
            fprintf(stderr, "type '%s' not found\n", tname);
            free(tname);
            return -1;
        }
        case QUERY_TYPE_CREATE_INDEX: {
            struct table *t = db_find_table_sv(db, q->table);
            if (!t) {
                fprintf(stderr, "table '" SV_FMT "' not found\n", SV_ARG(q->table));
                return -1;
            }
            char *col_name = sv_to_cstr(q->index_column);
            int col_idx = find_column_index(t, col_name);
            if (col_idx < 0) {
                fprintf(stderr, "column '%s' not found in table '" SV_FMT "'\n",
                        col_name, SV_ARG(q->table));
                free(col_name);
                return -1;
            }
            char *idx_name = sv_to_cstr(q->index_name);
            struct index idx;
            index_init(&idx, idx_name, col_name, col_idx);
            /* backfill existing rows */
            for (size_t i = 0; i < t->rows.count; i++) {
                if ((size_t)col_idx < t->rows.items[i].cells.count) {
                    index_insert(&idx, &t->rows.items[i].cells.items[col_idx], i);
                }
            }
            da_push(&t->indexes, idx);
            free(idx_name);
            free(col_name);
            return 0;
        }
        case QUERY_TYPE_DROP_INDEX: {
            /* search all tables for the named index */
            char *idx_name = sv_to_cstr(q->index_name);
            for (size_t ti = 0; ti < db->tables.count; ti++) {
                struct table *t = &db->tables.items[ti];
                for (size_t ii = 0; ii < t->indexes.count; ii++) {
                    if (strcmp(t->indexes.items[ii].name, idx_name) == 0) {
                        index_free(&t->indexes.items[ii]);
                        for (size_t j = ii; j + 1 < t->indexes.count; j++)
                            t->indexes.items[j] = t->indexes.items[j + 1];
                        t->indexes.count--;
                        free(idx_name);
                        return 0;
                    }
                }
            }
            fprintf(stderr, "index '%s' not found\n", idx_name);
            free(idx_name);
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
                    if ((c.type == COLUMN_TYPE_TEXT || c.type == COLUMN_TYPE_ENUM)
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
