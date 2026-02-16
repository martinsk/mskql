#include "database.h"
#include "parser.h"
#include "query.h"
#include "plan.h"
#include "catalog.h"
#include "stringview.h"
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

void db_init(struct database *db, const char *name)
{
    db->name = strdup(name);
    da_init(&db->tables);
    da_init(&db->types);
    da_init(&db->sequences);
    db->active_txn = NULL;
    db->total_generation = 0;
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
                        struct query *q, struct rows *result, struct bump_alloc *rb)
{
    struct table *t = db_find_table_sv(db, table_name);
    if (!t) {
        arena_set_error(&q->arena, "42P01", "table '%.*s' not found", (int)table_name.len, table_name.data);
        return -1;
    }
    /* COW trigger: save table state before first mutation in a transaction */
    if (db->active_txn && db->active_txn->in_transaction && db->active_txn->snapshot &&
        (q->query_type == QUERY_TYPE_INSERT || q->query_type == QUERY_TYPE_UPDATE ||
         q->query_type == QUERY_TYPE_DELETE)) {
        snapshot_cow_table(db->active_txn->snapshot, db, t->name);
    }
    return query_exec(t, q, result, db, rb);
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
                            struct rows *out, struct bump_alloc *rb)
{
    struct row full = {0};
    da_init(&full.cells);
    for (size_t c = 0; c < ncols1; c++) {
        struct cell cp;
        if (rb) cell_copy_bump(&cp, &r1->cells.items[c], rb);
        else    cell_copy(&cp, &r1->cells.items[c]);
        da_push(&full.cells, cp);
    }
    for (size_t c = 0; c < ncols2; c++) {
        struct cell cp;
        if (rb) cell_copy_bump(&cp, &r2->cells.items[c], rb);
        else    cell_copy(&cp, &r2->cells.items[c]);
        da_push(&full.cells, cp);
    }
    rows_push(out, full);
}

static void emit_null_right(struct row *r1, size_t ncols1,
                             struct table *t2, struct rows *out,
                             struct bump_alloc *rb)
{
    struct row full = {0};
    da_init(&full.cells);
    for (size_t c = 0; c < ncols1; c++) {
        struct cell cp;
        if (rb) cell_copy_bump(&cp, &r1->cells.items[c], rb);
        else    cell_copy(&cp, &r1->cells.items[c]);
        da_push(&full.cells, cp);
    }
    for (size_t c = 0; c < t2->columns.count; c++) {
        struct cell cp = { .type = t2->columns.items[c].type, .is_null = 1 };
        da_push(&full.cells, cp);
    }
    rows_push(out, full);
}

static void emit_null_left(struct table *t1, struct row *r2,
                            size_t ncols2, struct rows *out,
                            struct bump_alloc *rb)
{
    struct row full = {0};
    da_init(&full.cells);
    for (size_t c = 0; c < t1->columns.count; c++) {
        struct cell cp = { .type = t1->columns.items[c].type, .is_null = 1 };
        da_push(&full.cells, cp);
    }
    for (size_t c = 0; c < ncols2; c++) {
        struct cell cp;
        if (rb) cell_copy_bump(&cp, &r2->cells.items[c], rb);
        else    cell_copy(&cp, &r2->cells.items[c]);
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

/* JPL ownership: column names allocated here (make_aliased_name, strdup) are
 * freed by free_merged_columns → column_free, called from exec_join (same
 * logical scope within database.c). */
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

/* FNV-1a hash for a struct cell value (mirrors block_hash_cell in block.h).
 * Numeric types (INT, FLOAT, BIGINT, NUMERIC) are all hashed as double
 * so that INT(1) and FLOAT(1.0) produce the same hash — required for
 * correct cross-type equi-joins. */
static uint32_t cell_hash(const struct cell *c)
{
    if (c->is_null) return 0;
    uint32_t h = 2166136261u;
    switch (c->type) {
        case COLUMN_TYPE_SMALLINT:
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:
        case COLUMN_TYPE_BIGINT:
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC: {
            double dv;
            switch (c->type) {
            case COLUMN_TYPE_SMALLINT: dv = (double)c->value.as_smallint; break;
            case COLUMN_TYPE_INT:
            case COLUMN_TYPE_BOOLEAN:  dv = (double)c->value.as_int; break;
            case COLUMN_TYPE_BIGINT:   dv = (double)c->value.as_bigint; break;
            case COLUMN_TYPE_FLOAT:
            case COLUMN_TYPE_NUMERIC:  dv = c->value.as_float; break;
            case COLUMN_TYPE_TEXT:
            case COLUMN_TYPE_ENUM:
            case COLUMN_TYPE_DATE:
            case COLUMN_TYPE_TIME:
            case COLUMN_TYPE_TIMESTAMP:
            case COLUMN_TYPE_TIMESTAMPTZ:
            case COLUMN_TYPE_INTERVAL:
            case COLUMN_TYPE_UUID:     dv = 0.0; break;
            }
            uint8_t *p = (uint8_t *)&dv;
            for (int i = 0; i < 8; i++) { h ^= p[i]; h *= 16777619u; }
            break;
        }
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID:
            if (c->value.as_text) {
                const char *s = c->value.as_text;
                while (*s) { h ^= (uint8_t)*s++; h *= 16777619u; }
            }
            break;
    }
    return h;
}

/* perform a single join between two table descriptors, producing merged rows and columns */
static int join_cmp_match(const struct cell *a, const struct cell *b, enum cmp_op op)
{
    if (op == CMP_EQ) return cell_equal(a, b);
    int r = cell_compare(a, b);
    if (r == -2) return 0;
    switch (op) {
        case CMP_NE: return r != 0;
        case CMP_LT: return r < 0;
        case CMP_GT: return r > 0;
        case CMP_LE: return r <= 0;
        case CMP_GE: return r >= 0;
        case CMP_EQ:
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
            return r == 0;
    }
    return 0;
}

static int do_single_join(struct table *t1, const char *alias1,
                          struct table *t2, const char *alias2,
                          sv left_col_sv, sv right_col_sv, int join_type,
                          enum cmp_op join_op,
                          struct rows *out_rows,
                          struct table *out_meta,
                          struct query_arena *arena, uint32_t join_on_cond,
                          struct bump_alloc *rb)
{
    size_t ncols1 = t1->columns.count;
    size_t ncols2 = t2->columns.count;

    /* CROSS JOIN (type 4): cartesian product, no join columns */
    if (join_type == 4) {
        size_t max_join_rows = 10000000;
        size_t emitted = 0;
        for (size_t i = 0; i < t1->rows.count; i++) {
            for (size_t j = 0; j < t2->rows.count; j++) {
                if (emitted >= max_join_rows) {
                    arena_set_error(arena, "54000", "join result exceeds maximum row count (10000000)");
                    build_merged_columns_ex(t1, alias1, t2, alias2, out_meta);
                    return -1;
                }
                emit_merged_row(&t1->rows.items[i], ncols1,
                                &t2->rows.items[j], ncols2, out_rows, rb);
                emitted++;
            }
        }
        build_merged_columns_ex(t1, alias1, t2, alias2, out_meta);
        return 0;
    }

    /* build merged column metadata first (needed for condition evaluation) */
    build_merged_columns_ex(t1, alias1, t2, alias2, out_meta);

    int *t1_matched = bump_calloc(&arena->scratch, t1->rows.count, sizeof(int));
    int *t2_matched = bump_calloc(&arena->scratch, t2->rows.count, sizeof(int));

    /* Try to extract simple equi-join column indices for hash join.
     * Works for both the join_on_cond path (single COND_COMPARE EQ with
     * two column refs) and the simple left_col/right_col path. */
    int hj_t1_col = -1, hj_t2_col = -1;
    int use_hash_join = 0;

    if (join_on_cond != IDX_NONE && arena) {
        struct condition *cond = &COND(arena, join_on_cond);
        if (cond->type == COND_COMPARE && cond->op == CMP_EQ &&
            cond->column.len > 0 && cond->rhs_column.len > 0 &&
            cond->lhs_expr == IDX_NONE) {
            /* single equality on two column refs — extract indices from merged table */
            int lhs = table_find_column_sv(out_meta, cond->column);
            int rhs = table_find_column_sv(out_meta, cond->rhs_column);
            if (lhs >= 0 && rhs >= 0) {
                /* map merged-table indices back to t1/t2 indices */
                if (lhs < (int)ncols1 && rhs >= (int)ncols1) {
                    hj_t1_col = lhs;
                    hj_t2_col = rhs - (int)ncols1;
                    use_hash_join = 1;
                } else if (rhs < (int)ncols1 && lhs >= (int)ncols1) {
                    hj_t1_col = rhs;
                    hj_t2_col = lhs - (int)ncols1;
                    use_hash_join = 1;
                }
            }
        }
    } else if (join_op == CMP_EQ) {
        char buf[256], buf2[256];
        const char *left_col = extract_col_name(left_col_sv, buf, sizeof(buf));
        const char *right_col = extract_col_name(right_col_sv, buf2, sizeof(buf2));

        int left_in_t1 = (table_find_column(t1, left_col) >= 0);
        hj_t1_col = left_in_t1 ? table_find_column(t1, left_col) : table_find_column(t1, right_col);
        hj_t2_col = left_in_t1 ? table_find_column(t2, right_col) : table_find_column(t2, left_col);

        if (hj_t1_col >= 0 && hj_t2_col >= 0)
            use_hash_join = 1;
        else {
            arena_set_error(arena, "42703", "join error: could not resolve ON columns");
            return -1;
        }
    }

    if (use_hash_join) {
        /* Hash join: build on t2 (typically smaller), probe with t1 */
        size_t build_n = t2->rows.count;
        uint32_t nbuckets = 1;
        while (nbuckets < build_n * 2) nbuckets <<= 1;

        uint32_t *ht_buckets = bump_calloc(&arena->scratch, nbuckets, sizeof(uint32_t));
        uint32_t *ht_nexts = bump_calloc(&arena->scratch, build_n ? build_n : 1, sizeof(uint32_t));
        uint32_t *ht_hashes = bump_calloc(&arena->scratch, build_n ? build_n : 1, sizeof(uint32_t));
        memset(ht_buckets, 0xFF, nbuckets * sizeof(uint32_t)); /* IDX_NONE */

        /* build phase: hash t2 join column */
        for (size_t j = 0; j < build_n; j++) {
            uint32_t h = cell_hash(&t2->rows.items[j].cells.items[hj_t2_col]);
            uint32_t bucket = h & (nbuckets - 1);
            ht_hashes[j] = h;
            ht_nexts[j] = ht_buckets[bucket];
            ht_buckets[bucket] = (uint32_t)j;
        }

        /* probe phase: for each t1 row, look up in hash table */
        for (size_t i = 0; i < t1->rows.count; i++) {
            struct cell *probe = &t1->rows.items[i].cells.items[hj_t1_col];
            uint32_t h = cell_hash(probe);
            uint32_t bucket = h & (nbuckets - 1);
            uint32_t entry = ht_buckets[bucket];

            while (entry != 0xFFFFFFFF) {
                if (ht_hashes[entry] == h &&
                    cell_equal(probe, &t2->rows.items[entry].cells.items[hj_t2_col])) {
                    t1_matched[i] = 1;
                    t2_matched[entry] = 1;
                    emit_merged_row(&t1->rows.items[i], ncols1,
                                    &t2->rows.items[entry], ncols2, out_rows, rb);
                }
                entry = ht_nexts[entry];
            }
        }

    } else if (join_on_cond != IDX_NONE && arena) {
        /* complex ON condition: nested-loop fallback with eval_condition */
        for (size_t i = 0; i < t1->rows.count; i++) {
            struct row *r1 = &t1->rows.items[i];
            for (size_t j = 0; j < t2->rows.count; j++) {
                struct row *r2 = &t2->rows.items[j];
                struct row tmp = {0};
                da_init(&tmp.cells);
                for (size_t c = 0; c < ncols1; c++)
                    da_push(&tmp.cells, r1->cells.items[c]);
                for (size_t c = 0; c < ncols2; c++)
                    da_push(&tmp.cells, r2->cells.items[c]);
                int match = eval_condition(join_on_cond, arena, &tmp, out_meta, NULL);
                da_free(&tmp.cells);
                if (!match) continue;
                t1_matched[i] = 1;
                t2_matched[j] = 1;
                emit_merged_row(r1, ncols1, r2, ncols2, out_rows, rb);
            }
        }
    } else {
        /* non-equality join: nested-loop fallback */
        char buf[256], buf2[256];
        const char *left_col = extract_col_name(left_col_sv, buf, sizeof(buf));
        const char *right_col = extract_col_name(right_col_sv, buf2, sizeof(buf2));

        int left_in_t1 = (table_find_column(t1, left_col) >= 0);
        int t1_join_col = left_in_t1 ? table_find_column(t1, left_col) : table_find_column(t1, right_col);
        int t2_join_col = left_in_t1 ? table_find_column(t2, right_col) : table_find_column(t2, left_col);

        if (t1_join_col < 0 || t2_join_col < 0) {
            arena_set_error(arena, "42703", "join error: could not resolve ON columns");
            return -1;
        }

        for (size_t i = 0; i < t1->rows.count; i++) {
            struct row *r1 = &t1->rows.items[i];
            for (size_t j = 0; j < t2->rows.count; j++) {
                struct row *r2 = &t2->rows.items[j];
                if (!join_cmp_match(&r1->cells.items[t1_join_col], &r2->cells.items[t2_join_col], join_op))
                    continue;
                t1_matched[i] = 1;
                t2_matched[j] = 1;
                emit_merged_row(r1, ncols1, r2, ncols2, out_rows, rb);
            }
        }
    }

    /* LEFT or FULL: unmatched left rows */
    if (join_type == 1 || join_type == 3) {
        for (size_t i = 0; i < t1->rows.count; i++) {
            if (t1_matched[i]) continue;
            emit_null_right(&t1->rows.items[i], ncols1, t2, out_rows, rb);
        }
    }

    /* RIGHT or FULL: unmatched right rows */
    if (join_type == 2 || join_type == 3) {
        for (size_t j = 0; j < t2->rows.count; j++) {
            if (t2_matched[j]) continue;
            emit_null_left(t1, &t2->rows.items[j], ncols2, out_rows, rb);
        }
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

static int exec_join(struct database *db, struct query *q, struct rows *result, struct bump_alloc *rb)
{
    struct query_select *s = &q->select;
    struct table *t1 = db_find_table_sv(db, s->table);
    if (!t1) { arena_set_error(&q->arena, "42P01", "table '%.*s' not found", (int)s->table.len, s->table.data); return -1; }

    /* build merged table through successive joins */
    struct table merged_t = {0};
    da_init(&merged_t.columns);
    da_init(&merged_t.rows);
    da_init(&merged_t.indexes);
    struct rows merged = {0};

    /* first join */
    struct query_arena *a = &q->arena;
    struct join_info *ji = &a->joins.items[s->joins_start];
    char t1_alias_buf[128] = {0};
    if (s->table_alias.len > 0)
        snprintf(t1_alias_buf, sizeof(t1_alias_buf), "%.*s", (int)s->table_alias.len, s->table_alias.data);
    const char *a1 = t1_alias_buf[0] ? t1_alias_buf : NULL;

    if (ji->is_lateral && ji->lateral_subquery_sql != IDX_NONE) {
        /* LATERAL JOIN: for each row of t1, execute the subquery and cross-join */
        /* build merged column metadata: t1 columns + lateral result columns.
         * JPL ownership: strdup'd column names freed by free_merged_columns
         * at end of exec_join (same function scope). */
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
            const char *src = ASTRING(a, ji->lateral_subquery_sql);
            size_t rewritten_cap = strlen(src) * 4 + 4096;
            char *rewritten = (char *)malloc(rewritten_cap);
            if (!rewritten) continue; /* OOM — skip row */
            size_t wp = 0;
            while (*src && wp < rewritten_cap - 128) {
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
                            wp += (size_t)snprintf(rewritten + wp, rewritten_cap - wp, "NULL");
                        } else if (cv->type == COLUMN_TYPE_INT) {
                            wp += (size_t)snprintf(rewritten + wp, rewritten_cap - wp, "%d", cv->value.as_int);
                        } else if (cv->type == COLUMN_TYPE_FLOAT) {
                            wp += (size_t)snprintf(rewritten + wp, rewritten_cap - wp, "%g", cv->value.as_float);
                        } else if (column_type_is_text(cv->type) && cv->value.as_text) {
                            if (wp < rewritten_cap - 1) rewritten[wp++] = '\'';
                            for (const char *tp = cv->value.as_text; *tp && wp < rewritten_cap - 2; tp++) {
                                if (*tp == '\'') {
                                    if (wp < rewritten_cap - 2) rewritten[wp++] = '\'';
                                }
                                if (wp < rewritten_cap - 2) rewritten[wp++] = *tp;
                            }
                            if (wp < rewritten_cap - 1) rewritten[wp++] = '\'';
                        } else {
                            wp += (size_t)snprintf(rewritten + wp, rewritten_cap - wp, "NULL");
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
            free(rewritten);

            /* add lateral result columns on first iteration */
            if (!lat_cols_added && lat_rows.count > 0) {
                /* infer column names from the lateral subquery's column list */
                struct query lat_q = {0};
                int lat_parsed = (query_parse(ASTRING(a, ji->lateral_subquery_sql), &lat_q) == 0);
                if (lat_parsed) {
                    size_t ci = 0;
                    /* Try parsed_columns aliases first (works for expression/aggregate queries) */
                    if (lat_q.select.parsed_columns_count > 0) {
                        for (uint32_t pc = 0; pc < lat_q.select.parsed_columns_count && ci < lat_rows.data[0].cells.count; pc++) {
                            struct select_column *sc = &lat_q.arena.select_cols.items[lat_q.select.parsed_columns_start + pc];
                            struct column col = {0};
                            char buf[256];
                            sv col_name = sc->alias.len > 0 ? sc->alias : (sv){0};
                            if (col_name.len == 0 && sc->expr_idx != IDX_NONE) {
                                struct expr *e = &EXPR(&lat_q.arena, sc->expr_idx);
                                if (e->type == EXPR_COLUMN_REF) col_name = e->column_ref.column;
                            }
                            if (col_name.len > 0) {
                                if (lat_alias_buf[0])
                                    snprintf(buf, sizeof(buf), "%s.%.*s", lat_alias_buf, (int)col_name.len, col_name.data);
                                else
                                    snprintf(buf, sizeof(buf), "%.*s", (int)col_name.len, col_name.data);
                            } else {
                                snprintf(buf, sizeof(buf), "col%zu", ci + 1);
                            }
                            col.name = strdup(buf);
                            col.type = lat_rows.data[0].cells.items[ci].type;
                            da_push(&merged_t.columns, col);
                            ci++;
                        }
                    }
                    /* Try aggregate aliases */
                    if (ci == 0 && lat_q.select.aggregates_count > 0) {
                        for (uint32_t ai = 0; ai < lat_q.select.aggregates_count && ci < lat_rows.data[0].cells.count; ai++) {
                            struct agg_expr *ae = &lat_q.arena.aggregates.items[lat_q.select.aggregates_start + ai];
                            struct column col = {0};
                            char buf[256];
                            if (ae->alias.len > 0) {
                                if (lat_alias_buf[0])
                                    snprintf(buf, sizeof(buf), "%s.%.*s", lat_alias_buf, (int)ae->alias.len, ae->alias.data);
                                else
                                    snprintf(buf, sizeof(buf), "%.*s", (int)ae->alias.len, ae->alias.data);
                            } else {
                                snprintf(buf, sizeof(buf), "col%zu", ci + 1);
                            }
                            col.name = strdup(buf);
                            col.type = lat_rows.data[0].cells.items[ci].type;
                            da_push(&merged_t.columns, col);
                            ci++;
                        }
                    }
                    /* Try raw columns text */
                    if (ci == 0) {
                        sv cols = lat_q.select.columns;
                        while (cols.len > 0 && ci < lat_rows.data[0].cells.count) {
                            while (cols.len > 0 && (cols.data[0] == ' ' || cols.data[0] == '\t'))
                                { cols.data++; cols.len--; }
                            size_t end = 0;
                            while (end < cols.len && cols.data[end] != ',') end++;
                            sv col_sv = sv_from(cols.data, end);
                            while (col_sv.len > 0 && (col_sv.data[col_sv.len-1] == ' ' || col_sv.data[col_sv.len-1] == '\t'))
                                col_sv.len--;
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
                }
                query_free(&lat_q);
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
        /* materialize non-lateral subquery join as temp table */
        struct table *sq_temp = NULL;
        if (!ji->is_lateral && ji->lateral_subquery_sql != IDX_NONE && ji->join_alias.len > 0) {
            const char *sq_sql = ASTRING(a, ji->lateral_subquery_sql);
            char alias_buf[128];
            snprintf(alias_buf, sizeof(alias_buf), "%.*s", (int)ji->join_alias.len, ji->join_alias.data);
            sq_temp = materialize_subquery(db, sq_sql, alias_buf);
            ji->join_table = ji->join_alias;
        }
        struct table *t2 = db_find_table_sv(db, ji->join_table);
        if (!t2) {
            arena_set_error(&q->arena, "42P01", "table '%.*s' not found", (int)ji->join_table.len, ji->join_table.data);
            free_merged_rows(&merged);
            free_merged_columns(&merged_t);
            if (sq_temp) remove_temp_table(db, sq_temp);
            return -1;
        }
        resolve_join_cols(ji, t1, t2);
        char t2_alias_buf[128] = {0};
        if (ji->join_alias.len > 0)
            snprintf(t2_alias_buf, sizeof(t2_alias_buf), "%.*s", (int)ji->join_alias.len, ji->join_alias.data);
        const char *a2 = t2_alias_buf[0] ? t2_alias_buf : NULL;
        /* use table names as fallback aliases to disambiguate columns */
        if (!a1) a1 = t1->name;
        if (!a2) a2 = t2->name;
        if (do_single_join(t1, a1, t2, a2, ji->join_left_col, ji->join_right_col, ji->join_type,
                           ji->join_op, &merged, &merged_t, a, ji->join_on_cond, NULL) != 0) {
            free_merged_rows(&merged);
            free_merged_columns(&merged_t);
            if (sq_temp) remove_temp_table(db, sq_temp);
            return -1;
        }
        if (sq_temp) remove_temp_table(db, sq_temp);
    }

    /* subsequent joins: build a temp table from merged results, join with next */
    for (uint32_t jn = 1; jn < s->joins_count; jn++) {
        ji = &a->joins.items[s->joins_start + jn];
        struct table *tn = db_find_table_sv(db, ji->join_table);
        if (!tn) {
            arena_set_error(&q->arena, "42P01", "table '%.*s' not found", (int)ji->join_table.len, ji->join_table.data);
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
                           ji->join_op, &next_merged, &next_meta, a, ji->join_on_cond, NULL) != 0) {
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
    if (s->where.has_where && s->where.where_cond != IDX_NONE) {
        size_t write = 0;
        for (size_t i = 0; i < merged.count; i++) {
            merged_t.rows.items = merged.data;
            merged_t.rows.count = merged.count;
            if (eval_condition(s->where.where_cond, a, &merged.data[i], &merged_t, db)) {
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
    if (s->has_order_by && s->order_by_count > 0 && merged.count > 1) {
        int ord_cols[32];
        int ord_descs[32];
        size_t nord = s->order_by_count < 32 ? s->order_by_count : 32;
        for (size_t k = 0; k < nord; k++) {
            sv ordcol = a->order_items.items[s->order_by_start + k].column;
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
            ord_descs[k] = a->order_items.items[s->order_by_start + k].desc;
        }
        _jsort_ctx = (struct join_sort_ctx){ .cols = ord_cols, .descs = ord_descs, .ncols = nord };
        qsort(merged.data, merged.count, sizeof(struct row), cmp_rows_join);
    }

    /* GROUP BY / aggregate on joined result */
    if (s->has_group_by || s->aggregates_count > 0) {
        merged_t.rows.items = merged.data;
        merged_t.rows.count = merged.count;
        merged_t.rows.capacity = merged.count;
        int rc;
        if (s->has_group_by && (s->group_by_rollup || s->group_by_cube)) {
            /* ROLLUP/CUBE: run query_group_by for each grouping set */
            uint32_t orig_count = s->group_by_count;
            uint32_t orig_start = s->group_by_start;
            int orig_rollup = s->group_by_rollup;
            int orig_cube = s->group_by_cube;
            uint32_t nsets = 0;
            uint32_t sets[256];
            if (s->group_by_rollup) {
                for (uint32_t i = 0; i <= orig_count; i++) {
                    uint32_t mask = 0;
                    for (uint32_t j = 0; j < orig_count - i; j++)
                        mask |= (1u << j);
                    sets[nsets++] = mask;
                }
            } else {
                uint32_t total = 1u << orig_count;
                for (uint32_t m = total; m > 0; m--)
                    sets[nsets++] = m - 1;
            }
            int grp_col_idx[32];
            for (uint32_t k = 0; k < orig_count && k < 32; k++) {
                sv gbcol = ASV(a, orig_start + k);
                grp_col_idx[k] = table_find_column_sv(&merged_t, gbcol);
            }
            s->group_by_rollup = 0;
            s->group_by_cube = 0;
            uint32_t orig_exprs_start = s->group_by_exprs_start;
            for (uint32_t si = 0; si < nsets; si++) {
                uint32_t mask = sets[si];
                uint32_t tmp_start = (uint32_t)a->svs.count;
                uint32_t tmp_count = 0;
                for (uint32_t k = 0; k < orig_count; k++) {
                    if (mask & (1u << k)) {
                        arena_push_sv(a, ASV(a, orig_start + k));
                        tmp_count++;
                    }
                }
                /* ensure query_group_by doesn't read stale expression indices */
                s->group_by_exprs_start = (uint32_t)a->arg_indices.count;
                if (tmp_count == 0) {
                    s->has_group_by = 0;
                    /* WHERE already applied to merged rows — skip re-application */
                    int saved_has_where = s->where.has_where;
                    s->where.has_where = 0;
                    struct rows sub = {0};
                    query_aggregate(&merged_t, s, a, &sub, rb);
                    s->where.has_where = saved_has_where;
                    s->has_group_by = 1;
                    for (size_t r = 0; r < sub.count; r++) {
                        if (!s->agg_before_cols) {
                            struct row newrow = {0};
                            da_init(&newrow.cells);
                            for (uint32_t k = 0; k < orig_count; k++) {
                                struct cell nc = {0};
                                nc.type = (grp_col_idx[k] >= 0) ? merged_t.columns.items[grp_col_idx[k]].type : COLUMN_TYPE_TEXT;
                                nc.is_null = 1;
                                da_push(&newrow.cells, nc);
                            }
                            for (size_t ci = 0; ci < sub.data[r].cells.count; ci++) {
                                struct cell dup;
                                if (rb) cell_copy_bump(&dup, &sub.data[r].cells.items[ci], rb);
                                else    cell_copy(&dup, &sub.data[r].cells.items[ci]);
                                da_push(&newrow.cells, dup);
                            }
                            if (rb) da_free(&sub.data[r].cells);
                            else    row_free(&sub.data[r]);
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
                    s->group_by_col = ASV(a, tmp_start);
                    /* WHERE already applied to merged rows — skip re-application */
                    int saved_has_where2 = s->where.has_where;
                    s->where.has_where = 0;
                    struct rows sub = {0};
                    query_group_by(&merged_t, s, a, &sub, rb);
                    s->where.has_where = saved_has_where2;
                    for (size_t r = 0; r < sub.count; r++) {
                        if (!s->agg_before_cols) {
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
                                    nc.type = (grp_col_idx[k] >= 0) ? merged_t.columns.items[grp_col_idx[k]].type : COLUMN_TYPE_TEXT;
                                    nc.is_null = 1;
                                    da_push(&newrow.cells, nc);
                                }
                            }
                            for (size_t ci = sub_grp_i; ci < sub.data[r].cells.count; ci++) {
                                struct cell dup;
                                if (rb) cell_copy_bump(&dup, &sub.data[r].cells.items[ci], rb);
                                else    cell_copy(&dup, &sub.data[r].cells.items[ci]);
                                da_push(&newrow.cells, dup);
                            }
                            if (rb) da_free(&sub.data[r].cells);
                            else    row_free(&sub.data[r]);
                            rows_push(result, newrow);
                        } else {
                            rows_push(result, sub.data[r]);
                            sub.data[r] = (struct row){0};
                        }
                    }
                    free(sub.data);
                }
            }
            s->group_by_start = orig_start;
            s->group_by_count = orig_count;
            s->group_by_rollup = orig_rollup;
            s->group_by_cube = orig_cube;
            s->group_by_exprs_start = orig_exprs_start;
            rc = 0;
        } else if (s->has_group_by)
            rc = query_group_by(&merged_t, s, a, result, rb);
        else
            rc = query_aggregate(&merged_t, s, a, result, rb);
        free_merged_rows(&merged);
        free_merged_columns(&merged_t);
        return rc;
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
    /* Use parsed_columns with eval_expr when available (handles COALESCE, function expressions) */
    int use_parsed = (!select_all && s->parsed_columns_count > 0);
    if (use_parsed) {
        merged_t.rows.items = merged.data;
        merged_t.rows.count = merged.count;
        merged_t.rows.capacity = merged.count;
    }
    for (size_t i = start; i < end; i++) {
        struct row dst = {0};
        da_init(&dst.cells);

        if (select_all) {
            for (size_t c = 0; c < merged.data[i].cells.count; c++) {
                struct cell cp;
                if (rb) cell_copy_bump(&cp, &merged.data[i].cells.items[c], rb);
                else    cell_copy(&cp, &merged.data[i].cells.items[c]);
                da_push(&dst.cells, cp);
            }
        } else if (use_parsed) {
            for (uint32_t pc = 0; pc < s->parsed_columns_count; pc++) {
                struct select_column *sc = &a->select_cols.items[s->parsed_columns_start + pc];
                if (sc->expr_idx != IDX_NONE) {
                    struct cell val = eval_expr(sc->expr_idx, a, &merged_t, &merged.data[i], db, rb);
                    da_push(&dst.cells, val);
                } else {
                    struct cell nc = {0};
                    nc.type = COLUMN_TYPE_TEXT;
                    nc.is_null = 1;
                    da_push(&dst.cells, nc);
                }
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
                    struct cell cp;
                    if (rb) cell_copy_bump(&cp, &merged.data[i].cells.items[idx], rb);
                    else    cell_copy(&cp, &merged.data[i].cells.items[idx]);
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

/* recursively resolve subqueries in condition tree (arena-based).
 * Subquery SQL strings are stored in the arena; after consumption we set
 * the index to IDX_NONE so they won't be re-resolved.
 * New in_values are pushed into the arena cells pool. */
static void resolve_subqueries(struct database *db, struct query_arena *arena, uint32_t cond_idx, const char *outer_table)
{
    if (cond_idx == IDX_NONE) return;
    struct condition *c = &COND(arena, cond_idx);
    if (c->type == COND_AND || c->type == COND_OR) {
        resolve_subqueries(db, arena, c->left, outer_table);
        resolve_subqueries(db, arena, c->right, outer_table);
        return;
    }
    if (c->type == COND_NOT) {
        resolve_subqueries(db, arena, c->left, outer_table);
        return;
    }
    /* EXISTS / NOT EXISTS: defer to eval_condition for per-row evaluation
     * (supports correlated subqueries that reference outer table columns) */
    if (c->type == COND_COMPARE && c->subquery_sql != IDX_NONE &&
        (c->op == CMP_EXISTS || c->op == CMP_NOT_EXISTS)) {
        return; /* leave subquery_sql set for eval_condition */
    }
    if (c->type == COND_COMPARE && c->subquery_sql != IDX_NONE &&
        (c->op == CMP_IN || c->op == CMP_NOT_IN)) {
        struct query sq = {0};
        if (query_parse(ASTRING(arena, c->subquery_sql), &sq) == 0) {
            struct rows sq_result = {0};
            if (db_exec(db, &sq, &sq_result, NULL) == 0) {
                /* push resolved values into arena cells pool */
                uint32_t iv_start = (uint32_t)arena->cells.count;
                uint32_t iv_count = 0;
                for (size_t i = 0; i < sq_result.count; i++) {
                    if (sq_result.data[i].cells.count > 0) {
                        struct cell v = {0};
                        struct cell *src = &sq_result.data[i].cells.items[0];
                        v.type = src->type;
                        v.is_null = src->is_null;
                        if (column_type_is_text(src->type) && src->value.as_text)
                            v.value.as_text = bump_strdup(&arena->bump, src->value.as_text);
                        else
                            v.value = src->value;
                        arena_push_cell(arena, v);
                        iv_count++;
                    }
                }
                c->in_values_start = iv_start;
                c->in_values_count = iv_count;
                for (size_t i = 0; i < sq_result.count; i++)
                    row_free(&sq_result.data[i]);
                free(sq_result.data);
            }
        }
        query_free(&sq);
        c->subquery_sql = IDX_NONE;
    }
    /* scalar subquery: WHERE col > (SELECT ...) */
    if (c->type == COND_COMPARE && c->scalar_subquery_sql != IDX_NONE) {
        /* check if this is a correlated subquery (references outer table columns) */
        const char *sq_sql = ASTRING(arena, c->scalar_subquery_sql);
        int is_correlated = 0;
        if (outer_table) {
            char ref_prefix[256];
            snprintf(ref_prefix, sizeof(ref_prefix), "%s.", outer_table);
            if (strstr(sq_sql, ref_prefix)) is_correlated = 1;
        }
        if (!is_correlated) {
            /* non-correlated: resolve once */
            struct query sq = {0};
            if (query_parse(sq_sql, &sq) == 0) {
                struct rows sq_result = {0};
                if (db_exec(db, &sq, &sq_result, NULL) == 0) {
                    if (sq_result.count > 0 && sq_result.data[0].cells.count > 0) {
                        /* Don't call cell_free_text — the old value's text (if any)
                         * lives in the bump slab and must not be free()'d. */
                        struct cell *src = &sq_result.data[0].cells.items[0];
                        c->value.type = src->type;
                        c->value.is_null = src->is_null;
                        if (column_type_is_text(src->type) && src->value.as_text)
                            c->value.value.as_text = bump_strdup(&arena->bump, src->value.as_text);
                        else
                            c->value.value = src->value;
                    }
                    for (size_t i = 0; i < sq_result.count; i++)
                        row_free(&sq_result.data[i]);
                    free(sq_result.data);
                }
            }
            query_free(&sq);
            c->scalar_subquery_sql = IDX_NONE;
        }
        /* correlated: leave scalar_subquery_sql set for per-row eval in eval_condition */
    }
}

void snapshot_free(struct db_snapshot *snap)
{
    for (size_t i = 0; i < snap->orig_table_count; i++) {
        free(snap->table_names[i]);
        if (snap->saved_valid[i])
            table_free(&snap->saved_tables[i]);
    }
    free(snap->table_names);
    free(snap->table_generations);
    free(snap->saved_tables);
    free(snap->saved_valid);
    for (size_t i = 0; i < snap->types.count; i++)
        enum_type_free(&snap->types.items[i]);
    da_free(&snap->types);
    free(snap);
}

struct db_snapshot *snapshot_create(struct database *db)
{
    struct db_snapshot *snap = calloc(1, sizeof(*snap));
    size_t n = db->tables.count;
    snap->orig_table_count = n;
    snap->table_names = malloc(n * sizeof(char *));
    snap->table_generations = malloc(n * sizeof(uint64_t));
    snap->saved_tables = calloc(n, sizeof(struct table));
    snap->saved_valid = calloc(n, sizeof(int));
    for (size_t i = 0; i < n; i++) {
        snap->table_names[i] = strdup(db->tables.items[i].name);
        snap->table_generations[i] = db->tables.items[i].generation;
    }
    /* Types: small, just copy eagerly */
    da_init(&snap->types);
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

void snapshot_cow_table(struct db_snapshot *snap, struct database *db, const char *table_name)
{
    if (!snap) return;
    for (size_t i = 0; i < snap->orig_table_count; i++) {
        if (strcmp(snap->table_names[i], table_name) == 0) {
            if (snap->saved_valid[i]) return; /* already saved */
            /* Find the table in the current database and deep-copy it */
            struct table *t = db_find_table(db, table_name);
            if (t) {
                table_deep_copy(&snap->saved_tables[i], t);
                snap->saved_valid[i] = 1;
            }
            return;
        }
    }
    /* Table not in snapshot (created during transaction) — nothing to save */
}

static void snapshot_restore(struct database *db, struct db_snapshot *snap)
{
    /* Restore only the tables that were COW-saved */
    for (size_t i = 0; i < snap->orig_table_count; i++) {
        if (!snap->saved_valid[i]) continue;
        /* Find the table in the current database and replace it */
        struct table *t = db_find_table(db, snap->table_names[i]);
        if (t) {
            table_free(t);
            *t = snap->saved_tables[i];
            memset(&snap->saved_tables[i], 0, sizeof(struct table));
            snap->saved_valid[i] = 0;
        }
    }
    /* Remove tables created during the transaction (not in original snapshot) */
    for (size_t i = db->tables.count; i > 0; i--) {
        int found = 0;
        for (size_t j = 0; j < snap->orig_table_count; j++) {
            if (strcmp(db->tables.items[i - 1].name, snap->table_names[j]) == 0) {
                found = 1;
                break;
            }
        }
        if (!found) {
            table_free(&db->tables.items[i - 1]);
            for (size_t k = i - 1; k + 1 < db->tables.count; k++)
                db->tables.items[k] = db->tables.items[k + 1];
            db->tables.count--;
        }
    }
    /* Restore dropped tables from snapshot */
    for (size_t i = 0; i < snap->orig_table_count; i++) {
        if (!snap->saved_valid[i]) continue;
        /* Table was saved but not found in current db — it was dropped */
        if (!db_find_table(db, snap->table_names[i])) {
            da_push(&db->tables, snap->saved_tables[i]);
            memset(&snap->saved_tables[i], 0, sizeof(struct table));
            snap->saved_valid[i] = 0;
        }
    }
    /* Restore types */
    for (size_t i = 0; i < db->types.count; i++)
        enum_type_free(&db->types.items[i]);
    da_free(&db->types);
    memcpy(&db->types, &snap->types, sizeof(db->types));
    memset(&snap->types, 0, sizeof(snap->types));
}

/* Materialize a subquery into a temporary table added to db->tables.
 * Returns a pointer to the new table, or NULL on failure.
 * The caller is responsible for removing the table when done. */
/* JPL ownership: the temp table returned here (column names, row cells) is
 * freed by remove_temp_table → table_free, called from db_exec_query which
 * is the sole caller.  Ownership is: materialize allocates, db_exec_query
 * uses and then removes — a clear producer/consumer pair in the same file. */
struct table *materialize_subquery(struct database *db, const char *sql,
                                  const char *table_name)
{
    /* Buffer for rewritten SQL (must outlive sq since parser stores pointers into it) */
    char gs_rewritten[4096];
    struct query sq = {0};
    if (query_parse(sql, &sq) != 0) {
        query_free(&sq);
        /* Retry: rewrite "SELECT generate_series(...) ..." →
         * "SELECT * FROM generate_series(...) ..." so the parser can handle it */
        const char *gs = NULL;
        for (const char *sp = sql; *sp; sp++) {
            if (strncasecmp(sp, "generate_series", 15) == 0) { gs = sp; break; }
        }
        if (gs) {
            const char *p = sql;
            while (*p == ' ' || *p == '\t') p++;
            if (strncasecmp(p, "SELECT", 6) == 0) {
                /* Find the closing ')' of generate_series(...) */
                const char *gs_end = gs + 15; /* skip "generate_series" */
                while (*gs_end && *gs_end != '(') gs_end++;
                if (*gs_end == '(') {
                    int depth = 1;
                    gs_end++;
                    while (*gs_end && depth > 0) {
                        if (*gs_end == '(') depth++;
                        else if (*gs_end == ')') depth--;
                        gs_end++;
                    }
                }
                /* Check for "AS alias" after generate_series(...) */
                const char *ap = gs_end;
                while (*ap == ' ' || *ap == '\t') ap++;
                char col_alias[128] = {0};
                if (strncasecmp(ap, "as", 2) == 0 && (ap[2] == ' ' || ap[2] == '\t')) {
                    ap += 2;
                    while (*ap == ' ' || *ap == '\t') ap++;
                    size_t alen = 0;
                    while (ap[alen] && ap[alen] != ' ' && ap[alen] != '\t' &&
                           ap[alen] != ',' && ap[alen] != ')' && ap[alen] != ';') alen++;
                    if (alen > 0 && alen < sizeof(col_alias))
                        snprintf(col_alias, sizeof(col_alias), "%.*s", (int)alen, ap);
                }
                /* Build: SELECT * FROM generate_series(...) AS gs_tbl(col_alias) */
                size_t gs_call_len = (size_t)(gs_end - gs);
                if (col_alias[0])
                    snprintf(gs_rewritten, sizeof(gs_rewritten),
                             "SELECT * FROM %.*s AS _gs(%s)",
                             (int)gs_call_len, gs, col_alias);
                else
                    snprintf(gs_rewritten, sizeof(gs_rewritten),
                             "SELECT * FROM %.*s", (int)gs_call_len, gs);
                memset(&sq, 0, sizeof(sq));
                if (query_parse(gs_rewritten, &sq) != 0) {
                    query_free(&sq);
                    return NULL;
                }
                goto parse_ok;
            }
        }
        return NULL;
    }
    parse_ok: (void)0;
    struct rows sq_rows = {0};

    /* Try plan executor fast path for simple SELECTs */
    int used_plan = 0;
    if (sq.query_type == QUERY_TYPE_SELECT) {
        struct query_select *ss = &sq.select;
        struct table *src = NULL;
        if (ss->table.len > 0)
            src = db_find_table_sv(db, ss->table);
        if (src) {
            struct plan_result pr = plan_build_select(src, ss, &sq.arena, db);
            if (pr.status == PLAN_OK) {
                struct plan_exec_ctx ctx;
                plan_exec_init(&ctx, &sq.arena, db, pr.node);
                plan_exec_to_rows(&ctx, pr.node, &sq_rows, NULL);
                used_plan = 1;
            }
        }
    }

    if (!used_plan) {
        if (db_exec(db, &sq, &sq_rows, NULL) != 0) {
            query_free(&sq);
            return NULL;
        }
    }
    struct table ct = {0};
    ct.name = strdup(table_name);
    da_init(&ct.columns);
    da_init(&ct.rows);
    da_init(&ct.indexes);
    /* infer column names */
    if (sq.query_type == QUERY_TYPE_SELECT && sq_rows.count > 0) {
        size_t ncells = sq_rows.data[0].cells.count;
        /* For generate_series queries, use the column alias directly */
        if (sq.select.has_generate_series && ncells == 1) {
            struct column col = {0};
            if (sq.select.gs_col_alias.len > 0)
                col.name = sv_to_cstr(sq.select.gs_col_alias);
            else
                col.name = strdup("generate_series");
            col.type = sq_rows.data[0].cells.items[0].type;
            da_push(&ct.columns, col);
            goto columns_done;
        }
        struct table *src_t = db_find_table_sv(db, sq.select.table);
        if (src_t && sv_eq_cstr(sq.select.columns, "*")) {
            for (size_t c = 0; c < src_t->columns.count; c++) {
                struct column col = {0};
                col.name = strdup(src_t->columns.items[c].name);
                col.type = src_t->columns.items[c].type;
                da_push(&ct.columns, col);
            }
        } else if (src_t || sq.select.columns.len > 0) {
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
            for (uint32_t ai = 0; ai < sq.select.aggregates_count && ci < ncells; ai++, ci++) {
                struct agg_expr *ae = &sq.arena.aggregates.items[sq.select.aggregates_start + ai];
                struct column col = {0};
                if (ae->alias.len > 0)
                    col.name = sv_to_cstr(ae->alias);
                else
                    col.name = sv_to_cstr(ae->column);
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
        columns_done: (void)0;
    }
    /* move rows — transfer ownership, no cell_copy/strdup needed */
    for (size_t i = 0; i < sq_rows.count; i++)
        da_push(&ct.rows, sq_rows.data[i]);
    free(sq_rows.data);
    query_free(&sq);
    da_push(&db->tables, ct);
    return &db->tables.items[db->tables.count - 1];
}

/* Remove a temporary table from the database by pointer */
void remove_temp_table(struct database *db, struct table *t)
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

int db_exec(struct database *db, struct query *q, struct rows *result, struct bump_alloc *rb)
{
    /* handle transaction statements first (uses per-connection txn_state) */
    struct txn_state *txn = db->active_txn; /* may be NULL (wasm / internal) */
    if (q->query_type == QUERY_TYPE_BEGIN) {
        if (!txn) { arena_set_error(&q->arena, "25000", "no connection context for transaction"); return -1; }
        /* Support nested BEGIN: create a new snapshot with parent pointer */
        struct db_snapshot *snap = snapshot_create(db);
        snap->parent = txn->snapshot; /* NULL if outermost */
        txn->snapshot = snap;
        txn->in_transaction = 1;
        return 0;
    }
    if (q->query_type == QUERY_TYPE_COMMIT) {
        if (!txn || !txn->in_transaction) {
            arena_set_error(&q->arena, "25P01", "WARNING: no transaction in progress");
            return 0;
        }
        /* Pop the innermost snapshot (discard it, changes are kept) */
        struct db_snapshot *snap = txn->snapshot;
        txn->snapshot = snap->parent;
        snap->parent = NULL;
        snapshot_free(snap);
        if (!txn->snapshot)
            txn->in_transaction = 0;
        return 0;
    }
    if (q->query_type == QUERY_TYPE_ROLLBACK) {
        if (!txn || !txn->in_transaction) {
            arena_set_error(&q->arena, "25P01", "WARNING: no transaction in progress");
            return 0;
        }
        /* Restore only the innermost snapshot, then pop it */
        struct db_snapshot *snap = txn->snapshot;
        txn->snapshot = snap->parent;
        snap->parent = NULL;
        snapshot_restore(db, snap);
        snapshot_free(snap);
        if (!txn->snapshot)
            txn->in_transaction = 0;
        return 0;
    }

    switch (q->query_type) {
        case QUERY_TYPE_CREATE: {
            struct query_create_table *crt = &q->create_table;
            if (crt->if_not_exists && db_find_table_sv(db, crt->table))
                return 0;
            /* CREATE TABLE ... AS SELECT */
            if (crt->as_select_sql != IDX_NONE) {
                const char *sel_sql = ASTRING(&q->arena, crt->as_select_sql);
                struct query sel_q;
                if (query_parse(sel_sql, &sel_q) != 0) {
                    arena_set_error(&q->arena, "42601", "CREATE TABLE AS: invalid SELECT");
                    return -1;
                }
                struct rows sel_rows = {0};
                if (db_exec(db, &sel_q, &sel_rows, NULL) != 0) {
                    query_free(&sel_q);
                    arena_set_error(&q->arena, "42601", "CREATE TABLE AS: SELECT failed");
                    return -1;
                }
                struct table t;
                table_init_own(&t, sv_to_cstr(crt->table));
                /* infer columns from SELECT result */
                if (sel_rows.count > 0 && sel_rows.data[0].cells.count > 0) {
                    /* try to get column names from the select query */
                    sv cols = sel_q.select.columns;
                    size_t ncols = sel_rows.data[0].cells.count;
                    for (size_t ci = 0; ci < ncols; ci++) {
                        struct column col = {0};
                        col.type = sel_rows.data[0].cells.items[ci].type;
                        /* extract column name from raw columns text */
                        char colname[128];
                        int got_name = 0;
                        if (cols.len > 0 && ci == 0) {
                            /* parse comma-separated column names */
                            sv remaining = cols;
                            for (size_t k = 0; k <= ci && remaining.len > 0; k++) {
                                while (remaining.len > 0 && remaining.data[0] == ' ')
                                    { remaining.data++; remaining.len--; }
                                size_t end = 0;
                                int depth = 0;
                                while (end < remaining.len) {
                                    if (remaining.data[end] == '(') depth++;
                                    else if (remaining.data[end] == ')') depth--;
                                    else if (remaining.data[end] == ',' && depth == 0) break;
                                    end++;
                                }
                                if (k == ci) {
                                    sv cn = sv_from(remaining.data, end);
                                    while (cn.len > 0 && cn.data[cn.len-1] == ' ') cn.len--;
                                    /* check for AS alias */
                                    for (size_t p = 0; p + 2 < cn.len; p++) {
                                        if ((cn.data[p] == ' ') &&
                                            (cn.data[p+1] == 'A' || cn.data[p+1] == 'a') &&
                                            (cn.data[p+2] == 'S' || cn.data[p+2] == 's') &&
                                            (p+3 >= cn.len || cn.data[p+3] == ' ')) {
                                            cn = sv_from(cn.data + p + 3, cn.len - p - 3);
                                            while (cn.len > 0 && cn.data[0] == ' ')
                                                { cn.data++; cn.len--; }
                                            break;
                                        }
                                    }
                                    snprintf(colname, sizeof(colname), "%.*s", (int)cn.len, cn.data);
                                    got_name = 1;
                                }
                                if (end < remaining.len) { remaining.data += end + 1; remaining.len -= end + 1; }
                                else break;
                            }
                        }
                        if (!got_name || ci > 0) {
                            /* for subsequent columns, re-parse from cols */
                            sv remaining = cols;
                            size_t col_idx = 0;
                            while (remaining.len > 0 && col_idx <= ci) {
                                while (remaining.len > 0 && remaining.data[0] == ' ')
                                    { remaining.data++; remaining.len--; }
                                size_t end = 0;
                                int depth = 0;
                                while (end < remaining.len) {
                                    if (remaining.data[end] == '(') depth++;
                                    else if (remaining.data[end] == ')') depth--;
                                    else if (remaining.data[end] == ',' && depth == 0) break;
                                    end++;
                                }
                                if (col_idx == ci) {
                                    sv cn = sv_from(remaining.data, end);
                                    while (cn.len > 0 && cn.data[cn.len-1] == ' ') cn.len--;
                                    snprintf(colname, sizeof(colname), "%.*s", (int)cn.len, cn.data);
                                    got_name = 1;
                                }
                                col_idx++;
                                if (end < remaining.len) { remaining.data += end + 1; remaining.len -= end + 1; }
                                else break;
                            }
                        }
                        if (!got_name)
                            snprintf(colname, sizeof(colname), "col%zu", ci + 1);
                        col.name = strdup(colname);
                        table_add_column(&t, &col);
                        free(col.name);
                    }
                }
                /* copy rows */
                for (size_t ri = 0; ri < sel_rows.count; ri++) {
                    struct row nr = {0};
                    da_init(&nr.cells);
                    for (size_t ci = 0; ci < sel_rows.data[ri].cells.count; ci++) {
                        struct cell cp;
                        cell_copy(&cp, &sel_rows.data[ri].cells.items[ci]);
                        da_push(&nr.cells, cp);
                    }
                    da_push(&t.rows, nr);
                }
                da_push(&db->tables, t);
                /* store row count in result for command tag */
                if (result) {
                    struct row r = {0};
                    da_init(&r.cells);
                    struct cell c = { .type = COLUMN_TYPE_INT };
                    c.value.as_int = (int)sel_rows.count;
                    da_push(&r.cells, c);
                    rows_push(result, r);
                }
                /* free select results */
                for (size_t ri = 0; ri < sel_rows.count; ri++)
                    row_free(&sel_rows.data[ri]);
                free(sel_rows.data);
                query_free(&sel_q);
                return 0;
            }
            struct table t;
            table_init_own(&t, sv_to_cstr(crt->table));
            for (uint32_t i = 0; i < crt->columns_count; i++) {
                table_add_column(&t, &q->arena.columns.items[crt->columns_start + i]);
            }
            da_push(&db->tables, t);
            return 0;
        }
        case QUERY_TYPE_DROP: {
            sv drop_tbl = q->drop_table.table;
            for (size_t i = 0; i < db->tables.count; i++) {
                if (sv_eq_cstr(drop_tbl, db->tables.items[i].name)) {
                    /* COW trigger for DROP TABLE */
                    if (txn && txn->in_transaction && txn->snapshot)
                        snapshot_cow_table(txn->snapshot, db, db->tables.items[i].name);
                    table_free(&db->tables.items[i]);
                    /* shift remaining tables down */
                    for (size_t j = i; j + 1 < db->tables.count; j++) {
                        db->tables.items[j] = db->tables.items[j + 1];
                    }
                    db->tables.count--;
                    return 0;
                }
            }
            if (q->drop_table.if_exists) return 0;
            arena_set_error(&q->arena, "42P01", "table '%.*s' does not exist", (int)drop_tbl.len, drop_tbl.data);
            return -1;
        }
        case QUERY_TYPE_CREATE_TYPE: {
            /* Ownership transfer: enum_values strings were allocated by parser.c
             * and are moved into the database's enum_type via NULL-swap below.
             * After transfer, query_create_type_free (parser.c) skips the
             * NULLed slots, and enum_type_free (table.c) frees the strings
             * when the type is dropped. */
            struct query_create_type *ct = &q->create_type;
            if (db_find_type_sv(db, ct->type_name)) {
                arena_set_error(&q->arena, "42710", "type '%.*s' already exists", (int)ct->type_name.len, ct->type_name.data);
                return -1;
            }
            struct enum_type et;
            et.name = sv_to_cstr(ct->type_name);
            da_init(&et.values);
            for (uint32_t i = 0; i < ct->enum_values_count; i++) {
                /* copy the string (arena owns the original) */
                da_push(&et.values, strdup(q->arena.strings.items[ct->enum_values_start + i]));
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
            arena_set_error(&q->arena, "42704", "type '%.*s' does not exist", (int)dtn.len, dtn.data);
            return -1;
        }
        case QUERY_TYPE_CREATE_SEQUENCE: {
            struct query_create_sequence *cs = &q->create_seq;
            /* check for duplicate */
            for (size_t i = 0; i < db->sequences.count; i++) {
                if (sv_eq_cstr(cs->name, db->sequences.items[i].name)) {
                    arena_set_error(&q->arena, "42P07", "sequence '%.*s' already exists", (int)cs->name.len, cs->name.data);
                    return -1;
                }
            }
            struct sequence seq;
            seq.name = sv_to_cstr(cs->name);
            seq.current_value = cs->start_value;
            seq.increment = cs->increment;
            seq.min_value = cs->min_value;
            seq.max_value = cs->max_value;
            seq.has_been_called = 0;
            da_push(&db->sequences, seq);
            return 0;
        }
        case QUERY_TYPE_DROP_SEQUENCE: {
            sv sn = q->drop_seq.name;
            for (size_t i = 0; i < db->sequences.count; i++) {
                if (sv_eq_cstr(sn, db->sequences.items[i].name)) {
                    free(db->sequences.items[i].name);
                    for (size_t j = i; j + 1 < db->sequences.count; j++)
                        db->sequences.items[j] = db->sequences.items[j + 1];
                    db->sequences.count--;
                    return 0;
                }
            }
            arena_set_error(&q->arena, "42P01", "sequence '%.*s' does not exist", (int)sn.len, sn.data);
            return -1;
        }
        case QUERY_TYPE_CREATE_VIEW: {
            struct query_create_view *cv = &q->create_view;
            if (cv->sql_idx == IDX_NONE) {
                arena_set_error(&q->arena, "42601", "CREATE VIEW: missing SELECT body");
                return -1;
            }
            /* store view as a table with zero columns and the SQL in the name
             * prefixed with "VIEW:" so we can detect it later */
            const char *sql = ASTRING(&q->arena, cv->sql_idx);
            /* check for existing view/table */
            if (db_find_table_sv(db, cv->name)) {
                arena_set_error(&q->arena, "42P07", "relation '%.*s' already exists", (int)cv->name.len, cv->name.data);
                return -1;
            }
            struct table vt;
            char vname[512];
            snprintf(vname, sizeof(vname), "%.*s", (int)cv->name.len, cv->name.data);
            table_init(&vt, vname);
            /* store view SQL in a special field — we'll use a convention:
             * a table with 0 columns and view_sql != NULL is a view */
            vt.view_sql = strdup(sql);
            da_push(&db->tables, vt);
            return 0;
        }
        case QUERY_TYPE_DROP_VIEW: {
            sv vn = q->drop_view.name;
            for (size_t i = 0; i < db->tables.count; i++) {
                if (sv_eq_cstr(vn, db->tables.items[i].name) && db->tables.items[i].view_sql) {
                    table_free(&db->tables.items[i]);
                    for (size_t j = i; j + 1 < db->tables.count; j++)
                        db->tables.items[j] = db->tables.items[j + 1];
                    db->tables.count--;
                    return 0;
                }
            }
            arena_set_error(&q->arena, "42P01", "view '%.*s' does not exist", (int)vn.len, vn.data);
            return -1;
        }
        case QUERY_TYPE_EXPLAIN: {
            struct query_explain *ex = &q->explain;
            /* Copy inner SQL to a stack buffer so stringviews remain valid */
            size_t sql_len = ex->inner_sql.len;
            char inner_sql[sql_len + 1];
            memcpy(inner_sql, ex->inner_sql.data, sql_len);
            inner_sql[sql_len] = '\0';

            struct query inner_q;
            memset(&inner_q, 0, sizeof(inner_q));
            query_arena_init(&inner_q.arena);
            int prc = query_parse_into(inner_sql, &inner_q, &inner_q.arena);
            if (prc != 0) {
                if (inner_q.arena.errmsg[0])
                    arena_set_error(&q->arena, inner_q.arena.sqlstate, "%s", inner_q.arena.errmsg);
                query_arena_destroy(&inner_q.arena);
                return -1;
            }

            char explain_buf[4096];
            int explain_len = 0;

            if (inner_q.query_type == QUERY_TYPE_SELECT) {
                struct table *t = db_find_table_sv(db, inner_q.select.table);
                struct plan_result epr = { .node = IDX_NONE, .status = PLAN_NOTIMPL };
                if (t)
                    epr = plan_build_select(t, &inner_q.select, &inner_q.arena, db);
                if (epr.status == PLAN_OK) {
                    explain_len = plan_explain(&inner_q.arena, epr.node, explain_buf, sizeof(explain_buf));
                } else {
                    explain_len = snprintf(explain_buf, sizeof(explain_buf), "Legacy Row Executor");
                }
            } else {
                explain_len = snprintf(explain_buf, sizeof(explain_buf), "Legacy Row Executor");
            }

            /* Build result rows — one row per line */
            char *line = explain_buf;
            for (int i = 0; i <= explain_len; i++) {
                if (i == explain_len || explain_buf[i] == '\n') {
                    explain_buf[i] = '\0';
                    struct row r = {0};
                    da_init(&r.cells);
                    struct cell c = {0};
                    c.type = COLUMN_TYPE_TEXT;
                    c.value.as_text = rb ? bump_strdup(rb, line) : strdup(line);
                    da_push(&r.cells, c);
                    rows_push(result, r);
                    line = explain_buf + i + 1;
                }
            }

            query_arena_destroy(&inner_q.arena);
            return 0;
        }
        case QUERY_TYPE_TRUNCATE: {
            struct table *t = db_find_table_sv(db, q->del.table);
            if (!t) {
                arena_set_error(&q->arena, "42P01", "table '%.*s' does not exist", (int)q->del.table.len, q->del.table.data);
                return -1;
            }
            /* COW trigger for TRUNCATE */
            if (txn && txn->in_transaction && txn->snapshot)
                snapshot_cow_table(txn->snapshot, db, t->name);
            /* free all rows */
            for (size_t i = 0; i < t->rows.count; i++)
                row_free(&t->rows.items[i]);
            t->rows.count = 0;
            t->generation++;
            db->total_generation++;
            /* reset SERIAL counters */
            for (size_t i = 0; i < t->columns.count; i++) {
                if (t->columns.items[i].is_serial)
                    t->columns.items[i].serial_next = 1;
            }
            /* clear indexes */
            for (size_t i = 0; i < t->indexes.count; i++)
                index_reset(&t->indexes.items[i]);
            return 0;
        }
        case QUERY_TYPE_CREATE_INDEX: {
            struct query_create_index *ci = &q->create_index;
            struct table *t = db_find_table_sv(db, ci->table);
            if (!t) {
                arena_set_error(&q->arena, "42P01", "table '%.*s' does not exist", (int)ci->table.len, ci->table.data);
                return -1;
            }
            /* IF NOT EXISTS: check if index already exists */
            if (ci->if_not_exists) {
                for (size_t ii = 0; ii < t->indexes.count; ii++) {
                    if (sv_eq_cstr(ci->index_name, t->indexes.items[ii].name))
                        return 0;
                }
            }
            int col_idx = table_find_column_sv(t, ci->index_column);
            if (col_idx < 0) {
                arena_set_error(&q->arena, "42703", "column '%.*s' not found in table '%.*s'", (int)ci->index_column.len, ci->index_column.data, (int)ci->table.len, ci->table.data);
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
            arena_set_error(&q->arena, "42704", "index '%.*s' does not exist", (int)q->drop_index.index_name.len, q->drop_index.index_name.data);
            return -1;
        }
        case QUERY_TYPE_SELECT: {
            struct query_select *s = &q->select;
            /* CTE: create temporary tables from CTE definitions */
            struct table *cte_temps[32] = {0};
            size_t n_cte_temps = 0;

            /* multiple CTEs (new path) */
            struct query_arena *qa = &q->arena;
            if (s->ctes_count > 0) {
                for (uint32_t ci = 0; ci < s->ctes_count && n_cte_temps < 32; ci++) {
                    struct cte_def *cd = &qa->ctes.items[s->ctes_start + ci];
                    const char *cd_sql = ASTRING(qa, cd->sql_idx);
                    const char *cd_name = ASTRING(qa, cd->name_idx);
                    if (cd->is_recursive) {
                        /* Recursive CTE: the SQL should be "base UNION ALL recursive"
                         * We execute the base case, create the temp table, then
                         * repeatedly execute the recursive part until no new rows. */
                        /* find UNION ALL separator */
                        const char *union_pos = NULL;
                        const char *p = cd_sql;
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
                            cte_temps[n_cte_temps] = materialize_subquery(db, cd_sql, cd_name);
                            n_cte_temps++;
                            continue;
                        }
                        /* extract base SQL (before UNION) */
                        size_t base_len = (size_t)(union_pos - cd_sql);
                        char *base_sql = malloc(base_len + 1);
                        memcpy(base_sql, cd_sql, base_len);
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
                        struct table *ct = materialize_subquery(db, base_sql, cd_name);
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
                            ct = db_find_table(db, cd_name);
                            if (!ct) break;
                            struct rows rec_rows = {0};
                            if (db_exec_sql(db, rec_sql, &rec_rows) != 0) {
                                free(rec_rows.data);
                                break;
                            }
                            if (rec_rows.count == 0) { free(rec_rows.data); break; }

                            /* replace CTE table rows with only the new rows */
                            for (size_t ri = 0; ri < ct->rows.count; ri++)
                                row_free(&ct->rows.items[ri]);
                            ct->rows.count = 0;
                            ct->generation++;
                            db->total_generation++;

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
                        ct = db_find_table(db, cd_name);
                        if (ct) {
                            for (size_t ri = 0; ri < ct->rows.count; ri++)
                                row_free(&ct->rows.items[ri]);
                            ct->rows.count = 0;
                            for (size_t ri = 0; ri < accum.count; ri++)
                                da_push(&ct->rows, accum.data[ri]);
                            free(accum.data);
                        } else {
                            // NOTE: if ct is NULL the table was removed during iteration.
                            // Working table rows were freed when the table was dropped.
                            // We only need to free the accumulator copies here.
                            for (size_t ri = 0; ri < accum.count; ri++)
                                row_free(&accum.data[ri]);
                            free(accum.data);
                        }
                        free(rec_sql);
                    } else {
                        cte_temps[n_cte_temps] = materialize_subquery(db, cd_sql, cd_name);
                        n_cte_temps++;
                    }
                }
            } else if (s->cte_name != IDX_NONE && s->cte_sql != IDX_NONE) {
                /* legacy single CTE */
                cte_temps[0] = materialize_subquery(db, ASTRING(qa, s->cte_sql), ASTRING(qa, s->cte_name));
                n_cte_temps = 1;
            }

            /* FROM subquery: create temp table */
            struct table *from_sub_table = NULL;
            if (s->from_subquery_sql != IDX_NONE) {
                char alias_buf[256];
                if (s->from_subquery_alias.len > 0) {
                    snprintf(alias_buf, sizeof(alias_buf), "%.*s",
                             (int)s->from_subquery_alias.len, s->from_subquery_alias.data);
                } else {
                    snprintf(alias_buf, sizeof(alias_buf), "_from_sub");
                }
                from_sub_table = materialize_subquery(db, ASTRING(&q->arena, s->from_subquery_sql), alias_buf);
            }

            /* generate_series: materialize as temp table */
            struct table *gs_temp = NULL;
            if (s->has_generate_series) {
                /* evaluate start, stop, step expressions */
                struct cell c_start = eval_expr(s->gs_start_expr, &q->arena, NULL, NULL, db, NULL);
                struct cell c_stop  = eval_expr(s->gs_stop_expr, &q->arena, NULL, NULL, db, NULL);
                long long gs_step_val = 1;
                int is_ts = 0; /* 1 if timestamp/date series */
                double ts_step_sec = 0.0;
                if (s->gs_step_expr != IDX_NONE) {
                    struct cell c_step = eval_expr(s->gs_step_expr, &q->arena, NULL, NULL, db, NULL);
                    if (c_step.type == COLUMN_TYPE_INTERVAL ||
                        (column_type_is_text(c_step.type) && c_step.value.as_text)) {
                        is_ts = 1;
                        if (c_step.type == COLUMN_TYPE_INTERVAL || column_type_is_text(c_step.type))
                            ts_step_sec = parse_interval_to_seconds(c_step.value.as_text);
                    } else {
                        gs_step_val = (c_step.type == COLUMN_TYPE_BIGINT) ? c_step.value.as_bigint
                                    : (c_step.type == COLUMN_TYPE_FLOAT)  ? (long long)c_step.value.as_float
                                    : c_step.value.as_int;
                    }
                }
                /* detect timestamp/date series from start value */
                if (!is_ts && (c_start.type == COLUMN_TYPE_DATE ||
                               c_start.type == COLUMN_TYPE_TIMESTAMP ||
                               c_start.type == COLUMN_TYPE_TIMESTAMPTZ)) {
                    is_ts = 1;
                    if (ts_step_sec == 0.0) ts_step_sec = 86400.0; /* default 1 day */
                }

                /* determine column name */
                const char *col_name = "generate_series";
                char col_name_buf[256];
                if (s->gs_col_alias.len > 0) {
                    snprintf(col_name_buf, sizeof(col_name_buf), "%.*s",
                             (int)s->gs_col_alias.len, s->gs_col_alias.data);
                    col_name = col_name_buf;
                }

                /* determine table name */
                char gs_tbl_name[256];
                if (s->gs_alias.len > 0) {
                    snprintf(gs_tbl_name, sizeof(gs_tbl_name), "%.*s",
                             (int)s->gs_alias.len, s->gs_alias.data);
                } else {
                    snprintf(gs_tbl_name, sizeof(gs_tbl_name), "generate_series");
                }

                /* build temp table */
                struct table gt = {0};
                gt.name = strdup(gs_tbl_name);
                da_init(&gt.columns);
                da_init(&gt.rows);
                da_init(&gt.indexes);

                if (is_ts) {
                    /* timestamp/date series */
                    struct column col = {0};
                    col.name = strdup(col_name);
                    col.type = c_start.type;
                    da_push(&gt.columns, col);

                    struct tm tm_start, tm_stop;
                    const char *s_start = c_start.value.as_text ? c_start.value.as_text : "";
                    const char *s_stop  = c_stop.value.as_text  ? c_stop.value.as_text  : "";
                    parse_datetime(s_start, &tm_start);
                    parse_datetime(s_stop, &tm_stop);
                    time_t t_cur = mktime(&tm_start);
                    time_t t_end = mktime(&tm_stop);
                    long long step_sec = (long long)ts_step_sec;
                    if (step_sec == 0) step_sec = 86400;
                    int max_rows = 10000000;
                    while ((step_sec > 0 && t_cur <= t_end) ||
                           (step_sec < 0 && t_cur >= t_end)) {
                        if (--max_rows < 0) break;
                        struct tm *cur_tm = localtime(&t_cur);
                        char buf[32];
                        if (c_start.type == COLUMN_TYPE_DATE)
                            snprintf(buf, sizeof(buf), "%04d-%02d-%02d",
                                     cur_tm->tm_year + 1900, cur_tm->tm_mon + 1, cur_tm->tm_mday);
                        else
                            snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                                     cur_tm->tm_year + 1900, cur_tm->tm_mon + 1, cur_tm->tm_mday,
                                     cur_tm->tm_hour, cur_tm->tm_min, cur_tm->tm_sec);
                        struct row r = {0};
                        da_init(&r.cells);
                        struct cell c = {0};
                        c.type = c_start.type;
                        c.value.as_text = strdup(buf);
                        da_push(&r.cells, c);
                        da_push(&gt.rows, r);
                        t_cur += step_sec;
                    }
                } else {
                    /* integer series */
                    long long gs_start = (c_start.type == COLUMN_TYPE_BIGINT) ? c_start.value.as_bigint
                                       : (c_start.type == COLUMN_TYPE_FLOAT)  ? (long long)c_start.value.as_float
                                       : c_start.value.as_int;
                    long long gs_stop  = (c_stop.type == COLUMN_TYPE_BIGINT) ? c_stop.value.as_bigint
                                       : (c_stop.type == COLUMN_TYPE_FLOAT)  ? (long long)c_stop.value.as_float
                                       : c_stop.value.as_int;
                    if (gs_step_val == 0) gs_step_val = 1;

                    int use_bigint = (c_start.type == COLUMN_TYPE_BIGINT ||
                                      c_stop.type == COLUMN_TYPE_BIGINT ||
                                      gs_start > 2147483647LL || gs_start < -2147483648LL ||
                                      gs_stop > 2147483647LL || gs_stop < -2147483648LL);
                    struct column col = {0};
                    col.name = strdup(col_name);
                    col.type = use_bigint ? COLUMN_TYPE_BIGINT : COLUMN_TYPE_INT;
                    da_push(&gt.columns, col);

                    int max_rows = 10000000;
                    for (long long v = gs_start;
                         (gs_step_val > 0 && v <= gs_stop) || (gs_step_val < 0 && v >= gs_stop);
                         v += gs_step_val) {
                        if (--max_rows < 0) break;
                        struct row r = {0};
                        da_init(&r.cells);
                        struct cell c = {0};
                        if (use_bigint) {
                            c.type = COLUMN_TYPE_BIGINT;
                            c.value.as_bigint = v;
                        } else {
                            c.type = COLUMN_TYPE_INT;
                            c.value.as_int = (int)v;
                        }
                        da_push(&r.cells, c);
                        da_push(&gt.rows, r);
                    }
                }
                da_push(&db->tables, gt);
                gs_temp = &db->tables.items[db->tables.count - 1];
                /* rewrite table reference to point to the temp table */
                s->table = sv_from(gs_temp->name, strlen(gs_temp->name));
            }

            /* view expansion: if the table is a view, materialize it */
            struct table *view_temp = NULL;
            char view_temp_name[256] = {0};
            if (s->table.len > 0 && !from_sub_table) {
                struct table *maybe_view = db_find_table_sv(db, s->table);
                if (maybe_view && maybe_view->view_sql) {
                    snprintf(view_temp_name, sizeof(view_temp_name),
                             "_view_%.*s", (int)s->table.len, s->table.data);
                    view_temp = materialize_subquery(db, maybe_view->view_sql, view_temp_name);
                    if (view_temp) {
                        /* rewrite table reference to point to the temp table */
                        s->table = sv_from(view_temp->name, strlen(view_temp->name));
                    }
                }
            }

            /* resolve any IN (SELECT ...) / EXISTS subqueries */
            if (s->where.has_where && s->where.where_cond != IDX_NONE) {
                char outer_tbl[256] = {0};
                if (s->table.len > 0 && s->table.len < sizeof(outer_tbl)) {
                    memcpy(outer_tbl, s->table.data, s->table.len);
                    outer_tbl[s->table.len] = '\0';
                }
                resolve_subqueries(db, &q->arena, s->where.where_cond, outer_tbl[0] ? outer_tbl : NULL);
            }
            /* resolve subqueries in HAVING */
            if (s->has_having && s->having_cond != IDX_NONE)
                resolve_subqueries(db, &q->arena, s->having_cond, NULL);
            int sel_rc;
            if (s->table.len == 0 && s->parsed_columns_count > 0 && result) {
                /* SELECT <expr>, ... — no table, evaluate expression ASTs */
                struct row dst = {0};
                da_init(&dst.cells);
                for (uint32_t i = 0; i < s->parsed_columns_count; i++) {
                    struct select_column *sc = &q->arena.select_cols.items[s->parsed_columns_start + i];
                    struct cell c = eval_expr(sc->expr_idx, &q->arena, NULL, NULL, db, rb);
                    da_push(&dst.cells, c);
                }
                rows_push(result, dst);
                sel_rc = 0;
            } else if (s->table.len == 0 && s->insert_rows_count > 0 && result) {
                /* legacy: SELECT <literal> via insert_rows in arena */
                struct row *ir = &qa->rows.items[s->insert_rows_start];
                struct row dst = {0};
                da_init(&dst.cells);
                for (size_t i = 0; i < ir->cells.count; i++) {
                    struct cell c = {0};
                    c.is_null = ir->cells.items[i].is_null;
                    c.type = ir->cells.items[i].type;
                    if (column_type_is_text(c.type)
                        && ir->cells.items[i].value.as_text)
                        c.value.as_text = rb ? bump_strdup(rb, ir->cells.items[i].value.as_text)
                                             : strdup(ir->cells.items[i].value.as_text);
                    else
                        c.value = ir->cells.items[i].value;
                    da_push(&dst.cells, c);
                }
                rows_push(result, dst);
                sel_rc = 0;
            } else if (s->has_join) {
                sel_rc = exec_join(db, q, result, rb);
            } else {
                sel_rc = db_table_exec_query(db, s->table, q, result, rb);
            }

            /* UNION / INTERSECT / EXCEPT */
            // TODO: CONTAINER REUSE: the row-equality comparison loop (iterate cells,
            // call cell_equal) is duplicated three times below for UNION, INTERSECT, and
            // EXCEPT. Extract a rows_equal(row*, row*) helper into row.c.
            if (sel_rc == 0 && s->has_set_op && s->set_rhs_sql != IDX_NONE && result) {
                struct query rhs_q = {0};
                int rhs_parsed = (query_parse(ASTRING(&q->arena, s->set_rhs_sql), &rhs_q) == 0);
                if (rhs_parsed) {
                    struct rows rhs_rows = {0};
                    if (db_exec(db, &rhs_q, &rhs_rows, NULL) == 0) {
                        /* If the caller uses bump-allocated result text (pgwire path),
                         * re-home RHS heap-allocated text cells into the bump so they
                         * are compatible with arena_owns_text bulk-free semantics. */
                        if (rb) {
                            for (size_t i = 0; i < rhs_rows.count; i++) {
                                struct row *r = &rhs_rows.data[i];
                                for (size_t c = 0; c < r->cells.count; c++) {
                                    struct cell *cl = &r->cells.items[c];
                                    if (column_type_is_text(cl->type) && cl->value.as_text) {
                                        char *old = cl->value.as_text;
                                        cl->value.as_text = bump_strdup(rb, old);
                                        free(old);
                                    }
                                }
                            }
                            rhs_rows.arena_owns_text = 1;
                        }
                        if (s->set_op == 0) {
                            // TODO: UNION duplicate check is O(n*m); could hash or sort
                            // for better performance on large result sets
                            /* UNION [ALL] — append RHS rows */
                            for (size_t i = 0; i < rhs_rows.count; i++) {
                                if (!s->set_all) {
                                    /* check for duplicates */
                                    int dup = 0;
                                    for (size_t j = 0; j < result->count; j++) {
                                        if (row_equal_nullsafe(&result->data[j], &rhs_rows.data[i])) { dup = 1; break; }
                                    }
                                    if (dup) {
                                        if (rhs_rows.arena_owns_text)
                                            da_free(&rhs_rows.data[i].cells);
                                        else
                                            row_free(&rhs_rows.data[i]);
                                        continue;
                                    }
                                }
                                rows_push(result, rhs_rows.data[i]);
                                rhs_rows.data[i].cells.items = NULL;
                                rhs_rows.data[i].cells.count = 0;
                            }
                        } else if (s->set_op == 1) {
                            /* INTERSECT [ALL] — keep only rows in both.
                             * For ALL: each RHS row can match at most one LHS row. */
                            uint8_t *rhs_used = calloc(rhs_rows.count, 1);
                            size_t w = 0;
                            for (size_t i = 0; i < result->count; i++) {
                                int found = 0;
                                for (size_t j = 0; j < rhs_rows.count; j++) {
                                    if (rhs_used[j]) continue;
                                    if (row_equal_nullsafe(&result->data[i], &rhs_rows.data[j])) {
                                        found = 1;
                                        if (s->set_all) rhs_used[j] = 1;
                                        break;
                                    }
                                }
                                if (found) {
                                    if (w != i) result->data[w] = result->data[i];
                                    w++;
                                } else {
                                    if (result->arena_owns_text)
                                        da_free(&result->data[i].cells);
                                    else
                                        row_free(&result->data[i]);
                                }
                            }
                            result->count = w;
                            free(rhs_used);
                        } else if (s->set_op == 2) {
                            /* EXCEPT [ALL] — keep LHS rows not in RHS.
                             * For ALL: each RHS row removes at most one LHS row. */
                            uint8_t *rhs_used = calloc(rhs_rows.count, 1);
                            size_t w = 0;
                            for (size_t i = 0; i < result->count; i++) {
                                int found = 0;
                                for (size_t j = 0; j < rhs_rows.count; j++) {
                                    if (rhs_used[j]) continue;
                                    if (row_equal_nullsafe(&result->data[i], &rhs_rows.data[j])) {
                                        found = 1;
                                        if (s->set_all) rhs_used[j] = 1;
                                        break;
                                    }
                                }
                                if (!found) {
                                    if (w != i) result->data[w] = result->data[i];
                                    w++;
                                } else {
                                    if (result->arena_owns_text)
                                        da_free(&result->data[i].cells);
                                    else
                                        row_free(&result->data[i]);
                                }
                            }
                            result->count = w;
                            free(rhs_used);
                        }
                        for (size_t i = 0; i < rhs_rows.count; i++) {
                            if (rhs_rows.arena_owns_text)
                                da_free(&rhs_rows.data[i].cells);
                            else
                                row_free(&rhs_rows.data[i]);
                        }
                        free(rhs_rows.data);
                    }
                }
                query_free(&rhs_q);
            }

            /* ORDER BY on combined set operation result */
            if (sel_rc == 0 && s->has_set_op && s->set_order_by != IDX_NONE &&
                result && result->count > 1) {
                /* parse the ORDER BY clause stored as text */
                char ob_wrap[512];
                snprintf(ob_wrap, sizeof(ob_wrap), "SELECT x FROM t %s", ASTRING(qa, s->set_order_by));
                struct query ob_q = {0};
                if (query_parse(ob_wrap, &ob_q) == 0 && ob_q.select.has_order_by &&
                    ob_q.select.order_by_count > 0) {
                    /* resolve column indices against the source table */
                    struct table *src_t = db_find_table_sv(db, s->table);
                    struct query_arena *ob_a = &ob_q.arena;
                    int ord_cols[32];
                    int ord_descs[32];
                    size_t nord = ob_q.select.order_by_count < 32 ? ob_q.select.order_by_count : 32;
                    for (size_t k = 0; k < nord; k++) {
                        struct order_by_item *obi = &ob_a->order_items.items[ob_q.select.order_by_start + k];
                        ord_cols[k] = -1;
                        ord_descs[k] = obi->desc;
                        if (src_t) {
                            char obuf[256];
                            const char *ord_name = extract_col_name(
                                obi->column, obuf, sizeof(obuf));
                            ord_cols[k] = table_find_column(src_t, ord_name);
                            /* if not found, try resolving as a SELECT alias */
                            if (ord_cols[k] < 0 && s->columns.len > 0) {
                                ord_cols[k] = resolve_alias_to_column(src_t, s->columns,
                                    obi->column);
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

            /* clean up generate_series temp table */
            if (gs_temp)
                remove_temp_table(db, gs_temp);

            /* clean up view temp table */
            if (view_temp)
                remove_temp_table(db, view_temp);

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
            if (ins->insert_select_sql != IDX_NONE) {
                struct query sel_q = {0};
                const char *sel_sql = ASTRING(&q->arena, ins->insert_select_sql);
                char *cte_sql = NULL;
                if (ins->cte_name != IDX_NONE && ins->cte_sql != IDX_NONE) {
                    /* prepend WITH cte_name AS (cte_sql) to the SELECT */
                    const char *cn = ASTRING(&q->arena, ins->cte_name);
                    const char *cs = ASTRING(&q->arena, ins->cte_sql);
                    size_t needed = 5 + strlen(cn) + 5 + strlen(cs) + 2 + strlen(sel_sql) + 1;
                    cte_sql = malloc(needed);
                    snprintf(cte_sql, needed, "WITH %s AS (%s) %s", cn, cs, sel_sql);
                    sel_sql = cte_sql;
                }
                /* save table name before db_exec may invalidate ins pointer */
                char target_name[256];
                size_t tlen = ins->table.len < 255 ? ins->table.len : 255;
                memcpy(target_name, ins->table.data, tlen);
                target_name[tlen] = '\0';
                sv target_table = sv_from(target_name, tlen);
                if (query_parse(sel_sql, &sel_q) != 0) {
                    free(cte_sql);
                    query_free(&sel_q);
                    return -1;
                }
                /* NOTE: do NOT free cte_sql yet — sel_q has sv pointers into it */
                struct rows sel_rows = {0};
                if (db_exec(db, &sel_q, &sel_rows, NULL) != 0) {
                    query_free(&sel_q);
                    free(cte_sql);
                    return -1;
                }
                query_free(&sel_q);
                free(cte_sql);
                /* insert each result row into the target table */
                struct table *t = db_find_table_sv(db, target_table);
                if (!t) {
                    arena_set_error(&q->arena, "42P01", "table '%.*s' does not exist", (int)target_table.len, target_table.data);
                    for (size_t i = 0; i < sel_rows.count; i++) row_free(&sel_rows.data[i]);
                    free(sel_rows.data);
                    return -1;
                }
                for (size_t i = 0; i < sel_rows.count; i++) {
                    struct row r = {0};
                    da_init(&r.cells);
                    if (ins->insert_columns_count > 0) {
                        /* column list provided: build full-width row with defaults/NULLs */
                        for (size_t ci = 0; ci < t->columns.count; ci++) {
                            if (t->columns.items[ci].has_default && t->columns.items[ci].default_value) {
                                struct cell dup;
                                cell_copy(&dup, t->columns.items[ci].default_value);
                                da_push(&r.cells, dup);
                            } else {
                                struct cell null_cell = {0};
                                null_cell.type = t->columns.items[ci].type;
                                null_cell.is_null = 1;
                                da_push(&r.cells, null_cell);
                            }
                        }
                        for (uint32_t vi = 0; vi < ins->insert_columns_count && vi < (uint32_t)sel_rows.data[i].cells.count; vi++) {
                            sv col_name = ASV(&q->arena, ins->insert_columns_start + vi);
                            int ci = table_find_column_sv(t, col_name);
                            if (ci < 0) continue;
                            cell_free_text(&r.cells.items[ci]);
                            cell_copy(&r.cells.items[ci], &sel_rows.data[i].cells.items[vi]);
                        }
                    } else {
                        for (size_t c = 0; c < sel_rows.data[i].cells.count; c++) {
                            struct cell cp;
                            cell_copy(&cp, &sel_rows.data[i].cells.items[c]);
                            da_push(&r.cells, cp);
                        }
                        /* pad with DEFAULT or NULL if fewer values than columns */
                        while (r.cells.count < t->columns.count) {
                            size_t ci = r.cells.count;
                            if (t->columns.items[ci].has_default && t->columns.items[ci].default_value) {
                                struct cell dup;
                                cell_copy(&dup, t->columns.items[ci].default_value);
                                da_push(&r.cells, dup);
                            } else {
                                struct cell null_cell = {0};
                                null_cell.type = t->columns.items[ci].type;
                                null_cell.is_null = 1;
                                da_push(&r.cells, null_cell);
                            }
                        }
                    }
                    /* auto-increment SERIAL/BIGSERIAL columns */
                    for (size_t ci = 0; ci < t->columns.count && ci < r.cells.count; ci++) {
                        if (t->columns.items[ci].is_serial) {
                            struct cell *c = &r.cells.items[ci];
                            int is_null = c->is_null || (column_type_is_text(c->type) && !c->value.as_text);
                            if (is_null) {
                                long long val = t->columns.items[ci].serial_next++;
                                c->is_null = 0;
                                if (t->columns.items[ci].type == COLUMN_TYPE_BIGINT) {
                                    c->type = COLUMN_TYPE_BIGINT;
                                    c->value.as_bigint = val;
                                } else if (t->columns.items[ci].type == COLUMN_TYPE_SMALLINT) {
                                    c->type = COLUMN_TYPE_SMALLINT;
                                    c->value.as_smallint = (int16_t)val;
                                } else {
                                    c->type = COLUMN_TYPE_INT;
                                    c->value.as_int = (int)val;
                                }
                            } else {
                                long long v = (c->type == COLUMN_TYPE_BIGINT) ? c->value.as_bigint :
                                              (c->type == COLUMN_TYPE_SMALLINT) ? (long long)c->value.as_smallint : c->value.as_int;
                                if (v >= t->columns.items[ci].serial_next)
                                    t->columns.items[ci].serial_next = v + 1;
                            }
                        }
                    }
                    da_push(&t->rows, r);
                    t->generation++;
                    db->total_generation++;
                }
                int cnt = (int)sel_rows.count;
                for (size_t i = 0; i < sel_rows.count; i++) row_free(&sel_rows.data[i]);
                free(sel_rows.data);
                query_free(&sel_q);
                return cnt;
            }
            /* ON CONFLICT DO UPDATE — update conflicting rows with SET clauses */
            if (ins->has_on_conflict && ins->on_conflict_do_update) {
                uint32_t orig_count = ins->insert_rows_count;
                struct table *t = db_find_table_sv(db, ins->table);
                if (t) {
                    int conflict_col = -1;
                    if (ins->conflict_column.len > 0)
                        conflict_col = table_find_column_sv(t, ins->conflict_column);
                    else {
                        for (size_t c = 0; c < t->columns.count; c++) {
                            if (t->columns.items[c].is_unique || t->columns.items[c].is_primary_key) {
                                conflict_col = (int)c;
                                break;
                            }
                        }
                    }
                    if (conflict_col >= 0) {
                        struct row *ir_items = &q->arena.rows.items[ins->insert_rows_start];
                        uint32_t ir_count = ins->insert_rows_count;
                        for (uint32_t ri = 0; ri < ir_count; ) {
                            struct cell *new_cell = &ir_items[ri].cells.items[conflict_col];
                            int conflict = 0;
                            size_t conflict_row = 0;
                            for (size_t ei = 0; ei < t->rows.count; ei++) {
                                if (cell_equal(new_cell, &t->rows.items[ei].cells.items[conflict_col])) {
                                    conflict = 1;
                                    conflict_row = ei;
                                    break;
                                }
                            }
                            if (conflict) {
                                /* apply SET clauses to the existing row */
                                for (uint32_t sc = 0; sc < ins->conflict_set_count; sc++) {
                                    struct set_clause *scp = &q->arena.set_clauses.items[ins->conflict_set_start + sc];
                                    int ci = table_find_column_sv(t, scp->column);
                                    if (ci < 0) continue;
                                    struct cell val;
                                    if (scp->expr_idx != IDX_NONE) {
                                        /* check if expression is EXCLUDED.col — resolve from new row */
                                        struct expr *se = &EXPR(&q->arena, scp->expr_idx);
                                        if (se->type == EXPR_COLUMN_REF && se->column_ref.table.len > 0 &&
                                            sv_eq_ignorecase_cstr(se->column_ref.table, "excluded")) {
                                            int ecol = table_find_column_sv(t, se->column_ref.column);
                                            if (ecol >= 0 && (size_t)ecol < ir_items[ri].cells.count)
                                                cell_copy(&val, &ir_items[ri].cells.items[ecol]);
                                            else {
                                                memset(&val, 0, sizeof(val));
                                                val.type = COLUMN_TYPE_INT;
                                                val.is_null = 1;
                                            }
                                        } else {
                                            val = eval_expr(scp->expr_idx, &q->arena, t, &t->rows.items[conflict_row], db, NULL);
                                        }
                                    } else
                                        cell_copy(&val, &scp->value);
                                    struct cell *dst = &t->rows.items[conflict_row].cells.items[ci];
                                    if (column_type_is_text(dst->type) && dst->value.as_text)
                                        free(dst->value.as_text);
                                    *dst = val;
                                }
                                /* remove this insert row — it was handled as update.
                                 * Cell text is bump-allocated, so only free the DA
                                 * backing array (not per-cell text). */
                                da_free(&ir_items[ri].cells);
                                for (uint32_t j = ri; j + 1 < ir_count; j++)
                                    ir_items[j] = ir_items[j + 1];
                                ir_count--;
                                ins->insert_rows_count = ir_count;
                                memset(&ir_items[ir_count], 0, sizeof(ir_items[ir_count]));
                            } else {
                                ri++;
                            }
                        }
                    }
                }
                if (ins->insert_rows_count == 0) {
                    ins->insert_rows_count = orig_count;
                    return 0;
                }
                ins->insert_rows_count = orig_count;
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
                        /* map conflict_col (table index) to insert row cell index */
                        int insert_cell_idx = conflict_col;
                        if (ins->insert_columns_count > 0) {
                            insert_cell_idx = -1;
                            const char *conflict_name = t->columns.items[conflict_col].name;
                            for (uint32_t ci = 0; ci < ins->insert_columns_count; ci++) {
                                sv ic = q->arena.svs.items[ins->insert_columns_start + ci];
                                if (sv_eq_cstr(ic, conflict_name)) {
                                    insert_cell_idx = (int)ci;
                                    break;
                                }
                            }
                        }
                        if (insert_cell_idx < 0) goto skip_conflict_nothing;
                        /* filter out rows that conflict */
                        struct row *ir_items = &q->arena.rows.items[ins->insert_rows_start];
                        uint32_t ir_count = ins->insert_rows_count;
                        size_t orig_count = ir_count;
                        for (uint32_t ri = 0; ri < ir_count; ) {
                            struct cell *new_cell = &ir_items[ri].cells.items[insert_cell_idx];
                            int conflict = 0;
                            for (size_t ei = 0; ei < t->rows.count; ei++) {
                                if (cell_equal(new_cell, &t->rows.items[ei].cells.items[conflict_col])) {
                                    conflict = 1;
                                    break;
                                }
                            }
                            if (conflict) {
                                /* skip this row — free cells DA and shift remaining.
                                 * Cell text is bump-allocated, so only free the DA
                                 * backing array (not per-cell text).
                                 * Zero the vacated slot so arena_destroy won't double-free. */
                                da_free(&ir_items[ri].cells);
                                for (uint32_t j = ri; j + 1 < ir_count; j++)
                                    ir_items[j] = ir_items[j + 1];
                                ir_count--;
                                ins->insert_rows_count = ir_count;
                                /* zero the now-unused tail slot */
                                memset(&ir_items[ir_count], 0, sizeof(ir_items[ir_count]));
                            } else {
                                ri++;
                            }
                        }
                        (void)orig_count;
                    }
                }
skip_conflict_nothing:
                if (ins->insert_rows_count == 0) return 0;
            }
            return db_table_exec_query(db, ins->table, q, result, rb);
        }
        case QUERY_TYPE_DELETE:
            /* resolve subqueries in WHERE */
            if (q->del.where.has_where && q->del.where.where_cond != IDX_NONE)
                resolve_subqueries(db, &q->arena, q->del.where.where_cond, NULL);
            /* DELETE ... USING: build merged table to evaluate cross-table WHERE */
            if (q->del.using_table.len > 0 && q->del.where.has_where && q->del.where.where_cond != IDX_NONE) {
                struct table *dt = db_find_table_sv(db, q->del.table);
                struct table *ut = db_find_table_sv(db, q->del.using_table);
                if (!dt || !ut) {
                    arena_set_error(&q->arena, "42P01", "DELETE USING: table not found");
                    return -1;
                }
                /* COW trigger */
                if (db->active_txn && db->active_txn->in_transaction && db->active_txn->snapshot)
                    snapshot_cow_table(db->active_txn->snapshot, db, dt->name);
                /* build merged column metadata: t1 cols (qualified) + t2 cols (qualified) */
                struct table merged = {0};
                da_init(&merged.columns);
                da_init(&merged.rows);
                da_init(&merged.indexes);
                char t1name[128], t2name[128];
                snprintf(t1name, sizeof(t1name), "%.*s", (int)q->del.table.len, q->del.table.data);
                snprintf(t2name, sizeof(t2name), "%.*s", (int)q->del.using_table.len, q->del.using_table.data);
                for (size_t c = 0; c < dt->columns.count; c++) {
                    struct column col = {0};
                    char buf[256];
                    snprintf(buf, sizeof(buf), "%s.%s", t1name, dt->columns.items[c].name);
                    col.name = strdup(buf);
                    col.type = dt->columns.items[c].type;
                    da_push(&merged.columns, col);
                }
                for (size_t c = 0; c < ut->columns.count; c++) {
                    struct column col = {0};
                    char buf[256];
                    snprintf(buf, sizeof(buf), "%s.%s", t2name, ut->columns.items[c].name);
                    col.name = strdup(buf);
                    col.type = ut->columns.items[c].type;
                    da_push(&merged.columns, col);
                }
                /* find which target rows match any USING row */
                size_t dt_ncols = dt->columns.count;
                size_t ut_ncols = ut->columns.count;
                int *to_delete = calloc(dt->rows.count, sizeof(int));
                for (size_t i = 0; i < dt->rows.count; i++) {
                    for (size_t j = 0; j < ut->rows.count; j++) {
                        /* build merged row */
                        struct row mr = {0};
                        da_init(&mr.cells);
                        for (size_t c = 0; c < dt_ncols; c++)
                            da_push(&mr.cells, dt->rows.items[i].cells.items[c]);
                        for (size_t c = 0; c < ut_ncols; c++)
                            da_push(&mr.cells, ut->rows.items[j].cells.items[c]);
                        int match = eval_condition(q->del.where.where_cond, &q->arena, &mr, &merged, NULL);
                        da_free(&mr.cells);
                        if (match) { to_delete[i] = 1; break; }
                    }
                }
                /* delete matched rows (iterate backwards to avoid index shifting issues) */
                size_t deleted = 0;
                for (size_t i = dt->rows.count; i > 0; i--) {
                    size_t idx = i - 1;
                    if (to_delete[idx]) {
                        row_free(&dt->rows.items[idx]);
                        for (size_t k = idx; k + 1 < dt->rows.count; k++)
                            dt->rows.items[k] = dt->rows.items[k + 1];
                        dt->rows.count--;
                        deleted++;
                        dt->generation++;
                        db->total_generation++;
                    }
                }
                free(to_delete);
                for (size_t c = 0; c < merged.columns.count; c++) free(merged.columns.items[c].name);
                da_free(&merged.columns);
                /* indexes are rebuilt via generation change */
                /* store deleted count */
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
            return db_table_exec_query(db, q->del.table, q, result, rb);
        case QUERY_TYPE_UPDATE: {
            struct query_update *u = &q->update;
            /* resolve subqueries in WHERE */
            if (u->where.has_where && u->where.where_cond != IDX_NONE)
                resolve_subqueries(db, &q->arena, u->where.where_cond, NULL);
            /* UPDATE ... FROM — use parsed join columns */
            if (u->has_update_from) {
                struct table *t = db_find_table_sv(db, u->table);
                struct table *ft = db_find_table_sv(db, u->update_from_table);
                if (!t || !ft) {
                    arena_set_error(&q->arena, "42P01", "UPDATE FROM: table not found");
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
                /* build merged table descriptor: target columns + FROM columns
                 * (prefixed with FROM table name for qualified refs) */
                struct table merged_meta = {0};
                da_init(&merged_meta.columns);
                da_init(&merged_meta.rows);
                da_init(&merged_meta.indexes);
                for (size_t c = 0; c < t->columns.count; c++) {
                    struct column col = {0};
                    col.name = strdup(t->columns.items[c].name);
                    col.type = t->columns.items[c].type;
                    da_push(&merged_meta.columns, col);
                }
                char ft_name_buf[256];
                snprintf(ft_name_buf, sizeof(ft_name_buf), "%.*s",
                         (int)u->update_from_table.len, u->update_from_table.data);
                for (size_t c = 0; c < ft->columns.count; c++) {
                    struct column col = {0};
                    char buf[512];
                    snprintf(buf, sizeof(buf), "%s.%s", ft_name_buf, ft->columns.items[c].name);
                    col.name = strdup(buf);
                    col.type = ft->columns.items[c].type;
                    da_push(&merged_meta.columns, col);
                }

                size_t updated = 0;
                for (size_t i = 0; i < t->rows.count; i++) {
                    int matched = 0;
                    size_t matched_j = 0;
                    for (size_t j = 0; j < ft->rows.count; j++) {
                        if (t_join_col >= 0 && ft_join_col >= 0) {
                            if (cell_equal(&t->rows.items[i].cells.items[t_join_col],
                                            &ft->rows.items[j].cells.items[ft_join_col])) {
                                matched = 1;
                                matched_j = j;
                                break;
                            }
                        }
                    }
                    if (!matched) continue;
                    updated++;

                    /* build merged row: target row cells + FROM row cells */
                    struct row merged_row = {0};
                    da_init(&merged_row.cells);
                    for (size_t c = 0; c < t->rows.items[i].cells.count; c++)
                        da_push(&merged_row.cells, t->rows.items[i].cells.items[c]);
                    for (size_t c = 0; c < ft->rows.items[matched_j].cells.count; c++)
                        da_push(&merged_row.cells, ft->rows.items[matched_j].cells.items[c]);

                    /* evaluate all SET expressions against merged row */
                    uint32_t nsc = u->set_clauses_count;
                    struct cell *new_vals = bump_calloc(&q->arena.scratch, nsc, sizeof(struct cell));
                    int *col_idxs = bump_calloc(&q->arena.scratch, nsc, sizeof(int));
                    for (uint32_t sc = 0; sc < nsc; sc++) {
                        struct set_clause *scp = &q->arena.set_clauses.items[u->set_clauses_start + sc];
                        col_idxs[sc] = table_find_column_sv(t, scp->column);
                        if (col_idxs[sc] < 0) { new_vals[sc] = (struct cell){0}; continue; }
                        if (scp->expr_idx != IDX_NONE) {
                            new_vals[sc] = eval_expr(scp->expr_idx, &q->arena, &merged_meta, &merged_row, db, NULL);
                        } else {
                            cell_copy(&new_vals[sc], &scp->value);
                        }
                    }
                    for (size_t sc = 0; sc < nsc; sc++) {
                        if (col_idxs[sc] < 0) continue;
                        struct cell *dst = &t->rows.items[i].cells.items[col_idxs[sc]];
                        if (column_type_is_text(dst->type) && dst->value.as_text)
                            free(dst->value.as_text);
                        *dst = new_vals[sc];
                    }
                    da_free(&merged_row.cells);
                }
                /* free merged table descriptor */
                for (size_t c = 0; c < merged_meta.columns.count; c++)
                    free(merged_meta.columns.items[c].name);
                da_free(&merged_meta.columns);
                da_free(&merged_meta.rows);
                da_free(&merged_meta.indexes);
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
            return db_table_exec_query(db, u->table, q, result, rb);
        }
        case QUERY_TYPE_ALTER: {
            struct query_alter *a = &q->alter;
            struct table *t = db_find_table_sv(db, a->table);
            if (!t) {
                arena_set_error(&q->arena, "42P01", "table '%.*s' does not exist", (int)a->table.len, a->table.data);
                return -1;
            }
            /* COW trigger for ALTER TABLE */
            if (txn && txn->in_transaction && txn->snapshot)
                snapshot_cow_table(txn->snapshot, db, t->name);
            switch (a->alter_action) {
                case ALTER_ADD_COLUMN: {
                    table_add_column(t, &a->alter_new_col);
                    /* table_add_column deep-copies default_value — free the parser's copy */
                    if (a->alter_new_col.default_value) {
                        free(a->alter_new_col.default_value);
                        a->alter_new_col.default_value = NULL;
                    }
                    /* pad existing rows with default value (or NULL) for the new column */
                    size_t new_idx = t->columns.count - 1;
                    struct column *new_col = &t->columns.items[new_idx];
                    for (size_t i = 0; i < t->rows.count; i++) {
                        struct cell pad_cell = {0};
                        if (new_col->has_default && new_col->default_value) {
                            cell_copy(&pad_cell, new_col->default_value);
                        } else {
                            pad_cell.type = new_col->type;
                            pad_cell.is_null = 1;
                        }
                        da_push(&t->rows.items[i].cells, pad_cell);
                    }
                    return 0;
                }
                case ALTER_DROP_COLUMN: {
                    int col_idx = table_find_column_sv(t, a->alter_column);
                    if (col_idx < 0) {
                        arena_set_error(&q->arena, "42703", "column '%.*s' does not exist", (int)a->alter_column.len, a->alter_column.data);
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
                case ALTER_RENAME_TABLE: {
                    free(t->name);
                    t->name = sv_to_cstr(a->alter_new_name);
                    /* invalidate scan cache since table identity changed */
                    t->generation++;
                    db->total_generation++;
                    return 0;
                }
                case ALTER_RENAME_COLUMN: {
                    int col_idx = table_find_column_sv(t, a->alter_column);
                    if (col_idx < 0) {
                        arena_set_error(&q->arena, "42703", "column '%.*s' does not exist", (int)a->alter_column.len, a->alter_column.data);
                        return -1;
                    }
                    free(t->columns.items[col_idx].name);
                    t->columns.items[col_idx].name = sv_to_cstr(a->alter_new_name);
                    return 0;
                }
                case ALTER_COLUMN_TYPE: {
                    int col_idx = table_find_column_sv(t, a->alter_column);
                    if (col_idx < 0) {
                        arena_set_error(&q->arena, "42703", "column '%.*s' does not exist", (int)a->alter_column.len, a->alter_column.data);
                        return -1;
                    }
                    enum column_type old_type = t->columns.items[col_idx].type;
                    enum column_type new_type = a->alter_new_col.type;
                    t->columns.items[col_idx].type = new_type;
                    /* convert existing cell values to the new type */
                    if (old_type != new_type) {
                        for (size_t ri = 0; ri < t->rows.count; ri++) {
                            struct cell *c = &t->rows.items[ri].cells.items[col_idx];
                            if (c->is_null) { c->type = new_type; continue; }
                            if (new_type == COLUMN_TYPE_BOOLEAN) {
                                int bval = 0;
                                if (old_type == COLUMN_TYPE_INT) bval = (c->value.as_int != 0);
                                else if (old_type == COLUMN_TYPE_SMALLINT) bval = (c->value.as_smallint != 0);
                                else if (old_type == COLUMN_TYPE_BIGINT) bval = (c->value.as_bigint != 0);
                                else if (old_type == COLUMN_TYPE_FLOAT || old_type == COLUMN_TYPE_NUMERIC) bval = (c->value.as_float != 0.0);
                                else if (column_type_is_text(old_type) && c->value.as_text) bval = (c->value.as_text[0] == 't' || c->value.as_text[0] == 'T' || c->value.as_text[0] == '1');
                                c->type = COLUMN_TYPE_BOOLEAN;
                                c->value.as_bool = bval;
                            } else if (new_type == COLUMN_TYPE_INT) {
                                int ival = 0;
                                if (old_type == COLUMN_TYPE_BOOLEAN) ival = c->value.as_bool;
                                else if (old_type == COLUMN_TYPE_SMALLINT) ival = c->value.as_smallint;
                                else if (old_type == COLUMN_TYPE_BIGINT) ival = (int)c->value.as_bigint;
                                else if (old_type == COLUMN_TYPE_FLOAT || old_type == COLUMN_TYPE_NUMERIC) ival = (int)c->value.as_float;
                                else if (column_type_is_text(old_type) && c->value.as_text) ival = atoi(c->value.as_text);
                                c->type = COLUMN_TYPE_INT;
                                c->value.as_int = ival;
                            } else if (new_type == COLUMN_TYPE_BIGINT) {
                                long long bval = 0;
                                if (old_type == COLUMN_TYPE_INT) bval = c->value.as_int;
                                else if (old_type == COLUMN_TYPE_BOOLEAN) bval = c->value.as_bool;
                                else if (old_type == COLUMN_TYPE_SMALLINT) bval = c->value.as_smallint;
                                else if (old_type == COLUMN_TYPE_FLOAT || old_type == COLUMN_TYPE_NUMERIC) bval = (long long)c->value.as_float;
                                else if (column_type_is_text(old_type) && c->value.as_text) bval = atoll(c->value.as_text);
                                c->type = COLUMN_TYPE_BIGINT;
                                c->value.as_bigint = bval;
                            } else if (new_type == COLUMN_TYPE_FLOAT || new_type == COLUMN_TYPE_NUMERIC) {
                                double dval = 0.0;
                                if (old_type == COLUMN_TYPE_INT) dval = c->value.as_int;
                                else if (old_type == COLUMN_TYPE_BOOLEAN) dval = c->value.as_bool;
                                else if (old_type == COLUMN_TYPE_SMALLINT) dval = c->value.as_smallint;
                                else if (old_type == COLUMN_TYPE_BIGINT) dval = (double)c->value.as_bigint;
                                else if (column_type_is_text(old_type) && c->value.as_text) dval = atof(c->value.as_text);
                                c->type = new_type;
                                c->value.as_float = dval;
                            } else if (column_type_is_text(new_type)) {
                                char buf[64];
                                if (old_type == COLUMN_TYPE_INT) snprintf(buf, sizeof(buf), "%d", c->value.as_int);
                                else if (old_type == COLUMN_TYPE_BOOLEAN) snprintf(buf, sizeof(buf), "%s", c->value.as_bool ? "true" : "false");
                                else if (old_type == COLUMN_TYPE_SMALLINT) snprintf(buf, sizeof(buf), "%d", c->value.as_smallint);
                                else if (old_type == COLUMN_TYPE_BIGINT) snprintf(buf, sizeof(buf), "%lld", c->value.as_bigint);
                                else if (old_type == COLUMN_TYPE_FLOAT || old_type == COLUMN_TYPE_NUMERIC) snprintf(buf, sizeof(buf), "%g", c->value.as_float);
                                else buf[0] = '\0';
                                c->type = new_type;
                                c->value.as_text = strdup(buf);
                            } else {
                                c->type = new_type;
                            }
                        }
                        t->generation++;
                        db->total_generation++;
                    }
                    return 0;
                }
            }
            return -1;
        }
        case QUERY_TYPE_BEGIN:
        case QUERY_TYPE_COMMIT:
        case QUERY_TYPE_ROLLBACK:
            return 0; /* handled above */
        case QUERY_TYPE_COPY:
            return 0; /* handled in pgwire */
        case QUERY_TYPE_SET:
            return 0; /* no-op: silently accept SET/RESET/DISCARD */
        case QUERY_TYPE_SHOW: {
            /* return a single-row, single-column result */
            sv param = q->show.parameter;
            const char *val = "";
            if (sv_eq_ignorecase_cstr(param, "search_path"))
                val = "\"$user\", public";
            else if (sv_eq_ignorecase_cstr(param, "server_version"))
                val = "15.0";
            else if (sv_eq_ignorecase_cstr(param, "server_encoding"))
                val = "UTF8";
            else if (sv_eq_ignorecase_cstr(param, "client_encoding"))
                val = "UTF8";
            else if (sv_eq_ignorecase_cstr(param, "standard_conforming_strings"))
                val = "on";
            else if (sv_eq_ignorecase_cstr(param, "is_superuser"))
                val = "on";
            else if (sv_eq_ignorecase_cstr(param, "TimeZone") ||
                     sv_eq_ignorecase_cstr(param, "timezone"))
                val = "UTC";
            else if (sv_eq_ignorecase_cstr(param, "integer_datetimes"))
                val = "on";
            else if (sv_eq_ignorecase_cstr(param, "DateStyle"))
                val = "ISO, MDY";
            struct row r = {0};
            da_init(&r.cells);
            struct cell c = {0};
            c.type = COLUMN_TYPE_TEXT;
            c.value.as_text = rb ? bump_strdup(rb, val) : strdup(val);
            da_push(&r.cells, c);
            rows_push(result, r);
            return 0;
        }
    }
    return -1;
}

int db_exec_sql(struct database *db, const char *sql, struct rows *result)
{
    static __thread int subquery_depth = 0;
    if (subquery_depth >= 32) {
        fprintf(stderr, "subquery nesting depth exceeded (max 32)\n");
        return -1;
    }
    /* Refresh catalog tables if the query references system schemas.
     * Skip if we're already inside a subquery (catalog tables already populated). */
    if (subquery_depth == 0 &&
        (strstr(sql, "pg_") || strstr(sql, "information_schema")))
        catalog_refresh(db);
    struct query q = {0};
    if (query_parse(sql, &q) != 0) {
        query_free(&q);
        return -1;
    }
    subquery_depth++;
    int rc = db_exec(db, &q, result, NULL);
    subquery_depth--;
    query_free(&q);
    return rc;
}

void db_free(struct database *db)
{
    catalog_cleanup(db);
    free(db->name);
    for (size_t i = 0; i < db->tables.count; i++) {
        table_free(&db->tables.items[i]);
    }
    da_free(&db->tables);
    for (size_t i = 0; i < db->types.count; i++) {
        enum_type_free(&db->types.items[i]);
    }
    da_free(&db->types);
    for (size_t i = 0; i < db->sequences.count; i++) {
        free(db->sequences.items[i].name);
    }
    da_free(&db->sequences);
}

void db_reset(struct database *db)
{
    char *name = db->name;
    db->name = NULL;
    for (size_t i = 0; i < db->tables.count; i++)
        table_free(&db->tables.items[i]);
    da_free(&db->tables);
    for (size_t i = 0; i < db->types.count; i++)
        enum_type_free(&db->types.items[i]);
    da_free(&db->types);
    for (size_t i = 0; i < db->sequences.count; i++)
        free(db->sequences.items[i].name);
    da_free(&db->sequences);
    db_init(db, name);
    free(name);
}
