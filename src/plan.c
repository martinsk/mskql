#include "plan.h"
#include "query.h"
#include "parser.h"
#include "arena_helpers.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#define MAX_SORT_KEYS 32

/* ---- cell ↔ col_block conversion helpers ---- */

/* Write a struct cell value into a col_block at index i.
 * Does NOT set nulls — caller must handle that. */
static inline void cell_to_cb_at(struct col_block *cb, uint16_t i, const struct cell *cell)
{
    switch (cell->type) {
    case COLUMN_TYPE_SMALLINT:  cb->data.i16[i] = cell->value.as_smallint; break;
    case COLUMN_TYPE_INT:       cb->data.i32[i] = cell->value.as_int; break;
    case COLUMN_TYPE_BOOLEAN:   cb->data.i32[i] = cell->value.as_bool; break;
    case COLUMN_TYPE_DATE:      cb->data.i32[i] = cell->value.as_date; break;
    case COLUMN_TYPE_BIGINT:    cb->data.i64[i] = cell->value.as_bigint; break;
    case COLUMN_TYPE_TIME:      cb->data.i64[i] = cell->value.as_time; break;
    case COLUMN_TYPE_TIMESTAMP: case COLUMN_TYPE_TIMESTAMPTZ:
        cb->data.i64[i] = cell->value.as_timestamp; break;
    case COLUMN_TYPE_FLOAT:     cb->data.f64[i] = cell->value.as_float; break;
    case COLUMN_TYPE_NUMERIC:   cb->data.f64[i] = cell->value.as_numeric; break;
    case COLUMN_TYPE_INTERVAL:  cb->data.iv[i] = cell->value.as_interval; break;
    case COLUMN_TYPE_TEXT:  case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_UUID:
        cb->data.str[i] = cell->value.as_text; break;
    }
}

/* Read a col_block value at index i into a struct cell.
 * Sets cell->type and cell->value; does NOT handle null or text duplication. */
static inline void cb_to_cell_at(const struct col_block *cb, uint16_t i, struct cell *cell)
{
    cell->type = cb->type;
    switch (cb->type) {
    case COLUMN_TYPE_SMALLINT:  cell->value.as_smallint = cb->data.i16[i]; break;
    case COLUMN_TYPE_INT:       cell->value.as_int = cb->data.i32[i]; break;
    case COLUMN_TYPE_BOOLEAN:   cell->value.as_bool = cb->data.i32[i]; break;
    case COLUMN_TYPE_DATE:      cell->value.as_date = cb->data.i32[i]; break;
    case COLUMN_TYPE_BIGINT:    cell->value.as_bigint = cb->data.i64[i]; break;
    case COLUMN_TYPE_TIME:      cell->value.as_time = cb->data.i64[i]; break;
    case COLUMN_TYPE_TIMESTAMP: case COLUMN_TYPE_TIMESTAMPTZ:
        cell->value.as_timestamp = cb->data.i64[i]; break;
    case COLUMN_TYPE_FLOAT:     cell->value.as_float = cb->data.f64[i]; break;
    case COLUMN_TYPE_NUMERIC:   cell->value.as_numeric = cb->data.f64[i]; break;
    case COLUMN_TYPE_INTERVAL:  cell->value.as_interval = cb->data.iv[i]; break;
    case COLUMN_TYPE_TEXT:  case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_UUID:
        cell->value.as_text = cb->data.str[i]; break;
    }
}

/* Write a struct cell value into a flat void* array at index i (for scan_cache). */
static inline void cell_to_flat_at(void *data, size_t i, const struct cell *cell, enum column_type ct)
{
    switch (ct) {
    case COLUMN_TYPE_SMALLINT:  ((int16_t *)data)[i] = cell->value.as_smallint; break;
    case COLUMN_TYPE_INT:       ((int32_t *)data)[i] = cell->value.as_int; break;
    case COLUMN_TYPE_BOOLEAN:   ((int32_t *)data)[i] = cell->value.as_bool; break;
    case COLUMN_TYPE_DATE:      ((int32_t *)data)[i] = cell->value.as_date; break;
    case COLUMN_TYPE_BIGINT:    ((int64_t *)data)[i] = cell->value.as_bigint; break;
    case COLUMN_TYPE_TIME:      ((int64_t *)data)[i] = cell->value.as_time; break;
    case COLUMN_TYPE_TIMESTAMP: case COLUMN_TYPE_TIMESTAMPTZ:
        ((int64_t *)data)[i] = cell->value.as_timestamp; break;
    case COLUMN_TYPE_FLOAT:     ((double *)data)[i] = cell->value.as_float; break;
    case COLUMN_TYPE_NUMERIC:   ((double *)data)[i] = cell->value.as_numeric; break;
    case COLUMN_TYPE_INTERVAL:  ((struct interval *)data)[i] = cell->value.as_interval; break;
    case COLUMN_TYPE_TEXT:  case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_UUID:
        ((char **)data)[i] = cell->value.as_text; break;
    }
}

/* ---- Scan cache: build/invalidate flat columnar arrays on the table ---- */

static void scan_cache_free(struct scan_cache *sc)
{
    if (!sc->col_data) return;
    for (uint16_t i = 0; i < sc->ncols; i++) {
        free(sc->col_data[i]);
        free(sc->col_nulls[i]);
    }
    free(sc->col_data);
    free(sc->col_nulls);
    free(sc->col_types);
    memset(sc, 0, sizeof(*sc));
}

static void scan_cache_build(struct table *t)
{
    struct scan_cache *sc = &t->scan_cache;
    scan_cache_free(sc);

    uint16_t ncols = (uint16_t)t->columns.count;
    size_t nrows = t->rows.count;
    sc->generation = t->generation;
    sc->ncols = ncols;
    sc->nrows = nrows;
    sc->col_data = (void **)calloc(ncols, sizeof(void *));
    sc->col_nulls = (uint8_t **)calloc(ncols, sizeof(uint8_t *));
    sc->col_types = (enum column_type *)calloc(ncols, sizeof(enum column_type));

    for (uint16_t c = 0; c < ncols; c++) {
        enum column_type ct = t->columns.items[c].type;
        /* Detect actual type from first non-null cell */
        for (size_t r = 0; r < nrows; r++) {
            struct cell *cell = &t->rows.items[r].cells.items[c];
            if (!cell->is_null) { ct = cell->type; break; }
        }
        sc->col_types[c] = ct;

        size_t elem_sz = col_type_elem_size(ct);

        sc->col_data[c] = calloc(nrows ? nrows : 1, elem_sz);
        sc->col_nulls[c] = (uint8_t *)calloc(nrows ? nrows : 1, 1);

        /* Fill from row-store */
        switch (ct) {
        case COLUMN_TYPE_SMALLINT: {
            int16_t *dst = (int16_t *)sc->col_data[c];
            for (size_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[r].cells.items[c];
                if (cell->is_null || cell->type != COLUMN_TYPE_SMALLINT) {
                    sc->col_nulls[c][r] = 1;
                } else {
                    dst[r] = cell->value.as_smallint;
                }
            }
            break;
        }
        case COLUMN_TYPE_INT: {
            int32_t *dst = (int32_t *)sc->col_data[c];
            for (size_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[r].cells.items[c];
                if (cell->is_null || cell->type != COLUMN_TYPE_INT) {
                    sc->col_nulls[c][r] = 1;
                } else {
                    dst[r] = cell->value.as_int;
                }
            }
            break;
        }
        case COLUMN_TYPE_FLOAT: {
            double *dst = (double *)sc->col_data[c];
            for (size_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[r].cells.items[c];
                if (cell->is_null || cell->type != COLUMN_TYPE_FLOAT) {
                    sc->col_nulls[c][r] = 1;
                } else {
                    dst[r] = cell->value.as_float;
                }
            }
            break;
        }
        case COLUMN_TYPE_BIGINT: {
            int64_t *dst = (int64_t *)sc->col_data[c];
            for (size_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[r].cells.items[c];
                if (cell->is_null || cell->type != COLUMN_TYPE_BIGINT) {
                    sc->col_nulls[c][r] = 1;
                } else {
                    dst[r] = cell->value.as_bigint;
                }
            }
            break;
        }
        case COLUMN_TYPE_NUMERIC: {
            double *dst = (double *)sc->col_data[c];
            for (size_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[r].cells.items[c];
                if (cell->is_null || cell->type != COLUMN_TYPE_NUMERIC) {
                    sc->col_nulls[c][r] = 1;
                } else {
                    dst[r] = cell->value.as_numeric;
                }
            }
            break;
        }
        case COLUMN_TYPE_BOOLEAN: {
            int32_t *dst = (int32_t *)sc->col_data[c];
            for (size_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[r].cells.items[c];
                if (cell->is_null || cell->type != COLUMN_TYPE_BOOLEAN) {
                    sc->col_nulls[c][r] = 1;
                } else {
                    dst[r] = cell->value.as_bool;
                }
            }
            break;
        }
        case COLUMN_TYPE_DATE: {
            int32_t *dst = (int32_t *)sc->col_data[c];
            for (size_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[r].cells.items[c];
                if (cell->is_null || cell->type != ct) { sc->col_nulls[c][r] = 1; }
                else { dst[r] = cell->value.as_date; }
            }
            break;
        }
        case COLUMN_TYPE_TIME: {
            int64_t *dst = (int64_t *)sc->col_data[c];
            for (size_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[r].cells.items[c];
                if (cell->is_null || cell->type != ct) { sc->col_nulls[c][r] = 1; }
                else { dst[r] = cell->value.as_time; }
            }
            break;
        }
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ: {
            int64_t *dst = (int64_t *)sc->col_data[c];
            for (size_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[r].cells.items[c];
                if (cell->is_null || (cell->type != COLUMN_TYPE_TIMESTAMP && cell->type != COLUMN_TYPE_TIMESTAMPTZ)) {
                    sc->col_nulls[c][r] = 1;
                } else { dst[r] = cell->value.as_timestamp; }
            }
            break;
        }
        case COLUMN_TYPE_INTERVAL: {
            struct interval *dst = (struct interval *)sc->col_data[c];
            for (size_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[r].cells.items[c];
                if (cell->is_null || cell->type != ct) { sc->col_nulls[c][r] = 1; }
                else { dst[r] = cell->value.as_interval; }
            }
            break;
        }
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_UUID: {
            char **dst = (char **)sc->col_data[c];
            for (size_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[r].cells.items[c];
                if (cell->is_null || cell->type != ct
                    || (column_type_is_text(cell->type) && !cell->value.as_text)) {
                    sc->col_nulls[c][r] = 1;
                } else {
                    dst[r] = cell->value.as_text;
                }
            }
            break;
        }
        }
    }
}

/* Patch a single row in the scan cache in-place after an UPDATE.
 * The caller must ensure row_idx < sc->nrows and the cache is valid. */
void scan_cache_update_row(struct table *t, size_t row_idx)
{
    struct scan_cache *sc = &t->scan_cache;
    if (!sc->col_data || row_idx >= sc->nrows) return;

    for (uint16_t c = 0; c < sc->ncols; c++) {
        struct cell *cell = &t->rows.items[row_idx].cells.items[c];
        enum column_type ct = sc->col_types[c];

        if (cell->is_null || cell->type != ct) {
            sc->col_nulls[c][row_idx] = 1;
            continue;
        }
        sc->col_nulls[c][row_idx] = 0;
        cell_to_flat_at(sc->col_data[c], row_idx, cell, ct);
    }
    /* Keep cache generation in sync with table */
    sc->generation = t->generation;
}

/* Copy a slice of the scan cache into a row_block.
 * col_map[i] = which table column to read for output column i.
 * Returns number of rows copied. */
static uint16_t scan_cache_read(struct scan_cache *sc, size_t *cursor,
                                struct row_block *out, int *col_map, uint16_t ncols)
{
    size_t start = *cursor;
    size_t end = sc->nrows;
    if (end - start > BLOCK_CAPACITY)
        end = start + BLOCK_CAPACITY;

    uint16_t nrows = (uint16_t)(end - start);
    if (nrows == 0) return 0;

    out->count = nrows;

    for (uint16_t c = 0; c < ncols; c++) {
        int tc = col_map[c];
        struct col_block *cb = &out->cols[c];
        cb->type = sc->col_types[tc];
        cb->count = nrows;

        /* Copy slice from flat cache arrays into col_block */
        memcpy(cb->nulls, sc->col_nulls[tc] + start, nrows);

        enum column_type ct = sc->col_types[tc];
        switch (ct) {
        case COLUMN_TYPE_SMALLINT:
            memcpy(cb->data.i16, (int16_t *)sc->col_data[tc] + start, nrows * sizeof(int16_t)); break;
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:
            memcpy(cb->data.i32, (int32_t *)sc->col_data[tc] + start, nrows * sizeof(int32_t)); break;
        case COLUMN_TYPE_BIGINT:
            memcpy(cb->data.i64, (int64_t *)sc->col_data[tc] + start, nrows * sizeof(int64_t)); break;
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:
            memcpy(cb->data.f64, (double *)sc->col_data[tc] + start, nrows * sizeof(double)); break;
        case COLUMN_TYPE_DATE:
            memcpy(cb->data.i32, (int32_t *)sc->col_data[tc] + start, nrows * sizeof(int32_t)); break;
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
            memcpy(cb->data.i64, (int64_t *)sc->col_data[tc] + start, nrows * sizeof(int64_t)); break;
        case COLUMN_TYPE_INTERVAL:
            memcpy(cb->data.iv, (struct interval *)sc->col_data[tc] + start, nrows * sizeof(struct interval)); break;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_UUID:
            memcpy(cb->data.str, (char **)sc->col_data[tc] + start, nrows * sizeof(char *)); break;
        }
    }

    *cursor = end;
    return nrows;
}

/* Helper: copy a col_block value at index src_i to dst col_block at dst_i. */
static inline void cb_copy_value(struct col_block *dst, uint32_t dst_i,
                                 const struct col_block *src, uint16_t src_i)
{
    dst->nulls[dst_i] = src->nulls[src_i];
    memcpy(cb_data_ptr(dst, dst_i), cb_data_ptr(src, (uint32_t)src_i),
           col_type_elem_size(src->type));
}

/* Helper: bulk copy count values from src col_block to dst col_block. */
static inline void cb_bulk_copy(struct col_block *dst,
                                const struct col_block *src, uint32_t count)
{
    memcpy(dst->nulls, src->nulls, count);
    memcpy(cb_data_ptr(dst, 0), cb_data_ptr(src, 0), count * col_type_elem_size(src->type));
}

/* Helper: get a double value from a col_block at index i. */
static inline double cb_to_double(const struct col_block *cb, uint16_t i)
{
    switch (cb->type) {
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_NUMERIC:  return cb->data.f64[i];
    case COLUMN_TYPE_BIGINT:   return (double)cb->data.i64[i];
    case COLUMN_TYPE_SMALLINT: return (double)cb->data.i16[i];
    case COLUMN_TYPE_INT:
    case COLUMN_TYPE_BOOLEAN:  return (double)cb->data.i32[i];
    case COLUMN_TYPE_DATE:     return (double)cb->data.i32[i];
    case COLUMN_TYPE_TIME:
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_TIMESTAMPTZ:
        return (double)cb->data.i64[i];
    case COLUMN_TYPE_INTERVAL: return (double)interval_to_usec_approx(cb->data.iv[i]);
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_UUID:     return 0.0;
    }
    __builtin_unreachable();
}

/* Helper: get output column count for a plan node. */
uint16_t plan_node_ncols(struct query_arena *arena, uint32_t node_idx)
{
    if (node_idx == IDX_NONE) return 0;
    struct plan_node *pn = &PLAN_NODE(arena, node_idx);
    switch (pn->op) {
    case PLAN_SEQ_SCAN:       return pn->seq_scan.ncols;
    case PLAN_INDEX_SCAN:     return pn->index_scan.ncols;
    case PLAN_PROJECT:        return pn->project.ncols;
    case PLAN_HASH_JOIN: {
        uint16_t lc = plan_node_ncols(arena, pn->left);
        uint16_t rc = plan_node_ncols(arena, pn->right);
        return lc + rc;
    }
    case PLAN_HASH_AGG:
        return pn->hash_agg.ngroup_cols + (uint16_t)pn->hash_agg.agg_count;
    case PLAN_WINDOW:         return pn->window.out_ncols;
    case PLAN_SET_OP:         return pn->set_op.ncols;
    case PLAN_GENERATE_SERIES: return 1;
    case PLAN_EXPR_PROJECT:   return pn->expr_project.ncols;
    case PLAN_SORT:
    case PLAN_HASH_SEMI_JOIN:
    case PLAN_DISTINCT:
    case PLAN_FILTER:
    case PLAN_LIMIT:
        return plan_node_ncols(arena, pn->left);
    case PLAN_SIMPLE_AGG:
        return pn->simple_agg.agg_count;
    case PLAN_NESTED_LOOP:
        /* not yet implemented — fall through to child */
        return plan_node_ncols(arena, pn->left);
    }
    __builtin_unreachable();
}

/* ---- Hash table init (needs bump_alloc from arena.h) ---- */

static inline void block_ht_init(struct block_hash_table *ht, uint32_t capacity,
                                 struct bump_alloc *scratch)
{
    uint32_t nb = 1;
    while (nb < capacity * 2) nb <<= 1;
    ht->nbuckets = nb;
    ht->buckets = (uint32_t *)bump_alloc(scratch, nb * sizeof(uint32_t));
    memset(ht->buckets, 0xFF, nb * sizeof(uint32_t)); /* fill with IDX_NONE */
    ht->nexts = (uint32_t *)bump_alloc(scratch, capacity * sizeof(uint32_t));
    ht->hashes = (uint32_t *)bump_alloc(scratch, capacity * sizeof(uint32_t));
    ht->capacity = capacity;
    ht->count = 0;
}

/* ---- Row block init (needs bump_alloc from arena.h) ---- */

void row_block_alloc(struct row_block *rb, uint16_t ncols,
                     struct bump_alloc *scratch)
{
    rb->ncols = ncols;
    rb->count = 0;
    rb->cols = (struct col_block *)bump_calloc(scratch, ncols, sizeof(struct col_block));
    rb->sel = NULL;
    rb->sel_count = 0;
}

/* ---- Plan node allocation ---- */

uint32_t plan_alloc_node(struct query_arena *arena, enum plan_op op)
{
    struct plan_node node;
    memset(&node, 0, sizeof(node));
    node.op = op;
    node.left = IDX_NONE;
    node.right = IDX_NONE;
    da_push(&arena->plan_nodes, node);
    return (uint32_t)(arena->plan_nodes.count - 1);
}

/* ---- Block utility functions ---- */

/* Decompose row-store rows into columnar col_blocks.
 * Scans up to BLOCK_CAPACITY rows from table starting at *cursor.
 * col_map[i] = which table column to read for output column i.
 * Returns number of rows scanned. */
uint16_t scan_table_block(struct table *t, size_t *cursor,
                          struct row_block *out, int *col_map, uint16_t ncols,
                          struct bump_alloc *scratch)
{
    (void)scratch;
    size_t start = *cursor;
    size_t end = t->rows.count;
    if (end - start > BLOCK_CAPACITY)
        end = start + BLOCK_CAPACITY;

    uint16_t nrows = (uint16_t)(end - start);
    if (nrows == 0) return 0;

    out->count = nrows;

    for (uint16_t c = 0; c < ncols; c++) {
        int tc = col_map[c];
        struct col_block *cb = &out->cols[c];
        /* Use the first non-null cell's type to determine the col_block type.
         * This handles ALTER TABLE ALTER COLUMN TYPE where column def and
         * cell types may diverge. Fall back to column def if all null. */
        enum column_type col_type = t->columns.items[tc].type;
        cb->type = col_type;
        for (uint16_t r = 0; r < nrows; r++) {
            struct cell *cell = &t->rows.items[start + r].cells.items[tc];
            if (!cell->is_null) { cb->type = cell->type; col_type = cell->type; break; }
        }
        cb->count = nrows;

        /* Fast paths for common column types */
        switch (col_type) {
        case COLUMN_TYPE_SMALLINT:
            for (uint16_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[start + r].cells.items[tc];
                if (cell->is_null || cell->type != COLUMN_TYPE_SMALLINT) {
                    cb->nulls[r] = 1;
                } else {
                    cb->nulls[r] = 0;
                    cb->data.i16[r] = cell->value.as_smallint;
                }
            }
            break;
        case COLUMN_TYPE_INT:
            for (uint16_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[start + r].cells.items[tc];
                if (cell->is_null || cell->type != COLUMN_TYPE_INT) {
                    cb->nulls[r] = 1;
                } else {
                    cb->nulls[r] = 0;
                    cb->data.i32[r] = cell->value.as_int;
                }
            }
            break;
        case COLUMN_TYPE_FLOAT:
            for (uint16_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[start + r].cells.items[tc];
                if (cell->is_null || cell->type != COLUMN_TYPE_FLOAT) {
                    cb->nulls[r] = 1;
                } else {
                    cb->nulls[r] = 0;
                    cb->data.f64[r] = cell->value.as_float;
                }
            }
            break;
        case COLUMN_TYPE_BIGINT:
            for (uint16_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[start + r].cells.items[tc];
                if (cell->is_null || cell->type != COLUMN_TYPE_BIGINT) {
                    cb->nulls[r] = 1;
                } else {
                    cb->nulls[r] = 0;
                    cb->data.i64[r] = cell->value.as_bigint;
                }
            }
            break;
        case COLUMN_TYPE_NUMERIC:
        case COLUMN_TYPE_BOOLEAN:
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID: {
            for (uint16_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[start + r].cells.items[tc];
                if (cell->is_null || cell->type != cb->type
                    || (column_type_is_text(cell->type) && !cell->value.as_text)) {
                    cb->nulls[r] = 1;
                    continue;
                }
                cb->nulls[r] = 0;
                cell_to_cb_at(cb, r, cell);
            }
            break;
        }
        }
    }

    *cursor = end;
    return nrows;
}

/* Convert a row_block back to struct rows for final output.
 * When rb is non-NULL, text is bump-allocated (bulk-freed).
 * Otherwise text is strdup'd (caller owns the result rows). */
void block_to_rows(const struct row_block *blk, struct rows *result, struct bump_alloc *rb)
{
    uint16_t active = row_block_active_count(blk);
    for (uint16_t i = 0; i < active; i++) {
        uint16_t ri = row_block_row_idx(blk, i);
        struct row dst = {0};
        da_init(&dst.cells);
        for (uint16_t c = 0; c < blk->ncols; c++) {
            const struct col_block *cb = &blk->cols[c];
            struct cell cell = {0};
            cell.type = cb->type;
            if (cb->nulls[ri]) {
                cell.is_null = 1;
            } else {
                cb_to_cell_at(cb, ri, &cell);
                if (column_type_is_text(cb->type) && cell.value.as_text)
                    cell.value.as_text = rb ? bump_strdup(rb, cell.value.as_text) : strdup(cell.value.as_text);
            }
            da_push(&dst.cells, cell);
        }
        rows_push(result, dst);
    }
}

/* ---- Execution context ---- */

/* Count total nodes in the plan tree (for state array sizing). */
static uint32_t count_plan_nodes(struct query_arena *arena, uint32_t root)
{
    if (root == IDX_NONE) return 0;
    uint32_t n = 1;
    struct plan_node *pn = &PLAN_NODE(arena, root);
    n += count_plan_nodes(arena, pn->left);
    n += count_plan_nodes(arena, pn->right);
    return n;
}

void plan_exec_init(struct plan_exec_ctx *ctx, struct query_arena *arena,
                    struct database *db, uint32_t root_node)
{
    ctx->arena = arena;
    ctx->db = db;
    ctx->nnodes = (uint32_t)arena->plan_nodes.count;

    ctx->node_states = (void **)bump_calloc(&arena->scratch, ctx->nnodes, sizeof(void *));
    (void)root_node;
    (void)count_plan_nodes;
}

/* ---- Per-operator next_block implementations ---- */

static int seq_scan_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                         struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct scan_state *st = (struct scan_state *)ctx->node_states[node_idx];
    if (!st) {
        st = (struct scan_state *)bump_calloc(&ctx->arena->scratch, 1, sizeof(*st));
        st->cursor = 0;
        ctx->node_states[node_idx] = st;
    }

    row_block_reset(out);

    /* Try scan cache: if table hasn't changed, read from cached flat arrays */
    struct table *t = pn->seq_scan.table;
    struct scan_cache *sc = &t->scan_cache;
    if (sc->col_data && sc->generation == t->generation && sc->nrows == t->rows.count) {
        uint16_t n = scan_cache_read(sc, &st->cursor, out,
                                     pn->seq_scan.col_map, pn->seq_scan.ncols);
        if (n == 0) return -1;
        return 0;
    }

    /* Cache miss or stale — build cache on first scan, then read from it */
    if (st->cursor == 0 && t->rows.count > 0) {
        scan_cache_build(t);
        uint16_t n = scan_cache_read(&t->scan_cache, &st->cursor, out,
                                     pn->seq_scan.col_map, pn->seq_scan.ncols);
        if (n == 0) return -1;
        return 0;
    }

    /* Fallback: direct row-store scan (shouldn't normally reach here) */
    uint16_t n = scan_table_block(t, &st->cursor,
                                  out, pn->seq_scan.col_map,
                                  pn->seq_scan.ncols, &ctx->arena->scratch);
    if (n == 0) return -1;
    return 0;
}

static int index_scan_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                           struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct scan_state *st = (struct scan_state *)ctx->node_states[node_idx];
    if (!st) {
        st = (struct scan_state *)bump_calloc(&ctx->arena->scratch, 1, sizeof(*st));
        st->cursor = 0;
        ctx->node_states[node_idx] = st;
    }

    /* Only emit one block — all matching rows come from index_lookup */
    if (st->cursor > 0) return -1;
    st->cursor = 1;

    struct table *t = pn->index_scan.table;
    struct condition *cond = &COND(ctx->arena, pn->index_scan.cond_idx);

    size_t *ids = NULL;
    size_t id_count = 0;
    index_lookup(pn->index_scan.idx, &cond->value, &ids, &id_count);
    if (id_count == 0) return -1;

    /* Cap to BLOCK_CAPACITY (should rarely matter for point lookups) */
    if (id_count > BLOCK_CAPACITY) id_count = BLOCK_CAPACITY;

    row_block_reset(out);
    uint16_t ncols = pn->index_scan.ncols;
    int *col_map = pn->index_scan.col_map;
    uint16_t nrows = 0;

    for (size_t k = 0; k < id_count; k++) {
        size_t rid = ids[k];
        if (rid >= t->rows.count) continue;
        struct row *src = &t->rows.items[rid];
        for (uint16_t c = 0; c < ncols; c++) {
            int tc = col_map[c];
            struct col_block *cb = &out->cols[c];
            struct cell *cell = &src->cells.items[tc];
            if (nrows == 0) {
                /* Set type from first row's cell, fall back to column def */
                cb->type = cell->is_null ? t->columns.items[tc].type : cell->type;
            }
            if (cell->is_null) {
                cb->nulls[nrows] = 1;
            } else {
                cb->nulls[nrows] = 0;
                cell_to_cb_at(cb, nrows, cell);
            }
        }
        nrows++;
    }

    if (nrows == 0) return -1;

    out->count = nrows;
    for (uint16_t c = 0; c < ncols; c++)
        out->cols[c].count = nrows;

    return 0;
}

/* Coerce a TEXT comparison cell to the column's temporal type in-place. */
static void coerce_cmp_to_temporal(struct cell *c, enum column_type ct)
{
    if (!c || c->is_null || c->type != COLUMN_TYPE_TEXT || !c->value.as_text) return;
    const char *s = c->value.as_text;
    switch (ct) {
    case COLUMN_TYPE_DATE:        c->type = ct; c->value.as_date = date_from_str(s); break;
    case COLUMN_TYPE_TIME:        c->type = ct; c->value.as_time = time_from_str(s); break;
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_TIMESTAMPTZ: c->type = ct; c->value.as_timestamp = timestamp_from_str(s); break;
    case COLUMN_TYPE_INTERVAL:    c->type = ct; c->value.as_interval = interval_from_str(s); break;
    case COLUMN_TYPE_SMALLINT: case COLUMN_TYPE_INT: case COLUMN_TYPE_BIGINT:
    case COLUMN_TYPE_FLOAT: case COLUMN_TYPE_NUMERIC: case COLUMN_TYPE_BOOLEAN:
    case COLUMN_TYPE_TEXT: case COLUMN_TYPE_ENUM: case COLUMN_TYPE_UUID: break;
    }
}

/* Evaluate a single COND_COMPARE leaf against columnar data.
 * Returns number of matching rows written to sel. */
static uint16_t filter_eval_leaf(struct col_block *cb, int op,
                                  struct cell *cmp_val, struct cell *between_high,
                                  struct cell *in_values, uint32_t in_count,
                                  const char *like_pattern,
                                  uint32_t *cand, uint16_t cand_count,
                                  uint32_t *sel)
{
    /* Coerce TEXT comparison values to the column's temporal type */
    if (column_type_is_temporal(cb->type)) {
        coerce_cmp_to_temporal(cmp_val, cb->type);
        coerce_cmp_to_temporal(between_high, cb->type);
        for (uint32_t j = 0; j < in_count; j++)
            coerce_cmp_to_temporal(&in_values[j], cb->type);
    }

    uint16_t sel_count = 0;

    #define FLEAF_LOOP(COND) \
        for (uint16_t _c = 0; _c < cand_count; _c++) { \
            uint16_t r = (uint16_t)cand[_c]; \
            if (COND) sel[sel_count++] = r; \
        }

    switch (op) {

    case CMP_IS_NULL: {
        const uint8_t *nulls = cb->nulls;
        FLEAF_LOOP(nulls[r]);
        break;
    }

    case CMP_IS_NOT_NULL: {
        const uint8_t *nulls = cb->nulls;
        FLEAF_LOOP(!nulls[r]);
        break;
    }

    case CMP_BETWEEN: {
        if (!cmp_val || !between_high) break;
        const uint8_t *nulls = cb->nulls;
        switch (cb->type) {
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN: {
            int32_t lo = cmp_val->value.as_int, hi = between_high->value.as_int;
            const int32_t *vals = cb->data.i32;
            FLEAF_LOOP(!nulls[r] && vals[r] >= lo && vals[r] <= hi);
            break;
        }
        case COLUMN_TYPE_BIGINT: {
            int64_t lo = cmp_val->value.as_bigint, hi = between_high->value.as_bigint;
            const int64_t *vals = cb->data.i64;
            FLEAF_LOOP(!nulls[r] && vals[r] >= lo && vals[r] <= hi);
            break;
        }
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC: {
            double lo = cmp_val->type == COLUMN_TYPE_FLOAT ? cmp_val->value.as_float : (double)cmp_val->value.as_int;
            double hi = between_high->type == COLUMN_TYPE_FLOAT ? between_high->value.as_float : (double)between_high->value.as_int;
            const double *vals = cb->data.f64;
            FLEAF_LOOP(!nulls[r] && vals[r] >= lo && vals[r] <= hi);
            break;
        }
        case COLUMN_TYPE_SMALLINT: {
            int16_t lo = (int16_t)cmp_val->value.as_int, hi = (int16_t)between_high->value.as_int;
            const int16_t *vals = cb->data.i16;
            FLEAF_LOOP(!nulls[r] && vals[r] >= lo && vals[r] <= hi);
            break;
        }
        case COLUMN_TYPE_DATE: {
            int32_t lo = cmp_val->value.as_date, hi = between_high->value.as_date;
            const int32_t *vals = cb->data.i32;
            FLEAF_LOOP(!nulls[r] && vals[r] >= lo && vals[r] <= hi);
            break;
        }
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ: {
            int64_t lo = cmp_val->value.as_timestamp, hi = between_high->value.as_timestamp;
            const int64_t *vals = cb->data.i64;
            FLEAF_LOOP(!nulls[r] && vals[r] >= lo && vals[r] <= hi);
            break;
        }
        case COLUMN_TYPE_INTERVAL:
            break; /* BETWEEN on interval not meaningful */
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_UUID: {
            const char *lo = cmp_val->value.as_text ? cmp_val->value.as_text : "";
            const char *hi = between_high->value.as_text ? between_high->value.as_text : "";
            char * const *vals = cb->data.str;
            FLEAF_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], lo) >= 0 && strcmp(vals[r], hi) <= 0);
            break;
        }
        }
        break;
    }

    case CMP_IN: {
        if (!in_values || in_count == 0) break;
        const uint8_t *nulls = cb->nulls;
        switch (cb->type) {
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN: {
            const int32_t *vals = cb->data.i32;
            for (uint16_t _c = 0; _c < cand_count; _c++) {
                uint16_t r = (uint16_t)cand[_c];
                if (nulls[r]) continue;
                for (uint32_t j = 0; j < in_count; j++)
                    if (!in_values[j].is_null && in_values[j].value.as_int == vals[r]) { sel[sel_count++] = r; break; }
            }
            break;
        }
        case COLUMN_TYPE_BIGINT: {
            const int64_t *vals = cb->data.i64;
            for (uint16_t _c = 0; _c < cand_count; _c++) {
                uint16_t r = (uint16_t)cand[_c];
                if (nulls[r]) continue;
                for (uint32_t j = 0; j < in_count; j++)
                    if (!in_values[j].is_null && in_values[j].value.as_bigint == vals[r]) { sel[sel_count++] = r; break; }
            }
            break;
        }
        case COLUMN_TYPE_DATE: {
            const int32_t *vals = cb->data.i32;
            for (uint16_t _c = 0; _c < cand_count; _c++) {
                uint16_t r = (uint16_t)cand[_c];
                if (nulls[r]) continue;
                for (uint32_t j = 0; j < in_count; j++)
                    if (!in_values[j].is_null && in_values[j].value.as_date == vals[r]) { sel[sel_count++] = r; break; }
            }
            break;
        }
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ: {
            const int64_t *vals = cb->data.i64;
            for (uint16_t _c = 0; _c < cand_count; _c++) {
                uint16_t r = (uint16_t)cand[_c];
                if (nulls[r]) continue;
                for (uint32_t j = 0; j < in_count; j++)
                    if (!in_values[j].is_null && in_values[j].value.as_timestamp == vals[r]) { sel[sel_count++] = r; break; }
            }
            break;
        }
        case COLUMN_TYPE_INTERVAL: {
            const struct interval *vals = cb->data.iv;
            for (uint16_t _c = 0; _c < cand_count; _c++) {
                uint16_t r = (uint16_t)cand[_c];
                if (nulls[r]) continue;
                for (uint32_t j = 0; j < in_count; j++)
                    if (!in_values[j].is_null &&
                        vals[r].months == in_values[j].value.as_interval.months &&
                        vals[r].days == in_values[j].value.as_interval.days &&
                        vals[r].usec == in_values[j].value.as_interval.usec) { sel[sel_count++] = r; break; }
            }
            break;
        }
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_UUID: {
            char * const *vals = cb->data.str;
            for (uint16_t _c = 0; _c < cand_count; _c++) {
                uint16_t r = (uint16_t)cand[_c];
                if (nulls[r] || !vals[r]) continue;
                for (uint32_t j = 0; j < in_count; j++)
                    if (!in_values[j].is_null && in_values[j].value.as_text &&
                        strcmp(vals[r], in_values[j].value.as_text) == 0) { sel[sel_count++] = r; break; }
            }
            break;
        }
        case COLUMN_TYPE_SMALLINT:
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:
            break;
        }
        break;
    }

    case CMP_LIKE:
    case CMP_ILIKE: {
        if (!column_type_is_text(cb->type)) break;
        const char *pat = like_pattern ? like_pattern : (cmp_val ? cmp_val->value.as_text : "");
        if (!pat) pat = "";
        char * const *vals = cb->data.str;
        const uint8_t *nulls = cb->nulls;
        int icase = (op == CMP_ILIKE);
        FLEAF_LOOP(!nulls[r] && vals[r] && like_match(pat, vals[r], icase));
        break;
    }

    case CMP_EQ:
    case CMP_NE:
    case CMP_LT:
    case CMP_GT:
    case CMP_LE:
    case CMP_GE: {
        if (!cmp_val) break;
        const uint8_t *nulls = cb->nulls;
        switch (cb->type) {
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN: {
            int32_t cv = cmp_val->value.as_int;
            const int32_t *vals = cb->data.i32;
            switch (op) {
                case CMP_EQ: FLEAF_LOOP(!nulls[r] && vals[r] == cv); break;
                case CMP_NE: FLEAF_LOOP(!nulls[r] && vals[r] != cv); break;
                case CMP_LT: FLEAF_LOOP(!nulls[r] && vals[r] <  cv); break;
                case CMP_GT: FLEAF_LOOP(!nulls[r] && vals[r] >  cv); break;
                case CMP_LE: FLEAF_LOOP(!nulls[r] && vals[r] <= cv); break;
                case CMP_GE: FLEAF_LOOP(!nulls[r] && vals[r] >= cv); break;
                case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: break;
            }
            break;
        }
        case COLUMN_TYPE_BIGINT: {
            int64_t cv = cmp_val->value.as_bigint;
            const int64_t *vals = cb->data.i64;
            switch (op) {
                case CMP_EQ: FLEAF_LOOP(!nulls[r] && vals[r] == cv); break;
                case CMP_NE: FLEAF_LOOP(!nulls[r] && vals[r] != cv); break;
                case CMP_LT: FLEAF_LOOP(!nulls[r] && vals[r] <  cv); break;
                case CMP_GT: FLEAF_LOOP(!nulls[r] && vals[r] >  cv); break;
                case CMP_LE: FLEAF_LOOP(!nulls[r] && vals[r] <= cv); break;
                case CMP_GE: FLEAF_LOOP(!nulls[r] && vals[r] >= cv); break;
                case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: break;
            }
            break;
        }
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC: {
            double cv = cmp_val->type == COLUMN_TYPE_FLOAT ? cmp_val->value.as_float : (double)cmp_val->value.as_int;
            const double *vals = cb->data.f64;
            switch (op) {
                case CMP_EQ: FLEAF_LOOP(!nulls[r] && vals[r] == cv); break;
                case CMP_NE: FLEAF_LOOP(!nulls[r] && vals[r] != cv); break;
                case CMP_LT: FLEAF_LOOP(!nulls[r] && vals[r] <  cv); break;
                case CMP_GT: FLEAF_LOOP(!nulls[r] && vals[r] >  cv); break;
                case CMP_LE: FLEAF_LOOP(!nulls[r] && vals[r] <= cv); break;
                case CMP_GE: FLEAF_LOOP(!nulls[r] && vals[r] >= cv); break;
                case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: break;
            }
            break;
        }
        case COLUMN_TYPE_SMALLINT: {
            int16_t cv = (int16_t)cmp_val->value.as_int;
            const int16_t *vals = cb->data.i16;
            switch (op) {
                case CMP_EQ: FLEAF_LOOP(!nulls[r] && vals[r] == cv); break;
                case CMP_NE: FLEAF_LOOP(!nulls[r] && vals[r] != cv); break;
                case CMP_LT: FLEAF_LOOP(!nulls[r] && vals[r] <  cv); break;
                case CMP_GT: FLEAF_LOOP(!nulls[r] && vals[r] >  cv); break;
                case CMP_LE: FLEAF_LOOP(!nulls[r] && vals[r] <= cv); break;
                case CMP_GE: FLEAF_LOOP(!nulls[r] && vals[r] >= cv); break;
                case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: break;
            }
            break;
        }
        case COLUMN_TYPE_DATE: {
            int32_t cv = cmp_val->value.as_date;
            const int32_t *vals = cb->data.i32;
            switch (op) {
                case CMP_EQ: FLEAF_LOOP(!nulls[r] && vals[r] == cv); break;
                case CMP_NE: FLEAF_LOOP(!nulls[r] && vals[r] != cv); break;
                case CMP_LT: FLEAF_LOOP(!nulls[r] && vals[r] <  cv); break;
                case CMP_GT: FLEAF_LOOP(!nulls[r] && vals[r] >  cv); break;
                case CMP_LE: FLEAF_LOOP(!nulls[r] && vals[r] <= cv); break;
                case CMP_GE: FLEAF_LOOP(!nulls[r] && vals[r] >= cv); break;
                case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: break;
            }
            break;
        }
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ: {
            int64_t cv = cmp_val->value.as_timestamp;
            const int64_t *vals = cb->data.i64;
            switch (op) {
                case CMP_EQ: FLEAF_LOOP(!nulls[r] && vals[r] == cv); break;
                case CMP_NE: FLEAF_LOOP(!nulls[r] && vals[r] != cv); break;
                case CMP_LT: FLEAF_LOOP(!nulls[r] && vals[r] <  cv); break;
                case CMP_GT: FLEAF_LOOP(!nulls[r] && vals[r] >  cv); break;
                case CMP_LE: FLEAF_LOOP(!nulls[r] && vals[r] <= cv); break;
                case CMP_GE: FLEAF_LOOP(!nulls[r] && vals[r] >= cv); break;
                case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: break;
            }
            break;
        }
        case COLUMN_TYPE_INTERVAL: {
            /* For interval comparisons, use approximate usec */
            int64_t cv = interval_to_usec_approx(cmp_val->value.as_interval);
            switch (op) {
                case CMP_EQ: FLEAF_LOOP(!nulls[r] && interval_to_usec_approx(cb->data.iv[r]) == cv); break;
                case CMP_NE: FLEAF_LOOP(!nulls[r] && interval_to_usec_approx(cb->data.iv[r]) != cv); break;
                case CMP_LT: FLEAF_LOOP(!nulls[r] && interval_to_usec_approx(cb->data.iv[r]) <  cv); break;
                case CMP_GT: FLEAF_LOOP(!nulls[r] && interval_to_usec_approx(cb->data.iv[r]) >  cv); break;
                case CMP_LE: FLEAF_LOOP(!nulls[r] && interval_to_usec_approx(cb->data.iv[r]) <= cv); break;
                case CMP_GE: FLEAF_LOOP(!nulls[r] && interval_to_usec_approx(cb->data.iv[r]) >= cv); break;
                case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: break;
            }
            break;
        }
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_UUID: {
            const char *cv = cmp_val->value.as_text;
            if (!cv) cv = "";
            char * const *vals = cb->data.str;
            switch (op) {
                case CMP_EQ: FLEAF_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], cv) == 0); break;
                case CMP_NE: FLEAF_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], cv) != 0); break;
                case CMP_LT: FLEAF_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], cv) <  0); break;
                case CMP_GT: FLEAF_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], cv) >  0); break;
                case CMP_LE: FLEAF_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], cv) <= 0); break;
                case CMP_GE: FLEAF_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], cv) >= 0); break;
                case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: break;
            }
            break;
        }
        }
        break;
    }

    case CMP_NOT_IN:
    case CMP_IS_DISTINCT:
    case CMP_IS_NOT_DISTINCT:
    case CMP_EXISTS:
    case CMP_NOT_EXISTS:
    case CMP_REGEX_MATCH:
    case CMP_REGEX_NOT_MATCH:
        break;
    }

    #undef FLEAF_LOOP
    return sel_count;
}

/* Evaluate a condition tree against columnar data, producing a selection vector.
 * Handles COND_AND (intersect), COND_OR (union), and leaf COND_COMPARE.
 * t: table for column name resolution (maps condition column names to block indices).
 * cand/cand_count: input candidate rows to evaluate.
 * sel: output buffer (must hold cand_count entries).
 * Returns number of matching rows written to sel. */
static uint16_t filter_eval_cond_columnar(struct query_arena *arena,
                                           struct table *t,
                                           uint32_t cond_idx,
                                           struct row_block *blk,
                                           uint32_t *cand, uint16_t cand_count,
                                           uint32_t *sel,
                                           struct bump_alloc *scratch)
{
    if (cond_idx == IDX_NONE || cand_count == 0) return 0;
    struct condition *cond = &COND(arena, cond_idx);

    /* ---- COND_AND: intersect left and right ---- */
    if (cond->type == COND_AND) {
        uint32_t *tmp = (uint32_t *)bump_alloc(scratch, cand_count * sizeof(uint32_t));
        uint16_t left_count = filter_eval_cond_columnar(arena, t, cond->left, blk,
                                                         cand, cand_count, tmp, scratch);
        return filter_eval_cond_columnar(arena, t, cond->right, blk,
                                          tmp, left_count, sel, scratch);
    }

    /* ---- COND_OR: union left and right ---- */
    if (cond->type == COND_OR) {
        uint32_t *left_sel = (uint32_t *)bump_alloc(scratch, cand_count * sizeof(uint32_t));
        uint32_t *right_sel = (uint32_t *)bump_alloc(scratch, cand_count * sizeof(uint32_t));
        uint16_t left_count = filter_eval_cond_columnar(arena, t, cond->left, blk,
                                                         cand, cand_count, left_sel, scratch);
        uint16_t right_count = filter_eval_cond_columnar(arena, t, cond->right, blk,
                                                          cand, cand_count, right_sel, scratch);
        /* Merge-union (both are subsets of cand, in order) */
        uint16_t out_count = 0;
        uint16_t li = 0, ri = 0;
        while (li < left_count && ri < right_count) {
            if (left_sel[li] < right_sel[ri]) sel[out_count++] = left_sel[li++];
            else if (left_sel[li] > right_sel[ri]) sel[out_count++] = right_sel[ri++];
            else { sel[out_count++] = left_sel[li++]; ri++; }
        }
        while (li < left_count) sel[out_count++] = left_sel[li++];
        while (ri < right_count) sel[out_count++] = right_sel[ri++];
        return out_count;
    }

    /* ---- Leaf COND_COMPARE ---- */
    if (cond->type != COND_COMPARE) return 0;
    int fc = table_find_column_sv(t, cond->column);
    if (fc < 0 || fc >= blk->ncols) return 0;

    return filter_eval_leaf(&blk->cols[fc], (int)cond->op,
                             &cond->value, &cond->between_high,
                             cond->in_values_count > 0 ?
                                 &arena->cells.items[cond->in_values_start] : NULL,
                             cond->in_values_count,
                             cond->value.value.as_text,
                             cand, cand_count, sel);
}

static int filter_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                       struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);

    /* Pull block from child */
    int rc = plan_next_block(ctx, pn->left, out);
    if (rc != 0) return rc;

    /* If child already has a selection vector (stacked filters), we must
     * only consider those rows.  Build a candidate array for uniform iteration. */
    uint16_t cand_count;
    uint32_t *cand;
    if (out->sel) {
        cand_count = out->sel_count;
        cand = out->sel;
    } else {
        cand_count = out->count;
        cand = (uint32_t *)bump_alloc(&ctx->arena->scratch,
                                       cand_count * sizeof(uint32_t));
        for (uint16_t i = 0; i < cand_count; i++) cand[i] = i;
    }

    /* Allocate output selection vector */
    uint32_t *sel = (uint32_t *)bump_alloc(&ctx->arena->scratch,
                                           cand_count * sizeof(uint32_t));
    uint16_t sel_count = 0;

    /* Columnar compound evaluation path (OR, nested AND/OR trees).
     * col_idx == -1 signals that this filter uses filter_eval_cond_columnar. */
    if (pn->filter.col_idx == -1 && pn->filter.cond_idx != IDX_NONE) {
        /* Find the table from the child node chain for column resolution */
        struct table *t = NULL;
        uint32_t walk = pn->left;
        while (walk != IDX_NONE) {
            struct plan_node *wn = &PLAN_NODE(ctx->arena, walk);
            switch (wn->op) {
            case PLAN_SEQ_SCAN:
                t = wn->seq_scan.table;
                goto walk_done;
            case PLAN_FILTER:
            case PLAN_PROJECT:
            case PLAN_SORT:
            case PLAN_LIMIT:
            case PLAN_DISTINCT:
            case PLAN_EXPR_PROJECT:
                walk = wn->left;
                break;
            case PLAN_INDEX_SCAN:
            case PLAN_HASH_JOIN:
            case PLAN_NESTED_LOOP:
            case PLAN_HASH_AGG:
            case PLAN_SIMPLE_AGG:
            case PLAN_SET_OP:
            case PLAN_WINDOW:
            case PLAN_HASH_SEMI_JOIN:
            case PLAN_GENERATE_SERIES:
                goto walk_done;
            }
        }
        walk_done:;
        if (t) {
            sel_count = filter_eval_cond_columnar(ctx->arena, t, pn->filter.cond_idx,
                                                   out, cand, cand_count, sel,
                                                   &ctx->arena->scratch);
        }
        goto done;
    }

    /* Fast path: simple numeric comparison on a single column */
    if (pn->filter.col_idx >= 0 && pn->filter.col_idx < out->ncols) {
        struct col_block *cb = &out->cols[pn->filter.col_idx];
        int op = pn->filter.cmp_op;

        /* Coerce TEXT comparison values to the column's temporal type */
        if (column_type_is_temporal(cb->type)) {
            coerce_cmp_to_temporal(&pn->filter.cmp_val, cb->type);
            coerce_cmp_to_temporal(&pn->filter.between_high, cb->type);
        }

        /* Macro: iterate over candidate rows */
        #define FILTER_LOOP(COND) \
            for (uint16_t _c = 0; _c < cand_count; _c++) { \
                uint16_t r = (uint16_t)cand[_c]; \
                if (COND) sel[sel_count++] = r; \
            }

        switch (op) {

        case CMP_IS_NULL: {
            const uint8_t *nulls = cb->nulls;
            FILTER_LOOP(nulls[r]);
            goto done;
        }

        case CMP_IS_NOT_NULL: {
            const uint8_t *nulls = cb->nulls;
            FILTER_LOOP(!nulls[r]);
            goto done;
        }

        case CMP_BETWEEN: {
            const uint8_t *nulls = cb->nulls;
            switch (cb->type) {
            case COLUMN_TYPE_INT:
            case COLUMN_TYPE_BOOLEAN: {
                int32_t lo = pn->filter.cmp_val.value.as_int;
                int32_t hi = pn->filter.between_high.value.as_int;
                const int32_t *vals = cb->data.i32;
                FILTER_LOOP(!nulls[r] && vals[r] >= lo && vals[r] <= hi);
                break;
            }
            case COLUMN_TYPE_BIGINT: {
                int64_t lo = (pn->filter.cmp_val.type == COLUMN_TYPE_INT || pn->filter.cmp_val.type == COLUMN_TYPE_BOOLEAN)
                    ? (int64_t)pn->filter.cmp_val.value.as_int : pn->filter.cmp_val.value.as_bigint;
                int64_t hi = (pn->filter.between_high.type == COLUMN_TYPE_INT || pn->filter.between_high.type == COLUMN_TYPE_BOOLEAN)
                    ? (int64_t)pn->filter.between_high.value.as_int : pn->filter.between_high.value.as_bigint;
                const int64_t *vals = cb->data.i64;
                FILTER_LOOP(!nulls[r] && vals[r] >= lo && vals[r] <= hi);
                break;
            }
            case COLUMN_TYPE_FLOAT:
            case COLUMN_TYPE_NUMERIC: {
                double lo = pn->filter.cmp_val.type == COLUMN_TYPE_FLOAT
                    ? pn->filter.cmp_val.value.as_float : (double)pn->filter.cmp_val.value.as_int;
                double hi = pn->filter.between_high.type == COLUMN_TYPE_FLOAT
                    ? pn->filter.between_high.value.as_float : (double)pn->filter.between_high.value.as_int;
                const double *vals = cb->data.f64;
                FILTER_LOOP(!nulls[r] && vals[r] >= lo && vals[r] <= hi);
                break;
            }
            case COLUMN_TYPE_SMALLINT: {
                int16_t lo = (int16_t)pn->filter.cmp_val.value.as_int;
                int16_t hi = (int16_t)pn->filter.between_high.value.as_int;
                const int16_t *vals = cb->data.i16;
                FILTER_LOOP(!nulls[r] && vals[r] >= lo && vals[r] <= hi);
                break;
            }
            case COLUMN_TYPE_DATE: {
                int32_t lo = pn->filter.cmp_val.value.as_date;
                int32_t hi = pn->filter.between_high.value.as_date;
                const int32_t *vals = cb->data.i32;
                FILTER_LOOP(!nulls[r] && vals[r] >= lo && vals[r] <= hi);
                break;
            }
            case COLUMN_TYPE_TIME:
            case COLUMN_TYPE_TIMESTAMP:
            case COLUMN_TYPE_TIMESTAMPTZ: {
                int64_t lo = pn->filter.cmp_val.value.as_timestamp;
                int64_t hi = pn->filter.between_high.value.as_timestamp;
                const int64_t *vals = cb->data.i64;
                FILTER_LOOP(!nulls[r] && vals[r] >= lo && vals[r] <= hi);
                break;
            }
            case COLUMN_TYPE_INTERVAL:
                break;
            case COLUMN_TYPE_TEXT:
            case COLUMN_TYPE_ENUM:
            case COLUMN_TYPE_UUID: {
                const char *lo = pn->filter.cmp_val.value.as_text ? pn->filter.cmp_val.value.as_text : "";
                const char *hi = pn->filter.between_high.value.as_text ? pn->filter.between_high.value.as_text : "";
                char * const *vals = cb->data.str;
                FILTER_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], lo) >= 0 && strcmp(vals[r], hi) <= 0);
                break;
            }
            }
            goto done;
        }

        case CMP_IN: {
            if (!pn->filter.in_values || pn->filter.in_count == 0) goto done;
            const uint8_t *nulls = cb->nulls;
            uint32_t nv = pn->filter.in_count;
            struct cell *inv = pn->filter.in_values;
            switch (cb->type) {
            case COLUMN_TYPE_INT:
            case COLUMN_TYPE_BOOLEAN: {
                const int32_t *vals = cb->data.i32;
                for (uint16_t _c = 0; _c < cand_count; _c++) {
                    uint16_t r = (uint16_t)cand[_c];
                    if (nulls[r]) continue;
                    int32_t v = vals[r];
                    for (uint32_t j = 0; j < nv; j++) {
                        if (!inv[j].is_null && inv[j].value.as_int == v) {
                            sel[sel_count++] = r; break;
                        }
                    }
                }
                break;
            }
            case COLUMN_TYPE_BIGINT: {
                const int64_t *vals = cb->data.i64;
                for (uint16_t _c = 0; _c < cand_count; _c++) {
                    uint16_t r = (uint16_t)cand[_c];
                    if (nulls[r]) continue;
                    int64_t v = vals[r];
                    for (uint32_t j = 0; j < nv; j++) {
                        int64_t jv = (inv[j].type == COLUMN_TYPE_INT || inv[j].type == COLUMN_TYPE_BOOLEAN)
                            ? (int64_t)inv[j].value.as_int
                            : (inv[j].type == COLUMN_TYPE_SMALLINT)
                            ? (int64_t)inv[j].value.as_smallint
                            : inv[j].value.as_bigint;
                        if (!inv[j].is_null && jv == v) {
                            sel[sel_count++] = r; break;
                        }
                    }
                }
                break;
            }
            case COLUMN_TYPE_FLOAT:
            case COLUMN_TYPE_NUMERIC: {
                const double *vals = cb->data.f64;
                for (uint16_t _c = 0; _c < cand_count; _c++) {
                    uint16_t r = (uint16_t)cand[_c];
                    if (nulls[r]) continue;
                    double v = vals[r];
                    for (uint32_t j = 0; j < nv; j++) {
                        double jv = inv[j].type == COLUMN_TYPE_FLOAT ? inv[j].value.as_float
                                  : inv[j].type == COLUMN_TYPE_NUMERIC ? inv[j].value.as_numeric
                                  : (double)inv[j].value.as_int;
                        if (!inv[j].is_null && v == jv) {
                            sel[sel_count++] = r; break;
                        }
                    }
                }
                break;
            }
            case COLUMN_TYPE_SMALLINT: {
                const int16_t *vals = cb->data.i16;
                for (uint16_t _c = 0; _c < cand_count; _c++) {
                    uint16_t r = (uint16_t)cand[_c];
                    if (nulls[r]) continue;
                    int16_t v = vals[r];
                    for (uint32_t j = 0; j < nv; j++) {
                        if (!inv[j].is_null && (int16_t)inv[j].value.as_int == v) {
                            sel[sel_count++] = r; break;
                        }
                    }
                }
                break;
            }
            case COLUMN_TYPE_DATE: {
                const int32_t *vals = cb->data.i32;
                for (uint16_t _c = 0; _c < cand_count; _c++) {
                    uint16_t r = (uint16_t)cand[_c];
                    if (nulls[r]) continue;
                    for (uint32_t j = 0; j < nv; j++)
                        if (!inv[j].is_null && inv[j].value.as_date == vals[r]) { sel[sel_count++] = r; break; }
                }
                break;
            }
            case COLUMN_TYPE_TIME:
            case COLUMN_TYPE_TIMESTAMP:
            case COLUMN_TYPE_TIMESTAMPTZ: {
                const int64_t *vals = cb->data.i64;
                for (uint16_t _c = 0; _c < cand_count; _c++) {
                    uint16_t r = (uint16_t)cand[_c];
                    if (nulls[r]) continue;
                    for (uint32_t j = 0; j < nv; j++)
                        if (!inv[j].is_null && inv[j].value.as_timestamp == vals[r]) { sel[sel_count++] = r; break; }
                }
                break;
            }
            case COLUMN_TYPE_INTERVAL: {
                const struct interval *vals = cb->data.iv;
                for (uint16_t _c = 0; _c < cand_count; _c++) {
                    uint16_t r = (uint16_t)cand[_c];
                    if (nulls[r]) continue;
                    for (uint32_t j = 0; j < nv; j++)
                        if (!inv[j].is_null &&
                            vals[r].months == inv[j].value.as_interval.months &&
                            vals[r].days == inv[j].value.as_interval.days &&
                            vals[r].usec == inv[j].value.as_interval.usec) { sel[sel_count++] = r; break; }
                }
                break;
            }
            case COLUMN_TYPE_TEXT:
            case COLUMN_TYPE_ENUM:
            case COLUMN_TYPE_UUID: {
                char * const *vals = cb->data.str;
                for (uint16_t _c = 0; _c < cand_count; _c++) {
                    uint16_t r = (uint16_t)cand[_c];
                    if (nulls[r] || !vals[r]) continue;
                    for (uint32_t j = 0; j < nv; j++) {
                        if (!inv[j].is_null && inv[j].value.as_text &&
                            strcmp(vals[r], inv[j].value.as_text) == 0) {
                            sel[sel_count++] = r; break;
                        }
                    }
                }
                break;
            }
            }
            goto done;
        }

        case CMP_LIKE:
        case CMP_ILIKE: {
            if (!column_type_is_text(cb->type)) goto done;
            const char *pat = pn->filter.like_pattern;
            if (!pat) pat = "";
            char * const *vals = cb->data.str;
            const uint8_t *nulls = cb->nulls;
            int icase = (op == CMP_ILIKE);
            FILTER_LOOP(!nulls[r] && vals[r] && like_match(pat, vals[r], icase));
            goto done;
        }

        case CMP_EQ:
        case CMP_NE:
        case CMP_LT:
        case CMP_GT:
        case CMP_LE:
        case CMP_GE: {
            switch (cb->type) {
            case COLUMN_TYPE_SMALLINT: {
                int16_t cmp_val = (pn->filter.cmp_val.type == COLUMN_TYPE_SMALLINT)
                    ? pn->filter.cmp_val.value.as_smallint : (int16_t)pn->filter.cmp_val.value.as_int;
                const int16_t *vals = cb->data.i16;
                const uint8_t *nulls = cb->nulls;
                switch (op) {
                    case CMP_EQ: FILTER_LOOP(!nulls[r] && vals[r] == cmp_val); break;
                    case CMP_NE: FILTER_LOOP(!nulls[r] && vals[r] != cmp_val); break;
                    case CMP_LT: FILTER_LOOP(!nulls[r] && vals[r] <  cmp_val); break;
                    case CMP_GT: FILTER_LOOP(!nulls[r] && vals[r] >  cmp_val); break;
                    case CMP_LE: FILTER_LOOP(!nulls[r] && vals[r] <= cmp_val); break;
                    case CMP_GE: FILTER_LOOP(!nulls[r] && vals[r] >= cmp_val); break;
                    case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                    case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                    case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                    case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: break;
                }
                break;
            }
            case COLUMN_TYPE_INT:
            case COLUMN_TYPE_BOOLEAN: {
                int32_t cmp_val = pn->filter.cmp_val.value.as_int;
                const int32_t *vals = cb->data.i32;
                const uint8_t *nulls = cb->nulls;
                switch (op) {
                    case CMP_EQ: FILTER_LOOP(!nulls[r] && vals[r] == cmp_val); break;
                    case CMP_NE: FILTER_LOOP(!nulls[r] && vals[r] != cmp_val); break;
                    case CMP_LT: FILTER_LOOP(!nulls[r] && vals[r] <  cmp_val); break;
                    case CMP_GT: FILTER_LOOP(!nulls[r] && vals[r] >  cmp_val); break;
                    case CMP_LE: FILTER_LOOP(!nulls[r] && vals[r] <= cmp_val); break;
                    case CMP_GE: FILTER_LOOP(!nulls[r] && vals[r] >= cmp_val); break;
                    case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                    case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                    case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                    case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: goto fallback;
                }
                goto done;
            }
            case COLUMN_TYPE_BIGINT: {
                int64_t cmp_val = (pn->filter.cmp_val.type == COLUMN_TYPE_INT ||
                                   pn->filter.cmp_val.type == COLUMN_TYPE_BOOLEAN)
                    ? (int64_t)pn->filter.cmp_val.value.as_int
                    : (pn->filter.cmp_val.type == COLUMN_TYPE_SMALLINT)
                    ? (int64_t)pn->filter.cmp_val.value.as_smallint
                    : pn->filter.cmp_val.value.as_bigint;
                const int64_t *vals = cb->data.i64;
                const uint8_t *nulls = cb->nulls;
                switch (op) {
                    case CMP_EQ: FILTER_LOOP(!nulls[r] && vals[r] == cmp_val); break;
                    case CMP_NE: FILTER_LOOP(!nulls[r] && vals[r] != cmp_val); break;
                    case CMP_LT: FILTER_LOOP(!nulls[r] && vals[r] <  cmp_val); break;
                    case CMP_GT: FILTER_LOOP(!nulls[r] && vals[r] >  cmp_val); break;
                    case CMP_LE: FILTER_LOOP(!nulls[r] && vals[r] <= cmp_val); break;
                    case CMP_GE: FILTER_LOOP(!nulls[r] && vals[r] >= cmp_val); break;
                    case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                    case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                    case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                    case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: goto fallback;
                }
                goto done;
            }
            case COLUMN_TYPE_FLOAT:
            case COLUMN_TYPE_NUMERIC: {
                double cmp_val_f = pn->filter.cmp_val.type == COLUMN_TYPE_FLOAT
                    ? pn->filter.cmp_val.value.as_float
                    : (double)pn->filter.cmp_val.value.as_int;
                const double *vals = cb->data.f64;
                const uint8_t *nulls = cb->nulls;
                switch (op) {
                    case CMP_EQ: FILTER_LOOP(!nulls[r] && vals[r] == cmp_val_f); break;
                    case CMP_NE: FILTER_LOOP(!nulls[r] && vals[r] != cmp_val_f); break;
                    case CMP_LT: FILTER_LOOP(!nulls[r] && vals[r] <  cmp_val_f); break;
                    case CMP_GT: FILTER_LOOP(!nulls[r] && vals[r] >  cmp_val_f); break;
                    case CMP_LE: FILTER_LOOP(!nulls[r] && vals[r] <= cmp_val_f); break;
                    case CMP_GE: FILTER_LOOP(!nulls[r] && vals[r] >= cmp_val_f); break;
                    case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                    case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                    case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                    case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: goto fallback;
                }
                goto done;
            }
            case COLUMN_TYPE_DATE: {
                int32_t cv = pn->filter.cmp_val.value.as_date;
                const int32_t *vals = cb->data.i32;
                const uint8_t *nulls = cb->nulls;
                switch (op) {
                    case CMP_EQ: FILTER_LOOP(!nulls[r] && vals[r] == cv); break;
                    case CMP_NE: FILTER_LOOP(!nulls[r] && vals[r] != cv); break;
                    case CMP_LT: FILTER_LOOP(!nulls[r] && vals[r] <  cv); break;
                    case CMP_GT: FILTER_LOOP(!nulls[r] && vals[r] >  cv); break;
                    case CMP_LE: FILTER_LOOP(!nulls[r] && vals[r] <= cv); break;
                    case CMP_GE: FILTER_LOOP(!nulls[r] && vals[r] >= cv); break;
                    case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                    case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                    case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                    case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: goto fallback;
                }
                goto done;
            }
            case COLUMN_TYPE_TIME:
            case COLUMN_TYPE_TIMESTAMP:
            case COLUMN_TYPE_TIMESTAMPTZ: {
                int64_t cv = pn->filter.cmp_val.value.as_timestamp;
                const int64_t *vals = cb->data.i64;
                const uint8_t *nulls = cb->nulls;
                switch (op) {
                    case CMP_EQ: FILTER_LOOP(!nulls[r] && vals[r] == cv); break;
                    case CMP_NE: FILTER_LOOP(!nulls[r] && vals[r] != cv); break;
                    case CMP_LT: FILTER_LOOP(!nulls[r] && vals[r] <  cv); break;
                    case CMP_GT: FILTER_LOOP(!nulls[r] && vals[r] >  cv); break;
                    case CMP_LE: FILTER_LOOP(!nulls[r] && vals[r] <= cv); break;
                    case CMP_GE: FILTER_LOOP(!nulls[r] && vals[r] >= cv); break;
                    case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                    case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                    case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                    case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: goto fallback;
                }
                goto done;
            }
            case COLUMN_TYPE_INTERVAL:
                goto fallback;
            case COLUMN_TYPE_TEXT:
            case COLUMN_TYPE_ENUM:
            case COLUMN_TYPE_UUID: {
                const char *cmp_str = pn->filter.cmp_val.value.as_text;
                char * const *vals = cb->data.str;
                const uint8_t *nulls = cb->nulls;
                if (!cmp_str) cmp_str = "";
                switch (op) {
                    case CMP_EQ: FILTER_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], cmp_str) == 0); break;
                    case CMP_NE: FILTER_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], cmp_str) != 0); break;
                    case CMP_LT: FILTER_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], cmp_str) <  0); break;
                    case CMP_GT: FILTER_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], cmp_str) >  0); break;
                    case CMP_LE: FILTER_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], cmp_str) <= 0); break;
                    case CMP_GE: FILTER_LOOP(!nulls[r] && vals[r] && strcmp(vals[r], cmp_str) >= 0); break;
                    case CMP_IS_NULL: case CMP_IS_NOT_NULL: case CMP_IN: case CMP_NOT_IN:
                    case CMP_BETWEEN: case CMP_LIKE: case CMP_ILIKE: case CMP_IS_DISTINCT:
                    case CMP_IS_NOT_DISTINCT: case CMP_EXISTS: case CMP_NOT_EXISTS:
                    case CMP_REGEX_MATCH: case CMP_REGEX_NOT_MATCH: goto fallback;
                }
                goto done;
            }
            }
            break;
        }

        case CMP_NOT_IN:
        case CMP_IS_DISTINCT:
        case CMP_IS_NOT_DISTINCT:
        case CMP_EXISTS:
        case CMP_NOT_EXISTS:
        case CMP_REGEX_MATCH:
        case CMP_REGEX_NOT_MATCH:
            break;
        }

        #undef FILTER_LOOP
    }

fallback:
    /* Slow path: use eval_condition per row */
    if (pn->filter.cond_idx != IDX_NONE) {
        struct plan_node *parent = &PLAN_NODE(ctx->arena, pn->left);
        struct table *t = NULL;
        if (parent->op == PLAN_SEQ_SCAN)
            t = parent->seq_scan.table;

        if (t) {
            struct scan_state *sst = (struct scan_state *)ctx->node_states[pn->left];
            size_t base = sst ? (sst->cursor - out->count) : 0;
            for (uint16_t _c = 0; _c < cand_count; _c++) {
                uint16_t i = (uint16_t)cand[_c];
                size_t row_idx = base + i;
                if (row_idx < t->rows.count &&
                    eval_condition(pn->filter.cond_idx, ctx->arena,
                                   &t->rows.items[row_idx], t, ctx->db))
                    sel[sel_count++] = i;
            }
        }
    }

done:
    out->sel = sel;
    out->sel_count = sel_count;
    return 0;
}

static int project_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                        struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);

    /* Pull from child into a temporary block */
    struct row_block child_block;
    row_block_alloc(&child_block, out->ncols, &ctx->arena->scratch);

    /* We need the child's full column set first */
    uint16_t child_ncols = plan_node_ncols(ctx->arena, pn->left);
    if (child_ncols == 0) child_ncols = out->ncols;

    struct row_block input;
    row_block_alloc(&input, child_ncols, &ctx->arena->scratch);
    int rc = plan_next_block(ctx, pn->left, &input);
    if (rc != 0) return rc;

    /* Project: copy selected columns */
    out->count = input.count;
    out->sel = input.sel;
    out->sel_count = input.sel_count;
    for (uint16_t c = 0; c < pn->project.ncols; c++) {
        int src_col = pn->project.col_map[c];
        if (src_col >= 0 && src_col < input.ncols) {
            out->cols[c] = input.cols[src_col];
        }
    }
    return 0;
}

/* ---- Expression projection: evaluate arbitrary expressions per row ---- */

static int expr_project_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                             struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct table *t = pn->expr_project.table;
    uint16_t out_ncols = pn->expr_project.ncols;
    uint32_t *expr_indices = pn->expr_project.expr_indices;

    /* Pull a block from the child (full table scan) */
    uint16_t child_ncols = plan_node_ncols(ctx->arena, pn->left);
    if (child_ncols == 0) return -1;

    struct row_block input;
    row_block_alloc(&input, child_ncols, &ctx->arena->scratch);
    int rc = plan_next_block(ctx, pn->left, &input);
    if (rc != 0) return rc;

    uint16_t active = row_block_active_count(&input);
    if (active == 0) return -1;

    /* Build a temporary row struct for eval_expr.
     * We reuse the same cells array for each row — just update values. */
    struct row tmp_row = {0};
    tmp_row.cells.count = child_ncols;
    struct cell *tmp_cells = (struct cell *)bump_alloc(&ctx->arena->scratch,
                                                       child_ncols * sizeof(struct cell));
    tmp_row.cells.items = tmp_cells;

    /* Initialize output col_blocks */
    out->count = active;
    out->sel = NULL;
    out->sel_count = 0;

    /* Process each active row */
    for (uint16_t r = 0; r < active; r++) {
        uint16_t ri = row_block_row_idx(&input, r);

        /* Reconstruct row from col_blocks */
        for (uint16_t c = 0; c < child_ncols; c++) {
            struct col_block *cb = &input.cols[c];
            struct cell *cell = &tmp_cells[c];
            cell->type = cb->type;
            if (cb->nulls[ri]) {
                cell->is_null = 1;
                memset(&cell->value, 0, sizeof(cell->value));
            } else {
                cell->is_null = 0;
                cb_to_cell_at(cb, ri, cell);
            }
        }

        /* Evaluate each output expression */
        for (uint16_t c = 0; c < out_ncols; c++) {
            struct cell result = eval_expr(expr_indices[c], ctx->arena, t, &tmp_row,
                                           ctx->db, &ctx->arena->scratch);
            struct col_block *ocb = &out->cols[c];

            if (r == 0) {
                /* First row: set the output column type */
                ocb->type = result.is_null ? COLUMN_TYPE_TEXT : result.type;
            }

            if (result.is_null) {
                ocb->nulls[r] = 1;
            } else {
                ocb->nulls[r] = 0;
                enum column_type ot = ocb->type;
                if (ot == COLUMN_TYPE_SMALLINT) {
                    if (result.type == COLUMN_TYPE_SMALLINT)
                        ocb->data.i16[r] = result.value.as_smallint;
                    else if (result.type == COLUMN_TYPE_INT)
                        ocb->data.i16[r] = (int16_t)result.value.as_int;
                    else if (result.type == COLUMN_TYPE_BIGINT)
                        ocb->data.i16[r] = (int16_t)result.value.as_bigint;
                    else if (result.type == COLUMN_TYPE_FLOAT || result.type == COLUMN_TYPE_NUMERIC)
                        ocb->data.i16[r] = (int16_t)result.value.as_float;
                    else
                        ocb->data.i16[r] = 0;
                } else if (ot == COLUMN_TYPE_INT || ot == COLUMN_TYPE_BOOLEAN) {
                    if (result.type == COLUMN_TYPE_SMALLINT)
                        ocb->data.i32[r] = (int32_t)result.value.as_smallint;
                    else if (result.type == COLUMN_TYPE_INT || result.type == COLUMN_TYPE_BOOLEAN)
                        ocb->data.i32[r] = result.value.as_int;
                    else if (result.type == COLUMN_TYPE_BIGINT)
                        ocb->data.i32[r] = (int32_t)result.value.as_bigint;
                    else if (result.type == COLUMN_TYPE_FLOAT || result.type == COLUMN_TYPE_NUMERIC)
                        ocb->data.i32[r] = (int32_t)result.value.as_float;
                    else
                        ocb->data.i32[r] = 0;
                } else if (ot == COLUMN_TYPE_BIGINT) {
                    if (result.type == COLUMN_TYPE_BIGINT)
                        ocb->data.i64[r] = result.value.as_bigint;
                    else if (result.type == COLUMN_TYPE_SMALLINT)
                        ocb->data.i64[r] = result.value.as_smallint;
                    else if (result.type == COLUMN_TYPE_INT)
                        ocb->data.i64[r] = result.value.as_int;
                    else if (result.type == COLUMN_TYPE_FLOAT || result.type == COLUMN_TYPE_NUMERIC)
                        ocb->data.i64[r] = (int64_t)result.value.as_float;
                    else
                        ocb->data.i64[r] = 0;
                } else if (ot == COLUMN_TYPE_FLOAT || ot == COLUMN_TYPE_NUMERIC) {
                    if (result.type == COLUMN_TYPE_FLOAT || result.type == COLUMN_TYPE_NUMERIC)
                        ocb->data.f64[r] = result.value.as_float;
                    else if (result.type == COLUMN_TYPE_SMALLINT)
                        ocb->data.f64[r] = result.value.as_smallint;
                    else if (result.type == COLUMN_TYPE_INT)
                        ocb->data.f64[r] = result.value.as_int;
                    else if (result.type == COLUMN_TYPE_BIGINT)
                        ocb->data.f64[r] = (double)result.value.as_bigint;
                    else
                        ocb->data.f64[r] = 0.0;
                } else if (column_type_is_temporal(result.type)) {
                    ocb->type = result.type;
                    cell_to_cb_at(ocb, r, &result);
                } else {
                    /* Text-like types */
                    if (column_type_is_text(result.type) ||
                        result.type == COLUMN_TYPE_UUID ||
                        result.type == COLUMN_TYPE_ENUM) {
                        ocb->data.str[r] = result.value.as_text;
                        ocb->type = result.type;
                    } else {
                        /* Numeric result but text output column — convert */
                        char buf[64];
                        if (result.type == COLUMN_TYPE_SMALLINT)
                            snprintf(buf, sizeof(buf), "%d", (int)result.value.as_smallint);
                        else if (result.type == COLUMN_TYPE_INT)
                            snprintf(buf, sizeof(buf), "%d", result.value.as_int);
                        else if (result.type == COLUMN_TYPE_BIGINT)
                            snprintf(buf, sizeof(buf), "%lld", result.value.as_bigint);
                        else if (result.type == COLUMN_TYPE_FLOAT || result.type == COLUMN_TYPE_NUMERIC)
                            snprintf(buf, sizeof(buf), "%g", result.value.as_float);
                        else
                            buf[0] = '\0';
                        ocb->data.str[r] = bump_strdup(&ctx->arena->scratch, buf);
                    }
                }
                /* Update output type if first non-null row sets a more specific type */
                if (r == 0 || (ocb->type == COLUMN_TYPE_TEXT && !column_type_is_text(result.type) && !result.is_null)) {
                    /* Keep the type from first non-null result */
                }
            }
            ocb->count = r + 1;
        }
    }

    return 0;
}

static int limit_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                      struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct limit_state *st = (struct limit_state *)ctx->node_states[node_idx];
    if (!st) {
        st = (struct limit_state *)bump_calloc(&ctx->arena->scratch, 1, sizeof(*st));
        ctx->node_states[node_idx] = st;
    }

    /* Check if we've already emitted enough */
    if (pn->limit.has_limit && st->emitted >= pn->limit.limit)
        return -1;

    int rc = plan_next_block(ctx, pn->left, out);
    if (rc != 0) return rc;

    uint16_t active = row_block_active_count(out);

    /* Handle OFFSET: skip rows */
    if (pn->limit.has_offset && st->skipped < pn->limit.offset) {
        size_t to_skip = pn->limit.offset - st->skipped;
        if (to_skip >= active) {
            st->skipped += active;
            /* Skip this entire block, pull next */
            return plan_next_block(ctx, pn->left, out);
        }
        /* Partial skip: create selection vector starting after offset */
        uint32_t *sel = (uint32_t *)bump_alloc(&ctx->arena->scratch,
                                               active * sizeof(uint32_t));
        uint16_t sel_count = 0;
        for (uint16_t i = (uint16_t)to_skip; i < active; i++) {
            sel[sel_count++] = row_block_row_idx(out, i);
        }
        out->sel = sel;
        out->sel_count = sel_count;
        st->skipped = pn->limit.offset;
        active = sel_count;
    }

    /* Handle LIMIT: cap rows */
    if (pn->limit.has_limit) {
        size_t remaining = pn->limit.limit - st->emitted;
        if (active > remaining) {
            /* Truncate selection vector */
            if (!out->sel) {
                uint32_t *sel = (uint32_t *)bump_alloc(&ctx->arena->scratch,
                                                       remaining * sizeof(uint32_t));
                for (uint16_t i = 0; i < (uint16_t)remaining; i++)
                    sel[i] = i;
                out->sel = sel;
                out->sel_count = (uint16_t)remaining;
            } else {
                out->sel_count = (uint16_t)remaining;
            }
            active = (uint16_t)remaining;
        }
    }

    st->emitted += active;
    return 0;
}

/* ---- Hash join ---- */

/* jc_elem_size: use col_type_elem_size from block.h */
#define jc_elem_size col_type_elem_size

/* ---- flat_col helpers for hash join build side ---- */

static void flat_col_init(struct flat_col *fc, enum column_type type,
                          uint32_t cap, struct bump_alloc *scratch)
{
    fc->type = type;
    fc->nulls = (uint8_t *)bump_calloc(scratch, cap, 1);
    fc->data = bump_calloc(scratch, cap, jc_elem_size(type));
}

static void flat_col_grow(struct flat_col *fc, uint32_t old_cap, uint32_t new_cap,
                          struct bump_alloc *scratch)
{
    size_t esz = jc_elem_size(fc->type);
    uint8_t *new_nulls = (uint8_t *)bump_calloc(scratch, new_cap, 1);
    void *new_data = bump_calloc(scratch, new_cap, esz);
    memcpy(new_nulls, fc->nulls, old_cap);
    memcpy(new_data, fc->data, old_cap * esz);
    fc->nulls = new_nulls;
    fc->data = new_data;
}

static void flat_col_set_from_cb(struct flat_col *fc, uint32_t dst_i,
                                  const struct col_block *src, uint16_t src_i)
{
    fc->nulls[dst_i] = src->nulls[src_i];
    size_t esz = col_type_elem_size(fc->type);
    memcpy((uint8_t *)fc->data + dst_i * esz, cb_data_ptr(src, (uint32_t)src_i), esz);
}

static uint32_t flat_col_hash(const struct flat_col *fc, uint32_t idx)
{
    if (fc->nulls[idx]) return 0x9e3779b9u;
    uint32_t h = 2166136261u;
    if (column_type_is_text(fc->type)) {
        const char *s = ((const char **)fc->data)[idx];
        if (s) for (; *s; s++) { h ^= (uint8_t)*s; h *= 16777619u; }
    } else {
        size_t esz = jc_elem_size(fc->type);
        const uint8_t *p = (const uint8_t *)fc->data + idx * esz;
        for (size_t i = 0; i < esz; i++) { h ^= p[i]; h *= 16777619u; }
    }
    return h;
}

static int flat_col_eq(const struct flat_col *a, uint32_t ai,
                       const struct col_block *b, uint16_t bi)
{
    if (a->nulls[ai] != b->nulls[bi]) return 0;
    if (a->nulls[ai]) return 1;
    switch (a->type) {
    case COLUMN_TYPE_SMALLINT: return ((int16_t *)a->data)[ai] == b->data.i16[bi];
    case COLUMN_TYPE_BIGINT:   return ((int64_t *)a->data)[ai] == b->data.i64[bi];
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_NUMERIC:  return ((double *)a->data)[ai] == b->data.f64[bi];
    case COLUMN_TYPE_DATE:     return ((int32_t *)a->data)[ai] == b->data.i32[bi];
    case COLUMN_TYPE_TIME:
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_TIMESTAMPTZ:
        return ((int64_t *)a->data)[ai] == b->data.i64[bi];
    case COLUMN_TYPE_INTERVAL: {
        struct interval ia = ((const struct interval *)a->data)[ai];
        struct interval ib = b->data.iv[bi];
        return ia.months == ib.months && ia.days == ib.days && ia.usec == ib.usec;
    }
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_UUID: {
        const char *sa = ((const char **)a->data)[ai];
        const char *sb = b->data.str[bi];
        if (!sa && !sb) return 1;
        if (!sa || !sb) return 0;
        return strcmp(sa, sb) == 0;
    }
    case COLUMN_TYPE_INT:
    case COLUMN_TYPE_BOOLEAN:  return ((int32_t *)a->data)[ai] == b->data.i32[bi];
    }
}

static void flat_col_copy_to_out(const struct flat_col *fc, uint32_t src_i,
                                  struct col_block *dst, uint16_t dst_i)
{
    dst->nulls[dst_i] = fc->nulls[src_i];
    size_t esz = col_type_elem_size(fc->type);
    memcpy(cb_data_ptr(dst, (uint32_t)dst_i), (const uint8_t *)fc->data + src_i * esz, esz);
}

/* Restore hash join state from a table's join_cache into bump-allocated state */
static int hash_join_restore_from_cache(struct plan_exec_ctx *ctx,
                                        struct hash_join_state *st,
                                        struct join_cache *jc)
{
    st->build_ncols = jc->ncols;
    st->build_count = jc->nrows;
    st->build_cap = jc->nrows;
    st->build_cols = (struct flat_col *)bump_calloc(&ctx->arena->scratch,
                                                    jc->ncols, sizeof(struct flat_col));
    for (uint16_t c = 0; c < jc->ncols; c++) {
        st->build_cols[c].type = jc->col_types[c];
        size_t esz = jc_elem_size(jc->col_types[c]);
        st->build_cols[c].nulls = (uint8_t *)bump_alloc(&ctx->arena->scratch, jc->nrows);
        memcpy(st->build_cols[c].nulls, jc->col_nulls[c], jc->nrows);
        st->build_cols[c].data = bump_alloc(&ctx->arena->scratch, esz * jc->nrows);
        memcpy(st->build_cols[c].data, jc->col_data[c], esz * jc->nrows);
    }

    /* Restore hash table */
    st->ht.nbuckets = jc->nbuckets;
    st->ht.count = jc->nrows;
    st->ht.hashes = (uint32_t *)bump_alloc(&ctx->arena->scratch, jc->nrows * sizeof(uint32_t));
    memcpy(st->ht.hashes, jc->hashes, jc->nrows * sizeof(uint32_t));
    st->ht.nexts = (uint32_t *)bump_alloc(&ctx->arena->scratch, jc->nrows * sizeof(uint32_t));
    memcpy(st->ht.nexts, jc->nexts, jc->nrows * sizeof(uint32_t));
    st->ht.buckets = (uint32_t *)bump_alloc(&ctx->arena->scratch, jc->nbuckets * sizeof(uint32_t));
    memcpy(st->ht.buckets, jc->buckets, jc->nbuckets * sizeof(uint32_t));

    st->build_done = 1;
    return 0;
}

/* Save hash join build state to a table's join_cache (heap-allocated) */
static void hash_join_save_to_cache(struct hash_join_state *st,
                                    struct join_cache *jc,
                                    int key_col, uint64_t generation)
{
    /* Free old cache if present */
    if (jc->valid) {
        for (uint16_t i = 0; i < jc->ncols; i++) {
            free(jc->col_data[i]);
            free(jc->col_nulls[i]);
        }
        free(jc->col_data);
        free(jc->col_nulls);
        free(jc->col_types);
        free(jc->hashes);
        free(jc->nexts);
        free(jc->buckets);
    }

    jc->generation = generation;
    jc->key_col = key_col;
    jc->ncols = st->build_ncols;
    jc->nrows = st->build_count;
    jc->nbuckets = st->ht.nbuckets;

    jc->col_data = (void **)malloc(st->build_ncols * sizeof(void *));
    jc->col_nulls = (uint8_t **)malloc(st->build_ncols * sizeof(uint8_t *));
    jc->col_types = (enum column_type *)malloc(st->build_ncols * sizeof(enum column_type));

    for (uint16_t c = 0; c < st->build_ncols; c++) {
        jc->col_types[c] = st->build_cols[c].type;
        size_t esz = jc_elem_size(st->build_cols[c].type);
        jc->col_data[c] = malloc(esz * st->build_count);
        memcpy(jc->col_data[c], st->build_cols[c].data, esz * st->build_count);
        jc->col_nulls[c] = (uint8_t *)malloc(st->build_count);
        memcpy(jc->col_nulls[c], st->build_cols[c].nulls, st->build_count);
    }

    jc->hashes = (uint32_t *)malloc(st->build_count * sizeof(uint32_t));
    memcpy(jc->hashes, st->ht.hashes, st->build_count * sizeof(uint32_t));
    jc->nexts = (uint32_t *)malloc(st->build_count * sizeof(uint32_t));
    memcpy(jc->nexts, st->ht.nexts, st->build_count * sizeof(uint32_t));
    jc->buckets = (uint32_t *)malloc(st->ht.nbuckets * sizeof(uint32_t));
    memcpy(jc->buckets, st->ht.buckets, st->ht.nbuckets * sizeof(uint32_t));

    jc->valid = 1;
}

static void hash_join_build(struct plan_exec_ctx *ctx, uint32_t node_idx)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct hash_join_state *st = (struct hash_join_state *)ctx->node_states[node_idx];
    int key_col = pn->hash_join.inner_key_col;

    /* Check join cache on inner table */
    struct plan_node *inner = &PLAN_NODE(ctx->arena, pn->right);
    struct table *inner_t = NULL;
    if (inner->op == PLAN_SEQ_SCAN) inner_t = inner->seq_scan.table;

    if (inner_t && inner_t->join_cache.valid &&
        inner_t->join_cache.generation == inner_t->generation &&
        inner_t->join_cache.key_col == key_col) {
        hash_join_restore_from_cache(ctx, st, &inner_t->join_cache);
        return;
    }

    /* Determine inner column count */
    uint16_t inner_ncols = plan_node_ncols(ctx->arena, pn->right);

    /* Collect all rows from inner side into flat columns */
    uint32_t cap = 1024;
    st->build_ncols = inner_ncols;
    st->build_cols = (struct flat_col *)bump_calloc(&ctx->arena->scratch,
                                                    inner_ncols, sizeof(struct flat_col));
    st->build_cap = cap;
    st->build_count = 0;
    int types_inited = 0;

    struct row_block inner_block;
    row_block_alloc(&inner_block, inner_ncols, &ctx->arena->scratch);

    while (plan_next_block(ctx, pn->right, &inner_block) == 0) {
        uint16_t active = row_block_active_count(&inner_block);

        /* Init flat_col types from first block */
        if (!types_inited && active > 0) {
            for (uint16_t c = 0; c < inner_ncols; c++)
                flat_col_init(&st->build_cols[c], inner_block.cols[c].type,
                              cap, &ctx->arena->scratch);
            types_inited = 1;
        }

        /* Grow flat columns if needed */
        while (st->build_count + active > st->build_cap) {
            uint32_t old_cap = st->build_cap;
            uint32_t new_cap = old_cap * 2;
            for (uint16_t c = 0; c < inner_ncols; c++)
                flat_col_grow(&st->build_cols[c], old_cap, new_cap, &ctx->arena->scratch);
            st->build_cap = new_cap;
        }

        /* Append rows from this block */
        for (uint16_t i = 0; i < active; i++) {
            uint16_t ri = row_block_row_idx(&inner_block, i);
            uint32_t di = st->build_count++;
            for (uint16_t c = 0; c < inner_ncols; c++)
                flat_col_set_from_cb(&st->build_cols[c], di, &inner_block.cols[c], ri);
        }
        row_block_reset(&inner_block);
    }

    /* Build hash table on the join key column */
    block_ht_init(&st->ht, st->build_count > 0 ? st->build_count : 1,
                  &ctx->arena->scratch);

    struct flat_col *key_fc = &st->build_cols[key_col];
    for (uint32_t i = 0; i < st->build_count; i++) {
        uint32_t h = flat_col_hash(key_fc, i);
        uint32_t bucket = h & (st->ht.nbuckets - 1);
        st->ht.hashes[i] = h;
        st->ht.nexts[i] = st->ht.buckets[bucket];
        st->ht.buckets[bucket] = i;
        st->ht.count++;
    }

    st->build_done = 1;

    /* Save to join cache on inner table */
    if (inner_t && st->build_count > 0)
        hash_join_save_to_cache(st, &inner_t->join_cache, key_col, inner_t->generation);
}

static int hash_join_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                          struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct hash_join_state *st = (struct hash_join_state *)ctx->node_states[node_idx];
    if (!st) {
        st = (struct hash_join_state *)bump_calloc(&ctx->arena->scratch, 1, sizeof(*st));
        ctx->node_states[node_idx] = st;
    }

    if (!st->build_done)
        hash_join_build(ctx, node_idx);

    int join_type = pn->hash_join.join_type; /* 0=INNER,1=LEFT,2=RIGHT,3=FULL */

    /* Allocate matched bitmap for RIGHT/FULL on first call */
    if ((join_type == 2 || join_type == 3) && !st->matched && st->build_count > 0) {
        st->matched = (uint8_t *)bump_calloc(&ctx->arena->scratch,
                                              st->build_count, sizeof(uint8_t));
    }

    /* Determine outer (left child) column count */
    uint16_t outer_ncols = plan_node_ncols(ctx->arena, pn->left);

    /* ---- Phase 2: emit unmatched inner rows (RIGHT/FULL) ---- */
    if (st->outer_done) {
        if (join_type != 2 && join_type != 3) return -1;
        row_block_reset(out);
        uint16_t out_count = 0;
        while (st->right_emit_cursor < st->build_count && out_count < BLOCK_CAPACITY) {
            uint32_t bi = st->right_emit_cursor++;
            if (st->matched && st->matched[bi]) continue;
            /* Emit NULL outer cols + inner cols */
            for (uint16_t c = 0; c < outer_ncols; c++) {
                out->cols[c].nulls[out_count] = 1;
            }
            for (uint16_t c = 0; c < st->build_ncols; c++) {
                out->cols[outer_ncols + c].type = st->build_cols[c].type;
                flat_col_copy_to_out(&st->build_cols[c], bi,
                                     &out->cols[outer_ncols + c], out_count);
            }
            out_count++;
        }
        if (out_count == 0) return -1;
        out->count = out_count;
        for (uint16_t c = 0; c < out->ncols; c++)
            out->cols[c].count = out_count;
        return 0;
    }

    /* ---- Phase 1: probe outer rows against hash table ---- */
    struct row_block outer_block;
    row_block_alloc(&outer_block, outer_ncols, &ctx->arena->scratch);

    int rc = plan_next_block(ctx, pn->left, &outer_block);
    if (rc != 0) {
        /* Outer exhausted — transition to phase 2 for RIGHT/FULL */
        st->outer_done = 1;
        if (join_type == 2 || join_type == 3)
            return hash_join_next(ctx, node_idx, out); /* re-enter for phase 2 */
        return -1;
    }

    /* Probe: for each outer row, look up in hash table */
    int outer_key = pn->hash_join.outer_key_col;
    int inner_key = pn->hash_join.inner_key_col;
    struct col_block *outer_key_cb = &outer_block.cols[outer_key];
    struct flat_col *inner_key_fc = &st->build_cols[inner_key];

    row_block_reset(out);
    uint16_t out_count = 0;
    uint16_t active = row_block_active_count(&outer_block);

    for (uint16_t i = 0; i < active && out_count < BLOCK_CAPACITY; i++) {
        uint16_t oi = row_block_row_idx(&outer_block, i);

        /* NULL key: no match possible */
        if (outer_key_cb->nulls[oi]) {
            /* LEFT/FULL: emit outer row with NULL inner columns */
            if (join_type == 1 || join_type == 3) {
                for (uint16_t c = 0; c < outer_ncols; c++) {
                    out->cols[c].type = outer_block.cols[c].type;
                    cb_copy_value(&out->cols[c], out_count, &outer_block.cols[c], oi);
                }
                for (uint16_t c = 0; c < st->build_ncols; c++) {
                    out->cols[outer_ncols + c].type = st->build_cols[c].type;
                    out->cols[outer_ncols + c].nulls[out_count] = 1;
                    memset(cb_data_ptr(&out->cols[outer_ncols + c], (uint32_t)out_count), 0,
                           col_type_elem_size(st->build_cols[c].type));
                }
                out_count++;
            }
            continue;
        }

        uint32_t h = block_hash_cell(outer_key_cb, oi);
        uint32_t bucket = h & (st->ht.nbuckets - 1);
        uint32_t entry = st->ht.buckets[bucket];
        int found = 0;

        while (entry != IDX_NONE && entry != 0xFFFFFFFF && out_count < BLOCK_CAPACITY) {
            if (st->ht.hashes[entry] == h &&
                flat_col_eq(inner_key_fc, entry, outer_key_cb, oi)) {
                /* Match: emit combined row [outer cols | inner cols] */
                for (uint16_t c = 0; c < outer_ncols; c++) {
                    out->cols[c].type = outer_block.cols[c].type;
                    cb_copy_value(&out->cols[c], out_count, &outer_block.cols[c], oi);
                }
                for (uint16_t c = 0; c < st->build_ncols; c++) {
                    out->cols[outer_ncols + c].type = st->build_cols[c].type;
                    flat_col_copy_to_out(&st->build_cols[c], entry,
                                         &out->cols[outer_ncols + c], out_count);
                }
                out_count++;
                found = 1;
                /* Mark inner row as matched for RIGHT/FULL */
                if (st->matched) st->matched[entry] = 1;
            }
            entry = st->ht.nexts[entry];
        }

        /* LEFT/FULL: emit outer row with NULL inner columns when no match */
        if (!found && (join_type == 1 || join_type == 3) && out_count < BLOCK_CAPACITY) {
            for (uint16_t c = 0; c < outer_ncols; c++) {
                out->cols[c].type = outer_block.cols[c].type;
                cb_copy_value(&out->cols[c], out_count, &outer_block.cols[c], oi);
            }
            for (uint16_t c = 0; c < st->build_ncols; c++) {
                out->cols[outer_ncols + c].type = st->build_cols[c].type;
                out->cols[outer_ncols + c].nulls[out_count] = 1;
                memset(cb_data_ptr(&out->cols[outer_ncols + c], (uint32_t)out_count), 0,
                       col_type_elem_size(st->build_cols[c].type));
            }
            out_count++;
        }
    }

    if (out_count == 0) {
        /* No output from this probe block — try next block or phase 2 */
        return hash_join_next(ctx, node_idx, out);
    }

    out->count = out_count;
    for (uint16_t c = 0; c < out->ncols; c++)
        out->cols[c].count = out_count;

    return 0;
}

/* ---- Distinct hash set helpers for COUNT(DISTINCT) ---- */

static void distinct_set_init(struct distinct_set *ds, uint32_t cap, struct bump_alloc *scratch)
{
    ds->cap = cap;
    ds->count = 0;
    ds->slots = (uint64_t *)bump_calloc(scratch, cap, sizeof(uint64_t));
}

/* Insert a hash into the set. Returns 1 if the value is new, 0 if already seen.
 * Hash value 0 is reserved as empty sentinel, so we map 0 → 1. */
static int distinct_set_insert(struct distinct_set *ds, uint64_t h, struct bump_alloc *scratch)
{
    if (h == 0) h = 1; /* avoid sentinel collision */
    /* Resize at 75% load */
    if (ds->count * 4 >= ds->cap * 3) {
        uint32_t old_cap = ds->cap;
        uint64_t *old_slots = ds->slots;
        uint32_t new_cap = old_cap * 2;
        ds->slots = (uint64_t *)bump_calloc(scratch, new_cap, sizeof(uint64_t));
        ds->cap = new_cap;
        ds->count = 0;
        for (uint32_t i = 0; i < old_cap; i++) {
            if (old_slots[i] != 0) {
                uint32_t b = (uint32_t)(old_slots[i] & (new_cap - 1));
                while (ds->slots[b] != 0) b = (b + 1) & (new_cap - 1);
                ds->slots[b] = old_slots[i];
                ds->count++;
            }
        }
    }
    uint32_t b = (uint32_t)(h & (ds->cap - 1));
    while (ds->slots[b] != 0) {
        if (ds->slots[b] == h) return 0; /* already seen */
        b = (b + 1) & (ds->cap - 1);
    }
    ds->slots[b] = h;
    ds->count++;
    return 1;
}

static uint64_t distinct_hash_cell(struct col_block *cb, uint16_t ri)
{
    uint64_t h = 14695981039346656037ULL;
    switch (cb->type) {
        case COLUMN_TYPE_SMALLINT: h ^= (uint64_t)(uint16_t)cb->data.i16[ri]; break;
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:  h ^= (uint64_t)(uint32_t)cb->data.i32[ri]; break;
        case COLUMN_TYPE_BIGINT:   h ^= (uint64_t)cb->data.i64[ri]; break;
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC: {
            uint64_t bits;
            memcpy(&bits, &cb->data.f64[ri], sizeof(bits));
            h ^= bits;
            break;
        }
        case COLUMN_TYPE_DATE:
            h ^= (uint64_t)(uint32_t)cb->data.i32[ri]; break;
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
            h ^= (uint64_t)cb->data.i64[ri]; break;
        case COLUMN_TYPE_INTERVAL: {
            struct interval iv = cb->data.iv[ri];
            h ^= (uint64_t)(uint32_t)iv.months;
            h *= 1099511628211ULL;
            h ^= (uint64_t)(uint32_t)iv.days;
            h *= 1099511628211ULL;
            h ^= (uint64_t)iv.usec;
            break;
        }
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_UUID:
            if (cb->data.str[ri]) {
                const char *s = cb->data.str[ri];
                while (*s) { h ^= (uint64_t)(unsigned char)*s++; h *= 1099511628211ULL; }
            }
            break;
    }
    h *= 1099511628211ULL;
    return h;
}

/* ---- Hash aggregation ---- */

static int hash_agg_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                         struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct hash_agg_state *st = (struct hash_agg_state *)ctx->node_states[node_idx];
    if (!st) {
        st = (struct hash_agg_state *)bump_calloc(&ctx->arena->scratch, 1, sizeof(*st));
        st->group_cap = 256;
        st->ngroups = 0;
        st->input_done = 0;
        st->emit_cursor = 0;

        uint16_t ngrp = pn->hash_agg.ngroup_cols;
        st->group_keys = (struct col_block *)bump_calloc(&ctx->arena->scratch,
                                                         ngrp, sizeof(struct col_block));

        uint32_t agg_n = pn->hash_agg.agg_count;
        uint32_t max_groups = st->group_cap;
        st->sums = (double *)bump_calloc(&ctx->arena->scratch, agg_n * max_groups, sizeof(double));
        st->mins = (double *)bump_calloc(&ctx->arena->scratch, agg_n * max_groups, sizeof(double));
        st->maxs = (double *)bump_calloc(&ctx->arena->scratch, agg_n * max_groups, sizeof(double));
        st->text_mins = (const char **)bump_calloc(&ctx->arena->scratch, agg_n * max_groups, sizeof(const char *));
        st->text_maxs = (const char **)bump_calloc(&ctx->arena->scratch, agg_n * max_groups, sizeof(const char *));
        st->nonnull = (size_t *)bump_calloc(&ctx->arena->scratch, agg_n * max_groups, sizeof(size_t));
        st->grp_counts = (size_t *)bump_calloc(&ctx->arena->scratch, max_groups, sizeof(size_t));
        st->minmax_init = (int *)bump_calloc(&ctx->arena->scratch, agg_n * max_groups, sizeof(int));
        /* Initialize distinct sets for COUNT(DISTINCT) aggregates */
        st->distinct_sets = (struct distinct_set *)bump_calloc(&ctx->arena->scratch, agg_n * max_groups, sizeof(struct distinct_set));

        block_ht_init(&st->ht, max_groups, &ctx->arena->scratch);
        ctx->node_states[node_idx] = st;
    }

    /* Phase 1: consume all input blocks */
    if (!st->input_done) {
        uint16_t child_ncols = plan_node_ncols(ctx->arena, pn->left);

        struct row_block input;
        row_block_alloc(&input, child_ncols, &ctx->arena->scratch);

        while (plan_next_block(ctx, pn->left, &input) == 0) {
            uint16_t active = row_block_active_count(&input);
            uint16_t ngrp = pn->hash_agg.ngroup_cols;
            uint32_t agg_n = pn->hash_agg.agg_count;

            for (uint16_t i = 0; i < active; i++) {
                uint16_t ri = row_block_row_idx(&input, i);

                /* Hash the group key */
                uint32_t h = 2166136261u;
                for (uint16_t g = 0; g < ngrp; g++) {
                    int gc = pn->hash_agg.group_cols[g];
                    h ^= block_hash_cell(&input.cols[gc], ri);
                    h *= 16777619u;
                }

                /* Look up in hash table */
                uint32_t bucket = h & (st->ht.nbuckets - 1);
                uint32_t entry = st->ht.buckets[bucket];
                uint32_t group_idx = IDX_NONE;

                while (entry != IDX_NONE && entry != 0xFFFFFFFF) {
                    if (st->ht.hashes[entry] == h) {
                        /* Check key equality (GROUP BY: NULL == NULL) */
                        int eq = 1;
                        for (uint16_t g = 0; g < ngrp; g++) {
                            int gc = pn->hash_agg.group_cols[g];
                            int a_null = input.cols[gc].nulls[ri];
                            int b_null = st->group_keys[g].nulls[(uint16_t)entry];
                            if (a_null && b_null) continue; /* NULL == NULL for grouping */
                            if (a_null || b_null) { eq = 0; break; }
                            if (!block_cell_eq(&input.cols[gc], ri,
                                               &st->group_keys[g], (uint16_t)entry)) {
                                eq = 0;
                                break;
                            }
                        }
                        if (eq) { group_idx = entry; break; }
                    }
                    entry = st->ht.nexts[entry];
                }

                if (group_idx == IDX_NONE) {
                    /* Resize if at capacity */
                    if (st->ngroups >= st->group_cap) {
                        uint32_t old_cap = st->group_cap;
                        uint32_t new_cap = old_cap * 2;

                        /* Grow accumulator arrays */
                        #define GROW_ARR(type, field) do { \
                            type *_new = (type *)bump_calloc(&ctx->arena->scratch, \
                                (size_t)agg_n * new_cap, sizeof(type)); \
                            for (uint32_t _a = 0; _a < agg_n; _a++) \
                                memcpy(_new + _a * new_cap, st->field + _a * old_cap, \
                                       old_cap * sizeof(type)); \
                            st->field = _new; \
                        } while(0)
                        GROW_ARR(double, sums);
                        GROW_ARR(double, mins);
                        GROW_ARR(double, maxs);
                        GROW_ARR(const char *, text_mins);
                        GROW_ARR(const char *, text_maxs);
                        GROW_ARR(size_t, nonnull);
                        GROW_ARR(int, minmax_init);
                        GROW_ARR(struct distinct_set, distinct_sets);
                        #undef GROW_ARR

                        size_t *new_counts = (size_t *)bump_calloc(&ctx->arena->scratch,
                            new_cap, sizeof(size_t));
                        memcpy(new_counts, st->grp_counts, old_cap * sizeof(size_t));
                        st->grp_counts = new_counts;

                        /* Grow group key col_blocks — allocate fresh and bulk copy */
                        struct col_block *new_keys = (struct col_block *)bump_calloc(
                            &ctx->arena->scratch, ngrp, sizeof(struct col_block));
                        for (uint16_t g = 0; g < ngrp; g++) {
                            new_keys[g].type = st->group_keys[g].type;
                            new_keys[g].count = (uint16_t)(old_cap < BLOCK_CAPACITY ? old_cap : BLOCK_CAPACITY);
                            cb_bulk_copy(&new_keys[g], &st->group_keys[g], old_cap < BLOCK_CAPACITY ? old_cap : BLOCK_CAPACITY);
                        }
                        st->group_keys = new_keys;

                        /* Rebuild hash table with new capacity */
                        block_ht_init(&st->ht, new_cap, &ctx->arena->scratch);
                        for (uint32_t gi = 0; gi < st->ngroups; gi++) {
                            uint32_t gh = 2166136261u;
                            for (uint16_t g = 0; g < ngrp; g++) {
                                gh ^= block_hash_cell(&st->group_keys[g], (uint16_t)gi);
                                gh *= 16777619u;
                            }
                            st->ht.hashes[gi] = gh;
                            uint32_t gb = gh & (st->ht.nbuckets - 1);
                            st->ht.nexts[gi] = st->ht.buckets[gb];
                            st->ht.buckets[gb] = gi;
                            st->ht.count++;
                        }

                        st->group_cap = new_cap;
                        /* Recompute bucket for current row after rehash */
                        bucket = h & (st->ht.nbuckets - 1);
                    }

                    /* New group */
                    group_idx = st->ngroups++;
                    /* Store group key values */
                    for (uint16_t g = 0; g < ngrp; g++) {
                        int gc = pn->hash_agg.group_cols[g];
                        struct col_block *src = &input.cols[gc];
                        struct col_block *dst = &st->group_keys[g];
                        if (group_idx == 0) dst->type = src->type;
                        cb_copy_value(dst, group_idx, src, ri);
                    }
                    /* Insert into hash table */
                    st->ht.hashes[group_idx] = h;
                    st->ht.nexts[group_idx] = st->ht.buckets[bucket];
                    st->ht.buckets[bucket] = group_idx;
                    st->ht.count++;
                }

                /* Accumulate aggregates */
                st->grp_counts[group_idx]++;
                for (uint32_t a = 0; a < agg_n; a++) {
                    int ac = pn->hash_agg.agg_col_indices ? pn->hash_agg.agg_col_indices[a] : -1;

                    size_t idx = a * st->group_cap + group_idx;

                    /* FILTER (WHERE ...): evaluate per-aggregate filter condition */
                    struct agg_expr *ae_filt = &ctx->arena->aggregates.items[pn->hash_agg.agg_start + a];
                    if (ae_filt->filter_cond != IDX_NONE) {
                        if (!st->tmp_row_inited) {
                            st->tmp_row.cells.count = child_ncols;
                            st->tmp_row.cells.items = (struct cell *)bump_alloc(
                                &ctx->arena->scratch, child_ncols * sizeof(struct cell));
                            st->tmp_row_inited = 1;
                        }
                        for (uint16_t c = 0; c < child_ncols; c++) {
                            struct col_block *cb = &input.cols[c];
                            struct cell *cell = &st->tmp_row.cells.items[c];
                            cell->type = cb->type;
                            if (cb->nulls[ri]) {
                                cell->is_null = 1;
                                memset(&cell->value, 0, sizeof(cell->value));
                            } else {
                                cell->is_null = 0;
                                cb_to_cell_at(cb, ri, cell);
                            }
                        }
                        if (!eval_condition(ae_filt->filter_cond, ctx->arena,
                                            &st->tmp_row, pn->hash_agg.table, NULL))
                            continue;
                    }

                    if (ac == -1) {
                        /* COUNT(*): if FILTER is present, track filtered count in nonnull */
                        if (ae_filt->filter_cond != IDX_NONE)
                            st->nonnull[idx]++;
                        continue;
                    }

                    if (ac == -2) {
                        /* Expression-based aggregate: reconstruct temp row, eval expr */
                        struct agg_expr *ae = &ctx->arena->aggregates.items[pn->hash_agg.agg_start + a];
                        if (!st->tmp_row_inited) {
                            st->tmp_row.cells.count = child_ncols;
                            st->tmp_row.cells.items = (struct cell *)bump_alloc(
                                &ctx->arena->scratch, child_ncols * sizeof(struct cell));
                            st->tmp_row_inited = 1;
                        }
                        for (uint16_t c = 0; c < child_ncols; c++) {
                            struct col_block *cb = &input.cols[c];
                            struct cell *cell = &st->tmp_row.cells.items[c];
                            cell->type = cb->type;
                            if (cb->nulls[ri]) {
                                cell->is_null = 1;
                                memset(&cell->value, 0, sizeof(cell->value));
                            } else {
                                cell->is_null = 0;
                                cb_to_cell_at(cb, ri, cell);
                            }
                        }
                        struct cell cv = eval_expr(ae->expr_idx, ctx->arena,
                                                   pn->hash_agg.table, &st->tmp_row,
                                                   ctx->db, &ctx->arena->scratch);
                        if (cv.is_null) continue;
                        st->nonnull[idx]++;
                        double v = 0.0;
                        switch (cv.type) {
                            case COLUMN_TYPE_SMALLINT: v = (double)cv.value.as_smallint; break;
                            case COLUMN_TYPE_INT:      v = (double)cv.value.as_int; break;
                            case COLUMN_TYPE_BIGINT:   v = (double)cv.value.as_bigint; break;
                            case COLUMN_TYPE_FLOAT:
                            case COLUMN_TYPE_NUMERIC:  v = cv.value.as_float; break;
                            case COLUMN_TYPE_BOOLEAN:  v = (double)cv.value.as_bool; break;
                            case COLUMN_TYPE_DATE:     v = (double)cv.value.as_date; break;
                            case COLUMN_TYPE_TIME:
                            case COLUMN_TYPE_TIMESTAMP:
                            case COLUMN_TYPE_TIMESTAMPTZ:
                                v = (double)cv.value.as_timestamp; break;
                            case COLUMN_TYPE_INTERVAL:
                                v = (double)interval_to_usec_approx(cv.value.as_interval); break;
                            case COLUMN_TYPE_TEXT:
                            case COLUMN_TYPE_ENUM:
                            case COLUMN_TYPE_UUID:
                                break;
                        }
                        st->sums[idx] += v;
                        if (!st->minmax_init[idx] || v < st->mins[idx])
                            st->mins[idx] = v;
                        if (!st->minmax_init[idx] || v > st->maxs[idx])
                            st->maxs[idx] = v;
                        st->minmax_init[idx] = 1;
                        continue;
                    }

                    struct col_block *acb = &input.cols[ac];
                    if (acb->nulls[ri]) continue;

                    /* COUNT(DISTINCT): only count if value is new */
                    struct agg_expr *ae_acc = &ctx->arena->aggregates.items[pn->hash_agg.agg_start + a];
                    if (ae_acc->has_distinct) {
                        struct distinct_set *ds = &st->distinct_sets[idx];
                        if (ds->cap == 0)
                            distinct_set_init(ds, 16, &ctx->arena->scratch);
                        uint64_t dh = distinct_hash_cell(acb, ri);
                        if (!distinct_set_insert(ds, dh, &ctx->arena->scratch))
                            continue; /* duplicate — skip */
                    }
                    st->nonnull[idx]++;
                    if (column_type_is_text(acb->type)) {
                        const char *s = acb->data.str[ri];
                        if (s) {
                            if (!st->minmax_init[idx] || strcmp(s, st->text_mins[idx]) < 0)
                                st->text_mins[idx] = s;
                            if (!st->minmax_init[idx] || strcmp(s, st->text_maxs[idx]) > 0)
                                st->text_maxs[idx] = s;
                            st->minmax_init[idx] = 1;
                        }
                    } else if (acb->type == COLUMN_TYPE_DATE) {
                        int32_t v = acb->data.i32[ri];
                        st->sums[idx] += (double)v;
                        if (!st->minmax_init[idx] || v < (int32_t)st->mins[idx]) st->mins[idx] = (double)v;
                        if (!st->minmax_init[idx] || v > (int32_t)st->maxs[idx]) st->maxs[idx] = (double)v;
                        st->minmax_init[idx] = 1;
                    } else if (acb->type == COLUMN_TYPE_TIME ||
                               acb->type == COLUMN_TYPE_TIMESTAMP ||
                               acb->type == COLUMN_TYPE_TIMESTAMPTZ) {
                        int64_t v = acb->data.i64[ri];
                        int64_t cur_min, cur_max;
                        memcpy(&cur_min, &st->mins[idx], sizeof(int64_t));
                        memcpy(&cur_max, &st->maxs[idx], sizeof(int64_t));
                        if (!st->minmax_init[idx] || v < cur_min) memcpy(&st->mins[idx], &v, sizeof(double));
                        if (!st->minmax_init[idx] || v > cur_max) memcpy(&st->maxs[idx], &v, sizeof(double));
                        st->minmax_init[idx] = 1;
                    } else if (acb->type == COLUMN_TYPE_INTERVAL) {
                        int64_t v = interval_to_usec_approx(acb->data.iv[ri]);
                        double dv = (double)v;
                        st->sums[idx] += dv;
                        if (!st->minmax_init[idx] || dv < st->mins[idx]) st->mins[idx] = dv;
                        if (!st->minmax_init[idx] || dv > st->maxs[idx]) st->maxs[idx] = dv;
                        st->minmax_init[idx] = 1;
                    } else {
                        double v = cb_to_double(acb, ri);
                        st->sums[idx] += v;
                        if (!st->minmax_init[idx] || v < st->mins[idx])
                            st->mins[idx] = v;
                        if (!st->minmax_init[idx] || v > st->maxs[idx])
                            st->maxs[idx] = v;
                        st->minmax_init[idx] = 1;
                    }
                }
            }
            row_block_reset(&input);
        }
        st->input_done = 1;
    }

    /* Phase 2: emit results */
    if (st->emit_cursor >= st->ngroups) return -1;

    row_block_reset(out);
    uint16_t ngrp = pn->hash_agg.ngroup_cols;
    uint32_t agg_n = pn->hash_agg.agg_count;
    uint16_t out_count = 0;

    while (st->emit_cursor < st->ngroups && out_count < BLOCK_CAPACITY) {
        uint32_t g = st->emit_cursor++;

        if (!pn->hash_agg.agg_before_cols) {
            /* Group key columns first */
            for (uint16_t k = 0; k < ngrp; k++) {
                out->cols[k].type = st->group_keys[k].type;
                cb_copy_value(&out->cols[k], out_count, &st->group_keys[k], (uint16_t)g);
            }
        }

        uint16_t agg_offset = pn->hash_agg.agg_before_cols ? 0 : ngrp;
        for (uint32_t a = 0; a < agg_n; a++) {
            struct agg_expr *ae = &ctx->arena->aggregates.items[pn->hash_agg.agg_start + a];
            struct col_block *dst = &out->cols[agg_offset + a];
            size_t idx = a * st->group_cap + g;

            switch (ae->func) {
                case AGG_COUNT:
                    dst->type = COLUMN_TYPE_INT;
                    dst->nulls[out_count] = 0;
                    if (pn->hash_agg.agg_col_indices &&
                        pn->hash_agg.agg_col_indices[a] == -1 &&
                        ae->filter_cond == IDX_NONE)
                        dst->data.i32[out_count] = (int32_t)st->grp_counts[g];
                    else
                        dst->data.i32[out_count] = (int32_t)st->nonnull[idx];
                    break;
                case AGG_SUM: {
                    /* Determine source column type for correct output type */
                    int src_col = pn->hash_agg.agg_col_indices ? pn->hash_agg.agg_col_indices[a] : -1;
                    int src_is_float = 0;
                    if (src_col >= 0 && pn->hash_agg.table) {
                        enum column_type sct = pn->hash_agg.table->columns.items[src_col].type;
                        if (sct == COLUMN_TYPE_FLOAT || sct == COLUMN_TYPE_NUMERIC)
                            src_is_float = 1;
                    }
                    /* Use consistent type across all groups: BIGINT for int sums,
                     * FLOAT for float sums. Mixing INT/BIGINT across groups in the
                     * same col_block causes misreads. */
                    if (src_is_float) {
                        dst->type = COLUMN_TYPE_FLOAT;
                        if (st->nonnull[idx] == 0) {
                            dst->nulls[out_count] = 1;
                        } else {
                            dst->nulls[out_count] = 0;
                            dst->data.f64[out_count] = st->sums[idx];
                        }
                    } else {
                        dst->type = COLUMN_TYPE_BIGINT;
                        if (st->nonnull[idx] == 0) {
                            dst->nulls[out_count] = 1;
                        } else {
                            dst->nulls[out_count] = 0;
                            dst->data.i64[out_count] = (int64_t)st->sums[idx];
                        }
                    }
                    break;
                }
                case AGG_AVG:
                    dst->type = COLUMN_TYPE_FLOAT;
                    if (st->nonnull[idx] == 0) {
                        dst->nulls[out_count] = 1;
                    } else {
                        dst->nulls[out_count] = 0;
                        dst->data.f64[out_count] = st->sums[idx] / (double)st->nonnull[idx];
                    }
                    break;
                case AGG_MIN:
                case AGG_MAX: {
                    int src_col_mm = pn->hash_agg.agg_col_indices ? pn->hash_agg.agg_col_indices[a] : -1;
                    enum column_type src_type_mm = COLUMN_TYPE_INT;
                    if (src_col_mm >= 0 && pn->hash_agg.table)
                        src_type_mm = pn->hash_agg.table->columns.items[src_col_mm].type;
                    if (st->nonnull[idx] == 0) {
                        dst->type = src_type_mm;
                        dst->nulls[out_count] = 1;
                    } else if (column_type_is_text(src_type_mm)) {
                        dst->type = COLUMN_TYPE_TEXT;
                        dst->nulls[out_count] = 0;
                        dst->data.str[out_count] = (char *)(ae->func == AGG_MIN
                            ? st->text_mins[idx] : st->text_maxs[idx]);
                    } else if (src_type_mm == COLUMN_TYPE_FLOAT || src_type_mm == COLUMN_TYPE_NUMERIC) {
                        double mv = ae->func == AGG_MIN ? st->mins[idx] : st->maxs[idx];
                        dst->type = COLUMN_TYPE_FLOAT;
                        dst->nulls[out_count] = 0;
                        dst->data.f64[out_count] = mv;
                    } else if (src_type_mm == COLUMN_TYPE_BIGINT ||
                               src_type_mm == COLUMN_TYPE_TIME ||
                               src_type_mm == COLUMN_TYPE_TIMESTAMP ||
                               src_type_mm == COLUMN_TYPE_TIMESTAMPTZ) {
                        int64_t mv;
                        memcpy(&mv, ae->func == AGG_MIN ? &st->mins[idx] : &st->maxs[idx], sizeof(int64_t));
                        dst->type = src_type_mm;
                        dst->nulls[out_count] = 0;
                        dst->data.i64[out_count] = mv;
                    } else if (src_type_mm == COLUMN_TYPE_DATE) {
                        double mv = ae->func == AGG_MIN ? st->mins[idx] : st->maxs[idx];
                        dst->type = COLUMN_TYPE_DATE;
                        dst->nulls[out_count] = 0;
                        dst->data.i32[out_count] = (int32_t)mv;
                    } else {
                        double mv = ae->func == AGG_MIN ? st->mins[idx] : st->maxs[idx];
                        dst->type = COLUMN_TYPE_INT;
                        dst->nulls[out_count] = 0;
                        dst->data.i32[out_count] = (int32_t)mv;
                    }
                    break;
                }
                case AGG_STRING_AGG:
                case AGG_ARRAY_AGG:
                    /* not supported in plan executor — fall back to legacy path */
                    dst->type = COLUMN_TYPE_TEXT;
                    dst->nulls[out_count] = 1;
                    break;
                case AGG_NONE:
                    break;
            }
        }

        if (pn->hash_agg.agg_before_cols) {
            uint16_t grp_offset = (uint16_t)agg_n;
            for (uint16_t k = 0; k < ngrp; k++) {
                out->cols[grp_offset + k].type = st->group_keys[k].type;
                cb_copy_value(&out->cols[grp_offset + k], out_count,
                              &st->group_keys[k], (uint16_t)g);
            }
        }

        out_count++;
    }

    out->count = out_count;
    for (uint16_t c = 0; c < out->ncols; c++)
        out->cols[c].count = out_count;

    return 0;
}

/* ---- Simple Aggregate (no GROUP BY) ---- */

static int simple_agg_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                           struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct simple_agg_state *st = (struct simple_agg_state *)ctx->node_states[node_idx];
    uint32_t agg_n = pn->simple_agg.agg_count;

    if (!st) {
        st = (struct simple_agg_state *)bump_calloc(&ctx->arena->scratch, 1, sizeof(*st));
        ctx->node_states[node_idx] = st;
        st->sums      = (double *)bump_calloc(&ctx->arena->scratch, agg_n, sizeof(double));
        st->mins      = (double *)bump_calloc(&ctx->arena->scratch, agg_n, sizeof(double));
        st->maxs      = (double *)bump_calloc(&ctx->arena->scratch, agg_n, sizeof(double));
        st->text_mins = (const char **)bump_calloc(&ctx->arena->scratch, agg_n, sizeof(const char *));
        st->text_maxs = (const char **)bump_calloc(&ctx->arena->scratch, agg_n, sizeof(const char *));
        st->nonnull   = (size_t *)bump_calloc(&ctx->arena->scratch, agg_n, sizeof(size_t));
        st->minmax_init = (int *)bump_calloc(&ctx->arena->scratch, agg_n, sizeof(int));
        /* Initialize distinct sets for COUNT(DISTINCT) aggregates */
        st->distinct_sets = (struct distinct_set *)bump_calloc(&ctx->arena->scratch, agg_n, sizeof(struct distinct_set));
        for (uint32_t a = 0; a < agg_n; a++) {
            struct agg_expr *ae = &ctx->arena->aggregates.items[pn->simple_agg.agg_start + a];
            if (ae->has_distinct)
                distinct_set_init(&st->distinct_sets[a], 64, &ctx->arena->scratch);
        }
    }

    /* Phase 1: consume all input blocks */
    if (!st->input_done) {
        uint16_t child_ncols = plan_node_ncols(ctx->arena, pn->left);
        struct row_block input;
        row_block_alloc(&input, child_ncols, &ctx->arena->scratch);

        while (plan_next_block(ctx, pn->left, &input) == 0) {
            uint16_t active = row_block_active_count(&input);
            for (uint16_t r = 0; r < active; r++) {
                uint16_t ri = row_block_row_idx(&input, r);
                st->total_rows++;

                for (uint32_t a = 0; a < agg_n; a++) {
                    int ac = pn->simple_agg.agg_col_indices[a];

                    /* FILTER (WHERE ...): evaluate per-aggregate filter condition */
                    struct agg_expr *ae_filt = &ctx->arena->aggregates.items[pn->simple_agg.agg_start + a];
                    if (ae_filt->filter_cond != IDX_NONE) {
                        if (!st->tmp_row_inited) {
                            st->tmp_row.cells.count = child_ncols;
                            st->tmp_row.cells.items = (struct cell *)bump_alloc(
                                &ctx->arena->scratch, child_ncols * sizeof(struct cell));
                            st->tmp_row_inited = 1;
                        }
                        for (uint16_t c = 0; c < child_ncols; c++) {
                            struct col_block *cb = &input.cols[c];
                            struct cell *cell = &st->tmp_row.cells.items[c];
                            cell->type = cb->type;
                            if (cb->nulls[ri]) {
                                cell->is_null = 1;
                                memset(&cell->value, 0, sizeof(cell->value));
                            } else {
                                cell->is_null = 0;
                                cb_to_cell_at(cb, ri, cell);
                            }
                        }
                        if (!eval_condition(ae_filt->filter_cond, ctx->arena,
                                            &st->tmp_row, pn->simple_agg.table, NULL))
                            continue;
                    }

                    if (ac == -1) {
                        /* COUNT(*): if FILTER is present, track filtered count in nonnull */
                        if (ae_filt->filter_cond != IDX_NONE)
                            st->nonnull[a]++;
                        continue;
                    }

                    if (ac == -2) {
                        /* Expression-based aggregate */
                        struct agg_expr *ae = &ctx->arena->aggregates.items[pn->simple_agg.agg_start + a];
                        if (!st->tmp_row_inited) {
                            st->tmp_row.cells.count = child_ncols;
                            st->tmp_row.cells.items = (struct cell *)bump_alloc(
                                &ctx->arena->scratch, child_ncols * sizeof(struct cell));
                            st->tmp_row_inited = 1;
                        }
                        for (uint16_t c = 0; c < child_ncols; c++) {
                            struct col_block *cb = &input.cols[c];
                            struct cell *cell = &st->tmp_row.cells.items[c];
                            cell->type = cb->type;
                            if (cb->nulls[ri]) {
                                cell->is_null = 1;
                                memset(&cell->value, 0, sizeof(cell->value));
                            } else {
                                cell->is_null = 0;
                                cb_to_cell_at(cb, ri, cell);
                            }
                        }
                        struct cell cv = eval_expr(ae->expr_idx, ctx->arena,
                                                   pn->simple_agg.table, &st->tmp_row,
                                                   ctx->db, &ctx->arena->scratch);
                        if (cv.is_null) continue;
                        double v = 0.0;
                        switch (cv.type) {
                            case COLUMN_TYPE_SMALLINT: v = (double)cv.value.as_smallint; break;
                            case COLUMN_TYPE_INT:      v = (double)cv.value.as_int; break;
                            case COLUMN_TYPE_BIGINT:   v = (double)cv.value.as_bigint; break;
                            case COLUMN_TYPE_FLOAT:
                            case COLUMN_TYPE_NUMERIC:  v = cv.value.as_float; break;
                            case COLUMN_TYPE_BOOLEAN:  v = (double)cv.value.as_bool; break;
                            case COLUMN_TYPE_DATE:     v = (double)cv.value.as_date; break;
                            case COLUMN_TYPE_TIME:
                            case COLUMN_TYPE_TIMESTAMP:
                            case COLUMN_TYPE_TIMESTAMPTZ:
                                v = (double)cv.value.as_timestamp; break;
                            case COLUMN_TYPE_INTERVAL:
                                v = (double)interval_to_usec_approx(cv.value.as_interval); break;
                            case COLUMN_TYPE_TEXT:
                            case COLUMN_TYPE_ENUM:
                            case COLUMN_TYPE_UUID:     break;
                        }
                        /* COUNT(DISTINCT expr): hash the result and deduplicate */
                        if (st->distinct_sets[a].cap > 0) {
                            uint64_t dh = 14695981039346656037ULL;
                            uint64_t bits;
                            memcpy(&bits, &v, sizeof(bits));
                            dh ^= bits;
                            dh *= 1099511628211ULL;
                            if (!distinct_set_insert(&st->distinct_sets[a], dh, &ctx->arena->scratch))
                                continue; /* duplicate — skip */
                        }
                        st->nonnull[a]++;
                        st->sums[a] += v;
                        if (!st->minmax_init[a] || v < st->mins[a]) st->mins[a] = v;
                        if (!st->minmax_init[a] || v > st->maxs[a]) st->maxs[a] = v;
                        st->minmax_init[a] = 1;
                        continue;
                    }

                    struct col_block *acb = &input.cols[ac];
                    if (acb->nulls[ri]) continue;
                    /* COUNT(DISTINCT): only count if value is new */
                    if (st->distinct_sets[a].cap > 0) {
                        uint64_t dh = distinct_hash_cell(acb, ri);
                        if (!distinct_set_insert(&st->distinct_sets[a], dh, &ctx->arena->scratch))
                            continue; /* duplicate — skip */
                    }
                    st->nonnull[a]++;
                    if (column_type_is_text(acb->type)) {
                        const char *s = acb->data.str[ri];
                        if (s) {
                            if (!st->minmax_init[a] || strcmp(s, st->text_mins[a]) < 0)
                                st->text_mins[a] = s;
                            if (!st->minmax_init[a] || strcmp(s, st->text_maxs[a]) > 0)
                                st->text_maxs[a] = s;
                            st->minmax_init[a] = 1;
                        }
                    } else if (acb->type == COLUMN_TYPE_DATE) {
                        int32_t v = acb->data.i32[ri];
                        st->sums[a] += (double)v;
                        if (!st->minmax_init[a] || v < (int32_t)st->mins[a]) st->mins[a] = (double)v;
                        if (!st->minmax_init[a] || v > (int32_t)st->maxs[a]) st->maxs[a] = (double)v;
                        st->minmax_init[a] = 1;
                    } else if (acb->type == COLUMN_TYPE_TIME ||
                               acb->type == COLUMN_TYPE_TIMESTAMP ||
                               acb->type == COLUMN_TYPE_TIMESTAMPTZ) {
                        int64_t v = acb->data.i64[ri];
                        st->nonnull[a]--; /* undo the increment above; we handle nonnull here */
                        st->nonnull[a]++;
                        /* Store as int64 bits in the double slot */
                        int64_t cur_min, cur_max;
                        memcpy(&cur_min, &st->mins[a], sizeof(int64_t));
                        memcpy(&cur_max, &st->maxs[a], sizeof(int64_t));
                        if (!st->minmax_init[a] || v < cur_min) memcpy(&st->mins[a], &v, sizeof(double));
                        if (!st->minmax_init[a] || v > cur_max) memcpy(&st->maxs[a], &v, sizeof(double));
                        st->minmax_init[a] = 1;
                    } else if (acb->type == COLUMN_TYPE_INTERVAL) {
                        int64_t v = interval_to_usec_approx(acb->data.iv[ri]);
                        double dv = (double)v;
                        st->sums[a] += dv;
                        if (!st->minmax_init[a] || dv < st->mins[a]) st->mins[a] = dv;
                        if (!st->minmax_init[a] || dv > st->maxs[a]) st->maxs[a] = dv;
                        st->minmax_init[a] = 1;
                    } else {
                        double v = cb_to_double(acb, ri);
                        st->sums[a] += v;
                        if (!st->minmax_init[a] || v < st->mins[a]) st->mins[a] = v;
                        if (!st->minmax_init[a] || v > st->maxs[a]) st->maxs[a] = v;
                        st->minmax_init[a] = 1;
                    }
                }
            }
            row_block_reset(&input);
        }
        st->input_done = 1;
    }

    /* Phase 2: emit exactly one row */
    if (st->emit_done) return -1;
    st->emit_done = 1;

    row_block_reset(out);
    for (uint32_t a = 0; a < agg_n; a++) {
        struct agg_expr *ae = &ctx->arena->aggregates.items[pn->simple_agg.agg_start + a];
        struct col_block *dst = &out->cols[a];

        switch (ae->func) {
            case AGG_COUNT:
                dst->type = COLUMN_TYPE_INT;
                dst->nulls[0] = 0;
                if (pn->simple_agg.agg_col_indices[a] == -1 &&
                    ae->filter_cond == IDX_NONE)
                    dst->data.i32[0] = (int32_t)st->total_rows;
                else
                    dst->data.i32[0] = (int32_t)st->nonnull[a];
                break;
            case AGG_SUM: {
                int src_col = pn->simple_agg.agg_col_indices[a];
                int src_is_float = 0;
                if (src_col >= 0 && pn->simple_agg.table) {
                    enum column_type sct = pn->simple_agg.table->columns.items[src_col].type;
                    if (sct == COLUMN_TYPE_FLOAT || sct == COLUMN_TYPE_NUMERIC)
                        src_is_float = 1;
                }
                if (st->nonnull[a] == 0) {
                    dst->type = src_is_float ? COLUMN_TYPE_FLOAT : COLUMN_TYPE_BIGINT;
                    dst->nulls[0] = 1;
                } else if (src_is_float) {
                    dst->type = COLUMN_TYPE_FLOAT;
                    dst->nulls[0] = 0;
                    dst->data.f64[0] = st->sums[a];
                } else {
                    double sv = st->sums[a];
                    if (sv == (double)(int32_t)sv && sv >= -2147483648.0 && sv <= 2147483647.0) {
                        dst->type = COLUMN_TYPE_INT;
                        dst->nulls[0] = 0;
                        dst->data.i32[0] = (int32_t)sv;
                    } else {
                        dst->type = COLUMN_TYPE_BIGINT;
                        dst->nulls[0] = 0;
                        dst->data.i64[0] = (int64_t)sv;
                    }
                }
                break;
            }
            case AGG_AVG:
                dst->type = COLUMN_TYPE_FLOAT;
                if (st->nonnull[a] == 0) {
                    dst->nulls[0] = 1;
                } else {
                    dst->nulls[0] = 0;
                    dst->data.f64[0] = st->sums[a] / (double)st->nonnull[a];
                }
                break;
            case AGG_MIN:
            case AGG_MAX: {
                int src_col_mm = pn->simple_agg.agg_col_indices[a];
                enum column_type src_type_mm = COLUMN_TYPE_INT;
                if (src_col_mm >= 0 && pn->simple_agg.table)
                    src_type_mm = pn->simple_agg.table->columns.items[src_col_mm].type;
                if (st->nonnull[a] == 0) {
                    dst->type = src_type_mm;
                    dst->nulls[0] = 1;
                } else if (column_type_is_text(src_type_mm)) {
                    dst->type = COLUMN_TYPE_TEXT;
                    dst->nulls[0] = 0;
                    dst->data.str[0] = (char *)(ae->func == AGG_MIN
                        ? st->text_mins[a] : st->text_maxs[a]);
                } else if (src_type_mm == COLUMN_TYPE_FLOAT || src_type_mm == COLUMN_TYPE_NUMERIC) {
                    double mv = ae->func == AGG_MIN ? st->mins[a] : st->maxs[a];
                    dst->type = COLUMN_TYPE_FLOAT;
                    dst->nulls[0] = 0;
                    dst->data.f64[0] = mv;
                } else if (src_type_mm == COLUMN_TYPE_BIGINT ||
                           src_type_mm == COLUMN_TYPE_TIME ||
                           src_type_mm == COLUMN_TYPE_TIMESTAMP ||
                           src_type_mm == COLUMN_TYPE_TIMESTAMPTZ) {
                    int64_t mv;
                    memcpy(&mv, ae->func == AGG_MIN ? &st->mins[a] : &st->maxs[a], sizeof(int64_t));
                    dst->type = src_type_mm;
                    dst->nulls[0] = 0;
                    dst->data.i64[0] = mv;
                } else if (src_type_mm == COLUMN_TYPE_DATE) {
                    double mv = ae->func == AGG_MIN ? st->mins[a] : st->maxs[a];
                    dst->type = COLUMN_TYPE_DATE;
                    dst->nulls[0] = 0;
                    dst->data.i32[0] = (int32_t)mv;
                } else {
                    double mv = ae->func == AGG_MIN ? st->mins[a] : st->maxs[a];
                    dst->type = COLUMN_TYPE_INT;
                    dst->nulls[0] = 0;
                    dst->data.i32[0] = (int32_t)mv;
                }
                break;
            }
            case AGG_STRING_AGG:
            case AGG_ARRAY_AGG:
                dst->type = COLUMN_TYPE_TEXT;
                dst->nulls[0] = 1;
                break;
            case AGG_NONE:
                break;
        }
    }

    out->count = 1;
    for (uint16_t c = 0; c < out->ncols; c++)
        out->cols[c].count = 1;
    return 0;
}

/* ---- Sort ---- */

struct block_sort_ctx {
    struct col_block *all_cols;
    uint16_t          ncols;
    uint32_t          rows_per_block;
    int              *sort_cols;
    int              *sort_descs;
    int              *sort_nulls_first; /* per-key: -1=default, 0=NULLS LAST, 1=NULLS FIRST */
    uint16_t          nsort_cols;
    /* Flat arrays for fast comparator (one per sort key, bump-allocated) */
    void            **flat_keys;       /* [nsort_cols] contiguous typed array per sort key */
    uint8_t         **flat_nulls;      /* [nsort_cols] contiguous null bitmap per sort key */
    enum column_type *key_types;       /* [nsort_cols] */
    /* Flat arrays for ALL columns — used by emit phase to avoid block remap */
    void            **flat_col_data;   /* [ncols] contiguous typed arrays */
    uint8_t         **flat_col_nulls;  /* [ncols] contiguous null bitmaps */
    enum column_type *flat_col_types;  /* [ncols] column types */
};
static struct block_sort_ctx _bsort_ctx;

/* Fast comparator using flattened contiguous arrays — no block index math. */
static int sort_flat_cmp(const void *a, const void *b)
{
    uint32_t ia = *(const uint32_t *)a;
    uint32_t ib = *(const uint32_t *)b;

    for (uint16_t k = 0; k < _bsort_ctx.nsort_cols; k++) {
        uint8_t na = _bsort_ctx.flat_nulls[k][ia];
        uint8_t nb = _bsort_ctx.flat_nulls[k][ib];
        if (na && nb) { continue; }
        if (na || nb) {
            int nf = _bsort_ctx.sort_nulls_first ? _bsort_ctx.sort_nulls_first[k] : -1;
            int nulls_go_first = (nf == 1) || (nf == -1 && _bsort_ctx.sort_descs[k]);
            if (na) return nulls_go_first ? -1 : 1;
            else    return nulls_go_first ? 1 : -1;
        }

        int cmp = 0;
        enum column_type kt = _bsort_ctx.key_types[k];
        if (kt == COLUMN_TYPE_SMALLINT) {
            int16_t va = ((const int16_t *)_bsort_ctx.flat_keys[k])[ia];
            int16_t vb = ((const int16_t *)_bsort_ctx.flat_keys[k])[ib];
            cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
        } else if (kt == COLUMN_TYPE_INT || kt == COLUMN_TYPE_BOOLEAN) {
            int32_t va = ((const int32_t *)_bsort_ctx.flat_keys[k])[ia];
            int32_t vb = ((const int32_t *)_bsort_ctx.flat_keys[k])[ib];
            cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
        } else if (kt == COLUMN_TYPE_BIGINT ||
                   kt == COLUMN_TYPE_TIME ||
                   kt == COLUMN_TYPE_TIMESTAMP ||
                   kt == COLUMN_TYPE_TIMESTAMPTZ) {
            int64_t va = ((const int64_t *)_bsort_ctx.flat_keys[k])[ia];
            int64_t vb = ((const int64_t *)_bsort_ctx.flat_keys[k])[ib];
            cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
        } else if (kt == COLUMN_TYPE_FLOAT || kt == COLUMN_TYPE_NUMERIC) {
            double va = ((const double *)_bsort_ctx.flat_keys[k])[ia];
            double vb = ((const double *)_bsort_ctx.flat_keys[k])[ib];
            cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
        } else if (kt == COLUMN_TYPE_DATE) {
            int32_t va = ((const int32_t *)_bsort_ctx.flat_keys[k])[ia];
            int32_t vb = ((const int32_t *)_bsort_ctx.flat_keys[k])[ib];
            cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
        } else if (kt == COLUMN_TYPE_INTERVAL) {
            int64_t va = interval_to_usec_approx(((const struct interval *)_bsort_ctx.flat_keys[k])[ia]);
            int64_t vb = interval_to_usec_approx(((const struct interval *)_bsort_ctx.flat_keys[k])[ib]);
            cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
        } else {
            const char *sa = ((const char **)_bsort_ctx.flat_keys[k])[ia];
            const char *sb = ((const char **)_bsort_ctx.flat_keys[k])[ib];
            if (!sa && !sb) { continue; }
            if (!sa) cmp = -1;
            else if (!sb) cmp = 1;
            else cmp = strcmp(sa, sb);
        }
        if (_bsort_ctx.sort_descs[k]) cmp = -cmp;
        if (cmp != 0) return cmp;
    }
    return 0;
}

static int sort_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                     struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct sort_state *st = (struct sort_state *)ctx->node_states[node_idx];
    if (!st) {
        st = (struct sort_state *)bump_calloc(&ctx->arena->scratch, 1, sizeof(*st));
        st->block_cap = 16;
        st->collected = (struct row_block *)bump_calloc(&ctx->arena->scratch,
                                                         st->block_cap, sizeof(struct row_block));
        ctx->node_states[node_idx] = st;
    }

    if (!st->input_done) {
        uint16_t child_ncols = plan_node_ncols(ctx->arena, pn->left);
        if (child_ncols == 0) child_ncols = out->ncols;

        for (;;) {
            if (st->nblocks >= st->block_cap) {
                uint32_t new_cap = st->block_cap * 2;
                struct row_block *new_arr = (struct row_block *)bump_calloc(
                    &ctx->arena->scratch, new_cap, sizeof(struct row_block));
                memcpy(new_arr, st->collected, st->nblocks * sizeof(struct row_block));
                st->collected = new_arr;
                st->block_cap = new_cap;
            }

            struct row_block *blk = &st->collected[st->nblocks];
            row_block_alloc(blk, child_ncols, &ctx->arena->scratch);
            int rc = plan_next_block(ctx, pn->left, blk);
            if (rc != 0) break;

            if (blk->sel) {
                uint16_t active = blk->sel_count;
                struct row_block compact;
                row_block_alloc(&compact, child_ncols, &ctx->arena->scratch);
                compact.count = active;
                for (uint16_t c = 0; c < child_ncols; c++) {
                    compact.cols[c].type = blk->cols[c].type;
                    compact.cols[c].count = active;
                    for (uint16_t i = 0; i < active; i++) {
                        uint16_t ri = (uint16_t)blk->sel[i];
                        cb_copy_value(&compact.cols[c], i, &blk->cols[c], ri);
                    }
                }
                *blk = compact;
            }

            st->nblocks++;
        }

        uint32_t total = 0;
        for (uint32_t b = 0; b < st->nblocks; b++)
            total += st->collected[b].count;

        st->sorted_count = total;
        st->sorted_indices = (uint32_t *)bump_alloc(&ctx->arena->scratch,
                                                     (total ? total : 1) * sizeof(uint32_t));

        uint32_t idx = 0;
        for (uint32_t b = 0; b < st->nblocks; b++)
            for (uint16_t r = 0; r < st->collected[b].count; r++)
                st->sorted_indices[idx++] = b * BLOCK_CAPACITY + r;

        _bsort_ctx.ncols = child_ncols;
        _bsort_ctx.rows_per_block = BLOCK_CAPACITY;
        _bsort_ctx.sort_cols = pn->sort.sort_cols;
        _bsort_ctx.sort_descs = pn->sort.sort_descs;
        _bsort_ctx.sort_nulls_first = pn->sort.sort_nulls_first;
        _bsort_ctx.nsort_cols = pn->sort.nsort_cols;

        _bsort_ctx.all_cols = (struct col_block *)bump_alloc(&ctx->arena->scratch,
                                (st->nblocks ? st->nblocks : 1) * child_ncols * sizeof(struct col_block));
        for (uint32_t b = 0; b < st->nblocks; b++)
            memcpy(&_bsort_ctx.all_cols[b * child_ncols],
                   st->collected[b].cols,
                   child_ncols * sizeof(struct col_block));

        /* Build flat arrays for ALL columns — used by both sort comparator and emit */
        _bsort_ctx.flat_col_data = (void **)bump_alloc(&ctx->arena->scratch,
                                                        child_ncols * sizeof(void *));
        _bsort_ctx.flat_col_nulls = (uint8_t **)bump_alloc(&ctx->arena->scratch,
                                                            child_ncols * sizeof(uint8_t *));
        _bsort_ctx.flat_col_types = (enum column_type *)bump_alloc(&ctx->arena->scratch,
                                                                    child_ncols * sizeof(enum column_type));
        for (uint16_t ci = 0; ci < child_ncols; ci++) {
            enum column_type kt = COLUMN_TYPE_INT;
            if (st->nblocks > 0)
                kt = st->collected[0].cols[ci].type;
            _bsort_ctx.flat_col_types[ci] = kt;

            size_t elem_sz = col_type_elem_size(kt);

            _bsort_ctx.flat_col_data[ci] = bump_alloc(&ctx->arena->scratch,
                                                       (total ? total : 1) * elem_sz);
            _bsort_ctx.flat_col_nulls[ci] = (uint8_t *)bump_alloc(&ctx->arena->scratch,
                                                                   (total ? total : 1));

            uint32_t fi = 0;
            for (uint32_t b = 0; b < st->nblocks; b++) {
                struct col_block *src = &_bsort_ctx.all_cols[b * child_ncols + ci];
                uint16_t cnt = st->collected[b].count;
                memcpy(_bsort_ctx.flat_col_nulls[ci] + fi, src->nulls, cnt);
                memcpy((uint8_t *)_bsort_ctx.flat_col_data[ci] + fi * elem_sz,
                       cb_data_ptr(src, 0), cnt * elem_sz);
                fi += cnt;
            }
        }

        /* Point sort key flat arrays into the all-column flat arrays */
        uint16_t nsk = pn->sort.nsort_cols;
        _bsort_ctx.flat_keys = (void **)bump_alloc(&ctx->arena->scratch,
                                                     nsk * sizeof(void *));
        _bsort_ctx.flat_nulls = (uint8_t **)bump_alloc(&ctx->arena->scratch,
                                                         nsk * sizeof(uint8_t *));
        _bsort_ctx.key_types = (enum column_type *)bump_alloc(&ctx->arena->scratch,
                                                               nsk * sizeof(enum column_type));
        for (uint16_t k = 0; k < nsk; k++) {
            int sci = pn->sort.sort_cols[k];
            _bsort_ctx.flat_keys[k] = _bsort_ctx.flat_col_data[sci];
            _bsort_ctx.flat_nulls[k] = _bsort_ctx.flat_col_nulls[sci];
            _bsort_ctx.key_types[k] = _bsort_ctx.flat_col_types[sci];
        }

        /* With flat arrays, sorted_indices are simple 0..total-1 indices */
        for (uint32_t i = 0; i < total; i++)
            st->sorted_indices[i] = i;

        if (total > 1)
            qsort(st->sorted_indices, total, sizeof(uint32_t), sort_flat_cmp);

        st->input_done = 1;
        st->emit_cursor = 0;
    }

    if (st->emit_cursor >= st->sorted_count) return -1;

    uint16_t child_ncols = _bsort_ctx.ncols;
    row_block_reset(out);

    uint32_t remain = st->sorted_count - st->emit_cursor;
    uint16_t out_count = (remain < BLOCK_CAPACITY) ? (uint16_t)remain : BLOCK_CAPACITY;
    const uint32_t *idx = st->sorted_indices + st->emit_cursor;

    /* Column-oriented gather emit — switch on type once per column,
     * tight typed loop for the block. */
    for (uint16_t c = 0; c < child_ncols; c++) {
        struct col_block *ocb = &out->cols[c];
        enum column_type ct = _bsort_ctx.flat_col_types[c];
        const uint8_t *src_nulls = _bsort_ctx.flat_col_nulls[c];
        const void *src_data = _bsort_ctx.flat_col_data[c];
        ocb->type = ct;
        ocb->count = out_count;

        for (uint16_t r = 0; r < out_count; r++)
            ocb->nulls[r] = src_nulls[idx[r]];

        switch (ct) {
        case COLUMN_TYPE_SMALLINT: {
            const int16_t *s = (const int16_t *)src_data;
            for (uint16_t r = 0; r < out_count; r++)
                ocb->data.i16[r] = s[idx[r]];
            break;
        }
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:
        case COLUMN_TYPE_DATE: {
            const int32_t *s = (const int32_t *)src_data;
            for (uint16_t r = 0; r < out_count; r++)
                ocb->data.i32[r] = s[idx[r]];
            break;
        }
        case COLUMN_TYPE_BIGINT:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ: {
            const int64_t *s = (const int64_t *)src_data;
            for (uint16_t r = 0; r < out_count; r++)
                ocb->data.i64[r] = s[idx[r]];
            break;
        }
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC: {
            const double *s = (const double *)src_data;
            for (uint16_t r = 0; r < out_count; r++)
                ocb->data.f64[r] = s[idx[r]];
            break;
        }
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_UUID: {
            char *const *s = (char *const *)src_data;
            for (uint16_t r = 0; r < out_count; r++)
                ocb->data.str[r] = s[idx[r]];
            break;
        }
        case COLUMN_TYPE_INTERVAL: {
            const struct interval *s = (const struct interval *)src_data;
            for (uint16_t r = 0; r < out_count; r++)
                ocb->data.iv[r] = s[idx[r]];
            break;
        }
        }
    }

    st->emit_cursor += out_count;
    out->count = out_count;

    return 0;
}

/* ---- Window function executor ---- */

/* qsort comparator for window: sort by (partition_col, order_col) in flat arrays */
static struct {
    void    *part_data;
    uint8_t *part_nulls;
    enum column_type part_type;
    void    *ord_data;
    uint8_t *ord_nulls;
    enum column_type ord_type;
    int      ord_desc;
    int      has_part;
    int      has_ord;
} _wsort_ctx;

static int window_sort_cmp(const void *a, const void *b)
{
    uint32_t ia = *(const uint32_t *)a;
    uint32_t ib = *(const uint32_t *)b;

    if (_wsort_ctx.has_part) {
        int an = _wsort_ctx.part_nulls[ia], bn = _wsort_ctx.part_nulls[ib];
        if (an != bn) return an ? 1 : -1; /* NULLs last */
        if (!an) {
            int cmp = 0;
            enum column_type pt = _wsort_ctx.part_type;
            if (pt == COLUMN_TYPE_SMALLINT) {
                int16_t va = ((int16_t *)_wsort_ctx.part_data)[ia];
                int16_t vb = ((int16_t *)_wsort_ctx.part_data)[ib];
                cmp = (va > vb) - (va < vb);
            } else if (pt == COLUMN_TYPE_INT || pt == COLUMN_TYPE_BOOLEAN) {
                int32_t va = ((int32_t *)_wsort_ctx.part_data)[ia];
                int32_t vb = ((int32_t *)_wsort_ctx.part_data)[ib];
                cmp = (va > vb) - (va < vb);
            } else if (pt == COLUMN_TYPE_BIGINT) {
                int64_t va = ((int64_t *)_wsort_ctx.part_data)[ia];
                int64_t vb = ((int64_t *)_wsort_ctx.part_data)[ib];
                cmp = (va > vb) - (va < vb);
            } else if (pt == COLUMN_TYPE_FLOAT || pt == COLUMN_TYPE_NUMERIC) {
                double va = ((double *)_wsort_ctx.part_data)[ia];
                double vb = ((double *)_wsort_ctx.part_data)[ib];
                cmp = (va > vb) - (va < vb);
            } else {
                const char *sa = ((char **)_wsort_ctx.part_data)[ia];
                const char *sb = ((char **)_wsort_ctx.part_data)[ib];
                if (sa && sb) cmp = strcmp(sa, sb);
                else cmp = (sa ? 1 : 0) - (sb ? 1 : 0);
            }
            if (cmp != 0) return cmp;
        }
    }

    if (_wsort_ctx.has_ord) {
        int an = _wsort_ctx.ord_nulls[ia], bn = _wsort_ctx.ord_nulls[ib];
        if (an != bn) return an ? 1 : -1;
        if (!an) {
            int cmp = 0;
            enum column_type ot = _wsort_ctx.ord_type;
            if (ot == COLUMN_TYPE_SMALLINT) {
                int16_t va = ((int16_t *)_wsort_ctx.ord_data)[ia];
                int16_t vb = ((int16_t *)_wsort_ctx.ord_data)[ib];
                cmp = (va > vb) - (va < vb);
            } else if (ot == COLUMN_TYPE_INT || ot == COLUMN_TYPE_BOOLEAN) {
                int32_t va = ((int32_t *)_wsort_ctx.ord_data)[ia];
                int32_t vb = ((int32_t *)_wsort_ctx.ord_data)[ib];
                cmp = (va > vb) - (va < vb);
            } else if (ot == COLUMN_TYPE_BIGINT ||
                       ot == COLUMN_TYPE_TIME ||
                       ot == COLUMN_TYPE_TIMESTAMP ||
                       ot == COLUMN_TYPE_TIMESTAMPTZ) {
                int64_t va = ((int64_t *)_wsort_ctx.ord_data)[ia];
                int64_t vb = ((int64_t *)_wsort_ctx.ord_data)[ib];
                cmp = (va > vb) - (va < vb);
            } else if (ot == COLUMN_TYPE_FLOAT || ot == COLUMN_TYPE_NUMERIC) {
                double va = ((double *)_wsort_ctx.ord_data)[ia];
                double vb = ((double *)_wsort_ctx.ord_data)[ib];
                cmp = (va > vb) - (va < vb);
            } else if (ot == COLUMN_TYPE_DATE) {
                int32_t va = ((int32_t *)_wsort_ctx.ord_data)[ia];
                int32_t vb = ((int32_t *)_wsort_ctx.ord_data)[ib];
                cmp = (va > vb) - (va < vb);
            } else if (ot == COLUMN_TYPE_INTERVAL) {
                int64_t va = interval_to_usec_approx(((struct interval *)_wsort_ctx.ord_data)[ia]);
                int64_t vb = interval_to_usec_approx(((struct interval *)_wsort_ctx.ord_data)[ib]);
                cmp = (va > vb) - (va < vb);
            } else {
                const char *sa = ((char **)_wsort_ctx.ord_data)[ia];
                const char *sb = ((char **)_wsort_ctx.ord_data)[ib];
                if (sa && sb) cmp = strcmp(sa, sb);
                else cmp = (sa ? 1 : 0) - (sb ? 1 : 0);
            }
            if (_wsort_ctx.ord_desc) cmp = -cmp;
            if (cmp != 0) return cmp;
        }
    }

    return 0;
}

static inline double flat_col_to_double(void *data, uint8_t *nulls, enum column_type ct, uint32_t idx)
{
    if (nulls[idx]) return 0.0;
    if (ct == COLUMN_TYPE_SMALLINT) return (double)((int16_t *)data)[idx];
    if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN) return (double)((int32_t *)data)[idx];
    if (ct == COLUMN_TYPE_BIGINT) return (double)((int64_t *)data)[idx];
    if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC) return ((double *)data)[idx];
    return 0.0;
}

static inline int flat_col_ord_cmp(void *data, enum column_type ct, uint8_t *nulls, uint32_t a, uint32_t b)
{
    int an = nulls[a], bn = nulls[b];
    if (an && bn) return 0;
    if (an) return 1;
    if (bn) return -1;
    if (ct == COLUMN_TYPE_SMALLINT) {
        int16_t va = ((int16_t *)data)[a], vb = ((int16_t *)data)[b];
        return (va > vb) - (va < vb);
    }
    if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN) {
        int32_t va = ((int32_t *)data)[a], vb = ((int32_t *)data)[b];
        return (va > vb) - (va < vb);
    }
    if (ct == COLUMN_TYPE_BIGINT ||
        ct == COLUMN_TYPE_TIME ||
        ct == COLUMN_TYPE_TIMESTAMP ||
        ct == COLUMN_TYPE_TIMESTAMPTZ) {
        int64_t va = ((int64_t *)data)[a], vb = ((int64_t *)data)[b];
        return (va > vb) - (va < vb);
    }
    if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC) {
        double va = ((double *)data)[a], vb = ((double *)data)[b];
        return (va > vb) - (va < vb);
    }
    if (ct == COLUMN_TYPE_DATE) {
        int32_t va = ((int32_t *)data)[a], vb = ((int32_t *)data)[b];
        return (va > vb) - (va < vb);
    }
    if (ct == COLUMN_TYPE_INTERVAL) {
        int64_t va = interval_to_usec_approx(((struct interval *)data)[a]);
        int64_t vb = interval_to_usec_approx(((struct interval *)data)[b]);
        return (va > vb) - (va < vb);
    }
    /* TEXT types */
    {
        const char *sa = ((char **)data)[a];
        const char *sb = ((char **)data)[b];
        if (sa && sb) return strcmp(sa, sb);
        return (sa ? 1 : 0) - (sb ? 1 : 0);
    }
}

static int window_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                       struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct window_state *st = (struct window_state *)ctx->node_states[node_idx];
    if (!st) {
        st = (struct window_state *)bump_calloc(&ctx->arena->scratch, 1, sizeof(*st));
        ctx->node_states[node_idx] = st;
    }

    if (!st->input_done) {
        uint16_t child_ncols = plan_node_ncols(ctx->arena, pn->left);
        st->input_ncols = child_ncols;

        /* Collect all input blocks into flat arrays */
        struct row_block *collected = NULL;
        uint32_t nblocks = 0, block_cap = 16;
        collected = (struct row_block *)bump_calloc(&ctx->arena->scratch, block_cap, sizeof(struct row_block));

        for (;;) {
            if (nblocks >= block_cap) {
                uint32_t new_cap = block_cap * 2;
                struct row_block *na = (struct row_block *)bump_calloc(&ctx->arena->scratch, new_cap, sizeof(struct row_block));
                memcpy(na, collected, nblocks * sizeof(struct row_block));
                collected = na;
                block_cap = new_cap;
            }
            struct row_block *blk = &collected[nblocks];
            row_block_alloc(blk, child_ncols, &ctx->arena->scratch);
            if (plan_next_block(ctx, pn->left, blk) != 0) break;
            if (blk->sel) {
                uint16_t active = blk->sel_count;
                struct row_block compact;
                row_block_alloc(&compact, child_ncols, &ctx->arena->scratch);
                compact.count = active;
                for (uint16_t c = 0; c < child_ncols; c++) {
                    compact.cols[c].type = blk->cols[c].type;
                    compact.cols[c].count = active;
                    for (uint16_t i = 0; i < active; i++)
                        cb_copy_value(&compact.cols[c], i, &blk->cols[c], (uint16_t)blk->sel[i]);
                }
                *blk = compact;
            }
            nblocks++;
        }

        uint32_t total = 0;
        for (uint32_t b = 0; b < nblocks; b++) total += collected[b].count;
        st->total_rows = total;

        if (total == 0) { st->input_done = 1; return -1; }

        /* Build flat columnar arrays for all input columns */
        st->flat_data = (void **)bump_alloc(&ctx->arena->scratch, child_ncols * sizeof(void *));
        st->flat_nulls = (uint8_t **)bump_alloc(&ctx->arena->scratch, child_ncols * sizeof(uint8_t *));
        st->flat_types = (enum column_type *)bump_alloc(&ctx->arena->scratch, child_ncols * sizeof(enum column_type));

        for (uint16_t ci = 0; ci < child_ncols; ci++) {
            enum column_type kt = nblocks > 0 ? collected[0].cols[ci].type : COLUMN_TYPE_INT;
            st->flat_types[ci] = kt;
            size_t esz = col_type_elem_size(kt);

            st->flat_data[ci] = bump_alloc(&ctx->arena->scratch, total * esz);
            st->flat_nulls[ci] = (uint8_t *)bump_alloc(&ctx->arena->scratch, total);

            uint32_t fi = 0;
            for (uint32_t b = 0; b < nblocks; b++) {
                struct col_block *src = &collected[b].cols[ci];
                uint16_t cnt = collected[b].count;
                memcpy(st->flat_nulls[ci] + fi, src->nulls, cnt);
                memcpy((uint8_t *)st->flat_data[ci] + fi * esz,
                       cb_data_ptr(src, 0), cnt * esz);
                fi += cnt;
            }
        }

        /* Build sorted index */
        st->sorted = (uint32_t *)bump_alloc(&ctx->arena->scratch, total * sizeof(uint32_t));
        for (uint32_t i = 0; i < total; i++) st->sorted[i] = i;

        int spc = pn->window.sort_part_col;
        int soc = pn->window.sort_ord_col;
        _wsort_ctx.has_part = (spc >= 0);
        _wsort_ctx.has_ord = (soc >= 0);
        if (spc >= 0) {
            _wsort_ctx.part_data = st->flat_data[spc];
            _wsort_ctx.part_nulls = st->flat_nulls[spc];
            _wsort_ctx.part_type = st->flat_types[spc];
        }
        if (soc >= 0) {
            _wsort_ctx.ord_data = st->flat_data[soc];
            _wsort_ctx.ord_nulls = st->flat_nulls[soc];
            _wsort_ctx.ord_type = st->flat_types[soc];
            _wsort_ctx.ord_desc = pn->window.sort_ord_desc;
        }
        if (_wsort_ctx.has_part || _wsort_ctx.has_ord)
            qsort(st->sorted, total, sizeof(uint32_t), window_sort_cmp);

        /* Build partition boundaries */
        st->part_starts = (uint32_t *)bump_alloc(&ctx->arena->scratch, (total + 1) * sizeof(uint32_t));
        st->nparts = 0;
        st->part_starts[st->nparts++] = 0;
        if (spc >= 0) {
            for (uint32_t i = 1; i < total; i++) {
                uint32_t a = st->sorted[i - 1], b = st->sorted[i];
                int an = st->flat_nulls[spc][a], bn = st->flat_nulls[spc][b];
                if (an != bn) { st->part_starts[st->nparts++] = i; continue; }
                if (an) continue; /* both NULL — same partition */
                if (flat_col_ord_cmp(st->flat_data[spc], st->flat_types[spc], st->flat_nulls[spc], a, b) != 0)
                    st->part_starts[st->nparts++] = i;
            }
        }
        st->part_starts[st->nparts] = total;

        /* Compute window values */
        uint16_t nw = pn->window.n_win;
        st->win_i32 = (int32_t *)bump_calloc(&ctx->arena->scratch, nw * total, sizeof(int32_t));
        st->win_f64 = (double *)bump_calloc(&ctx->arena->scratch, nw * total, sizeof(double));
        st->win_null = (uint8_t *)bump_calloc(&ctx->arena->scratch, nw * total, sizeof(uint8_t));
        st->win_is_dbl = (int *)bump_calloc(&ctx->arena->scratch, nw, sizeof(int));
        st->win_str = (char **)bump_calloc(&ctx->arena->scratch, nw * total, sizeof(char *));
        st->win_is_str = (int *)bump_calloc(&ctx->arena->scratch, nw, sizeof(int));

        for (uint16_t w = 0; w < nw; w++) {
            int wf = pn->window.win_func[w];
            int oc = pn->window.win_ord_col[w];
            int ac = pn->window.win_arg_col[w];

            /* Per-expression partition boundaries: if this expression's partition
             * column differs from the global sort partition, re-sort st->sorted
             * by this expression's partition column and rebuild boundaries. */
            uint32_t *w_part_starts = st->part_starts;
            uint32_t w_nparts = st->nparts;
            int wpc = pn->window.win_part_col[w];
            if (wpc >= 0 && wpc != spc) {
                /* Re-sort by this expression's partition column */
                _wsort_ctx.has_part = 1;
                _wsort_ctx.part_data = st->flat_data[wpc];
                _wsort_ctx.part_nulls = st->flat_nulls[wpc];
                _wsort_ctx.part_type = st->flat_types[wpc];
                _wsort_ctx.has_ord = (oc >= 0);
                if (oc >= 0) {
                    _wsort_ctx.ord_data = st->flat_data[oc];
                    _wsort_ctx.ord_nulls = st->flat_nulls[oc];
                    _wsort_ctx.ord_type = st->flat_types[oc];
                    _wsort_ctx.ord_desc = pn->window.sort_ord_desc;
                }
                qsort(st->sorted, total, sizeof(uint32_t), window_sort_cmp);

                w_part_starts = (uint32_t *)bump_alloc(&ctx->arena->scratch, (total + 1) * sizeof(uint32_t));
                w_nparts = 0;
                w_part_starts[w_nparts++] = 0;
                for (uint32_t i = 1; i < total; i++) {
                    uint32_t a = st->sorted[i - 1], b = st->sorted[i];
                    int an = st->flat_nulls[wpc][a], bn = st->flat_nulls[wpc][b];
                    if (an != bn) { w_part_starts[w_nparts++] = i; continue; }
                    if (an) continue;
                    if (flat_col_ord_cmp(st->flat_data[wpc], st->flat_types[wpc], st->flat_nulls[wpc], a, b) != 0)
                        w_part_starts[w_nparts++] = i;
                }
                w_part_starts[w_nparts] = total;
            } else if (wpc < 0 && spc >= 0) {
                /* No partition for this expr but global has one — use single partition */
                w_part_starts = (uint32_t *)bump_alloc(&ctx->arena->scratch, 2 * sizeof(uint32_t));
                w_part_starts[0] = 0;
                w_part_starts[1] = total;
                w_nparts = 1;
            }

            for (uint32_t p = 0; p < w_nparts; p++) {
                uint32_t ps = w_part_starts[p];
                uint32_t pe = w_part_starts[p + 1];
                uint32_t psize = pe - ps;

                switch (wf) {
                case WIN_ROW_NUMBER:
                    for (uint32_t i = ps; i < pe; i++)
                        st->win_i32[st->sorted[i] * nw + w] = (int32_t)(i - ps + 1);
                    break;
                case WIN_RANK: {
                    int32_t rank = 1;
                    for (uint32_t i = ps; i < pe; i++) {
                        if (i > ps && oc >= 0 &&
                            flat_col_ord_cmp(st->flat_data[oc], st->flat_types[oc], st->flat_nulls[oc],
                                             st->sorted[i], st->sorted[i-1]) != 0)
                            rank = (int32_t)(i - ps + 1);
                        st->win_i32[st->sorted[i] * nw + w] = rank;
                    }
                    break;
                }
                case WIN_DENSE_RANK: {
                    int32_t rank = 1;
                    for (uint32_t i = ps; i < pe; i++) {
                        if (i > ps && oc >= 0 &&
                            flat_col_ord_cmp(st->flat_data[oc], st->flat_types[oc], st->flat_nulls[oc],
                                             st->sorted[i], st->sorted[i-1]) != 0)
                            rank++;
                        st->win_i32[st->sorted[i] * nw + w] = rank;
                    }
                    break;
                }
                case WIN_NTILE: {
                    int nb = pn->window.win_offset[w] > 0 ? pn->window.win_offset[w] : 1;
                    for (uint32_t i = ps; i < pe; i++)
                        st->win_i32[st->sorted[i] * nw + w] = (int32_t)(((i - ps) * (uint32_t)nb) / psize) + 1;
                    break;
                }
                case WIN_PERCENT_RANK: {
                    st->win_is_dbl[w] = 1;
                    if (psize <= 1) {
                        for (uint32_t i = ps; i < pe; i++) st->win_f64[st->sorted[i] * nw + w] = 0.0;
                    } else {
                        int32_t rank = 1;
                        for (uint32_t i = ps; i < pe; i++) {
                            if (i > ps && oc >= 0 &&
                                flat_col_ord_cmp(st->flat_data[oc], st->flat_types[oc], st->flat_nulls[oc],
                                                 st->sorted[i], st->sorted[i-1]) != 0)
                                rank = (int32_t)(i - ps + 1);
                            st->win_f64[st->sorted[i] * nw + w] = (double)(rank - 1) / (double)(psize - 1);
                        }
                    }
                    break;
                }
                case WIN_CUME_DIST: {
                    st->win_is_dbl[w] = 1;
                    if (oc >= 0) {
                        uint32_t i = ps;
                        while (i < pe) {
                            uint32_t j = i + 1;
                            while (j < pe && flat_col_ord_cmp(st->flat_data[oc], st->flat_types[oc],
                                    st->flat_nulls[oc], st->sorted[j], st->sorted[i]) == 0)
                                j++;
                            double cd = (double)(j - ps) / (double)psize;
                            for (uint32_t k = i; k < j; k++) st->win_f64[st->sorted[k] * nw + w] = cd;
                            i = j;
                        }
                    } else {
                        for (uint32_t i = ps; i < pe; i++) st->win_f64[st->sorted[i] * nw + w] = 1.0;
                    }
                    break;
                }
                case WIN_LAG:
                case WIN_LEAD: {
                    int offset = pn->window.win_offset[w];
                    int is_text = column_type_is_text(st->flat_types[ac >= 0 ? ac : 0]);
                    for (uint32_t i = ps; i < pe; i++) {
                        uint32_t pos = i - ps;
                        uint32_t target = 0;
                        int in_range = 0;
                        if (wf == WIN_LAG) {
                            if (pos >= (uint32_t)offset) { target = i - (uint32_t)offset; in_range = 1; }
                        } else {
                            target = i + (uint32_t)offset;
                            if (target < pe) in_range = 1;
                        }
                        uint32_t oi = st->sorted[i];
                        if (in_range && ac >= 0) {
                            uint32_t si = st->sorted[target];
                            if (st->flat_nulls[ac][si]) {
                                st->win_null[oi * nw + w] = 1;
                            } else if (is_text) {
                                st->win_str[oi * nw + w] = ((char **)st->flat_data[ac])[si];
                                st->win_is_str[w] = 1;
                            } else {
                                st->win_f64[oi * nw + w] = flat_col_to_double(st->flat_data[ac], st->flat_nulls[ac], st->flat_types[ac], si);
                                st->win_is_dbl[w] = 1;
                            }
                        } else {
                            st->win_null[oi * nw + w] = 1;
                        }
                    }
                    break;
                }
                case WIN_FIRST_VALUE:
                case WIN_LAST_VALUE: {
                    int is_text_fv = (ac >= 0) && column_type_is_text(st->flat_types[ac]);
                    for (uint32_t i = ps; i < pe; i++) {
                        uint32_t target = (wf == WIN_FIRST_VALUE) ? ps : (pe - 1);
                        uint32_t oi = st->sorted[i];
                        if (ac >= 0) {
                            uint32_t si = st->sorted[target];
                            if (st->flat_nulls[ac][si]) {
                                st->win_null[oi * nw + w] = 1;
                            } else if (is_text_fv) {
                                st->win_str[oi * nw + w] = ((char **)st->flat_data[ac])[si];
                                st->win_is_str[w] = 1;
                            } else {
                                st->win_f64[oi * nw + w] = flat_col_to_double(st->flat_data[ac], st->flat_nulls[ac], st->flat_types[ac], si);
                                st->win_is_dbl[w] = 1;
                            }
                        } else {
                            st->win_null[oi * nw + w] = 1;
                        }
                    }
                    break;
                }
                case WIN_NTH_VALUE: {
                    int nth = pn->window.win_offset[w];
                    int is_text_nv = (ac >= 0) && column_type_is_text(st->flat_types[ac]);
                    for (uint32_t i = ps; i < pe; i++) {
                        uint32_t oi = st->sorted[i];
                        if (nth >= 1 && (uint32_t)nth <= psize && ac >= 0) {
                            uint32_t si = st->sorted[ps + (uint32_t)(nth - 1)];
                            if (st->flat_nulls[ac][si]) {
                                st->win_null[oi * nw + w] = 1;
                            } else if (is_text_nv) {
                                st->win_str[oi * nw + w] = ((char **)st->flat_data[ac])[si];
                                st->win_is_str[w] = 1;
                            } else {
                                st->win_f64[oi * nw + w] = flat_col_to_double(st->flat_data[ac], st->flat_nulls[ac], st->flat_types[ac], si);
                                st->win_is_dbl[w] = 1;
                            }
                        } else {
                            st->win_null[oi * nw + w] = 1;
                        }
                    }
                    break;
                }
                case WIN_SUM:
                case WIN_COUNT:
                case WIN_AVG: {
                    if (!pn->window.win_has_frame[w] && oc < 0) {
                        /* no frame, no ORDER BY: partition total */
                        double part_sum = 0.0;
                        int part_nn = 0;
                        for (uint32_t i = ps; i < pe; i++) {
                            if (ac >= 0) {
                                uint32_t si = st->sorted[i];
                                if (!st->flat_nulls[ac][si]) {
                                    part_sum += flat_col_to_double(st->flat_data[ac], st->flat_nulls[ac], st->flat_types[ac], si);
                                    part_nn++;
                                }
                            }
                        }
                        for (uint32_t i = ps; i < pe; i++) {
                            uint32_t oi = st->sorted[i];
                            if (wf == WIN_SUM) {
                                if (ac >= 0 && (st->flat_types[ac] == COLUMN_TYPE_FLOAT || st->flat_types[ac] == COLUMN_TYPE_NUMERIC)) {
                                    st->win_is_dbl[w] = 1;
                                    st->win_f64[oi * nw + w] = part_sum;
                                } else {
                                    st->win_i32[oi * nw + w] = (int32_t)part_sum;
                                }
                            } else if (wf == WIN_COUNT) {
                                st->win_i32[oi * nw + w] = (ac >= 0) ? part_nn : (int32_t)psize;
                            } else {
                                st->win_is_dbl[w] = 1;
                                if (part_nn > 0) st->win_f64[oi * nw + w] = part_sum / (double)part_nn;
                                else st->win_null[oi * nw + w] = 1;
                            }
                        }
                    } else if (!pn->window.win_has_frame[w] && oc >= 0) {
                        /* ORDER BY without explicit frame: implicit UNBOUNDED PRECEDING TO CURRENT ROW */
                        double running_sum = 0.0;
                        int running_nn = 0;
                        int running_count = 0;
                        for (uint32_t i = ps; i < pe; i++) {
                            uint32_t si = st->sorted[i];
                            if (ac >= 0 && !st->flat_nulls[ac][si]) {
                                running_sum += flat_col_to_double(st->flat_data[ac], st->flat_nulls[ac], st->flat_types[ac], si);
                                running_nn++;
                            }
                            running_count++;
                            uint32_t oi = st->sorted[i];
                            if (wf == WIN_SUM) {
                                if (ac >= 0 && (st->flat_types[ac] == COLUMN_TYPE_FLOAT || st->flat_types[ac] == COLUMN_TYPE_NUMERIC)) {
                                    st->win_is_dbl[w] = 1;
                                    st->win_f64[oi * nw + w] = running_sum;
                                } else {
                                    st->win_i32[oi * nw + w] = (int32_t)running_sum;
                                }
                            } else if (wf == WIN_COUNT) {
                                st->win_i32[oi * nw + w] = (ac >= 0) ? running_nn : running_count;
                            } else {
                                st->win_is_dbl[w] = 1;
                                if (running_nn > 0) st->win_f64[oi * nw + w] = running_sum / (double)running_nn;
                                else st->win_null[oi * nw + w] = 1;
                            }
                        }
                    } else {
                        for (uint32_t i = ps; i < pe; i++) {
                            uint32_t my_pos = i - ps;
                            uint32_t fs = 0, fe = psize;
                            switch (pn->window.win_frame_start[w]) {
                                case FRAME_UNBOUNDED_PRECEDING: fs = 0; break;
                                case FRAME_CURRENT_ROW: fs = my_pos; break;
                                case FRAME_N_PRECEDING: fs = (my_pos >= (uint32_t)pn->window.win_frame_start_n[w]) ? my_pos - (uint32_t)pn->window.win_frame_start_n[w] : 0; break;
                                case FRAME_N_FOLLOWING: fs = my_pos + (uint32_t)pn->window.win_frame_start_n[w]; break;
                                case FRAME_UNBOUNDED_FOLLOWING: fs = psize; break;
                            }
                            switch (pn->window.win_frame_end[w]) {
                                case FRAME_UNBOUNDED_FOLLOWING: fe = psize; break;
                                case FRAME_CURRENT_ROW: fe = my_pos + 1; break;
                                case FRAME_N_FOLLOWING: fe = my_pos + (uint32_t)pn->window.win_frame_end_n[w] + 1; if (fe > psize) fe = psize; break;
                                case FRAME_N_PRECEDING: fe = (my_pos >= (uint32_t)pn->window.win_frame_end_n[w]) ? my_pos - (uint32_t)pn->window.win_frame_end_n[w] + 1 : 0; break;
                                case FRAME_UNBOUNDED_PRECEDING: fe = 0; break;
                            }
                            if (fs > psize) fs = psize;
                            double frame_sum = 0.0;
                            int frame_nn = 0, frame_count = 0;
                            for (uint32_t fi = fs; fi < fe; fi++) {
                                uint32_t si = st->sorted[ps + fi];
                                frame_count++;
                                if (ac >= 0 && !st->flat_nulls[ac][si]) {
                                    frame_sum += flat_col_to_double(st->flat_data[ac], st->flat_nulls[ac], st->flat_types[ac], si);
                                    frame_nn++;
                                }
                            }
                            uint32_t oi = st->sorted[i];
                            if (wf == WIN_SUM) {
                                if (ac >= 0 && (st->flat_types[ac] == COLUMN_TYPE_FLOAT || st->flat_types[ac] == COLUMN_TYPE_NUMERIC)) {
                                    st->win_is_dbl[w] = 1; st->win_f64[oi * nw + w] = frame_sum;
                                } else st->win_i32[oi * nw + w] = (int32_t)frame_sum;
                            } else if (wf == WIN_COUNT) {
                                st->win_i32[oi * nw + w] = (ac >= 0) ? frame_nn : frame_count;
                            } else {
                                st->win_is_dbl[w] = 1;
                                if (frame_nn > 0) st->win_f64[oi * nw + w] = frame_sum / (double)frame_nn;
                                else st->win_null[oi * nw + w] = 1;
                            }
                        }
                    }
                    break;
                }
                } /* switch */
            } /* partitions */
        } /* window exprs */

        /* Re-sort sorted[] back to the original global sort order for emit.
         * Per-expression partition handling may have re-sorted it. */
        if (spc >= 0 || soc >= 0) {
            _wsort_ctx.has_part = (spc >= 0);
            if (spc >= 0) {
                _wsort_ctx.part_data = st->flat_data[spc];
                _wsort_ctx.part_nulls = st->flat_nulls[spc];
                _wsort_ctx.part_type = st->flat_types[spc];
            }
            _wsort_ctx.has_ord = (soc >= 0);
            if (soc >= 0) {
                _wsort_ctx.ord_data = st->flat_data[soc];
                _wsort_ctx.ord_nulls = st->flat_nulls[soc];
                _wsort_ctx.ord_type = st->flat_types[soc];
                _wsort_ctx.ord_desc = pn->window.sort_ord_desc;
            }
            qsort(st->sorted, total, sizeof(uint32_t), window_sort_cmp);
        }

        st->input_done = 1;
        st->emit_cursor = 0;
    }

    /* Emit phase: output blocks with passthrough + window columns */
    if (st->emit_cursor >= st->total_rows) return -1;

    uint16_t n_pass = pn->window.n_pass;
    uint16_t nw = pn->window.n_win;
    row_block_reset(out);

    uint32_t remain = st->total_rows - st->emit_cursor;
    uint16_t out_count = (remain < BLOCK_CAPACITY) ? (uint16_t)remain : BLOCK_CAPACITY;
    const uint32_t *idx = st->sorted + st->emit_cursor;

    /* Column-oriented gather emit for passthrough columns */
    for (uint16_t c = 0; c < n_pass; c++) {
        int sci = pn->window.pass_cols[c];
        struct col_block *ocb = &out->cols[c];
        enum column_type ct = st->flat_types[sci];
        const uint8_t *src_nulls = st->flat_nulls[sci];
        const void *src_data = st->flat_data[sci];
        ocb->type = ct;
        ocb->count = out_count;

        for (uint16_t r = 0; r < out_count; r++)
            ocb->nulls[r] = src_nulls[idx[r]];

        switch (ct) {
        case COLUMN_TYPE_SMALLINT: {
            const int16_t *s = (const int16_t *)src_data;
            for (uint16_t r = 0; r < out_count; r++)
                ocb->data.i16[r] = s[idx[r]];
            break;
        }
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:
        case COLUMN_TYPE_DATE: {
            const int32_t *s = (const int32_t *)src_data;
            for (uint16_t r = 0; r < out_count; r++)
                ocb->data.i32[r] = s[idx[r]];
            break;
        }
        case COLUMN_TYPE_BIGINT:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ: {
            const int64_t *s = (const int64_t *)src_data;
            for (uint16_t r = 0; r < out_count; r++)
                ocb->data.i64[r] = s[idx[r]];
            break;
        }
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC: {
            const double *s = (const double *)src_data;
            for (uint16_t r = 0; r < out_count; r++)
                ocb->data.f64[r] = s[idx[r]];
            break;
        }
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_UUID: {
            char *const *s = (char *const *)src_data;
            for (uint16_t r = 0; r < out_count; r++)
                ocb->data.str[r] = s[idx[r]];
            break;
        }
        case COLUMN_TYPE_INTERVAL: {
            const struct interval *s = (const struct interval *)src_data;
            for (uint16_t r = 0; r < out_count; r++)
                ocb->data.iv[r] = s[idx[r]];
            break;
        }
        }
    }

    /* Window result columns — indexed by original row index */
    for (uint16_t w = 0; w < nw; w++) {
        uint16_t oc = n_pass + w;
        struct col_block *ocb = &out->cols[oc];
        ocb->type = st->win_is_str[w] ? COLUMN_TYPE_TEXT
                  : st->win_is_dbl[w] ? COLUMN_TYPE_FLOAT
                  : COLUMN_TYPE_INT;
        ocb->count = out_count;

        if (st->win_is_str[w]) {
            for (uint16_t r = 0; r < out_count; r++) {
                uint32_t fi = idx[r];
                ocb->nulls[r] = st->win_null[fi * nw + w];
                ocb->data.str[r] = st->win_str[fi * nw + w];
            }
        } else if (st->win_is_dbl[w]) {
            for (uint16_t r = 0; r < out_count; r++) {
                uint32_t fi = idx[r];
                ocb->nulls[r] = st->win_null[fi * nw + w];
                ocb->data.f64[r] = st->win_f64[fi * nw + w];
            }
        } else {
            for (uint16_t r = 0; r < out_count; r++) {
                uint32_t fi = idx[r];
                ocb->nulls[r] = st->win_null[fi * nw + w];
                ocb->data.i32[r] = st->win_i32[fi * nw + w];
            }
        }
    }

    st->emit_cursor += out_count;
    out->count = out_count;

    return 0;
}

/* ---- Hash semi-join (for WHERE col IN (SELECT ...)) ---- */

/* Hash a value from flat arrays at index i */
static inline uint32_t semi_hash_flat(enum column_type type, const void *data, uint32_t i)
{
    switch (type) {
        case COLUMN_TYPE_SMALLINT:
            return block_hash_i32((int32_t)((const int16_t *)data)[i]);
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:
            return block_hash_i32(((const int32_t *)data)[i]);
        case COLUMN_TYPE_BIGINT:
            return block_hash_i64(((const int64_t *)data)[i]);
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:
            return block_hash_f64(((const double *)data)[i]);
        case COLUMN_TYPE_DATE:
            return block_hash_i32(((const int32_t *)data)[i]);
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
            return block_hash_i64(((const int64_t *)data)[i]);
        case COLUMN_TYPE_INTERVAL: {
            struct interval iv = ((const struct interval *)data)[i];
            uint64_t h = 14695981039346656037ULL;
            h ^= (uint64_t)(uint32_t)iv.months; h *= 1099511628211ULL;
            h ^= (uint64_t)(uint32_t)iv.days; h *= 1099511628211ULL;
            h ^= (uint64_t)iv.usec; h *= 1099511628211ULL;
            return h;
        }
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_UUID:
            return block_hash_str(((const char **)data)[i]);
    }
    return 0;
}

/* Compare a col_block value at oi with a flat array value at fi */
static inline int semi_eq_cb_flat(const struct col_block *cb, uint16_t oi,
                                  enum column_type type, const void *data, uint32_t fi)
{
    if (cb->nulls[oi]) return 0;
    switch (type) {
        case COLUMN_TYPE_SMALLINT:
            return cb->data.i16[oi] == ((const int16_t *)data)[fi];
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:
            return cb->data.i32[oi] == ((const int32_t *)data)[fi];
        case COLUMN_TYPE_BIGINT:
            return cb->data.i64[oi] == ((const int64_t *)data)[fi];
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:
            return cb->data.f64[oi] == ((const double *)data)[fi];
        case COLUMN_TYPE_DATE:
            return cb->data.i32[oi] == ((const int32_t *)data)[fi];
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
            return cb->data.i64[oi] == ((const int64_t *)data)[fi];
        case COLUMN_TYPE_INTERVAL: {
            struct interval ia = cb->data.iv[oi];
            struct interval ib = ((const struct interval *)data)[fi];
            return ia.months == ib.months && ia.days == ib.days && ia.usec == ib.usec;
        }
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_UUID: {
            const char *a = cb->data.str[oi];
            const char *b = ((const char **)data)[fi];
            if (!a || !b) return a == b;
            return strcmp(a, b) == 0;
        }
    }
    return 0;
}

static size_t semi_elem_size(enum column_type type)
{
    switch (type) {
        case COLUMN_TYPE_SMALLINT:
            return sizeof(int16_t);
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:
            return sizeof(int32_t);
        case COLUMN_TYPE_BIGINT:
            return sizeof(int64_t);
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:
            return sizeof(double);
        case COLUMN_TYPE_DATE:
            return sizeof(int32_t);
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
            return sizeof(int64_t);
        case COLUMN_TYPE_INTERVAL:
            return sizeof(struct interval);
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_UUID:
            return sizeof(char *);
    }
    return sizeof(char *);
}

static void hash_semi_join_build(struct plan_exec_ctx *ctx, uint32_t node_idx)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct hash_semi_join_state *st = (struct hash_semi_join_state *)ctx->node_states[node_idx];

    uint16_t inner_ncols = plan_node_ncols(ctx->arena, pn->right);
    if (inner_ncols == 0) inner_ncols = 1;

    int key_col = pn->hash_semi_join.inner_key_col;

    /* Initial capacity for flat key arrays */
    uint32_t cap = 4096;
    st->build_count = 0;
    st->build_cap = cap;
    st->key_type = COLUMN_TYPE_INT; /* will be set from first block */

    /* Collect all key values from inner side into flat bump-allocated arrays */
    struct row_block inner_block;
    row_block_alloc(&inner_block, inner_ncols, &ctx->arena->scratch);

    int type_set = 0;
    size_t elem_sz = sizeof(int32_t);

    /* Pre-allocate flat arrays */
    st->key_data = bump_alloc(&ctx->arena->scratch, cap * sizeof(double)); /* max elem size */
    st->key_nulls = (uint8_t *)bump_calloc(&ctx->arena->scratch, cap, 1);

    while (plan_next_block(ctx, pn->right, &inner_block) == 0) {
        uint16_t active = row_block_active_count(&inner_block);
        struct col_block *src_key = &inner_block.cols[key_col];

        if (!type_set) {
            st->key_type = src_key->type;
            elem_sz = semi_elem_size(src_key->type);
            type_set = 1;
        }

        for (uint16_t i = 0; i < active; i++) {
            uint16_t ri = row_block_row_idx(&inner_block, i);
            if (src_key->nulls[ri]) continue; /* skip NULLs — IN ignores them */

            /* Grow if needed */
            if (st->build_count >= st->build_cap) {
                uint32_t new_cap = st->build_cap * 2;
                void *new_data = bump_alloc(&ctx->arena->scratch, new_cap * elem_sz);
                memcpy(new_data, st->key_data, st->build_count * elem_sz);
                uint8_t *new_nulls = (uint8_t *)bump_calloc(&ctx->arena->scratch, new_cap, 1);
                memcpy(new_nulls, st->key_nulls, st->build_count);
                st->key_data = new_data;
                st->key_nulls = new_nulls;
                st->build_cap = new_cap;
            }

            uint32_t di = st->build_count;
            st->key_nulls[di] = 0;
            switch (st->key_type) {
                case COLUMN_TYPE_SMALLINT:
                    ((int16_t *)st->key_data)[di] = src_key->data.i16[ri];
                    break;
                case COLUMN_TYPE_INT:
                case COLUMN_TYPE_BOOLEAN:
                    ((int32_t *)st->key_data)[di] = src_key->data.i32[ri];
                    break;
                case COLUMN_TYPE_BIGINT:
                    ((int64_t *)st->key_data)[di] = src_key->data.i64[ri];
                    break;
                case COLUMN_TYPE_FLOAT:
                case COLUMN_TYPE_NUMERIC:
                    ((double *)st->key_data)[di] = src_key->data.f64[ri];
                    break;
                case COLUMN_TYPE_DATE:
                    ((int32_t *)st->key_data)[di] = src_key->data.i32[ri];
                    break;
                case COLUMN_TYPE_TIME:
                case COLUMN_TYPE_TIMESTAMP:
                case COLUMN_TYPE_TIMESTAMPTZ:
                    ((int64_t *)st->key_data)[di] = src_key->data.i64[ri];
                    break;
                case COLUMN_TYPE_INTERVAL:
                    ((struct interval *)st->key_data)[di] = src_key->data.iv[ri];
                    break;
                case COLUMN_TYPE_TEXT:
                case COLUMN_TYPE_ENUM:
                case COLUMN_TYPE_UUID:
                    ((char **)st->key_data)[di] = src_key->data.str[ri];
                    break;
            }
            st->build_count++;
        }
        row_block_reset(&inner_block);
    }

    /* Build hash table on collected keys */
    uint32_t n = st->build_count;
    block_ht_init(&st->ht, n > 0 ? n * 2 : 1, &ctx->arena->scratch);

    for (uint32_t i = 0; i < n; i++) {
        uint32_t h = semi_hash_flat(st->key_type, st->key_data, i);
        uint32_t bucket = h & (st->ht.nbuckets - 1);
        st->ht.hashes[i] = h;
        st->ht.nexts[i] = st->ht.buckets[bucket];
        st->ht.buckets[bucket] = i;
        st->ht.count++;
    }

    st->build_done = 1;
}

static int hash_semi_join_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                               struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct hash_semi_join_state *st = (struct hash_semi_join_state *)ctx->node_states[node_idx];
    if (!st) {
        st = (struct hash_semi_join_state *)bump_calloc(&ctx->arena->scratch, 1, sizeof(*st));
        ctx->node_states[node_idx] = st;
    }

    if (!st->build_done)
        hash_semi_join_build(ctx, node_idx);

    /* Empty build side → no matches possible */
    if (st->build_count == 0) return -1;

    uint16_t outer_ncols = plan_node_ncols(ctx->arena, pn->left);

    struct row_block outer_block;
    row_block_alloc(&outer_block, outer_ncols, &ctx->arena->scratch);

    /* Keep pulling outer blocks until we find matches or exhaust input */
    for (;;) {
        int rc = plan_next_block(ctx, pn->left, &outer_block);
        if (rc != 0) return -1;

        int outer_key = pn->hash_semi_join.outer_key_col;
        struct col_block *outer_key_cb = &outer_block.cols[outer_key];

        row_block_reset(out);
        uint16_t out_count = 0;
        uint16_t active = row_block_active_count(&outer_block);

        for (uint16_t i = 0; i < active && out_count < BLOCK_CAPACITY; i++) {
            uint16_t oi = row_block_row_idx(&outer_block, i);
            if (outer_key_cb->nulls[oi]) continue;

            uint32_t h = block_hash_cell(outer_key_cb, oi);
            uint32_t bucket = h & (st->ht.nbuckets - 1);
            uint32_t entry = st->ht.buckets[bucket];

            int found = 0;
            while (entry != IDX_NONE && entry != 0xFFFFFFFF) {
                if (st->ht.hashes[entry] == h &&
                    semi_eq_cb_flat(outer_key_cb, oi, st->key_type, st->key_data, entry)) {
                    found = 1;
                    break;
                }
                entry = st->ht.nexts[entry];
            }

            if (found) {
                for (uint16_t c = 0; c < outer_ncols; c++) {
                    out->cols[c].type = outer_block.cols[c].type;
                    cb_copy_value(&out->cols[c], out_count, &outer_block.cols[c], oi);
                }
                out_count++;
            }
        }

        if (out_count > 0) {
            out->count = out_count;
            for (uint16_t c = 0; c < outer_ncols; c++)
                out->cols[c].count = out_count;
            return 0;
        }

        row_block_reset(&outer_block);
    }
}

/* ---- Set operations (UNION / INTERSECT / EXCEPT) ---- */

/* Hash a full row across all columns in flat arrays */
static inline uint32_t set_op_hash_row(void **col_data, uint8_t **col_nulls,
                                        enum column_type *col_types, uint16_t ncols,
                                        uint32_t row_idx)
{
    uint32_t h = 2166136261u;
    for (uint16_t c = 0; c < ncols; c++) {
        if (col_nulls[c][row_idx]) {
            h ^= 0x9e3779b9u;
            h *= 16777619u;
            continue;
        }
        uint32_t ch;
        enum column_type ct = col_types[c];
        if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN)
            ch = block_hash_i32(((int32_t *)col_data[c])[row_idx]);
        else if (ct == COLUMN_TYPE_BIGINT)
            ch = block_hash_i64(((int64_t *)col_data[c])[row_idx]);
        else if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC)
            ch = block_hash_f64(((double *)col_data[c])[row_idx]);
        else
            ch = block_hash_str(((char **)col_data[c])[row_idx]);
        h ^= ch;
        h *= 16777619u;
    }
    return h;
}

/* Compare row at idx_a in flat arrays A with row at idx_b in flat arrays B */
static inline int set_op_rows_eq(void **da, uint8_t **na, enum column_type *ta,
                                  void **db, uint8_t **nb,
                                  uint16_t ncols, uint32_t idx_a, uint32_t idx_b)
{
    for (uint16_t c = 0; c < ncols; c++) {
        int an = na[c][idx_a], bn = nb[c][idx_b];
        if (an && bn) continue;       /* both NULL — equal for set ops */
        if (an || bn) return 0;       /* one NULL — not equal */
        enum column_type ct = ta[c];
        if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN) {
            if (((int32_t *)da[c])[idx_a] != ((int32_t *)db[c])[idx_b]) return 0;
        } else if (ct == COLUMN_TYPE_BIGINT) {
            if (((int64_t *)da[c])[idx_a] != ((int64_t *)db[c])[idx_b]) return 0;
        } else if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC) {
            if (((double *)da[c])[idx_a] != ((double *)db[c])[idx_b]) return 0;
        } else {
            const char *sa = ((char **)da[c])[idx_a];
            const char *sb = ((char **)db[c])[idx_b];
            if (!sa && !sb) continue;
            if (!sa || !sb) return 0;
            if (strcmp(sa, sb) != 0) return 0;
        }
    }
    return 1;
}

static size_t set_op_elem_size(enum column_type ct)
{
    if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN) return sizeof(int32_t);
    if (ct == COLUMN_TYPE_BIGINT) return sizeof(int64_t);
    if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC) return sizeof(double);
    return sizeof(char *);
}

/* Grow flat arrays to new_cap */
static void set_op_grow(struct set_op_state *st, struct bump_alloc *scratch)
{
    uint32_t new_cap = st->row_cap * 2;
    for (uint16_t c = 0; c < st->ncols; c++) {
        size_t esz = set_op_elem_size(st->col_types[c]);
        void *new_data = bump_alloc(scratch, new_cap * esz);
        memcpy(new_data, st->col_data[c], st->row_count * esz);
        st->col_data[c] = new_data;
        uint8_t *new_nulls = (uint8_t *)bump_calloc(scratch, new_cap, 1);
        memcpy(new_nulls, st->col_nulls[c], st->row_count);
        st->col_nulls[c] = new_nulls;
    }
    if (st->matched) {
        uint8_t *new_matched = (uint8_t *)bump_calloc(scratch, new_cap, 1);
        memcpy(new_matched, st->matched, st->row_count);
        st->matched = new_matched;
    }
    /* Rebuild hash table with new capacity */
    block_ht_init(&st->ht, new_cap, scratch);
    for (uint32_t i = 0; i < st->row_count; i++) {
        uint32_t h = set_op_hash_row(st->col_data, st->col_nulls, st->col_types, st->ncols, i);
        uint32_t bucket = h & (st->ht.nbuckets - 1);
        st->ht.hashes[i] = h;
        st->ht.nexts[i] = st->ht.buckets[bucket];
        st->ht.buckets[bucket] = i;
        st->ht.count++;
    }
    st->row_cap = new_cap;
}

/* Copy a row from col_block (respecting selection vector) into flat arrays */
static inline void set_op_copy_from_block(struct set_op_state *st, struct row_block *blk,
                                           uint16_t ri, uint32_t dst_idx)
{
    for (uint16_t c = 0; c < st->ncols; c++) {
        struct col_block *cb = &blk->cols[c];
        st->col_nulls[c][dst_idx] = cb->nulls[ri];
        if (dst_idx == 0 && st->col_types[c] == COLUMN_TYPE_INT)
            st->col_types[c] = cb->type; /* set type from first block */
        enum column_type ct = st->col_types[c];
        if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN)
            ((int32_t *)st->col_data[c])[dst_idx] = cb->data.i32[ri];
        else if (ct == COLUMN_TYPE_BIGINT)
            ((int64_t *)st->col_data[c])[dst_idx] = cb->data.i64[ri];
        else if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC)
            ((double *)st->col_data[c])[dst_idx] = cb->data.f64[ri];
        else
            ((char **)st->col_data[c])[dst_idx] = cb->data.str[ri];
    }
}

/* Force-insert a row into the hash table without dedup check (for INTERSECT/EXCEPT ALL). */
static void set_op_ht_insert_force(struct set_op_state *st, uint32_t row_idx)
{
    uint32_t h = set_op_hash_row(st->col_data, st->col_nulls, st->col_types, st->ncols, row_idx);
    uint32_t bucket = h & (st->ht.nbuckets - 1);
    st->ht.hashes[row_idx] = h;
    st->ht.nexts[row_idx] = st->ht.buckets[bucket];
    st->ht.buckets[bucket] = row_idx;
    st->ht.count++;
}

/* Try to insert a row into the hash table. Returns 1 if inserted (new), 0 if duplicate found. */
static int set_op_ht_insert(struct set_op_state *st, uint32_t row_idx)
{
    uint32_t h = set_op_hash_row(st->col_data, st->col_nulls, st->col_types, st->ncols, row_idx);
    uint32_t bucket = h & (st->ht.nbuckets - 1);
    uint32_t entry = st->ht.buckets[bucket];
    while (entry != IDX_NONE && entry != 0xFFFFFFFF) {
        if (st->ht.hashes[entry] == h &&
            set_op_rows_eq(st->col_data, st->col_nulls, st->col_types,
                           st->col_data, st->col_nulls,
                           st->ncols, entry, row_idx))
            return 0; /* duplicate */
        entry = st->ht.nexts[entry];
    }
    st->ht.hashes[row_idx] = h;
    st->ht.nexts[row_idx] = st->ht.buckets[bucket];
    st->ht.buckets[bucket] = row_idx;
    st->ht.count++;
    return 1;
}

static int set_op_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                       struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct set_op_state *st = (struct set_op_state *)ctx->node_states[node_idx];
    if (!st) {
        st = (struct set_op_state *)bump_calloc(&ctx->arena->scratch, 1, sizeof(*st));
        st->ncols = pn->set_op.ncols;
        st->row_cap = 4096;
        st->row_count = 0;
        st->phase = 0;
        st->emit_cursor = 0;

        /* Allocate flat arrays */
        st->col_data = (void **)bump_alloc(&ctx->arena->scratch, st->ncols * sizeof(void *));
        st->col_nulls = (uint8_t **)bump_alloc(&ctx->arena->scratch, st->ncols * sizeof(uint8_t *));
        st->col_types = (enum column_type *)bump_alloc(&ctx->arena->scratch, st->ncols * sizeof(enum column_type));
        for (uint16_t c = 0; c < st->ncols; c++) {
            st->col_types[c] = COLUMN_TYPE_INT; /* will be set from first block */
            st->col_data[c] = bump_calloc(&ctx->arena->scratch, st->row_cap, sizeof(double));
            st->col_nulls[c] = (uint8_t *)bump_calloc(&ctx->arena->scratch, st->row_cap, 1);
        }

        /* For INTERSECT/EXCEPT, allocate matched flags */
        if (pn->set_op.set_op == 1 || pn->set_op.set_op == 2)
            st->matched = (uint8_t *)bump_calloc(&ctx->arena->scratch, st->row_cap, 1);

        block_ht_init(&st->ht, st->row_cap, &ctx->arena->scratch);
        ctx->node_states[node_idx] = st;
    }

    /* Phase 0: collect all LHS rows */
    if (st->phase == 0) {
        uint16_t child_ncols = plan_node_ncols(ctx->arena, pn->left);
        struct row_block lhs_block;
        row_block_alloc(&lhs_block, child_ncols, &ctx->arena->scratch);

        while (plan_next_block(ctx, pn->left, &lhs_block) == 0) {
            uint16_t active = row_block_active_count(&lhs_block);
            for (uint16_t i = 0; i < active; i++) {
                uint16_t ri = row_block_row_idx(&lhs_block, i);

                /* Grow if needed */
                if (st->row_count >= st->row_cap)
                    set_op_grow(st, &ctx->arena->scratch);

                /* Set types from first row */
                if (st->row_count == 0) {
                    for (uint16_t c = 0; c < st->ncols; c++)
                        st->col_types[c] = lhs_block.cols[c].type;
                }

                uint32_t di = st->row_count;
                set_op_copy_from_block(st, &lhs_block, ri, di);

                if (pn->set_op.set_op == 0 && !pn->set_op.set_all) {
                    /* UNION (not ALL): dedup LHS rows */
                    st->row_count++;
                    if (!set_op_ht_insert(st, di))
                        st->row_count--;
                } else if (pn->set_op.set_op == 0) {
                    /* UNION ALL: just collect, no hash table needed */
                    st->row_count++;
                } else {
                    /* INTERSECT/EXCEPT (ALL or not): keep all LHS rows,
                     * force-insert into hash table for RHS probing */
                    st->row_count++;
                    set_op_ht_insert_force(st, di);
                }
            }
            row_block_reset(&lhs_block);
        }
        st->phase = 1;
    }

    /* Phase 1: process RHS rows */
    if (st->phase == 1) {
        uint16_t child_ncols = plan_node_ncols(ctx->arena, pn->right);
        struct row_block rhs_block;
        row_block_alloc(&rhs_block, child_ncols, &ctx->arena->scratch);

        int op = pn->set_op.set_op;

        while (plan_next_block(ctx, pn->right, &rhs_block) == 0) {
            uint16_t active = row_block_active_count(&rhs_block);
            for (uint16_t i = 0; i < active; i++) {
                uint16_t ri = row_block_row_idx(&rhs_block, i);

                if (op == 0) {
                    /* UNION / UNION ALL */
                    if (st->row_count >= st->row_cap)
                        set_op_grow(st, &ctx->arena->scratch);

                    uint32_t di = st->row_count;
                    set_op_copy_from_block(st, &rhs_block, ri, di);

                    if (pn->set_op.set_all) {
                        st->row_count++;
                    } else {
                        st->row_count++;
                        if (!set_op_ht_insert(st, di))
                            st->row_count--;
                    }
                } else if (op == 1) {
                    /* INTERSECT / INTERSECT ALL: mark LHS rows that match this RHS row.
                     * For ALL: mark only ONE unmarked LHS row per RHS row.
                     * For non-ALL: mark all matching LHS rows. */
                    int set_all = pn->set_op.set_all;
                    uint32_t h = 2166136261u;
                    for (uint16_t c2 = 0; c2 < st->ncols; c2++) {
                        struct col_block *cb = &rhs_block.cols[c2];
                        if (cb->nulls[ri]) { h ^= 0x9e3779b9u; h *= 16777619u; continue; }
                        h ^= block_hash_cell(cb, ri); h *= 16777619u;
                    }
                    uint32_t bucket = h & (st->ht.nbuckets - 1);
                    uint32_t entry = st->ht.buckets[bucket];
                    while (entry != IDX_NONE && entry != 0xFFFFFFFF) {
                        if (st->ht.hashes[entry] == h) {
                            int eq = 1;
                            for (uint16_t c2 = 0; c2 < st->ncols; c2++) {
                                struct col_block *cb = &rhs_block.cols[c2];
                                int an = st->col_nulls[c2][entry], bn = cb->nulls[ri];
                                if (an && bn) continue;
                                if (an || bn) { eq = 0; break; }
                                enum column_type ct = st->col_types[c2];
                                if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN) {
                                    if (((int32_t *)st->col_data[c2])[entry] != cb->data.i32[ri]) { eq = 0; break; }
                                } else if (ct == COLUMN_TYPE_BIGINT) {
                                    if (((int64_t *)st->col_data[c2])[entry] != cb->data.i64[ri]) { eq = 0; break; }
                                } else if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC) {
                                    if (((double *)st->col_data[c2])[entry] != cb->data.f64[ri]) { eq = 0; break; }
                                } else {
                                    const char *sa = ((char **)st->col_data[c2])[entry];
                                    const char *sb = cb->data.str[ri];
                                    if (!sa && !sb) continue;
                                    if (!sa || !sb) { eq = 0; break; }
                                    if (strcmp(sa, sb) != 0) { eq = 0; break; }
                                }
                            }
                            if (eq) {
                                if (set_all && !st->matched[entry]) {
                                    st->matched[entry] = 1;
                                    break; /* only mark one per RHS row */
                                }
                                st->matched[entry] = 1;
                            }
                        }
                        entry = st->ht.nexts[entry];
                    }
                } else {
                    /* EXCEPT / EXCEPT ALL: mark matching LHS rows to remove.
                     * For ALL: mark only ONE unmarked LHS row per RHS row.
                     * For non-ALL: mark all matching LHS rows. */
                    int set_all = pn->set_op.set_all;
                    uint32_t h = 2166136261u;
                    for (uint16_t c2 = 0; c2 < st->ncols; c2++) {
                        struct col_block *cb = &rhs_block.cols[c2];
                        if (cb->nulls[ri]) { h ^= 0x9e3779b9u; h *= 16777619u; continue; }
                        h ^= block_hash_cell(cb, ri); h *= 16777619u;
                    }
                    uint32_t bucket = h & (st->ht.nbuckets - 1);
                    uint32_t entry = st->ht.buckets[bucket];
                    while (entry != IDX_NONE && entry != 0xFFFFFFFF) {
                        if (st->ht.hashes[entry] == h) {
                            int eq = 1;
                            for (uint16_t c2 = 0; c2 < st->ncols; c2++) {
                                struct col_block *cb = &rhs_block.cols[c2];
                                int an = st->col_nulls[c2][entry], bn = cb->nulls[ri];
                                if (an && bn) continue;
                                if (an || bn) { eq = 0; break; }
                                enum column_type ct = st->col_types[c2];
                                if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN) {
                                    if (((int32_t *)st->col_data[c2])[entry] != cb->data.i32[ri]) { eq = 0; break; }
                                } else if (ct == COLUMN_TYPE_BIGINT) {
                                    if (((int64_t *)st->col_data[c2])[entry] != cb->data.i64[ri]) { eq = 0; break; }
                                } else if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC) {
                                    if (((double *)st->col_data[c2])[entry] != cb->data.f64[ri]) { eq = 0; break; }
                                } else {
                                    const char *sa = ((char **)st->col_data[c2])[entry];
                                    const char *sb = cb->data.str[ri];
                                    if (!sa && !sb) continue;
                                    if (!sa || !sb) { eq = 0; break; }
                                    if (strcmp(sa, sb) != 0) { eq = 0; break; }
                                }
                            }
                            if (eq) {
                                if (set_all && !st->matched[entry]) {
                                    st->matched[entry] = 1;
                                    break; /* only remove one per RHS row */
                                }
                                st->matched[entry] = 1;
                            }
                        }
                        entry = st->ht.nexts[entry];
                    }
                }
            }
            row_block_reset(&rhs_block);
        }
        st->phase = 2;
        st->emit_cursor = 0;
    }

    /* Phase 2: emit results */
    if (st->emit_cursor >= st->row_count) return -1;

    int op = pn->set_op.set_op;
    row_block_reset(out);
    uint16_t out_count = 0;

    /* Set column types */
    for (uint16_t c = 0; c < st->ncols; c++)
        out->cols[c].type = st->col_types[c];

    while (st->emit_cursor < st->row_count && out_count < BLOCK_CAPACITY) {
        uint32_t fi = st->emit_cursor++;

        /* For INTERSECT: only emit matched rows */
        if (op == 1 && !st->matched[fi]) continue;
        /* For EXCEPT: only emit unmatched rows */
        if (op == 2 && st->matched[fi]) continue;

        for (uint16_t c = 0; c < st->ncols; c++) {
            out->cols[c].nulls[out_count] = st->col_nulls[c][fi];
            enum column_type ct = st->col_types[c];
            if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN)
                out->cols[c].data.i32[out_count] = ((int32_t *)st->col_data[c])[fi];
            else if (ct == COLUMN_TYPE_BIGINT)
                out->cols[c].data.i64[out_count] = ((int64_t *)st->col_data[c])[fi];
            else if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC)
                out->cols[c].data.f64[out_count] = ((double *)st->col_data[c])[fi];
            else
                out->cols[c].data.str[out_count] = ((char **)st->col_data[c])[fi];
        }
        out_count++;
    }

    if (out_count == 0) return -1;

    out->count = out_count;
    for (uint16_t c = 0; c < st->ncols; c++)
        out->cols[c].count = out_count;

    return 0;
}

/* ---- Hash-based DISTINCT ---- */

static int distinct_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                         struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct set_op_state *st = (struct set_op_state *)ctx->node_states[node_idx];
    if (!st) {
        uint16_t ncols = plan_node_ncols(ctx->arena, pn->left);
        st = (struct set_op_state *)bump_calloc(&ctx->arena->scratch, 1, sizeof(*st));
        st->ncols = ncols;
        st->row_cap = 4096;
        st->row_count = 0;
        st->phase = 0;
        st->emit_cursor = 0;

        st->col_data = (void **)bump_alloc(&ctx->arena->scratch, ncols * sizeof(void *));
        st->col_nulls = (uint8_t **)bump_alloc(&ctx->arena->scratch, ncols * sizeof(uint8_t *));
        st->col_types = (enum column_type *)bump_alloc(&ctx->arena->scratch, ncols * sizeof(enum column_type));
        for (uint16_t c = 0; c < ncols; c++) {
            st->col_types[c] = COLUMN_TYPE_INT;
            st->col_data[c] = bump_calloc(&ctx->arena->scratch, st->row_cap, sizeof(double));
            st->col_nulls[c] = (uint8_t *)bump_calloc(&ctx->arena->scratch, st->row_cap, 1);
        }

        block_ht_init(&st->ht, st->row_cap, &ctx->arena->scratch);
        ctx->node_states[node_idx] = st;
    }

    /* Phase 0: collect all input rows, dedup via hash table */
    if (st->phase == 0) {
        struct row_block input;
        row_block_alloc(&input, st->ncols, &ctx->arena->scratch);

        while (plan_next_block(ctx, pn->left, &input) == 0) {
            uint16_t active = row_block_active_count(&input);
            for (uint16_t i = 0; i < active; i++) {
                uint16_t ri = row_block_row_idx(&input, i);

                if (st->row_count >= st->row_cap)
                    set_op_grow(st, &ctx->arena->scratch);

                if (st->row_count == 0) {
                    for (uint16_t c = 0; c < st->ncols; c++)
                        st->col_types[c] = input.cols[c].type;
                }

                uint32_t di = st->row_count;
                set_op_copy_from_block(st, &input, ri, di);

                st->row_count++;
                if (!set_op_ht_insert(st, di))
                    st->row_count--;
            }
            row_block_reset(&input);
        }
        st->phase = 1;
        st->emit_cursor = 0;
    }

    /* Phase 1: emit unique rows */
    if (st->emit_cursor >= st->row_count) return -1;

    row_block_reset(out);
    uint16_t out_count = 0;

    for (uint16_t c = 0; c < st->ncols; c++)
        out->cols[c].type = st->col_types[c];

    while (st->emit_cursor < st->row_count && out_count < BLOCK_CAPACITY) {
        uint32_t fi = st->emit_cursor++;

        for (uint16_t c = 0; c < st->ncols; c++) {
            out->cols[c].nulls[out_count] = st->col_nulls[c][fi];
            enum column_type ct = st->col_types[c];
            if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN)
                out->cols[c].data.i32[out_count] = ((int32_t *)st->col_data[c])[fi];
            else if (ct == COLUMN_TYPE_BIGINT)
                out->cols[c].data.i64[out_count] = ((int64_t *)st->col_data[c])[fi];
            else if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC)
                out->cols[c].data.f64[out_count] = ((double *)st->col_data[c])[fi];
            else
                out->cols[c].data.str[out_count] = ((char **)st->col_data[c])[fi];
        }
        out_count++;
    }

    if (out_count == 0) return -1;

    out->count = out_count;
    for (uint16_t c = 0; c < st->ncols; c++)
        out->cols[c].count = out_count;

    return 0;
}

/* ---- Generate Series ---- */

static int gen_series_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                           struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct gen_series_state *st = (struct gen_series_state *)ctx->node_states[node_idx];
    if (!st) {
        st = (struct gen_series_state *)bump_calloc(&ctx->arena->scratch, 1, sizeof(*st));
        ctx->node_states[node_idx] = st;
        st->current = pn->gen_series.start;
        st->done = 0;
    }
    if (st->done) return -1;

    long long stop = pn->gen_series.stop;
    long long step = pn->gen_series.step;
    int use_bigint = pn->gen_series.use_bigint;

    row_block_reset(out);
    out->cols[0].type = use_bigint ? COLUMN_TYPE_BIGINT : COLUMN_TYPE_INT;

    uint16_t count = 0;
    long long v = st->current;

    if (use_bigint) {
        int64_t *dst = out->cols[0].data.i64;
        uint8_t *nul = out->cols[0].nulls;
        if (step > 0) {
            while (v <= stop && count < BLOCK_CAPACITY) {
                dst[count] = (int64_t)v;
                nul[count] = 0;
                count++;
                v += step;
            }
        } else {
            while (v >= stop && count < BLOCK_CAPACITY) {
                dst[count] = (int64_t)v;
                nul[count] = 0;
                count++;
                v += step;
            }
        }
    } else {
        int32_t *dst = out->cols[0].data.i32;
        uint8_t *nul = out->cols[0].nulls;
        if (step > 0) {
            while (v <= stop && count < BLOCK_CAPACITY) {
                dst[count] = (int32_t)v;
                nul[count] = 0;
                count++;
                v += step;
            }
        } else {
            while (v >= stop && count < BLOCK_CAPACITY) {
                dst[count] = (int32_t)v;
                nul[count] = 0;
                count++;
                v += step;
            }
        }
    }

    st->current = v;
    if (count == 0) { st->done = 1; return -1; }

    /* Check if we've exhausted the series */
    if (step > 0 && v > stop) st->done = 1;
    if (step < 0 && v < stop) st->done = 1;

    out->count = count;
    out->cols[0].count = count;
    return 0;
}

/* ---- Dispatcher ---- */

int plan_next_block(struct plan_exec_ctx *ctx, uint32_t node_idx,
                    struct row_block *out)
{
    if (node_idx == IDX_NONE) return -1;
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);

    switch (pn->op) {
    case PLAN_SEQ_SCAN:        return seq_scan_next(ctx, node_idx, out);
    case PLAN_INDEX_SCAN:      return index_scan_next(ctx, node_idx, out);
    case PLAN_FILTER:          return filter_next(ctx, node_idx, out);
    case PLAN_PROJECT:         return project_next(ctx, node_idx, out);
    case PLAN_LIMIT:           return limit_next(ctx, node_idx, out);
    case PLAN_HASH_JOIN:       return hash_join_next(ctx, node_idx, out);
    case PLAN_HASH_AGG:        return hash_agg_next(ctx, node_idx, out);
    case PLAN_SORT:            return sort_next(ctx, node_idx, out);
    case PLAN_WINDOW:          return window_next(ctx, node_idx, out);
    case PLAN_HASH_SEMI_JOIN:  return hash_semi_join_next(ctx, node_idx, out);
    case PLAN_SET_OP:          return set_op_next(ctx, node_idx, out);
    case PLAN_DISTINCT:        return distinct_next(ctx, node_idx, out);
    case PLAN_GENERATE_SERIES: return gen_series_next(ctx, node_idx, out);
    case PLAN_EXPR_PROJECT:    return expr_project_next(ctx, node_idx, out);
    case PLAN_SIMPLE_AGG:       return simple_agg_next(ctx, node_idx, out);
    case PLAN_NESTED_LOOP:
        /* not yet implemented */
        return -1;
    }
    __builtin_unreachable();
}

/* ---- Full plan execution to rows ---- */

int plan_exec_to_rows(struct plan_exec_ctx *ctx, uint32_t root_node,
                      struct rows *result, struct bump_alloc *rb)
{
    struct plan_node *root = &PLAN_NODE(ctx->arena, root_node);
    uint16_t ncols = 0;

    /* Determine output column count from root node */
    (void)root;
    ncols = plan_node_ncols(ctx->arena, root_node);
    if (ncols == 0 && root->op == PLAN_HASH_JOIN) {
        uint16_t lc = plan_node_ncols(ctx->arena, root->left);
        uint16_t rc = plan_node_ncols(ctx->arena, root->right);
        ncols = lc + rc;
    }

    if (ncols == 0) return -1;

    struct row_block block;
    row_block_alloc(&block, ncols, &ctx->arena->scratch);

    while (plan_next_block(ctx, root_node, &block) == 0) {
        block_to_rows(&block, result, rb);
        row_block_reset(&block);
    }

    return 0;
}

/* ---- EXPLAIN ---- */

/* Format a cell value into buf for EXPLAIN output. Returns bytes written. */
static int cell_value_to_str(const struct cell *c, char *buf, int buflen)
{
    if (c->is_null) return snprintf(buf, buflen, "NULL");
    switch (c->type) {
    case COLUMN_TYPE_INT:      return snprintf(buf, buflen, "%d", c->value.as_int);
    case COLUMN_TYPE_SMALLINT: return snprintf(buf, buflen, "%d", (int)c->value.as_smallint);
    case COLUMN_TYPE_BIGINT:   return snprintf(buf, buflen, "%lld", c->value.as_bigint);
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_NUMERIC:  return snprintf(buf, buflen, "%g", c->value.as_float);
    case COLUMN_TYPE_BOOLEAN:  return snprintf(buf, buflen, "%s", c->value.as_bool ? "true" : "false");
    case COLUMN_TYPE_DATE: {
        char tmp[32]; date_to_str(c->value.as_date, tmp, sizeof(tmp));
        return snprintf(buf, buflen, "'%s'", tmp);
    }
    case COLUMN_TYPE_TIME: {
        char tmp[32]; time_to_str(c->value.as_time, tmp, sizeof(tmp));
        return snprintf(buf, buflen, "'%s'", tmp);
    }
    case COLUMN_TYPE_TIMESTAMP: {
        char tmp[32]; timestamp_to_str(c->value.as_timestamp, tmp, sizeof(tmp));
        return snprintf(buf, buflen, "'%s'", tmp);
    }
    case COLUMN_TYPE_TIMESTAMPTZ: {
        char tmp[40]; timestamptz_to_str(c->value.as_timestamp, tmp, sizeof(tmp));
        return snprintf(buf, buflen, "'%s'", tmp);
    }
    case COLUMN_TYPE_INTERVAL: {
        char tmp[64]; interval_to_str(c->value.as_interval, tmp, sizeof(tmp));
        return snprintf(buf, buflen, "'%s'", tmp);
    }
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_UUID:
        if (c->value.as_text)
            return snprintf(buf, buflen, "'%s'", c->value.as_text);
        return snprintf(buf, buflen, "?");
    }
    __builtin_unreachable();
}

static int plan_explain_node(struct query_arena *arena, uint32_t node_idx,
                              char *buf, int buflen, int depth);

static const char *cmp_op_str(enum cmp_op op)
{
    switch (op) {
    case CMP_EQ:             return "=";
    case CMP_NE:             return "!=";
    case CMP_LT:             return "<";
    case CMP_GT:             return ">";
    case CMP_LE:             return "<=";
    case CMP_GE:             return ">=";
    case CMP_IS_NULL:        return "IS NULL";
    case CMP_IS_NOT_NULL:    return "IS NOT NULL";
    case CMP_IN:             return "IN";
    case CMP_NOT_IN:         return "NOT IN";
    case CMP_BETWEEN:        return "BETWEEN";
    case CMP_LIKE:           return "LIKE";
    case CMP_ILIKE:          return "ILIKE";
    case CMP_IS_DISTINCT:    return "IS DISTINCT FROM";
    case CMP_IS_NOT_DISTINCT: return "IS NOT DISTINCT FROM";
    case CMP_EXISTS:         return "EXISTS";
    case CMP_NOT_EXISTS:     return "NOT EXISTS";
    case CMP_REGEX_MATCH:    return "~";
    case CMP_REGEX_NOT_MATCH: return "!~";
    }
    __builtin_unreachable();
}

static int explain_index_scan(struct query_arena *arena, struct plan_node *pn,
                               char *buf, int buflen)
{
    const char *tname = pn->index_scan.table ? pn->index_scan.table->name : "?";
    if (pn->index_scan.cond_idx != IDX_NONE) {
        struct condition *cond = &arena->conditions.items[pn->index_scan.cond_idx];
        char vbuf[64] = "";
        cell_value_to_str(&cond->value, vbuf, sizeof(vbuf));
        return snprintf(buf, buflen, "Index Scan on %s (" SV_FMT " %s %s)\n",
                        tname, (int)cond->column.len, cond->column.data,
                        cmp_op_str(cond->op), vbuf);
    }
    return snprintf(buf, buflen, "Index Scan on %s\n", tname);
}

static int explain_filter(struct query_arena *arena, struct plan_node *pn,
                           char *buf, int buflen, int depth)
{
    int written = 0, n;
    if (pn->filter.cond_idx != IDX_NONE) {
        struct condition *cond = &arena->conditions.items[pn->filter.cond_idx];
        char vbuf[64] = "";
        cell_value_to_str(&cond->value, vbuf, sizeof(vbuf));
        n = snprintf(buf, buflen, "Filter: (" SV_FMT " %s %s)\n",
                     (int)cond->column.len, cond->column.data,
                     cmp_op_str(cond->op), vbuf);
    } else {
        n = snprintf(buf, buflen, "Filter\n");
    }
    if (n > 0) written += n;
    n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
    if (n > 0) written += n;
    return written;
}

static int explain_sort(struct query_arena *arena, struct plan_node *pn,
                         char *buf, int buflen, int depth)
{
    int written = 0, n;
    n = snprintf(buf, buflen, "Sort");
    if (n > 0) written += n;
    if (pn->sort.nsort_cols > 0 && pn->left != IDX_NONE) {
        /* walk child chain to find SEQ_SCAN for column names */
        struct plan_node *child = &PLAN_NODE(arena, pn->left);
        struct table *st = NULL;
        if (child->op == PLAN_SEQ_SCAN) st = child->seq_scan.table;
        else if (child->op == PLAN_FILTER && child->left != IDX_NONE) {
            struct plan_node *gc = &PLAN_NODE(arena, child->left);
            if (gc->op == PLAN_SEQ_SCAN) st = gc->seq_scan.table;
        }
        if (st) {
            n = snprintf(buf + written, buflen - written, " (");
            if (n > 0) written += n;
            for (uint16_t k = 0; k < pn->sort.nsort_cols; k++) {
                int ci = pn->sort.sort_cols[k];
                if (ci >= 0 && (size_t)ci < st->columns.count) {
                    if (k > 0) { n = snprintf(buf + written, buflen - written, ", "); if (n > 0) written += n; }
                    n = snprintf(buf + written, buflen - written, "%s", st->columns.items[ci].name);
                    if (n > 0) written += n;
                    if (pn->sort.sort_descs[k]) {
                        n = snprintf(buf + written, buflen - written, " DESC");
                        if (n > 0) written += n;
                    }
                }
            }
            n = snprintf(buf + written, buflen - written, ")");
            if (n > 0) written += n;
        }
    }
    n = snprintf(buf + written, buflen - written, "\n");
    if (n > 0) written += n;
    n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
    if (n > 0) written += n;
    return written;
}

static int explain_limit(struct plan_node *pn, char *buf, int buflen)
{
    if (pn->limit.has_limit && pn->limit.has_offset)
        return snprintf(buf, buflen, "Limit (%zu, offset %zu)\n",
                        pn->limit.limit, pn->limit.offset);
    if (pn->limit.has_limit)
        return snprintf(buf, buflen, "Limit (%zu)\n", pn->limit.limit);
    return snprintf(buf, buflen, "Limit\n");
}

static int explain_set_op(struct plan_node *pn, char *buf, int buflen)
{
    const char *opname = "SetOp";
    if (pn->set_op.set_op == 0) opname = pn->set_op.set_all ? "Append (UNION ALL)" : "HashSetOp Union";
    else if (pn->set_op.set_op == 1) opname = "HashSetOp Intersect";
    else if (pn->set_op.set_op == 2) opname = "HashSetOp Except";
    return snprintf(buf, buflen, "%s\n", opname);
}

/* Emit label + recurse into left child (common pattern for unary nodes). */
static int explain_unary(struct query_arena *arena, struct plan_node *pn,
                          const char *label, char *buf, int buflen, int depth)
{
    int written = 0, n;
    n = snprintf(buf, buflen, "%s\n", label);
    if (n > 0) written += n;
    n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
    if (n > 0) written += n;
    return written;
}

/* Emit label + recurse into left and right children (common pattern for binary nodes). */
static int explain_binary(struct query_arena *arena, struct plan_node *pn,
                           const char *label, char *buf, int buflen, int depth)
{
    int written = 0, n;
    n = snprintf(buf, buflen, "%s\n", label);
    if (n > 0) written += n;
    n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
    if (n > 0) written += n;
    n = plan_explain_node(arena, pn->right, buf + written, buflen - written, depth + 1);
    if (n > 0) written += n;
    return written;
}

static int plan_explain_node(struct query_arena *arena, uint32_t node_idx,
                              char *buf, int buflen, int depth)
{
    if (node_idx == IDX_NONE || buflen <= 0) return 0;
    struct plan_node *pn = &PLAN_NODE(arena, node_idx);
    int written = 0;
    int n;

    /* indent */
    for (int i = 0; i < depth * 2 && written < buflen - 1; i++)
        buf[written++] = ' ';

    switch (pn->op) {
    case PLAN_SEQ_SCAN:
        n = snprintf(buf + written, buflen - written, "Seq Scan on %s\n",
                     pn->seq_scan.table ? pn->seq_scan.table->name : "?");
        if (n > 0) written += n;
        break;
    case PLAN_INDEX_SCAN:
        n = explain_index_scan(arena, pn, buf + written, buflen - written);
        if (n > 0) written += n;
        break;
    case PLAN_FILTER:
        n = explain_filter(arena, pn, buf + written, buflen - written, depth);
        if (n > 0) written += n;
        break;
    case PLAN_PROJECT:
        n = explain_unary(arena, pn, "Project", buf + written, buflen - written, depth);
        if (n > 0) written += n;
        break;
    case PLAN_EXPR_PROJECT:
        n = explain_unary(arena, pn, "Project (expressions)", buf + written, buflen - written, depth);
        if (n > 0) written += n;
        break;
    case PLAN_SORT:
        n = explain_sort(arena, pn, buf + written, buflen - written, depth);
        if (n > 0) written += n;
        break;
    case PLAN_HASH_JOIN:
        n = explain_binary(arena, pn, "Hash Join", buf + written, buflen - written, depth);
        if (n > 0) written += n;
        break;
    case PLAN_HASH_SEMI_JOIN:
        n = explain_binary(arena, pn, "Hash Semi Join", buf + written, buflen - written, depth);
        if (n > 0) written += n;
        break;
    case PLAN_LIMIT:
        n = explain_limit(pn, buf + written, buflen - written);
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        break;
    case PLAN_DISTINCT:
        n = explain_unary(arena, pn, "HashAggregate (DISTINCT)", buf + written, buflen - written, depth);
        if (n > 0) written += n;
        break;
    case PLAN_HASH_AGG:
        n = explain_unary(arena, pn, "HashAggregate", buf + written, buflen - written, depth);
        if (n > 0) written += n;
        break;
    case PLAN_SIMPLE_AGG:
        n = explain_unary(arena, pn, "Aggregate", buf + written, buflen - written, depth);
        if (n > 0) written += n;
        break;
    case PLAN_SET_OP:
        n = explain_set_op(pn, buf + written, buflen - written);
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->right, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        break;
    case PLAN_WINDOW:
        n = explain_unary(arena, pn, "WindowAgg", buf + written, buflen - written, depth);
        if (n > 0) written += n;
        break;
    case PLAN_GENERATE_SERIES:
        n = snprintf(buf + written, buflen - written, "Function Scan on generate_series\n");
        if (n > 0) written += n;
        break;
    case PLAN_NESTED_LOOP:
        n = explain_binary(arena, pn, "Nested Loop", buf + written, buflen - written, depth);
        if (n > 0) written += n;
        break;
    }
    return written;
}

int plan_explain(struct query_arena *arena, uint32_t node_idx, char *buf, int buflen)
{
    int written = plan_explain_node(arena, node_idx, buf, buflen, 0);
    /* strip trailing newline */
    if (written > 0 && buf[written - 1] == '\n')
        buf[--written] = '\0';
    else if (written < buflen)
        buf[written] = '\0';
    return written;
}

/* ---- Plan builder helpers ---- */

/* Append a PLAN_LIMIT node if the query has LIMIT/OFFSET.
 * Returns the (possibly new) current node index. */
static uint32_t build_limit(uint32_t current, struct query_select *s,
                             struct query_arena *arena)
{
    if (!s->has_limit && !s->has_offset) return current;
    uint32_t limit_idx = plan_alloc_node(arena, PLAN_LIMIT);
    PLAN_NODE(arena, limit_idx).left = current;
    PLAN_NODE(arena, limit_idx).limit.has_limit = s->has_limit;
    PLAN_NODE(arena, limit_idx).limit.limit = s->has_limit ? (size_t)s->limit_count : 0;
    PLAN_NODE(arena, limit_idx).limit.has_offset = s->has_offset;
    PLAN_NODE(arena, limit_idx).limit.offset = s->has_offset ? (size_t)s->offset_count : 0;
    return limit_idx;
}

/* Check if a table has mixed cell types (e.g. after ALTER COLUMN TYPE).
 * Returns 1 if mixed types detected, 0 if safe for plan executor. */
static int table_has_mixed_types(struct table *tbl)
{
    if (tbl->rows.count == 0) return 0;
    struct row *first = &tbl->rows.items[0];
    for (size_t c = 0; c < first->cells.count && c < tbl->columns.count; c++) {
        if (!first->cells.items[c].is_null &&
            first->cells.items[c].type != tbl->columns.items[c].type)
            return 1;
    }
    return 0;
}

/* Append a PLAN_PROJECT node with a pre-resolved column map.
 * Returns the new node index. */
static uint32_t append_project_node(uint32_t current, struct query_arena *arena,
                                     uint16_t ncols, int *col_map)
{
    uint32_t proj_idx = plan_alloc_node(arena, PLAN_PROJECT);
    PLAN_NODE(arena, proj_idx).left = current;
    PLAN_NODE(arena, proj_idx).project.ncols = ncols;
    PLAN_NODE(arena, proj_idx).project.col_map = col_map;
    return proj_idx;
}

/* Create a PLAN_SEQ_SCAN node for a table with an identity column map.
 * Returns the new node index. */
static uint32_t build_seq_scan(struct table *tbl, struct query_arena *arena)
{
    uint16_t ncols = (uint16_t)tbl->columns.count;
    int *col_map = (int *)bump_alloc(&arena->scratch, ncols * sizeof(int));
    for (uint16_t i = 0; i < ncols; i++) col_map[i] = (int)i;
    uint32_t scan_idx = plan_alloc_node(arena, PLAN_SEQ_SCAN);
    PLAN_NODE(arena, scan_idx).seq_scan.table = tbl;
    PLAN_NODE(arena, scan_idx).seq_scan.ncols = ncols;
    PLAN_NODE(arena, scan_idx).seq_scan.col_map = col_map;
    PLAN_NODE(arena, scan_idx).est_rows = (double)tbl->rows.count;
    return scan_idx;
}

/* Append a PLAN_FILTER node with pre-validated parameters.
 * cond_idx may be IDX_NONE (e.g. for RHS/inner filters from parsed subqueries). */
static uint32_t append_filter_node(uint32_t current, struct query_arena *arena,
                                    uint32_t cond_idx, int col_idx,
                                    int cmp_op, struct cell cmp_val)
{
    uint32_t filter_idx = plan_alloc_node(arena, PLAN_FILTER);
    PLAN_NODE(arena, filter_idx).left = current;
    PLAN_NODE(arena, filter_idx).filter.cond_idx = cond_idx;
    PLAN_NODE(arena, filter_idx).filter.col_idx = col_idx;
    PLAN_NODE(arena, filter_idx).filter.cmp_op = cmp_op;
    PLAN_NODE(arena, filter_idx).filter.cmp_val = cmp_val;
    return filter_idx;
}

/* Forward declaration — defined later alongside other multi-table helpers */
static int find_col_in_tables_a(sv col, struct table **tables, uint16_t *offsets,
                                sv *aliases, int ntables);

/* Column resolver: abstracts column lookup for single-table vs multi-table contexts.
 * resolve(col, ctx) returns the column index (>=0) or -1 if not found.
 * col_type(col_idx, ctx) returns the column type for a resolved index. */
struct col_resolver {
    int (*resolve)(sv col, void *ctx);
    enum column_type (*col_type)(int col_idx, void *ctx);
    void *ctx;
};

/* Single-table resolver context */
struct single_table_ctx { struct table *tbl; };

static int single_table_resolve(sv col, void *ctx) {
    return table_find_column_sv(((struct single_table_ctx *)ctx)->tbl, col);
}
static enum column_type single_table_col_type(int col_idx, void *ctx) {
    return ((struct single_table_ctx *)ctx)->tbl->columns.items[col_idx].type;
}

/* Multi-table (join) resolver context */
struct multi_table_ctx {
    struct table **tables;
    uint16_t *offsets;
    sv *aliases;
    int ntables;
};

static int multi_table_resolve(sv col, void *ctx) {
    struct multi_table_ctx *m = (struct multi_table_ctx *)ctx;
    return find_col_in_tables_a(col, m->tables, m->offsets, m->aliases, m->ntables);
}
static enum column_type multi_table_col_type(int col_idx, void *ctx) {
    struct multi_table_ctx *m = (struct multi_table_ctx *)ctx;
    for (int ti = m->ntables - 1; ti >= 0; ti--) {
        if (col_idx >= m->offsets[ti]) {
            int local = col_idx - m->offsets[ti];
            return m->tables[ti]->columns.items[local].type;
        }
    }
    return COLUMN_TYPE_INT; /* fallback */
}

/* Try to validate and append an extended filter for a single COND_COMPARE.
 * Handles IS NULL, IS NOT NULL, BETWEEN, IN-list, LIKE/ILIKE in addition to
 * the basic comparison ops.  Returns current unchanged if not handleable. */
static uint32_t try_append_extended_filter_r(uint32_t current, struct col_resolver *cr,
                                              struct query_arena *plan_arena,
                                              struct query_arena *cond_arena,
                                              uint32_t cond_idx)
{
    if (cond_idx == IDX_NONE) return current;
    struct condition *cond = &COND(cond_arena, cond_idx);
    if (cond->type != COND_COMPARE) return current;
    if (cond->lhs_expr != IDX_NONE) return current;
    if (cond->rhs_column.len != 0) return current;
    if (cond->scalar_subquery_sql != IDX_NONE) return current;
    if (cond->subquery_sql != IDX_NONE) return current;
    if (cond->is_any || cond->is_all) return current;
    if (cond->array_values_count > 0) return current;

    int fc = cr->resolve(cond->column, cr->ctx);
    if (fc < 0) return current;
    enum column_type ct = cr->col_type(fc, cr->ctx);

    switch (cond->op) {

    case CMP_IS_NULL:
    case CMP_IS_NOT_NULL: {
        struct cell dummy = {0};
        uint32_t fi = plan_alloc_node(plan_arena, PLAN_FILTER);
        PLAN_NODE(plan_arena, fi).left = current;
        PLAN_NODE(plan_arena, fi).filter.cond_idx = cond_idx;
        PLAN_NODE(plan_arena, fi).filter.col_idx = fc;
        PLAN_NODE(plan_arena, fi).filter.cmp_op = (int)cond->op;
        PLAN_NODE(plan_arena, fi).filter.cmp_val = dummy;
        return fi;
    }

    case CMP_BETWEEN: {
        if (ct != COLUMN_TYPE_INT && ct != COLUMN_TYPE_BOOLEAN &&
            ct != COLUMN_TYPE_FLOAT && ct != COLUMN_TYPE_NUMERIC &&
            ct != COLUMN_TYPE_BIGINT && ct != COLUMN_TYPE_SMALLINT &&
            !column_type_is_text(ct))
            return current;
        uint32_t fi = plan_alloc_node(plan_arena, PLAN_FILTER);
        PLAN_NODE(plan_arena, fi).left = current;
        PLAN_NODE(plan_arena, fi).filter.cond_idx = cond_idx;
        PLAN_NODE(plan_arena, fi).filter.col_idx = fc;
        PLAN_NODE(plan_arena, fi).filter.cmp_op = CMP_BETWEEN;
        PLAN_NODE(plan_arena, fi).filter.cmp_val = cond->value;
        PLAN_NODE(plan_arena, fi).filter.between_high = cond->between_high;
        return fi;
    }

    case CMP_IN: {
        if (cond->in_values_count == 0 || cond->subquery_sql != IDX_NONE)
            return current;
        if (ct != COLUMN_TYPE_INT && ct != COLUMN_TYPE_BOOLEAN &&
            ct != COLUMN_TYPE_FLOAT && ct != COLUMN_TYPE_NUMERIC &&
            ct != COLUMN_TYPE_BIGINT && ct != COLUMN_TYPE_SMALLINT &&
            !column_type_is_text(ct))
            return current;
        /* Copy IN values into plan arena scratch */
        uint32_t nv = cond->in_values_count;
        struct cell *vals = (struct cell *)bump_alloc(&plan_arena->scratch,
                                                      nv * sizeof(struct cell));
        for (uint32_t i = 0; i < nv; i++) {
            vals[i] = cond_arena->cells.items[cond->in_values_start + i];
            /* Copy text pointers into plan arena so they survive subquery arena free */
            if (column_type_is_text(vals[i].type) && vals[i].value.as_text)
                vals[i].value.as_text = bump_strdup(&plan_arena->scratch, vals[i].value.as_text);
        }
        uint32_t fi = plan_alloc_node(plan_arena, PLAN_FILTER);
        PLAN_NODE(plan_arena, fi).left = current;
        PLAN_NODE(plan_arena, fi).filter.cond_idx = cond_idx;
        PLAN_NODE(plan_arena, fi).filter.col_idx = fc;
        PLAN_NODE(plan_arena, fi).filter.cmp_op = CMP_IN;
        PLAN_NODE(plan_arena, fi).filter.cmp_val = cond->value;
        PLAN_NODE(plan_arena, fi).filter.in_values = vals;
        PLAN_NODE(plan_arena, fi).filter.in_count = nv;
        return fi;
    }

    case CMP_LIKE:
    case CMP_ILIKE: {
        if (!column_type_is_text(ct)) return current;
        const char *pat = cond->value.value.as_text;
        if (!pat) return current;
        uint32_t fi = plan_alloc_node(plan_arena, PLAN_FILTER);
        PLAN_NODE(plan_arena, fi).left = current;
        PLAN_NODE(plan_arena, fi).filter.cond_idx = cond_idx;
        PLAN_NODE(plan_arena, fi).filter.col_idx = fc;
        PLAN_NODE(plan_arena, fi).filter.cmp_op = (int)cond->op;
        PLAN_NODE(plan_arena, fi).filter.cmp_val = cond->value;
        PLAN_NODE(plan_arena, fi).filter.like_pattern = bump_strdup(&plan_arena->scratch, pat);
        return fi;
    }

    case CMP_EQ:
    case CMP_NE:
    case CMP_LT:
    case CMP_GT:
    case CMP_LE:
    case CMP_GE:
    case CMP_NOT_IN:
    case CMP_IS_DISTINCT:
    case CMP_IS_NOT_DISTINCT:
    case CMP_EXISTS:
    case CMP_NOT_EXISTS:
    case CMP_REGEX_MATCH:
    case CMP_REGEX_NOT_MATCH:
        break;
    }

    /* Fall through to basic comparison handler */
    return current;
}

/* Validate that a condition tree can be fully handled by the plan filter.
 * Returns 1 if all leaves are handleable, 0 otherwise. */
static int validate_compound_filter_r(struct col_resolver *cr, struct query_arena *cond_arena,
                                       uint32_t cond_idx)
{
    if (cond_idx == IDX_NONE) return 0;
    struct condition *cond = &COND(cond_arena, cond_idx);

    if (cond->type == COND_AND || cond->type == COND_OR) {
        if (cond->left == IDX_NONE || cond->right == IDX_NONE) return 0;
        return validate_compound_filter_r(cr, cond_arena, cond->left) &&
               validate_compound_filter_r(cr, cond_arena, cond->right);
    }

    if (cond->type != COND_COMPARE) return 0;
    if (cond->lhs_expr != IDX_NONE) return 0;
    if (cond->rhs_column.len != 0) return 0;
    if (cond->scalar_subquery_sql != IDX_NONE) return 0;
    if (cond->is_any || cond->is_all) return 0;
    if (cond->array_values_count > 0) return 0;

    int fc = cr->resolve(cond->column, cr->ctx);
    if (fc < 0) return 0;
    enum column_type ct = cr->col_type(fc, cr->ctx);

    switch (cond->op) {

    case CMP_IS_NULL:
    case CMP_IS_NOT_NULL:
        return 1;

    case CMP_BETWEEN:
        return (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN ||
                ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC ||
                ct == COLUMN_TYPE_BIGINT || ct == COLUMN_TYPE_SMALLINT ||
                column_type_is_text(ct));

    case CMP_IN:
        if (cond->in_values_count == 0 || cond->subquery_sql != IDX_NONE)
            return 0;
        return (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN ||
                ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC ||
                ct == COLUMN_TYPE_BIGINT || ct == COLUMN_TYPE_SMALLINT ||
                column_type_is_text(ct));

    case CMP_LIKE:
    case CMP_ILIKE:
        return column_type_is_text(ct) && cond->value.value.as_text != NULL;

    case CMP_EQ:
    case CMP_NE:
    case CMP_LT:
    case CMP_GT:
    case CMP_LE:
    case CMP_GE:
        if (cond->subquery_sql != IDX_NONE) return 0;
        if (cond->in_values_count > 0) return 0;
        if (column_type_is_text(ct)) {
            if (!cond->value.is_null && !column_type_is_text(cond->value.type))
                return 0;
        } else if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN ||
                   ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC ||
                   ct == COLUMN_TYPE_BIGINT || ct == COLUMN_TYPE_SMALLINT) {
            if (!cond->value.is_null && cond->value.type != ct &&
                !(ct == COLUMN_TYPE_FLOAT && cond->value.type == COLUMN_TYPE_INT))
                return 0;
            if (ct == COLUMN_TYPE_INT && cond->value.type == COLUMN_TYPE_FLOAT)
                return 0;
        } else {
            return 0;
        }
        return 1;

    case CMP_NOT_IN:
    case CMP_IS_DISTINCT:
    case CMP_IS_NOT_DISTINCT:
    case CMP_EXISTS:
    case CMP_NOT_EXISTS:
    case CMP_REGEX_MATCH:
    case CMP_REGEX_NOT_MATCH:
        return 0;
    }
    __builtin_unreachable();
}

/* Append a single filter leaf (basic or extended) using resolver. */
static uint32_t append_single_filter_leaf_r(uint32_t current, struct col_resolver *cr,
                                             struct query_arena *plan_arena,
                                             struct query_arena *cond_arena,
                                             uint32_t cond_idx)
{
    struct condition *cond = &COND(cond_arena, cond_idx);

    /* Try extended ops first (IS NULL, BETWEEN, IN-list, LIKE) */
    uint32_t ext = try_append_extended_filter_r(current, cr, plan_arena, cond_arena, cond_idx);
    if (ext != current) return ext;

    /* Basic comparison (CMP_EQ..CMP_GE) */
    int fc = cr->resolve(cond->column, cr->ctx);
    return append_filter_node(current, plan_arena, cond_idx, fc,
                              (int)cond->op, cond->value);
}

/* Recursively decompose a compound WHERE (COND_AND tree) into stacked
 * PLAN_FILTER nodes using resolver.  Caller must validate first. */
static uint32_t append_compound_filter_r(uint32_t current, struct col_resolver *cr,
                                          struct query_arena *plan_arena,
                                          struct query_arena *cond_arena,
                                          uint32_t cond_idx)
{
    if (cond_idx == IDX_NONE) return current;
    struct condition *cond = &COND(cond_arena, cond_idx);

    if (cond->type == COND_AND) {
        current = append_compound_filter_r(current, cr, plan_arena, cond_arena, cond->left);
        current = append_compound_filter_r(current, cr, plan_arena, cond_arena, cond->right);
        return current;
    }

    /* COND_OR: create a single filter node with col_idx=-1.
     * filter_next will use filter_eval_cond_columnar for runtime evaluation. */
    if (cond->type == COND_OR) {
        struct cell dummy = {0};
        uint32_t fi = plan_alloc_node(plan_arena, PLAN_FILTER);
        PLAN_NODE(plan_arena, fi).left = current;
        PLAN_NODE(plan_arena, fi).filter.cond_idx = cond_idx;
        PLAN_NODE(plan_arena, fi).filter.col_idx = -1;
        PLAN_NODE(plan_arena, fi).filter.cmp_op = 0;
        PLAN_NODE(plan_arena, fi).filter.cmp_val = dummy;
        return fi;
    }

    return append_single_filter_leaf_r(current, cr, plan_arena, cond_arena, cond_idx);
}

/* Try to validate and append a compound filter.  Returns current unchanged
 * if the condition tree can't be fully handled. */
static uint32_t try_append_compound_filter_r(uint32_t current, struct col_resolver *cr,
                                              struct query_arena *plan_arena,
                                              struct query_arena *cond_arena,
                                              uint32_t cond_idx)
{
    if (cond_idx == IDX_NONE) return current;
    if (!validate_compound_filter_r(cr, cond_arena, cond_idx))
        return current;
    return append_compound_filter_r(current, cr, plan_arena, cond_arena, cond_idx);
}

/* ---- Backward-compatible single-table wrappers ---- */

static struct col_resolver make_single_table_resolver(struct single_table_ctx *stc, struct table *tbl) {
    stc->tbl = tbl;
    return (struct col_resolver){ single_table_resolve, single_table_col_type, stc };
}

static int validate_compound_filter(struct table *tbl, struct query_arena *cond_arena,
                                     uint32_t cond_idx) {
    struct single_table_ctx stc;
    struct col_resolver cr = make_single_table_resolver(&stc, tbl);
    return validate_compound_filter_r(&cr, cond_arena, cond_idx);
}

static uint32_t try_append_compound_filter(uint32_t current, struct table *tbl,
                                            struct query_arena *plan_arena,
                                            struct query_arena *cond_arena,
                                            uint32_t cond_idx) {
    struct single_table_ctx stc;
    struct col_resolver cr = make_single_table_resolver(&stc, tbl);
    return try_append_compound_filter_r(current, &cr, plan_arena, cond_arena, cond_idx);
}

/* Try to validate and append a simple comparison filter for a WHERE clause.
 * Handles COND_COMPARE with col op literal (numeric or text).
 * Returns current unchanged if the WHERE can't be handled as a plan filter.
 * plan_arena is used for node allocation; cond_arena for condition lookup
 * (they differ when the condition comes from a parsed sub-query). */
static uint32_t try_append_having_filter(uint32_t current,
                                          struct query_select *s,
                                          struct query_arena *arena);

static uint32_t try_append_simple_filter(uint32_t current, struct table *tbl,
                                          struct query_arena *plan_arena,
                                          struct query_arena *cond_arena,
                                          uint32_t where_cond_idx)
{
    if (where_cond_idx == IDX_NONE) return current;
    struct condition *cond = &COND(cond_arena, where_cond_idx);
    if (cond->type != COND_COMPARE) return current;
    if (cond->lhs_expr != IDX_NONE) return current;
    if (cond->rhs_column.len != 0) return current;
    if (cond->scalar_subquery_sql != IDX_NONE) return current;
    if (cond->subquery_sql != IDX_NONE) return current;
    if (cond->in_values_count > 0) return current;
    if (cond->array_values_count > 0) return current;
    if (cond->is_any || cond->is_all) return current;
    if (cond->op > CMP_GE) return current;

    int fc = table_find_column_sv(tbl, cond->column);
    if (fc < 0) return current;
    enum column_type ct = tbl->columns.items[fc].type;

    if (column_type_is_text(ct)) {
        if (!cond->value.is_null && !column_type_is_text(cond->value.type))
            return current;
    } else if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN ||
               ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC ||
               ct == COLUMN_TYPE_BIGINT) {
        if (!cond->value.is_null && cond->value.type != ct &&
            !(ct == COLUMN_TYPE_FLOAT && cond->value.type == COLUMN_TYPE_INT))
            return current;
        if (ct == COLUMN_TYPE_INT && cond->value.type == COLUMN_TYPE_FLOAT)
            return current;
    } else {
        return current;
    }

    return append_filter_node(current, plan_arena, where_cond_idx, fc,
                              (int)cond->op, cond->value);
}

/* Append a PLAN_SORT node given pre-resolved sort arrays.
 * sort_nf_buf may be NULL (no NULLS FIRST/LAST info).
 * Returns the (possibly new) current node index, unchanged if nsort==0. */
static uint32_t append_sort_node(uint32_t current, struct query_arena *arena,
                                 const int *cols_buf, const int *descs_buf,
                                 const int *nf_buf, uint16_t nsort)
{
    if (nsort == 0) return current;
    int *sort_cols = (int *)bump_alloc(&arena->scratch, nsort * sizeof(int));
    int *sort_descs = (int *)bump_alloc(&arena->scratch, nsort * sizeof(int));
    memcpy(sort_cols, cols_buf, nsort * sizeof(int));
    memcpy(sort_descs, descs_buf, nsort * sizeof(int));
    uint32_t sort_idx = plan_alloc_node(arena, PLAN_SORT);
    PLAN_NODE(arena, sort_idx).left = current;
    PLAN_NODE(arena, sort_idx).sort.sort_cols = sort_cols;
    PLAN_NODE(arena, sort_idx).sort.sort_descs = sort_descs;
    PLAN_NODE(arena, sort_idx).sort.nsort_cols = nsort;
    if (nf_buf) {
        int *sort_nf = (int *)bump_alloc(&arena->scratch, nsort * sizeof(int));
        memcpy(sort_nf, nf_buf, nsort * sizeof(int));
        PLAN_NODE(arena, sort_idx).sort.sort_nulls_first = sort_nf;
    }
    return sort_idx;
}

/* ---- Plan builder: join path ---- */

/* Build a lightweight merged table descriptor on the bump allocator for
 * eval_expr column resolution across multiple joined tables.
 * Column names are bump-allocated strings: both "alias.col" qualified names
 * and bare "col" names are added so eval_expr can resolve either form.
 * The table has no rows — only column metadata for name lookup. */
static struct table *build_merged_table_desc(struct table **tables, int ntables,
                                              sv *aliases, uint16_t cum_cols,
                                              struct query_arena *arena)
{
    struct table *mt = (struct table *)bump_calloc(&arena->scratch, 1, sizeof(struct table));
    mt->columns.items = (struct column *)bump_calloc(&arena->scratch, cum_cols, sizeof(struct column));
    mt->columns.count = cum_cols;
    mt->columns.capacity = cum_cols;

    size_t ci = 0;
    for (int ti = 0; ti < ntables; ti++) {
        sv alias = aliases[ti];
        for (size_t c = 0; c < tables[ti]->columns.count; c++) {
            const char *base = tables[ti]->columns.items[c].name;
            size_t base_len = strlen(base);
            if (alias.len > 0) {
                /* "alias.col" qualified name */
                size_t qlen = alias.len + 1 + base_len + 1;
                char *qname = (char *)bump_alloc(&arena->scratch, qlen);
                memcpy(qname, alias.data, alias.len);
                qname[alias.len] = '.';
                memcpy(qname + alias.len + 1, base, base_len);
                qname[qlen - 1] = '\0';
                mt->columns.items[ci].name = qname;
            } else {
                /* bare column name (bump-copy) */
                char *name = (char *)bump_alloc(&arena->scratch, base_len + 1);
                memcpy(name, base, base_len + 1);
                mt->columns.items[ci].name = name;
            }
            mt->columns.items[ci].type = tables[ti]->columns.items[c].type;
            ci++;
        }
    }
    return mt;
}

/* Find a column in the merged column space.
 * table_prefix: optional table alias/name qualifier (empty sv = unqualified).
 * col: the bare column name.
 * aliases: optional per-table aliases for prefix matching (NULL = none). */
static int find_col_in_tables_q(sv table_prefix, sv col,
                                struct table **tables, uint16_t *offsets,
                                sv *aliases, int ntables)
{
    /* If there's a qualifier, match it against aliases or table names first */
    if (table_prefix.len > 0) {
        for (int ti = 0; ti < ntables; ti++) {
            int match = 0;
            if (aliases && aliases[ti].len > 0 && sv_eq_ignorecase(table_prefix, aliases[ti]))
                match = 1;
            else if (tables[ti]->name && sv_eq_ignorecase_cstr(table_prefix, tables[ti]->name))
                match = 1;
            if (match) {
                int ci = table_find_column_sv(tables[ti], col);
                if (ci >= 0) return offsets[ti] + ci;
            }
        }
    }

    /* No qualifier or qualifier didn't match — search all tables for bare name */
    for (int ti = 0; ti < ntables; ti++) {
        int ci = table_find_column_sv(tables[ti], col);
        if (ci >= 0) return offsets[ti] + ci;
    }
    return -1;
}

/* Legacy wrapper: parses "table.col" from a single sv (e.g. ORDER BY, GROUP BY). */
static int find_col_in_tables_a(sv col, struct table **tables, uint16_t *offsets,
                                sv *aliases, int ntables)
{
    sv prefix = {0};
    sv bare = col;
    for (size_t p = 0; p < col.len; p++) {
        if (col.data[p] == '.') {
            prefix = sv_from(col.data, p);
            bare = sv_from(col.data + p + 1, col.len - p - 1);
            break;
        }
    }
    return find_col_in_tables_q(prefix, bare, tables, offsets, aliases, ntables);
}

/* Extract equi-join key columns from a join_info.
 * On success, sets *outer_key (index in left table) and *inner_key (index in right table).
 * Returns 0 on success, -1 on failure. */
static int extract_join_keys(struct join_info *ji, struct query_arena *arena,
                             struct table **left_tables, uint16_t *left_offsets,
                             sv *left_aliases, int left_ntables,
                             struct table *right_table,
                             int *outer_key, int *inner_key)
{
    if (ji->join_on_cond != IDX_NONE) {
        struct condition *cond = &COND(arena, ji->join_on_cond);
        if (cond->type != COND_COMPARE || cond->op != CMP_EQ) return -1;
        if (cond->lhs_expr != IDX_NONE) return -1;
        if (cond->column.len == 0 || cond->rhs_column.len == 0) return -1;

        int lhs_left = find_col_in_tables_a(cond->column, left_tables, left_offsets, left_aliases, left_ntables);
        int lhs_right = table_find_column_sv(right_table, cond->column);
        int rhs_left = find_col_in_tables_a(cond->rhs_column, left_tables, left_offsets, left_aliases, left_ntables);
        int rhs_right = table_find_column_sv(right_table, cond->rhs_column);

        /* Strip prefix for right-table lookup */
        if (lhs_right < 0) {
            sv bare = cond->column;
            for (size_t p = 0; p < cond->column.len; p++)
                if (cond->column.data[p] == '.') { bare = sv_from(cond->column.data + p + 1, cond->column.len - p - 1); break; }
            lhs_right = table_find_column_sv(right_table, bare);
        }
        if (rhs_right < 0) {
            sv bare = cond->rhs_column;
            for (size_t p = 0; p < cond->rhs_column.len; p++)
                if (cond->rhs_column.data[p] == '.') { bare = sv_from(cond->rhs_column.data + p + 1, cond->rhs_column.len - p - 1); break; }
            rhs_right = table_find_column_sv(right_table, bare);
        }

        if (lhs_left >= 0 && rhs_right >= 0) {
            *outer_key = lhs_left; *inner_key = rhs_right; return 0;
        } else if (rhs_left >= 0 && lhs_right >= 0) {
            *outer_key = rhs_left; *inner_key = lhs_right; return 0;
        }
        return -1;
    } else if (ji->join_left_col.len > 0 && ji->join_right_col.len > 0 &&
               ji->join_op == CMP_EQ) {
        int left_l = find_col_in_tables_a(ji->join_left_col, left_tables, left_offsets, left_aliases, left_ntables);
        int left_r = table_find_column_sv(right_table, ji->join_left_col);
        int right_l = find_col_in_tables_a(ji->join_right_col, left_tables, left_offsets, left_aliases, left_ntables);
        int right_r = table_find_column_sv(right_table, ji->join_right_col);
        if (left_l >= 0 && right_r >= 0) {
            *outer_key = left_l; *inner_key = right_r; return 0;
        } else if (right_l >= 0 && left_r >= 0) {
            *outer_key = right_l; *inner_key = left_r; return 0;
        }
        return -1;
    }
    return -1;
}

/* Try to build a plan for a join query (single or multi-table).
 * Supports: INNER equi-joins, post-join GROUP BY + aggregates, ORDER BY. */
static struct plan_result build_join(struct table *t, struct query_select *s,
                                     struct query_arena *arena, struct database *db)
{
    /* Bail out for unsupported features */
    if (s->select_exprs_count > 0) return PLAN_RES_NOTIMPL; /* window functions */
    if (s->has_set_op)          return PLAN_RES_NOTIMPL;
    if (s->ctes_count > 0)     return PLAN_RES_NOTIMPL;
    if (s->cte_sql != IDX_NONE) return PLAN_RES_NOTIMPL;
    if (s->from_subquery_sql != IDX_NONE) return PLAN_RES_NOTIMPL;
    if (s->has_recursive_cte)   return PLAN_RES_NOTIMPL;
    if (s->has_distinct_on)     return PLAN_RES_NOTIMPL;
    if (s->insert_rows_count > 0) return PLAN_RES_NOTIMPL;
    /* HAVING without GROUP BY is handled in build_simple_agg */
    if (s->group_by_rollup || s->group_by_cube) return PLAN_RES_NOTIMPL;

    struct table *t1 = t;
    if (!t1) return PLAN_RES_ERR;
    if (table_has_mixed_types(t1)) return PLAN_RES_NOTIMPL;

    /* Collect all tables, aliases, and validate all joins are INNER equi-joins */
    int max_tables = (int)s->joins_count + 1;
    struct table **tables = (struct table **)bump_alloc(&arena->scratch,
                                                         max_tables * sizeof(struct table *));
    uint16_t *offsets = (uint16_t *)bump_alloc(&arena->scratch,
                                                max_tables * sizeof(uint16_t));
    sv *aliases = (sv *)bump_alloc(&arena->scratch, max_tables * sizeof(sv));
    int ntables = 0;

    tables[ntables] = t1;
    offsets[ntables] = 0;
    aliases[ntables] = s->table_alias;
    ntables++;

    uint16_t cum_cols = (uint16_t)t1->columns.count;

    int has_non_inner = 0;
    for (uint32_t j = 0; j < s->joins_count; j++) {
        struct join_info *ji = &arena->joins.items[s->joins_start + j];
        if (ji->join_type == 4) return PLAN_RES_NOTIMPL; /* CROSS join */
        if (ji->is_lateral)     return PLAN_RES_NOTIMPL;
        if (ji->is_natural)     return PLAN_RES_NOTIMPL;
        if (ji->has_using)      return PLAN_RES_NOTIMPL;
        if (ji->join_type != 0) has_non_inner = 1;

        struct table *tn = db_find_table_sv(db, ji->join_table);
        if (!tn) return PLAN_RES_ERR;
        if (tn->view_sql) return PLAN_RES_NOTIMPL;
        if (table_has_mixed_types(tn)) return PLAN_RES_NOTIMPL;

        tables[ntables] = tn;
        offsets[ntables] = cum_cols;
        aliases[ntables] = ji->join_alias;
        ntables++;
        cum_cols += (uint16_t)tn->columns.count;
    }
    /* Multi-table non-INNER joins need the legacy executor */
    if (ntables > 2 && has_non_inner) return PLAN_RES_NOTIMPL;

    /* Extract join keys for each join */
    int *outer_keys = (int *)bump_alloc(&arena->scratch, s->joins_count * sizeof(int));
    int *inner_keys = (int *)bump_alloc(&arena->scratch, s->joins_count * sizeof(int));
    for (uint32_t j = 0; j < s->joins_count; j++) {
        struct join_info *ji = &arena->joins.items[s->joins_start + j];
        if (extract_join_keys(ji, arena, tables, offsets, aliases, (int)(j + 1),
                              tables[j + 1], &outer_keys[j], &inner_keys[j]) != 0)
            return PLAN_RES_ERR;
        /* Bail out for cross-type join keys (e.g. INT vs FLOAT) */
        enum column_type inner_kt = tables[j + 1]->columns.items[inner_keys[j]].type;
        /* Find outer key's type in the cumulative column space */
        enum column_type outer_kt = COLUMN_TYPE_INT;
        for (int ti = (int)j; ti >= 0; ti--) {
            if (outer_keys[j] >= offsets[ti]) {
                int local = outer_keys[j] - offsets[ti];
                outer_kt = tables[ti]->columns.items[local].type;
                break;
            }
        }
        if (outer_kt != inner_kt) return PLAN_RES_ERR;
    }

    /* --- Resolve post-join operations --- */

    int has_agg = (s->has_group_by && s->aggregates_count > 0);
    int select_all_join = sv_eq_cstr(s->columns, "*");
    /* Detect table.* pattern (e.g. SELECT t.*) and treat as SELECT * */
    if (!select_all_join && s->parsed_columns_count == 1) {
        struct select_column *sc0 = &arena->select_cols.items[s->parsed_columns_start];
        if (sc0->expr_idx != IDX_NONE) {
            struct expr *e0 = &EXPR(arena, sc0->expr_idx);
            if (e0->type == EXPR_COLUMN_REF && e0->column_ref.column.len == 1 &&
                e0->column_ref.column.data[0] == '*')
                select_all_join = 1;
        }
    }

    /* If we have aggregates, validate them */
    int *agg_col_idxs = NULL;
    int *grp_col_idxs = NULL;
    if (has_agg) {
        /* Validate aggregates */
        agg_col_idxs = (int *)bump_alloc(&arena->scratch, s->aggregates_count * sizeof(int));
        for (uint32_t a = 0; a < s->aggregates_count; a++) {
            struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
            if (ae->func == AGG_STRING_AGG || ae->func == AGG_ARRAY_AGG || ae->func == AGG_NONE)
                return PLAN_RES_NOTIMPL;
            if (ae->has_distinct && ae->func != AGG_COUNT) return PLAN_RES_NOTIMPL;
            if (ae->expr_idx != IDX_NONE) {
                agg_col_idxs[a] = -2; /* expression-based aggregate */
            } else if (sv_eq_cstr(ae->column, "*")) {
                agg_col_idxs[a] = -1; /* COUNT(*) */
            } else {
                int ci = find_col_in_tables_a(ae->column, tables, offsets, aliases, ntables);
                if (ci < 0) return PLAN_RES_ERR;
                agg_col_idxs[a] = ci;
            }
        }

        /* Validate GROUP BY columns */
        grp_col_idxs = (int *)bump_alloc(&arena->scratch, s->group_by_count * sizeof(int));
        for (uint32_t g = 0; g < s->group_by_count; g++) {
            sv gcol = arena->svs.items[s->group_by_start + g];
            grp_col_idxs[g] = find_col_in_tables_a(gcol, tables, offsets, aliases, ntables);
            if (grp_col_idxs[g] < 0) return PLAN_RES_ERR;
        }
    }

    /* Resolve projection if no aggregates */
    int need_project_join = 0;
    int *proj_map_join = NULL;
    uint16_t proj_ncols_join = 0;

    int  join_sort_cols[MAX_SORT_KEYS];
    int  join_sort_descs[MAX_SORT_KEYS];
    int  join_sort_nf[MAX_SORT_KEYS];
    uint16_t join_sort_nord = 0;

    int need_expr_project_join = 0;
    uint32_t *expr_proj_indices_join = NULL;

    if (!has_agg) {
        if (select_all_join) {
            /* No projection */
        } else if (s->parsed_columns_count > 0) {
            proj_ncols_join = (uint16_t)s->parsed_columns_count;
            proj_map_join = (int *)bump_alloc(&arena->scratch, proj_ncols_join * sizeof(int));
            int all_column_refs = 1;
            for (uint32_t i = 0; i < s->parsed_columns_count; i++) {
                struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + i];
                if (sc->expr_idx == IDX_NONE) { all_column_refs = 0; break; }
                struct expr *e = &EXPR(arena, sc->expr_idx);
                if (e->type != EXPR_COLUMN_REF) { all_column_refs = 0; break; }
                int ci = find_col_in_tables_q(e->column_ref.table, e->column_ref.column, tables, offsets, aliases, ntables);
                if (ci < 0) { all_column_refs = 0; break; }
                proj_map_join[i] = ci;
            }
            if (all_column_refs) {
                need_project_join = 1;
            } else {
                /* Fall back to expression projection */
                expr_proj_indices_join = (uint32_t *)bump_alloc(&arena->scratch,
                    proj_ncols_join * sizeof(uint32_t));
                int expr_ok = 1;
                for (uint32_t i = 0; i < s->parsed_columns_count; i++) {
                    struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + i];
                    if (sc->expr_idx == IDX_NONE) { expr_ok = 0; break; }
                    struct expr *e = &EXPR(arena, sc->expr_idx);
                    if (e->type == EXPR_SUBQUERY) { expr_ok = 0; break; }
                    expr_proj_indices_join[i] = sc->expr_idx;
                }
                if (!expr_ok) return PLAN_RES_NOTIMPL;
                need_expr_project_join = 1;
            }
        } else {
            return PLAN_RES_NOTIMPL;
        }

        /* Pre-validate ORDER BY columns in merged column space */
        if (s->has_order_by && s->order_by_count > 0) {
            join_sort_nord = s->order_by_count < MAX_SORT_KEYS ? (uint16_t)s->order_by_count : MAX_SORT_KEYS;
            for (uint16_t k = 0; k < join_sort_nord; k++) {
                struct order_by_item *obi = &arena->order_items.items[s->order_by_start + k];
                join_sort_descs[k] = obi->desc;
                join_sort_nf[k] = obi->nulls_first;
                join_sort_cols[k] = find_col_in_tables_a(obi->column, tables, offsets, aliases, ntables);
                /* Try matching against SELECT aliases */
                if (join_sort_cols[k] < 0 && need_project_join) {
                    for (uint32_t pc = 0; pc < s->parsed_columns_count; pc++) {
                        struct select_column *scp = &arena->select_cols.items[s->parsed_columns_start + pc];
                        if (scp->alias.len > 0 && sv_eq_ignorecase(obi->column, scp->alias)) {
                            join_sort_cols[k] = proj_map_join[pc];
                            break;
                        }
                    }
                }
                if (join_sort_cols[k] < 0) return PLAN_RES_ERR;
            }
        }
    }

    /* Pre-validate WHERE clause for post-join filter using multi-table resolver */
    struct multi_table_ctx mtc = { tables, offsets, aliases, ntables };
    struct col_resolver join_cr = { multi_table_resolve, multi_table_col_type, &mtc };
    int join_filter_ok = 0;
    if (s->where.has_where && s->where.where_cond != IDX_NONE) {
        if (validate_compound_filter_r(&join_cr, arena, s->where.where_cond))
            join_filter_ok = 1;
        else
            return PLAN_RES_NOTIMPL;
    }

    /* --- All validation passed, build plan nodes --- */

    /* Build left-deep hash join tree */
    uint32_t current = build_seq_scan(tables[0], arena);

    for (uint32_t j = 0; j < s->joins_count; j++) {
        struct join_info *ji = &arena->joins.items[s->joins_start + j];
        uint32_t right_scan = build_seq_scan(tables[j + 1], arena);

        uint32_t join_idx = plan_alloc_node(arena, PLAN_HASH_JOIN);
        PLAN_NODE(arena, join_idx).left = current;
        PLAN_NODE(arena, join_idx).right = right_scan;
        PLAN_NODE(arena, join_idx).hash_join.outer_key_col = outer_keys[j];
        PLAN_NODE(arena, join_idx).hash_join.inner_key_col = inner_keys[j];
        PLAN_NODE(arena, join_idx).hash_join.join_type = ji->join_type;
        current = join_idx;
    }

    /* Append post-join WHERE filter(s) */
    if (join_filter_ok)
        current = append_compound_filter_r(current, &join_cr, arena, arena, s->where.where_cond);

    if (has_agg) {
        /* Append HASH_AGG node */
        uint32_t agg_idx = plan_alloc_node(arena, PLAN_HASH_AGG);
        PLAN_NODE(arena, agg_idx).left = current;
        PLAN_NODE(arena, agg_idx).hash_agg.ngroup_cols = (uint16_t)s->group_by_count;
        PLAN_NODE(arena, agg_idx).hash_agg.group_cols = grp_col_idxs;
        PLAN_NODE(arena, agg_idx).hash_agg.agg_start = s->aggregates_start;
        PLAN_NODE(arena, agg_idx).hash_agg.agg_count = s->aggregates_count;
        PLAN_NODE(arena, agg_idx).hash_agg.agg_before_cols = s->agg_before_cols;
        PLAN_NODE(arena, agg_idx).hash_agg.agg_col_indices = agg_col_idxs;
        /* Build merged table descriptor for eval_expr column resolution */
        PLAN_NODE(arena, agg_idx).hash_agg.table =
            build_merged_table_desc(tables, ntables, aliases, cum_cols, arena);
        current = agg_idx;

        /* Append HAVING filter if present */
        if (s->has_having && s->having_cond != IDX_NONE) {
            current = try_append_having_filter(current, s, arena);
            if (current == IDX_NONE) return PLAN_RES_NOTIMPL;
        }

        /* Append SORT if ORDER BY present */
        if (s->has_order_by && s->order_by_count > 0) {
            uint16_t ngrp = (uint16_t)s->group_by_count;
            uint32_t agg_n = s->aggregates_count;
            uint16_t agg_offset = s->agg_before_cols ? 0 : ngrp;
            uint16_t grp_offset = s->agg_before_cols ? (uint16_t)agg_n : 0;

            int sort_cols_buf[MAX_SORT_KEYS];
            int sort_descs_buf[MAX_SORT_KEYS];
            uint16_t sort_nord = s->order_by_count < MAX_SORT_KEYS ? (uint16_t)s->order_by_count : MAX_SORT_KEYS;
            int sort_ok = 1;

            for (uint16_t k = 0; k < sort_nord; k++) {
                struct order_by_item *obi = &arena->order_items.items[s->order_by_start + k];
                sort_cols_buf[k] = -1;
                sort_descs_buf[k] = obi->desc;

                /* Match GROUP BY columns */
                for (uint32_t g = 0; g < s->group_by_count; g++) {
                    sv gcol = arena->svs.items[s->group_by_start + g];
                    if (sv_eq_ignorecase(obi->column, gcol)) {
                        sort_cols_buf[k] = (int)(grp_offset + g);
                        break;
                    }
                    /* Strip prefixes and compare */
                    sv bare_ord = obi->column, bare_grp = gcol;
                    for (size_t p = 0; p < obi->column.len; p++)
                        if (obi->column.data[p] == '.') { bare_ord = sv_from(obi->column.data + p + 1, obi->column.len - p - 1); break; }
                    for (size_t p = 0; p < gcol.len; p++)
                        if (gcol.data[p] == '.') { bare_grp = sv_from(gcol.data + p + 1, gcol.len - p - 1); break; }
                    if (sv_eq_ignorecase(bare_ord, bare_grp)) {
                        sort_cols_buf[k] = (int)(grp_offset + g);
                        break;
                    }
                }

                /* Match aggregate aliases or function names */
                if (sort_cols_buf[k] < 0) {
                    for (uint32_t a = 0; a < agg_n; a++) {
                        struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
                        if (ae->alias.len > 0 && sv_eq_ignorecase(obi->column, ae->alias)) {
                            sort_cols_buf[k] = (int)(agg_offset + a);
                            break;
                        }
                        const char *fname = NULL;
                        switch (ae->func) {
                            case AGG_SUM:        fname = "sum";   break;
                            case AGG_COUNT:      fname = "count"; break;
                            case AGG_AVG:        fname = "avg";   break;
                            case AGG_MIN:        fname = "min";   break;
                            case AGG_MAX:        fname = "max";   break;
                            case AGG_STRING_AGG: break;
                            case AGG_ARRAY_AGG:  break;
                            case AGG_NONE:       break;
                        }
                        if (fname && sv_eq_ignorecase(obi->column, sv_from_cstr(fname))) {
                            sort_cols_buf[k] = (int)(agg_offset + a);
                            break;
                        }
                    }
                }

                if (sort_cols_buf[k] < 0) { sort_ok = 0; break; }
            }

            if (sort_ok && sort_nord > 0)
                current = append_sort_node(current, arena, sort_cols_buf, sort_descs_buf, NULL, sort_nord);
        }
    } else {
        /* No aggregates — sort on merged columns, then project */
        if (join_sort_nord > 0)
            current = append_sort_node(current, arena, join_sort_cols, join_sort_descs, join_sort_nf, join_sort_nord);
        if (need_project_join)
            current = append_project_node(current, arena, proj_ncols_join, proj_map_join);
        if (need_expr_project_join) {
            struct table *merged = build_merged_table_desc(tables, ntables, aliases, cum_cols, arena);
            uint32_t eproj_idx = plan_alloc_node(arena, PLAN_EXPR_PROJECT);
            PLAN_NODE(arena, eproj_idx).left = current;
            PLAN_NODE(arena, eproj_idx).expr_project.ncols = proj_ncols_join;
            PLAN_NODE(arena, eproj_idx).expr_project.expr_indices = expr_proj_indices_join;
            PLAN_NODE(arena, eproj_idx).expr_project.table = merged;
            current = eproj_idx;
        }
    }

    if (s->has_distinct) {
        uint32_t dist_idx = plan_alloc_node(arena, PLAN_DISTINCT);
        PLAN_NODE(arena, dist_idx).left = current;
        current = dist_idx;
    }

    current = build_limit(current, s, arena);

    return PLAN_RES_OK(current);
}

/* ---- Plan builder: set operations path ---- */

/* Try to build a plan for a UNION/INTERSECT/EXCEPT query. */
static struct plan_result build_set_op(struct table *t, struct query_select *s,
                                       struct query_arena *arena, struct database *db)
{
    /* Parse the RHS SQL */
    const char *rhs_sql = ASTRING(arena, s->set_rhs_sql);
    struct query rhs_q = {0};
    if (query_parse(rhs_sql, &rhs_q) != 0) return PLAN_RES_ERR;
    if (rhs_q.query_type != QUERY_TYPE_SELECT) { query_free(&rhs_q); return PLAN_RES_ERR; }

    struct query_select *rs = &rhs_q.select;
    /* Only handle simple single-table RHS SELECTs */
    if (rs->has_join || rs->has_group_by || rs->aggregates_count > 0 ||
        rs->has_set_op || rs->ctes_count > 0 || rs->has_distinct ||
        rs->from_subquery_sql != IDX_NONE || rs->has_generate_series ||
        rs->select_exprs_count > 0) {
        query_free(&rhs_q);
        return PLAN_RES_NOTIMPL;
    }

    struct table *t2 = db_find_table_sv(db, rs->table);
    if (!t2 || t2->view_sql) { query_free(&rhs_q); return PLAN_RES_ERR; }

    /* Resolve LHS columns */
    int select_all_lhs = sv_eq_cstr(s->columns, "*");
    uint16_t lhs_ncols;
    int *lhs_col_map;
    if (select_all_lhs) {
        lhs_ncols = (uint16_t)t->columns.count;
        lhs_col_map = (int *)bump_alloc(&arena->scratch, lhs_ncols * sizeof(int));
        for (uint16_t i = 0; i < lhs_ncols; i++) lhs_col_map[i] = (int)i;
    } else if (s->parsed_columns_count > 0) {
        lhs_ncols = (uint16_t)s->parsed_columns_count;
        lhs_col_map = (int *)bump_alloc(&arena->scratch, lhs_ncols * sizeof(int));
        for (uint32_t i = 0; i < s->parsed_columns_count; i++) {
            struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + i];
            if (sc->expr_idx == IDX_NONE) { query_free(&rhs_q); return PLAN_RES_NOTIMPL; }
            struct expr *e = &EXPR(arena, sc->expr_idx);
            if (e->type != EXPR_COLUMN_REF) { query_free(&rhs_q); return PLAN_RES_NOTIMPL; }
            int ci = table_find_column_sv(t, e->column_ref.column);
            if (ci < 0) { query_free(&rhs_q); return PLAN_RES_ERR; }
            lhs_col_map[i] = ci;
        }
    } else {
        query_free(&rhs_q);
        return PLAN_RES_NOTIMPL;
    }

    /* Resolve RHS columns */
    int select_all_rhs = sv_eq_cstr(rs->columns, "*");
    uint16_t rhs_ncols;
    int *rhs_col_map;
    if (select_all_rhs) {
        rhs_ncols = (uint16_t)t2->columns.count;
        rhs_col_map = (int *)bump_alloc(&arena->scratch, rhs_ncols * sizeof(int));
        for (uint16_t i = 0; i < rhs_ncols; i++) rhs_col_map[i] = (int)i;
    } else if (rs->parsed_columns_count > 0) {
        rhs_ncols = (uint16_t)rs->parsed_columns_count;
        rhs_col_map = (int *)bump_alloc(&arena->scratch, rhs_ncols * sizeof(int));
        for (uint32_t i = 0; i < rs->parsed_columns_count; i++) {
            struct select_column *sc = &rhs_q.arena.select_cols.items[rs->parsed_columns_start + i];
            if (sc->expr_idx == IDX_NONE) { query_free(&rhs_q); return PLAN_RES_NOTIMPL; }
            struct expr *e = &EXPR(&rhs_q.arena, sc->expr_idx);
            if (e->type != EXPR_COLUMN_REF) { query_free(&rhs_q); return PLAN_RES_NOTIMPL; }
            int ci = table_find_column_sv(t2, e->column_ref.column);
            if (ci < 0) { query_free(&rhs_q); return PLAN_RES_ERR; }
            rhs_col_map[i] = ci;
        }
    } else {
        /* Try legacy text-based column resolution */
        if (rs->columns.len > 0 && !sv_eq_cstr(rs->columns, "*")) {
            /* Parse comma-separated column list from raw text */
            query_free(&rhs_q);
            return PLAN_RES_NOTIMPL;
        }
        query_free(&rhs_q);
        return PLAN_RES_NOTIMPL;
    }

    /* Column counts must match */
    if (lhs_ncols != rhs_ncols) { query_free(&rhs_q); return PLAN_RES_ERR; }

    /* Check column type compatibility between LHS and RHS */
    for (uint16_t i = 0; i < lhs_ncols; i++) {
        enum column_type lt = t->columns.items[lhs_col_map[i]].type;
        enum column_type rt = t2->columns.items[rhs_col_map[i]].type;
        if (lt != rt) { query_free(&rhs_q); return PLAN_RES_ERR; }
    }

    /* Check for mixed cell types in both tables */
    if (table_has_mixed_types(t) || table_has_mixed_types(t2)) {
        query_free(&rhs_q);
        return PLAN_RES_NOTIMPL;
    }

    /* Build LHS plan: SEQ_SCAN (→ PROJECT if not SELECT *) */
    uint32_t lhs_current = build_seq_scan(t, arena);

    if (!select_all_lhs)
        lhs_current = append_project_node(lhs_current, arena, lhs_ncols, lhs_col_map);

    /* Build RHS plan: SEQ_SCAN (→ FILTER) (→ PROJECT if not SELECT *) */
    uint32_t rhs_current = build_seq_scan(t2, arena);

    /* Add filter on RHS if it has a simple WHERE */
    if (rs->where.has_where)
        rhs_current = try_append_simple_filter(rhs_current, t2, arena, &rhs_q.arena, rs->where.where_cond);

    if (!select_all_rhs)
        rhs_current = append_project_node(rhs_current, arena, rhs_ncols, rhs_col_map);

    /* Build SET_OP node */
    uint32_t set_idx = plan_alloc_node(arena, PLAN_SET_OP);
    PLAN_NODE(arena, set_idx).left = lhs_current;
    PLAN_NODE(arena, set_idx).right = rhs_current;
    PLAN_NODE(arena, set_idx).set_op.set_op = s->set_op;
    PLAN_NODE(arena, set_idx).set_op.set_all = s->set_all;
    PLAN_NODE(arena, set_idx).set_op.ncols = lhs_ncols;
    uint32_t current = set_idx;

    /* Add SORT node if set_order_by is present */
    if (s->set_order_by != IDX_NONE) {
        const char *ob_sql = ASTRING(arena, s->set_order_by);
        /* Skip "ORDER BY" prefix */
        const char *p = ob_sql;
        while (*p == ' ') p++;
        if (strncasecmp(p, "ORDER", 5) == 0) p += 5;
        while (*p == ' ') p++;
        if (strncasecmp(p, "BY", 2) == 0) p += 2;
        while (*p == ' ') p++;

        int sort_cols_buf[MAX_SORT_KEYS];
        int sort_descs_buf[MAX_SORT_KEYS];
        uint16_t sort_nord = 0;
        int sort_ok = 1;

        while (*p && sort_nord < MAX_SORT_KEYS) {
            while (*p == ' ') p++;
            if (!*p) break;

            /* Extract column name */
            const char *start = p;
            while (*p && *p != ' ' && *p != ',' && *p != '\t' && *p != '\n') p++;
            sv col_name = sv_from(start, (size_t)(p - start));

            /* Find column index in LHS output */
            int sci = -1;
            if (select_all_lhs) {
                sci = table_find_column_sv(t, col_name);
            } else {
                for (uint32_t pc = 0; pc < s->parsed_columns_count; pc++) {
                    struct select_column *scp = &arena->select_cols.items[s->parsed_columns_start + pc];
                    if (scp->alias.len > 0 && sv_eq_ignorecase(col_name, scp->alias)) { sci = (int)pc; break; }
                    if (scp->expr_idx != IDX_NONE) {
                        struct expr *e = &EXPR(arena, scp->expr_idx);
                        if (e->type == EXPR_COLUMN_REF && sv_eq_ignorecase(col_name, e->column_ref.column)) { sci = (int)pc; break; }
                    }
                }
            }
            if (sci < 0) { sort_ok = 0; break; }

            sort_cols_buf[sort_nord] = sci;
            sort_descs_buf[sort_nord] = 0;

            /* Check for ASC/DESC */
            while (*p == ' ') p++;
            if (strncasecmp(p, "DESC", 4) == 0 && (!p[4] || p[4] == ' ' || p[4] == ',')) {
                sort_descs_buf[sort_nord] = 1;
                p += 4;
            } else if (strncasecmp(p, "ASC", 3) == 0 && (!p[3] || p[3] == ' ' || p[3] == ',')) {
                p += 3;
            }
            sort_nord++;

            /* Skip comma */
            while (*p == ' ') p++;
            if (*p == ',') { p++; continue; }
            break;
        }

        if (sort_ok && sort_nord > 0)
            current = append_sort_node(current, arena, sort_cols_buf, sort_descs_buf, NULL, sort_nord);
    }

    /* Also handle ORDER BY from the main query (s->has_order_by) */
    if (s->has_order_by && s->order_by_count > 0) {
        uint16_t sort_nord = s->order_by_count < MAX_SORT_KEYS ? (uint16_t)s->order_by_count : MAX_SORT_KEYS;
        int sort_cols_buf2[MAX_SORT_KEYS];
        int sort_descs_buf2[MAX_SORT_KEYS];
        int sort_ok = 1;
        for (uint16_t k = 0; k < sort_nord; k++) {
            struct order_by_item *obi = &arena->order_items.items[s->order_by_start + k];
            sort_descs_buf2[k] = obi->desc;
            sort_cols_buf2[k] = -1;
            if (select_all_lhs) {
                sort_cols_buf2[k] = table_find_column_sv(t, obi->column);
            } else {
                for (uint32_t pc = 0; pc < s->parsed_columns_count; pc++) {
                    struct select_column *scp = &arena->select_cols.items[s->parsed_columns_start + pc];
                    if (scp->alias.len > 0 && sv_eq_ignorecase(obi->column, scp->alias)) { sort_cols_buf2[k] = (int)pc; break; }
                    if (scp->expr_idx != IDX_NONE) {
                        struct expr *e = &EXPR(arena, scp->expr_idx);
                        if (e->type == EXPR_COLUMN_REF && sv_eq_ignorecase(obi->column, e->column_ref.column)) { sort_cols_buf2[k] = (int)pc; break; }
                    }
                }
            }
            if (sort_cols_buf2[k] < 0) { sort_ok = 0; break; }
        }
        if (sort_ok && sort_nord > 0)
            current = append_sort_node(current, arena, sort_cols_buf2, sort_descs_buf2, NULL, sort_nord);
    }

    current = build_limit(current, s, arena);

    query_free(&rhs_q);
    return PLAN_RES_OK(current);
}

/* ---- Plan builder: window path ---- */

/* Try to build a plan for a window function query. */
static struct plan_result build_window(struct table *t, struct query_select *s,
                                       struct query_arena *arena)
{
    size_t nexprs = s->select_exprs_count;
    /* Count passthrough and window expressions, resolve column indices */
    uint16_t n_pass = 0, n_win = 0;
    for (size_t e = 0; e < nexprs; e++) {
        struct select_expr *se = &arena->select_exprs.items[s->select_exprs_start + e];
        if (se->kind == SEL_COLUMN) n_pass++;
        else n_win++;
    }
    if (n_win == 0) return PLAN_RES_ERR; /* shouldn't happen */

    /* Validate all columns exist and resolve indices */
    int *pass_cols = (int *)bump_alloc(&arena->scratch, (n_pass ? n_pass : 1) * sizeof(int));
    int *wpc = (int *)bump_alloc(&arena->scratch, n_win * sizeof(int));
    int *woc = (int *)bump_alloc(&arena->scratch, n_win * sizeof(int));
    int *wod = (int *)bump_alloc(&arena->scratch, n_win * sizeof(int));
    int *wac = (int *)bump_alloc(&arena->scratch, n_win * sizeof(int));
    int *wfn = (int *)bump_alloc(&arena->scratch, n_win * sizeof(int));
    int *woff = (int *)bump_alloc(&arena->scratch, n_win * sizeof(int));
    int *whf = (int *)bump_alloc(&arena->scratch, n_win * sizeof(int));
    int *wfs = (int *)bump_alloc(&arena->scratch, n_win * sizeof(int));
    int *wfe = (int *)bump_alloc(&arena->scratch, n_win * sizeof(int));
    int *wfsn = (int *)bump_alloc(&arena->scratch, n_win * sizeof(int));
    int *wfen = (int *)bump_alloc(&arena->scratch, n_win * sizeof(int));

    uint16_t pi = 0, wi = 0;
    int global_part = -1, global_ord = -1, global_ord_desc = 0;
    for (size_t e = 0; e < nexprs; e++) {
        struct select_expr *se = &arena->select_exprs.items[s->select_exprs_start + e];
        if (se->kind == SEL_COLUMN) {
            int ci = table_find_column_sv(t, se->column);
            if (ci < 0) return PLAN_RES_ERR;
            pass_cols[pi++] = ci;
        } else {
            int pc = -1, oc = -1, ac = -1;
            if (se->win.has_partition) {
                pc = table_find_column_sv(t, se->win.partition_col);
                if (pc < 0) return PLAN_RES_ERR;
                if (global_part < 0) global_part = pc;
            }
            if (se->win.has_order) {
                oc = table_find_column_sv(t, se->win.order_col);
                if (oc < 0) return PLAN_RES_ERR;
                if (global_ord < 0) { global_ord = oc; global_ord_desc = se->win.order_desc; }
            }
            if (se->win.arg_column.len > 0 && !sv_eq_cstr(se->win.arg_column, "*")) {
                ac = table_find_column_sv(t, se->win.arg_column);
                if (ac < 0) return PLAN_RES_ERR;
            }
            /* LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE on text columns
             * are handled via win_str arrays in the executor. */
            wpc[wi] = pc;
            woc[wi] = oc;
            wod[wi] = se->win.order_desc;
            wac[wi] = ac;
            wfn[wi] = (int)se->win.func;
            woff[wi] = se->win.offset;
            whf[wi] = se->win.has_frame;
            wfs[wi] = (int)se->win.frame_start;
            wfe[wi] = (int)se->win.frame_end;
            wfsn[wi] = se->win.frame_start_n;
            wfen[wi] = se->win.frame_end_n;
            wi++;
        }
    }

    /* Bail out if table has mixed cell types */
    if (table_has_mixed_types(t))
        return PLAN_RES_NOTIMPL;

    /* Build scan → (filter →) window (→ sort) plan */
    uint32_t current = build_seq_scan(t, arena);

    /* Add filter if WHERE is simple enough */
    if (s->where.has_where)
        current = try_append_simple_filter(current, t, arena, arena, s->where.where_cond);

    /* Add WINDOW node */
    uint32_t win_idx = plan_alloc_node(arena, PLAN_WINDOW);
    PLAN_NODE(arena, win_idx).left = current;
    PLAN_NODE(arena, win_idx).window.out_ncols = n_pass + n_win;
    PLAN_NODE(arena, win_idx).window.n_pass = n_pass;
    PLAN_NODE(arena, win_idx).window.pass_cols = pass_cols;
    PLAN_NODE(arena, win_idx).window.n_win = n_win;
    PLAN_NODE(arena, win_idx).window.win_part_col = wpc;
    PLAN_NODE(arena, win_idx).window.win_ord_col = woc;
    PLAN_NODE(arena, win_idx).window.win_ord_desc = wod;
    PLAN_NODE(arena, win_idx).window.win_arg_col = wac;
    PLAN_NODE(arena, win_idx).window.win_func = wfn;
    PLAN_NODE(arena, win_idx).window.win_offset = woff;
    PLAN_NODE(arena, win_idx).window.win_has_frame = whf;
    PLAN_NODE(arena, win_idx).window.win_frame_start = wfs;
    PLAN_NODE(arena, win_idx).window.win_frame_end = wfe;
    PLAN_NODE(arena, win_idx).window.win_frame_start_n = wfsn;
    PLAN_NODE(arena, win_idx).window.win_frame_end_n = wfen;
    PLAN_NODE(arena, win_idx).window.sort_part_col = global_part;
    PLAN_NODE(arena, win_idx).window.sort_ord_col = global_ord;
    PLAN_NODE(arena, win_idx).window.sort_ord_desc = global_ord_desc;
    current = win_idx;

    /* Add SORT node if outer ORDER BY is present */
    if (s->has_order_by && s->order_by_count > 0) {
        /* The output layout is: passthrough columns first (indices 0..n_pass-1),
         * then window columns (indices n_pass..n_pass+n_win-1).
         * We need to map ORDER BY column names to these output indices. */
        int sort_cols_buf[MAX_SORT_KEYS];
        int sort_descs_buf[MAX_SORT_KEYS];
        uint16_t sort_nord = s->order_by_count < MAX_SORT_KEYS ? (uint16_t)s->order_by_count : MAX_SORT_KEYS;
        int sort_ok = 1;
        for (uint16_t k = 0; k < sort_nord; k++) {
            struct order_by_item *obi = &arena->order_items.items[s->order_by_start + k];
            sort_cols_buf[k] = -1;
            sort_descs_buf[k] = obi->desc;
            /* Walk select_exprs to find the output column index */
            uint16_t pass_i = 0, win_i = 0;
            for (size_t e = 0; e < nexprs; e++) {
                struct select_expr *se = &arena->select_exprs.items[s->select_exprs_start + e];
                if (se->kind == SEL_COLUMN) {
                    sv col = se->column;
                    sv bare_col = col, bare_ord = obi->column;
                    for (size_t kk = 0; kk < col.len; kk++)
                        if (col.data[kk] == '.') { bare_col = sv_from(col.data+kk+1, col.len-kk-1); break; }
                    for (size_t kk = 0; kk < obi->column.len; kk++)
                        if (obi->column.data[kk] == '.') { bare_ord = sv_from(obi->column.data+kk+1, obi->column.len-kk-1); break; }
                    if (sv_eq_ignorecase(bare_col, bare_ord)) { sort_cols_buf[k] = (int)pass_i; break; }
                    if (se->alias.len > 0 && sv_eq_ignorecase(se->alias, obi->column)) { sort_cols_buf[k] = (int)pass_i; break; }
                    pass_i++;
                } else {
                    if (se->alias.len > 0 && sv_eq_ignorecase(se->alias, obi->column)) {
                        sort_cols_buf[k] = (int)(n_pass + win_i);
                        break;
                    }
                    win_i++;
                }
            }
            if (sort_cols_buf[k] < 0) { sort_ok = 0; break; }
        }
        if (sort_ok && sort_nord > 0)
            current = append_sort_node(current, arena, sort_cols_buf, sort_descs_buf, NULL, sort_nord);
    }

    current = build_limit(current, s, arena);

    return PLAN_RES_OK(current);
}

/* ---- Plan builder: simple aggregate path (no GROUP BY) ---- */

/* Try to build a plan for an aggregate-only query (no GROUP BY). */
static struct plan_result build_simple_agg(struct table *t, struct query_select *s,
                                           struct query_arena *arena)
{
    if (!t) return PLAN_RES_ERR;
    if (table_has_mixed_types(t)) return PLAN_RES_NOTIMPL;

    /* Validate aggregates */
    int *agg_col_idxs = (int *)bump_alloc(&arena->scratch, s->aggregates_count * sizeof(int));
    for (uint32_t a = 0; a < s->aggregates_count; a++) {
        struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
        if (ae->func == AGG_STRING_AGG || ae->func == AGG_ARRAY_AGG || ae->func == AGG_NONE)
            return PLAN_RES_NOTIMPL;
        if (ae->expr_idx != IDX_NONE) {
            agg_col_idxs[a] = -2;
        } else if (sv_eq_cstr(ae->column, "*")) {
            agg_col_idxs[a] = -1;
        } else {
            int ci = table_find_column_sv(t, ae->column);
            if (ci < 0) return PLAN_RES_ERR;
            agg_col_idxs[a] = ci;
        }
    }

    /* Build: SEQ_SCAN → (FILTER) → SIMPLE_AGG */
    uint32_t current = build_seq_scan(t, arena);

    if (s->where.has_where) {
        uint32_t filtered = try_append_compound_filter(current, t, arena, arena, s->where.where_cond);
        if (filtered == current) {
            filtered = try_append_simple_filter(current, t, arena, arena, s->where.where_cond);
            if (filtered == current) return PLAN_RES_NOTIMPL; /* WHERE too complex */
        }
        current = filtered;
    }

    uint32_t agg_idx = plan_alloc_node(arena, PLAN_SIMPLE_AGG);
    PLAN_NODE(arena, agg_idx).left = current;
    PLAN_NODE(arena, agg_idx).simple_agg.agg_start = s->aggregates_start;
    PLAN_NODE(arena, agg_idx).simple_agg.agg_count = s->aggregates_count;
    PLAN_NODE(arena, agg_idx).simple_agg.agg_col_indices = agg_col_idxs;
    PLAN_NODE(arena, agg_idx).simple_agg.table = t;
    current = agg_idx;

    /* HAVING filter (without GROUP BY) */
    if (s->has_having && s->having_cond != IDX_NONE) {
        current = try_append_having_filter(current, s, arena);
        if (current == IDX_NONE) return PLAN_RES_NOTIMPL;
    }

    return PLAN_RES_OK(current);
}

/* ---- Plan builder: aggregate path ---- */

/* Append a HAVING filter after a HASH_AGG node.
 * Resolves HAVING column names against the aggregate output layout
 * (group cols + agg cols).  Returns updated current node on success,
 * IDX_NONE if the HAVING condition can't be handled. */
static uint32_t try_append_having_filter(uint32_t current,
                                          struct query_select *s,
                                          struct query_arena *arena)
{
    uint16_t ngrp = (uint16_t)s->group_by_count;
    uint32_t agg_n = s->aggregates_count;
    uint16_t agg_offset = s->agg_before_cols ? 0 : ngrp;
    uint16_t grp_offset = s->agg_before_cols ? (uint16_t)agg_n : 0;

    struct condition *hcond = &COND(arena, s->having_cond);
    if (hcond->type != COND_COMPARE) return IDX_NONE;
    if (hcond->lhs_expr != IDX_NONE) return IDX_NONE;
    if (hcond->rhs_column.len != 0) return IDX_NONE;
    if (hcond->scalar_subquery_sql != IDX_NONE) return IDX_NONE;
    if (hcond->subquery_sql != IDX_NONE) return IDX_NONE;
    if (hcond->is_any || hcond->is_all) return IDX_NONE;

    int having_col = -1;

    /* Match against GROUP BY column names */
    for (uint32_t g = 0; g < s->group_by_count; g++) {
        sv gcol = arena->svs.items[s->group_by_start + g];
        if (sv_eq_ignorecase(hcond->column, gcol)) {
            having_col = (int)(grp_offset + g);
            break;
        }
        /* Strip table prefix and compare */
        sv bare_h = hcond->column, bare_g = gcol;
        for (size_t p = 0; p < hcond->column.len; p++)
            if (hcond->column.data[p] == '.') { bare_h = sv_from(hcond->column.data + p + 1, hcond->column.len - p - 1); break; }
        for (size_t p = 0; p < gcol.len; p++)
            if (gcol.data[p] == '.') { bare_g = sv_from(gcol.data + p + 1, gcol.len - p - 1); break; }
        if (sv_eq_ignorecase(bare_h, bare_g)) {
            having_col = (int)(grp_offset + g);
            break;
        }
    }

    /* Match against aggregate aliases or function names */
    if (having_col < 0) {
        for (uint32_t a = 0; a < agg_n; a++) {
            struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
            if (ae->alias.len > 0 && sv_eq_ignorecase(hcond->column, ae->alias)) {
                having_col = (int)(agg_offset + a);
                break;
            }
            const char *fname = NULL;
            switch (ae->func) {
                case AGG_SUM:        fname = "sum";   break;
                case AGG_COUNT:      fname = "count"; break;
                case AGG_AVG:        fname = "avg";   break;
                case AGG_MIN:        fname = "min";   break;
                case AGG_MAX:        fname = "max";   break;
                case AGG_STRING_AGG: break;
                case AGG_ARRAY_AGG:  break;
                case AGG_NONE:       break;
            }
            if (fname && sv_eq_ignorecase(hcond->column, sv_from_cstr(fname))) {
                having_col = (int)(agg_offset + a);
                break;
            }
        }
    }

    if (having_col < 0) return IDX_NONE;

    /* Support extended HAVING ops: IS NULL, BETWEEN, IN-list, basic comparisons */
    switch (hcond->op) {

    case CMP_IS_NULL:
    case CMP_IS_NOT_NULL: {
        struct cell dummy = {0};
        return append_filter_node(current, arena, s->having_cond,
                                   having_col, (int)hcond->op, dummy);
    }

    case CMP_BETWEEN: {
        uint32_t fi = plan_alloc_node(arena, PLAN_FILTER);
        PLAN_NODE(arena, fi).left = current;
        PLAN_NODE(arena, fi).filter.cond_idx = s->having_cond;
        PLAN_NODE(arena, fi).filter.col_idx = having_col;
        PLAN_NODE(arena, fi).filter.cmp_op = CMP_BETWEEN;
        PLAN_NODE(arena, fi).filter.cmp_val = hcond->value;
        PLAN_NODE(arena, fi).filter.between_high = hcond->between_high;
        return fi;
    }

    case CMP_IN: {
        if (hcond->in_values_count == 0 || hcond->subquery_sql != IDX_NONE)
            return IDX_NONE;
        uint32_t nv = hcond->in_values_count;
        struct cell *vals = (struct cell *)bump_alloc(&arena->scratch,
                                                      nv * sizeof(struct cell));
        for (uint32_t i = 0; i < nv; i++) {
            vals[i] = arena->cells.items[hcond->in_values_start + i];
            if (column_type_is_text(vals[i].type) && vals[i].value.as_text)
                vals[i].value.as_text = bump_strdup(&arena->scratch, vals[i].value.as_text);
        }
        uint32_t fi = plan_alloc_node(arena, PLAN_FILTER);
        PLAN_NODE(arena, fi).left = current;
        PLAN_NODE(arena, fi).filter.cond_idx = s->having_cond;
        PLAN_NODE(arena, fi).filter.col_idx = having_col;
        PLAN_NODE(arena, fi).filter.cmp_op = CMP_IN;
        PLAN_NODE(arena, fi).filter.cmp_val = hcond->value;
        PLAN_NODE(arena, fi).filter.in_values = vals;
        PLAN_NODE(arena, fi).filter.in_count = nv;
        return fi;
    }

    case CMP_EQ:
    case CMP_NE:
    case CMP_LT:
    case CMP_GT:
    case CMP_LE:
    case CMP_GE:
        return append_filter_node(current, arena, s->having_cond,
                                   having_col, (int)hcond->op, hcond->value);

    case CMP_LIKE:
    case CMP_ILIKE:
    case CMP_NOT_IN:
    case CMP_IS_DISTINCT:
    case CMP_IS_NOT_DISTINCT:
    case CMP_EXISTS:
    case CMP_NOT_EXISTS:
    case CMP_REGEX_MATCH:
    case CMP_REGEX_NOT_MATCH:
        break;
    }
    return IDX_NONE;
}

/* Validate aggregate columns, returning PLAN_OK on success or an error status.
 * On success, agg_col_idxs and grp_col_idxs are populated. */
static enum plan_status validate_agg_columns(struct table *t, struct query_select *s,
                                              struct query_arena *arena,
                                              int *agg_col_idxs, int **out_grp_col_idxs)
{
    for (uint32_t a = 0; a < s->aggregates_count; a++) {
        struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
        if (ae->func == AGG_STRING_AGG || ae->func == AGG_ARRAY_AGG || ae->func == AGG_NONE)
            return PLAN_NOTIMPL;
        if (ae->expr_idx != IDX_NONE) {
            agg_col_idxs[a] = -2; /* expression-based aggregate */
        } else if (sv_eq_cstr(ae->column, "*")) {
            agg_col_idxs[a] = -1; /* COUNT(*) */
        } else {
            int ci = table_find_column_sv(t, ae->column);
            if (ci < 0) return PLAN_ERROR;
            agg_col_idxs[a] = ci;
        }
    }
    if (s->group_by_count > 0) {
        int *grp = (int *)bump_alloc(&arena->scratch, s->group_by_count * sizeof(int));
        for (uint32_t g = 0; g < s->group_by_count; g++) {
            sv gcol = arena->svs.items[s->group_by_start + g];
            grp[g] = table_find_column_sv(t, gcol);
            /* try matching by SELECT alias (parsed_columns path) */
            if (grp[g] < 0 && s->parsed_columns_count > 0) {
                for (uint32_t pc = 0; pc < s->parsed_columns_count; pc++) {
                    struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + pc];
                    if (sc->alias.len > 0 && sv_eq_ignorecase(sc->alias, gcol) && sc->expr_idx != IDX_NONE) {
                        struct expr *e = &EXPR(arena, sc->expr_idx);
                        if (e->type == EXPR_COLUMN_REF) {
                            grp[g] = table_find_column_sv(t, e->column_ref.column);
                            if (grp[g] >= 0) break;
                        }
                    }
                }
            }
            /* fallback: scan raw s->columns text for "colname AS alias" */
            if (grp[g] < 0 && s->columns.len > 0) {
                const char *p = s->columns.data;
                const char *end = s->columns.data + s->columns.len;
                while (p < end) {
                    while (p < end && (*p == ' ' || *p == '\t')) p++;
                    const char *col_start = p;
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
                        if (sv_eq_ignorecase(alias_sv, gcol)) {
                            const char *cn_start = col_start;
                            const char *cn_end = as_pos;
                            while (cn_end > cn_start && cn_end[-1] == ' ') cn_end--;
                            sv cn = sv_from(cn_start, (size_t)(cn_end - cn_start));
                            grp[g] = table_find_column_sv(t, cn);
                        }
                    }
                    if (p < end && *p == ',') p++;
                }
            }
            /* check for expression-based GROUP BY key */
            if (grp[g] < 0 && s->group_by_exprs_start + g < (uint32_t)arena->arg_indices.count) {
                uint32_t expr_idx = arena->arg_indices.items[s->group_by_exprs_start + g];
                if (expr_idx != IDX_NONE)
                    return PLAN_NOTIMPL; /* expression GROUP BY — fall to legacy */
            }
            if (grp[g] < 0) return PLAN_NOTIMPL;
        }
        *out_grp_col_idxs = grp;
    }
    return PLAN_OK;
}

/* Try to build a plan for a GROUP BY + aggregates query. */
static struct plan_result build_aggregate(struct table *t, struct query_select *s,
                                          struct query_arena *arena)
{
    int *agg_col_idxs = (int *)bump_alloc(&arena->scratch, s->aggregates_count * sizeof(int));
    int *grp_col_idxs = NULL;
    enum plan_status vs = validate_agg_columns(t, s, arena, agg_col_idxs, &grp_col_idxs);
    if (vs != PLAN_OK)
        return (struct plan_result){ .node = IDX_NONE, .status = vs };

    /* Build: SEQ_SCAN → (FILTER) → HASH_AGG */
    uint32_t scan_idx = build_seq_scan(t, arena);

    /* Apply WHERE filter before aggregation */
    if (s->where.has_where && s->where.where_cond != IDX_NONE) {
        uint32_t filtered = try_append_compound_filter(scan_idx, t, arena, arena, s->where.where_cond);
        if (filtered == scan_idx) {
            filtered = try_append_simple_filter(scan_idx, t, arena, arena, s->where.where_cond);
            if (filtered == scan_idx) return PLAN_RES_NOTIMPL;
        }
        scan_idx = filtered;
    }

    uint32_t agg_idx = plan_alloc_node(arena, PLAN_HASH_AGG);
    PLAN_NODE(arena, agg_idx).left = scan_idx;
    PLAN_NODE(arena, agg_idx).hash_agg.ngroup_cols = (uint16_t)s->group_by_count;
    PLAN_NODE(arena, agg_idx).hash_agg.group_cols = grp_col_idxs;
    PLAN_NODE(arena, agg_idx).hash_agg.agg_start = s->aggregates_start;
    PLAN_NODE(arena, agg_idx).hash_agg.agg_count = s->aggregates_count;
    PLAN_NODE(arena, agg_idx).hash_agg.agg_before_cols = s->agg_before_cols;
    PLAN_NODE(arena, agg_idx).hash_agg.agg_col_indices = agg_col_idxs;
    PLAN_NODE(arena, agg_idx).hash_agg.table = t;

    uint32_t current = agg_idx;

    /* Add HAVING filter if present */
    if (s->has_having && s->having_cond != IDX_NONE) {
        current = try_append_having_filter(current, s, arena);
        if (current == IDX_NONE) return PLAN_RES_NOTIMPL;
    }

    /* Add SORT node if ORDER BY is present */
    if (s->has_order_by && s->order_by_count > 0) {
        uint16_t ngrp = (uint16_t)s->group_by_count;
        uint32_t agg_n = s->aggregates_count;
        uint16_t agg_offset = s->agg_before_cols ? 0 : ngrp;
        uint16_t grp_offset = s->agg_before_cols ? (uint16_t)agg_n : 0;

        int sort_cols_buf[MAX_SORT_KEYS];
        int sort_descs_buf[MAX_SORT_KEYS];
        uint16_t sort_nord = s->order_by_count < MAX_SORT_KEYS ? (uint16_t)s->order_by_count : MAX_SORT_KEYS;
        int sort_ok = 1;

        for (uint16_t k = 0; k < sort_nord; k++) {
            struct order_by_item *obi = &arena->order_items.items[s->order_by_start + k];
            sort_cols_buf[k] = -1;
            sort_descs_buf[k] = obi->desc;

            /* Try matching against GROUP BY column names */
            for (uint32_t g = 0; g < s->group_by_count; g++) {
                sv gcol = arena->svs.items[s->group_by_start + g];
                if (sv_eq_ignorecase(obi->column, gcol)) {
                    sort_cols_buf[k] = (int)(grp_offset + g);
                    break;
                }
            }

            /* Try matching against aggregate aliases or function names */
            if (sort_cols_buf[k] < 0) {
                for (uint32_t a = 0; a < agg_n; a++) {
                    struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
                    if (ae->alias.len > 0 && sv_eq_ignorecase(obi->column, ae->alias)) {
                        sort_cols_buf[k] = (int)(agg_offset + a);
                        break;
                    }
                    /* Match bare function name: sum, count, avg, min, max */
                    const char *fname = NULL;
                    switch (ae->func) {
                        case AGG_SUM:        fname = "sum";   break;
                        case AGG_COUNT:      fname = "count"; break;
                        case AGG_AVG:        fname = "avg";   break;
                        case AGG_MIN:        fname = "min";   break;
                        case AGG_MAX:        fname = "max";   break;
                        case AGG_STRING_AGG: break;
                        case AGG_ARRAY_AGG:  break;
                        case AGG_NONE:       break;
                    }
                    if (fname && sv_eq_ignorecase(obi->column, sv_from_cstr(fname))) {
                        sort_cols_buf[k] = (int)(agg_offset + a);
                        break;
                    }
                }
            }

            /* Try matching GROUP BY column base names (strip table prefix) */
            if (sort_cols_buf[k] < 0) {
                sv bare_ord = obi->column;
                for (size_t p = 0; p < obi->column.len; p++)
                    if (obi->column.data[p] == '.') { bare_ord = sv_from(obi->column.data + p + 1, obi->column.len - p - 1); break; }
                for (uint32_t g = 0; g < s->group_by_count; g++) {
                    sv gcol = arena->svs.items[s->group_by_start + g];
                    sv bare_grp = gcol;
                    for (size_t p = 0; p < gcol.len; p++)
                        if (gcol.data[p] == '.') { bare_grp = sv_from(gcol.data + p + 1, gcol.len - p - 1); break; }
                    if (sv_eq_ignorecase(bare_ord, bare_grp)) {
                        sort_cols_buf[k] = (int)(grp_offset + g);
                        break;
                    }
                }
            }

            if (sort_cols_buf[k] < 0) { sort_ok = 0; break; }
        }

        if (sort_ok && sort_nord > 0)
            current = append_sort_node(current, arena, sort_cols_buf, sort_descs_buf, NULL, sort_nord);
    }

    current = build_limit(current, s, arena);

    return PLAN_RES_OK(current);
}

/* ---- Semi-join builder ---- */

/* Try to build a hash semi-join plan for an IN-subquery WHERE clause.
 * On success, the caller must call query_free on *out_sq after use. */
static struct plan_result build_semi_join(struct table *t, struct query_select *s,
                                          struct query_arena *arena, struct database *db,
                                          struct query *out_sq)
{
    struct condition *cond = &COND(arena, s->where.where_cond);

    /* Find outer key column */
    int semi_outer_key = table_find_column_sv(t, cond->column);
    if (semi_outer_key < 0) return PLAN_RES_ERR;

    /* Parse the subquery */
    const char *sq_sql = ASTRING(arena, cond->subquery_sql);
    if (query_parse(sq_sql, out_sq) != 0) return PLAN_RES_ERR;
    if (out_sq->query_type != QUERY_TYPE_SELECT) {
        query_free(out_sq); return PLAN_RES_ERR;
    }

    struct query_select *isq = &out_sq->select;
    /* Must be a simple single-table SELECT with one column */
    if (isq->has_join || isq->has_group_by || isq->aggregates_count > 0 ||
        isq->has_set_op || isq->ctes_count > 0 || isq->has_distinct ||
        isq->from_subquery_sql != IDX_NONE || isq->has_generate_series) {
        query_free(out_sq); return PLAN_RES_NOTIMPL;
    }

    /* Find inner table */
    struct table *semi_inner_t = db_find_table_sv(db, isq->table);
    if (!semi_inner_t) { query_free(out_sq); return PLAN_RES_ERR; }
    if (semi_inner_t->view_sql) { query_free(out_sq); return PLAN_RES_NOTIMPL; }

    /* Resolve inner SELECT column — must be a single column ref */
    int semi_inner_key = -1;
    if (isq->parsed_columns_count == 1) {
        struct select_column *sc = &out_sq->arena.select_cols.items[isq->parsed_columns_start];
        if (sc->expr_idx != IDX_NONE) {
            struct expr *e = &EXPR(&out_sq->arena, sc->expr_idx);
            if (e->type == EXPR_COLUMN_REF) {
                semi_inner_key = table_find_column_sv(semi_inner_t, e->column_ref.column);
            }
        }
    } else if (sv_eq_cstr(isq->columns, "*") && semi_inner_t->columns.count == 1) {
        semi_inner_key = 0;
    } else {
        /* Try legacy column name resolution */
        if (isq->columns.len > 0 && !sv_eq_cstr(isq->columns, "*")) {
            semi_inner_key = table_find_column_sv(semi_inner_t, isq->columns);
        }
    }
    if (semi_inner_key < 0) { query_free(out_sq); return PLAN_RES_ERR; }

    /* Validate inner WHERE (optional simple numeric filter) */
    int semi_inner_filter_col = -1;
    int semi_inner_filter_op = -1;
    struct cell semi_inner_filter_val = {0};
    if (isq->where.has_where && isq->where.where_cond != IDX_NONE) {
        struct condition *icond = &COND(&out_sq->arena, isq->where.where_cond);
        if (icond->type == COND_COMPARE && icond->lhs_expr == IDX_NONE &&
            icond->rhs_column.len == 0 && icond->subquery_sql == IDX_NONE &&
            icond->scalar_subquery_sql == IDX_NONE &&
            icond->in_values_count == 0 && !icond->is_any && !icond->is_all) {
            int fc = table_find_column_sv(semi_inner_t, icond->column);
            if (fc >= 0 && icond->op <= CMP_GE) {
                enum column_type fct = semi_inner_t->columns.items[fc].type;
                if (fct == COLUMN_TYPE_INT || fct == COLUMN_TYPE_BIGINT ||
                    fct == COLUMN_TYPE_FLOAT || fct == COLUMN_TYPE_NUMERIC ||
                    fct == COLUMN_TYPE_BOOLEAN || column_type_is_text(fct)) {
                    semi_inner_filter_col = fc;
                    semi_inner_filter_op = (int)icond->op;
                    semi_inner_filter_val = icond->value;
                    /* Copy text into plan arena — sub-query arena is freed after build */
                    if (column_type_is_text(fct) && semi_inner_filter_val.value.as_text)
                        semi_inner_filter_val.value.as_text =
                            bump_strdup(&arena->scratch, semi_inner_filter_val.value.as_text);
                } else {
                    query_free(out_sq); return PLAN_RES_NOTIMPL;
                }
            } else {
                query_free(out_sq); return PLAN_RES_NOTIMPL;
            }
        } else {
            query_free(out_sq); return PLAN_RES_NOTIMPL; /* complex inner WHERE */
        }
    }

    /* Check for mixed cell types in inner table */
    if (table_has_mixed_types(semi_inner_t)) {
        query_free(out_sq); return PLAN_RES_NOTIMPL;
    }

    /* --- Build plan nodes --- */

    /* Build outer scan (left / probe side) */
    uint16_t scan_ncols = (uint16_t)t->columns.count;
    int *col_map = (int *)bump_alloc(&arena->scratch, scan_ncols * sizeof(int));
    for (uint16_t i = 0; i < scan_ncols; i++)
        col_map[i] = (int)i;

    uint32_t outer_scan = plan_alloc_node(arena, PLAN_SEQ_SCAN);
    PLAN_NODE(arena, outer_scan).seq_scan.table = t;
    PLAN_NODE(arena, outer_scan).seq_scan.ncols = scan_ncols;
    PLAN_NODE(arena, outer_scan).seq_scan.col_map = col_map;
    PLAN_NODE(arena, outer_scan).est_rows = (double)t->rows.count;

    /* Build inner scan (right / build side) */
    uint16_t inner_ncols = (uint16_t)semi_inner_t->columns.count;
    uint32_t inner_current = build_seq_scan(semi_inner_t, arena);

    /* Add filter on inner side if subquery has WHERE */
    if (semi_inner_filter_col >= 0)
        inner_current = append_filter_node(inner_current, arena, IDX_NONE,
                                           semi_inner_filter_col, semi_inner_filter_op,
                                           semi_inner_filter_val);

    /* If inner SELECT is a single column (not SELECT *), add projection */
    if (semi_inner_key != 0 || inner_ncols > 1) {
        /* Project to just the key column */
        int *inner_proj = (int *)bump_alloc(&arena->scratch, sizeof(int));
        inner_proj[0] = semi_inner_key;
        inner_current = append_project_node(inner_current, arena, 1, inner_proj);
        semi_inner_key = 0; /* after projection, key is column 0 */
    }

    /* Build HASH_SEMI_JOIN node */
    uint32_t semi_idx = plan_alloc_node(arena, PLAN_HASH_SEMI_JOIN);
    PLAN_NODE(arena, semi_idx).left = outer_scan;
    PLAN_NODE(arena, semi_idx).right = inner_current;
    PLAN_NODE(arena, semi_idx).hash_semi_join.outer_key_col = semi_outer_key;
    PLAN_NODE(arena, semi_idx).hash_semi_join.inner_key_col = semi_inner_key;

    return PLAN_RES_OK(semi_idx);
}

/* ---- Single-table plan builder ---- */

/* Build a plan for a single-table SELECT (no joins, no window functions,
 * no set operations, no aggregates).  Handles simple WHERE filters,
 * IN-subquery semi-joins, index scans, ORDER BY, projection, DISTINCT,
 * and LIMIT/OFFSET. */
static struct plan_result build_single_table(struct table *t, struct query_select *s,
                                             struct query_arena *arena, struct database *db)
{
    /* Bail out for queries we don't handle yet */
    if (s->has_join)            return PLAN_RES_NOTIMPL;
    if (s->select_exprs_count > 0) return PLAN_RES_NOTIMPL; /* window functions — handled above */
    if (s->has_group_by)        return PLAN_RES_NOTIMPL;
    if (s->aggregates_count > 0) return PLAN_RES_NOTIMPL;
    if (s->has_set_op)          return PLAN_RES_NOTIMPL;
    if (s->ctes_count > 0)     return PLAN_RES_NOTIMPL;
    if (s->cte_sql != IDX_NONE) return PLAN_RES_NOTIMPL;
    if (s->from_subquery_sql != IDX_NONE) return PLAN_RES_NOTIMPL;
    if (s->has_recursive_cte)   return PLAN_RES_NOTIMPL;
    if (s->has_distinct_on)     return PLAN_RES_NOTIMPL;
    if (!t)                     return PLAN_RES_ERR;
    if (s->insert_rows_count > 0) return PLAN_RES_NOTIMPL; /* literal SELECT */

    int select_all = sv_eq_cstr(s->columns, "*");
    /* Detect table.* pattern (e.g. SELECT t.*) and treat as SELECT * */
    if (!select_all && s->parsed_columns_count == 1) {
        struct select_column *sc0 = &arena->select_cols.items[s->parsed_columns_start];
        if (sc0->expr_idx != IDX_NONE) {
            struct expr *e0 = &EXPR(arena, sc0->expr_idx);
            if (e0->type == EXPR_COLUMN_REF && e0->column_ref.column.len == 1 &&
                e0->column_ref.column.data[0] == '*')
                select_all = 1;
        }
    }

    /* Determine projection: either SELECT * or all parsed_columns are simple column refs,
     * or expression-based columns (UPPER(x), ABS(y), etc.) */
    int need_project = 0;
    int need_expr_project = 0;
    int *proj_map = NULL;
    uint32_t *expr_proj_indices = NULL;
    uint16_t proj_ncols = 0;

    if (select_all) {
        /* No projection needed — return all columns */
    } else if (s->parsed_columns_count > 0) {
        proj_ncols = (uint16_t)s->parsed_columns_count;
        proj_map = (int *)bump_alloc(&arena->scratch, proj_ncols * sizeof(int));
        int all_column_refs = 1;
        for (uint32_t i = 0; i < s->parsed_columns_count; i++) {
            struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + i];
            if (sc->expr_idx == IDX_NONE) { all_column_refs = 0; break; }
            struct expr *e = &EXPR(arena, sc->expr_idx);
            if (e->type != EXPR_COLUMN_REF) { all_column_refs = 0; break; }
            int ci = table_find_column_sv(t, e->column_ref.column);
            if (ci < 0) { all_column_refs = 0; break; }
            proj_map[i] = ci;
        }
        if (all_column_refs) {
            need_project = 1;
        } else {
            /* Check if we can use expression projection */
            expr_proj_indices = (uint32_t *)bump_alloc(&arena->scratch, proj_ncols * sizeof(uint32_t));
            int expr_ok = 1;
            for (uint32_t i = 0; i < s->parsed_columns_count; i++) {
                struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + i];
                if (sc->expr_idx == IDX_NONE) { expr_ok = 0; break; }
                /* Bail out for subquery expressions — too complex */
                struct expr *e = &EXPR(arena, sc->expr_idx);
                if (e->type == EXPR_SUBQUERY) { expr_ok = 0; break; }
                expr_proj_indices[i] = sc->expr_idx;
            }
            if (!expr_ok) return PLAN_RES_NOTIMPL;
            need_expr_project = 1;
        }
    } else {
        /* Legacy text-based column list — can't handle */
        return PLAN_RES_NOTIMPL;
    }

    /* ORDER BY + expr_project: sort on raw columns before expression eval.
     * DISTINCT + expr_project: EXPR_PROJECT runs before DISTINCT.
     * Both are handled by the existing node construction order. */

    /* Pre-validate ORDER BY columns BEFORE allocating plan nodes */
    int  sort_cols_buf[MAX_SORT_KEYS];
    int  sort_descs_buf[MAX_SORT_KEYS];
    int  sort_nf_buf[MAX_SORT_KEYS];
    uint16_t sort_nord = 0;
    if (s->has_order_by && s->order_by_count > 0) {
        sort_nord = s->order_by_count < MAX_SORT_KEYS ? (uint16_t)s->order_by_count : MAX_SORT_KEYS;
        for (uint16_t k = 0; k < sort_nord; k++) {
            struct order_by_item *obi = &arena->order_items.items[s->order_by_start + k];
            sort_descs_buf[k] = obi->desc;
            sort_nf_buf[k] = obi->nulls_first;
            sort_cols_buf[k] = table_find_column_sv(t, obi->column);
            if (sort_cols_buf[k] < 0 && need_project) {
                for (uint32_t pc = 0; pc < s->parsed_columns_count; pc++) {
                    struct select_column *scp = &arena->select_cols.items[s->parsed_columns_start + pc];
                    if (scp->alias.len > 0 && sv_eq_ignorecase(obi->column, scp->alias)) {
                        if (scp->expr_idx != IDX_NONE && EXPR(arena, scp->expr_idx).type == EXPR_COLUMN_REF)
                            sort_cols_buf[k] = table_find_column_sv(t, EXPR(arena, scp->expr_idx).column_ref.column);
                        break;
                    }
                }
            }
            if (sort_cols_buf[k] < 0) return PLAN_RES_ERR;
        }
    }

    /* Pre-validate WHERE clause BEFORE allocating plan nodes */
    int have_source = 0;
    uint32_t current = IDX_NONE;

    /* Extra filter condition index — set when COND_AND is decomposed into
     * a semi-join (IN-subquery) + a simple comparison filter. */
    uint32_t extra_filter_cond = IDX_NONE;
    /* Compound filter condition — set when WHERE is a COND_AND tree or
     * an extended op (IS NULL, BETWEEN, IN-list, LIKE) that passed validation. */
    uint32_t compound_filter_cond = IDX_NONE;

    if (s->where.has_where) {
        if (s->where.where_cond == IDX_NONE)
            return PLAN_RES_ERR;
        struct condition *cond = &COND(arena, s->where.where_cond);

        /* ---- COND_AND: try to decompose into IN-subquery + simple filter ---- */
        if (cond->type == COND_AND && cond->left != IDX_NONE && cond->right != IDX_NONE && db) {
            struct condition *lc = &COND(arena, cond->left);
            struct condition *rc = &COND(arena, cond->right);
            /* Identify which child is the IN-subquery and which is the simple filter */
            uint32_t in_cond_idx = IDX_NONE, other_cond_idx = IDX_NONE;
            if (lc->type == COND_COMPARE && lc->op == CMP_IN && lc->subquery_sql != IDX_NONE) {
                in_cond_idx = cond->left;
                other_cond_idx = cond->right;
            } else if (rc->type == COND_COMPARE && rc->op == CMP_IN && rc->subquery_sql != IDX_NONE) {
                in_cond_idx = cond->right;
                other_cond_idx = cond->left;
            }
            if (in_cond_idx != IDX_NONE && other_cond_idx != IDX_NONE) {
                /* Temporarily rewrite where_cond to point at just the IN child
                 * so build_semi_join sees a simple COND_COMPARE */
                uint32_t saved_where = s->where.where_cond;
                s->where.where_cond = in_cond_idx;

                if (!table_has_mixed_types(t)) {
                    struct query semi_sq = {0};
                    struct plan_result semi_pr = build_semi_join(t, s, arena, db, &semi_sq);
                    if (semi_pr.status == PLAN_OK) {
                        current = semi_pr.node;
                        query_free(&semi_sq);
                        have_source = 1;
                        extra_filter_cond = other_cond_idx;
                    }
                }

                s->where.where_cond = saved_where;
                /* If semi-join failed, fall through to bail out below */
            }
        }

        if (!have_source) {
            /* ---- IN-subquery → hash semi-join (must be checked first) ---- */
            if (cond->type == COND_COMPARE && cond->op == CMP_IN &&
                cond->subquery_sql != IDX_NONE && db) {
                if (!table_has_mixed_types(t)) {
                    struct query semi_sq = {0};
                    struct plan_result semi_pr = build_semi_join(t, s, arena, db, &semi_sq);
                    if (semi_pr.status == PLAN_OK) {
                        current = semi_pr.node;
                        query_free(&semi_sq);
                        have_source = 1;
                    } else if (semi_pr.status == PLAN_ERROR) {
                        return semi_pr;
                    }
                    /* NOTIMPL falls through to compound filter below */
                }
            }

            /* ---- Unified compound filter: handles COND_AND trees, extended ops,
             * and basic comparisons through a single validation path ---- */
            if (!have_source) {
                if (validate_compound_filter(t, arena, s->where.where_cond)) {
                    compound_filter_cond = s->where.where_cond;
                } else {
                    return PLAN_RES_NOTIMPL;
                }
            }
        }
    }

    /* Build scan source if not already set by semi-join */
    if (!have_source) {
        /* Bail out if outer table has mixed cell types (e.g. after ALTER COLUMN TYPE) */
        if (table_has_mixed_types(t))
            return PLAN_RES_NOTIMPL;

        /* --- All validation passed, now allocate plan nodes --- */

        /* Try index scan for simple equality WHERE on an indexed column */
        int used_index = 0;
        if (compound_filter_cond != IDX_NONE) {
            struct condition *cond = &COND(arena, compound_filter_cond);
            if (cond->type == COND_COMPARE && cond->op == CMP_EQ &&
                cond->subquery_sql == IDX_NONE) {
                int fc = table_find_column_sv(t, cond->column);
                if (fc >= 0) {
                    uint16_t scan_ncols = (uint16_t)t->columns.count;
                    int *col_map = (int *)bump_alloc(&arena->scratch, scan_ncols * sizeof(int));
                    for (uint16_t i = 0; i < scan_ncols; i++)
                        col_map[i] = (int)i;
                    for (size_t ix = 0; ix < t->indexes.count; ix++) {
                        if (strcmp(t->indexes.items[ix].column_name,
                                   t->columns.items[fc].name) == 0) {
                            uint32_t idx_node = plan_alloc_node(arena, PLAN_INDEX_SCAN);
                            PLAN_NODE(arena, idx_node).index_scan.table = t;
                            PLAN_NODE(arena, idx_node).index_scan.idx = &t->indexes.items[ix];
                            PLAN_NODE(arena, idx_node).index_scan.cond_idx = compound_filter_cond;
                            PLAN_NODE(arena, idx_node).index_scan.ncols = scan_ncols;
                            PLAN_NODE(arena, idx_node).index_scan.col_map = col_map;
                            PLAN_NODE(arena, idx_node).est_rows = 1.0;
                            current = idx_node;
                            used_index = 1;
                            compound_filter_cond = IDX_NONE; /* consumed by index scan */
                            break;
                        }
                    }
                }
            }
        }

        if (!used_index) {
            current = build_seq_scan(t, arena);

            /* Add compound filter (COND_AND tree, extended op, or basic comparison) */
            if (compound_filter_cond != IDX_NONE) {
                current = try_append_compound_filter(current, t, arena, arena, compound_filter_cond);
            }
        }
    }

    /* Append extra filter from COND_AND decomposition (e.g. AND amount > 100) */
    if (extra_filter_cond != IDX_NONE) {
        uint32_t prev = current;
        current = try_append_compound_filter(current, t, arena, arena, extra_filter_cond);
        if (current == prev)
            current = try_append_simple_filter(current, t, arena, arena, extra_filter_cond);
    }

    /* Add SORT node if ORDER BY was validated */
    if (sort_nord > 0)
        current = append_sort_node(current, arena, sort_cols_buf, sort_descs_buf, sort_nf_buf, sort_nord);

    /* Add projection node if specific columns are selected */
    if (need_project)
        current = append_project_node(current, arena, proj_ncols, proj_map);

    /* Add expression projection node for computed columns */
    if (need_expr_project) {
        uint32_t eproj_idx = plan_alloc_node(arena, PLAN_EXPR_PROJECT);
        PLAN_NODE(arena, eproj_idx).left = current;
        PLAN_NODE(arena, eproj_idx).expr_project.ncols = proj_ncols;
        PLAN_NODE(arena, eproj_idx).expr_project.expr_indices = expr_proj_indices;
        PLAN_NODE(arena, eproj_idx).expr_project.table = t;
        current = eproj_idx;
    }

    /* Add DISTINCT node if present (after sort+project, before limit) */
    if (s->has_distinct) {
        uint32_t dist_idx = plan_alloc_node(arena, PLAN_DISTINCT);
        PLAN_NODE(arena, dist_idx).left = current;
        current = dist_idx;
    }

    current = build_limit(current, s, arena);

    return PLAN_RES_OK(current);
}

/* ---- Plan builder ---- */

/* Try to build a block-oriented plan for a SELECT query.
 * Returns a plan_result: PLAN_OK with a valid node index on success,
 * PLAN_NOTIMPL for unimplemented features (fall back to legacy),
 * or PLAN_ERROR for real errors (bad column, type mismatch, etc.). */
struct plan_result plan_build_select(struct table *t, struct query_select *s,
                                     struct query_arena *arena, struct database *db)
{
    /* ---- Generate series fast path ---- */
    if (s->has_generate_series && s->gs_start_expr != IDX_NONE &&
        s->gs_stop_expr != IDX_NONE) {
        /* Only handle simple SELECT * integer series via plan executor */
        if (s->has_join || s->has_group_by || s->aggregates_count > 0 ||
            s->has_set_op || s->ctes_count > 0 || s->has_distinct ||
            s->select_exprs_count > 0 || s->where.has_where ||
            s->has_order_by || s->parsed_columns_count > 0)
            return PLAN_RES_NOTIMPL;
        if (!sv_eq_cstr(s->columns, "*"))
            return PLAN_RES_NOTIMPL;

        struct cell c_start = eval_expr(s->gs_start_expr, arena, NULL, NULL, db, NULL);
        struct cell c_stop  = eval_expr(s->gs_stop_expr, arena, NULL, NULL, db, NULL);

        /* Bail out for timestamp/date series — keep on legacy path */
        if (c_start.type == COLUMN_TYPE_DATE || c_start.type == COLUMN_TYPE_TIMESTAMP ||
            c_start.type == COLUMN_TYPE_TIMESTAMPTZ)
            return PLAN_RES_NOTIMPL;
        if (c_start.type == COLUMN_TYPE_INTERVAL || c_stop.type == COLUMN_TYPE_INTERVAL)
            return PLAN_RES_NOTIMPL;

        long long gs_start = (c_start.type == COLUMN_TYPE_BIGINT) ? c_start.value.as_bigint
                           : (c_start.type == COLUMN_TYPE_FLOAT)  ? (long long)c_start.value.as_float
                           : c_start.value.as_int;
        long long gs_stop  = (c_stop.type == COLUMN_TYPE_BIGINT) ? c_stop.value.as_bigint
                           : (c_stop.type == COLUMN_TYPE_FLOAT)  ? (long long)c_stop.value.as_float
                           : c_stop.value.as_int;
        long long gs_step  = 1;
        if (s->gs_step_expr != IDX_NONE) {
            struct cell c_step = eval_expr(s->gs_step_expr, arena, NULL, NULL, db, NULL);
            if (c_step.type == COLUMN_TYPE_INTERVAL || column_type_is_text(c_step.type))
                return PLAN_RES_NOTIMPL; /* timestamp step — bail to legacy */
            gs_step = (c_step.type == COLUMN_TYPE_BIGINT) ? c_step.value.as_bigint
                    : (c_step.type == COLUMN_TYPE_FLOAT)  ? (long long)c_step.value.as_float
                    : c_step.value.as_int;
        }
        if (gs_step == 0) gs_step = 1;

        int use_bigint = (c_start.type == COLUMN_TYPE_BIGINT ||
                          c_stop.type == COLUMN_TYPE_BIGINT ||
                          gs_start > 2147483647LL || gs_start < -2147483648LL ||
                          gs_stop > 2147483647LL || gs_stop < -2147483648LL);

        uint32_t gs_idx = plan_alloc_node(arena, PLAN_GENERATE_SERIES);
        PLAN_NODE(arena, gs_idx).gen_series.start = gs_start;
        PLAN_NODE(arena, gs_idx).gen_series.stop = gs_stop;
        PLAN_NODE(arena, gs_idx).gen_series.step = gs_step;
        PLAN_NODE(arena, gs_idx).gen_series.use_bigint = use_bigint;
        uint32_t current = gs_idx;

        current = build_limit(current, s, arena);

        return PLAN_RES_OK(current);
    }

    /* ---- Join fast path (single or multi-table) ---- */
    if (s->has_join && s->joins_count >= 1 && db) {
        return build_join(t, s, arena, db);
    }

    /* ---- Window function fast path ---- */
    if (s->select_exprs_count > 0 && !s->has_join && !s->has_group_by &&
        s->aggregates_count == 0 && !s->has_set_op && s->ctes_count == 0 &&
        s->cte_sql == IDX_NONE && s->from_subquery_sql == IDX_NONE &&
        !s->has_recursive_cte && !s->has_distinct && !s->has_distinct_on &&
        t && s->insert_rows_count == 0) {
        return build_window(t, s, arena);
    }

    /* ---- Set operations fast path (UNION / INTERSECT / EXCEPT) ---- */
    if (s->has_set_op && s->set_rhs_sql != IDX_NONE && !s->has_join &&
        !s->has_group_by && s->aggregates_count == 0 && s->ctes_count == 0 &&
        s->cte_sql == IDX_NONE && s->from_subquery_sql == IDX_NONE &&
        !s->has_recursive_cte && s->select_exprs_count == 0 &&
        !s->where.has_where &&
        t && db && s->insert_rows_count == 0) {
        return build_set_op(t, s, arena, db);
    }

    /* ---- Simple aggregate (no GROUP BY) fast path ---- */
    if (!s->has_group_by && s->aggregates_count > 0 && t && !s->has_join &&
        !s->has_set_op && s->ctes_count == 0 && s->cte_sql == IDX_NONE &&
        s->from_subquery_sql == IDX_NONE && !s->has_recursive_cte &&
        s->select_exprs_count == 0 && s->insert_rows_count == 0 &&
        !s->has_distinct && !s->has_distinct_on) {
        struct plan_result sa_pr = build_simple_agg(t, s, arena);
        if (sa_pr.status == PLAN_OK) return sa_pr;
    }

    /* ---- Single-table GROUP BY + aggregates fast path ---- */
    if (s->has_group_by && s->aggregates_count > 0 && t && !s->has_join &&
        !s->has_set_op && s->ctes_count == 0 && s->cte_sql == IDX_NONE &&
        s->from_subquery_sql == IDX_NONE && !s->has_recursive_cte &&
        s->select_exprs_count == 0 && s->insert_rows_count == 0 &&
        !s->has_distinct && !s->has_distinct_on &&
        !s->group_by_rollup && !s->group_by_cube) {
        struct plan_result agg_pr = build_aggregate(t, s, arena);
        if (agg_pr.status == PLAN_OK) return agg_pr;
    }

    /* ---- Single-table path ---- */
    return build_single_table(t, s, arena, db);
}
