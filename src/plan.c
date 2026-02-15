#include "plan.h"
#include "query.h"
#include "parser.h"
#include "arena_helpers.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

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

        size_t elem_sz;
        switch (ct) {
        case COLUMN_TYPE_SMALLINT: elem_sz = sizeof(int16_t); break;
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:  elem_sz = sizeof(int32_t); break;
        case COLUMN_TYPE_BIGINT:   elem_sz = sizeof(int64_t); break;
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:  elem_sz = sizeof(double); break;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID:
        default:                   elem_sz = sizeof(char *); break;
        }

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
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID:
        default: {
            /* text types */
            char **dst = (char **)sc->col_data[c];
            for (size_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[r].cells.items[c];
                if (cell->is_null || cell->type != ct
                    || (column_type_is_text(cell->type) && !cell->value.as_text)) {
                    sc->col_nulls[c][r] = 1;
                } else {
                    dst[r] = cell->value.as_text; /* pointer into table — valid as long as table exists */
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

        switch (ct) {
        case COLUMN_TYPE_SMALLINT: ((int16_t *)sc->col_data[c])[row_idx] = cell->value.as_smallint; break;
        case COLUMN_TYPE_INT:      ((int32_t *)sc->col_data[c])[row_idx] = cell->value.as_int; break;
        case COLUMN_TYPE_BIGINT:   ((int64_t *)sc->col_data[c])[row_idx] = cell->value.as_bigint; break;
        case COLUMN_TYPE_FLOAT:    ((double *)sc->col_data[c])[row_idx] = cell->value.as_float; break;
        case COLUMN_TYPE_NUMERIC:  ((double *)sc->col_data[c])[row_idx] = cell->value.as_numeric; break;
        case COLUMN_TYPE_BOOLEAN:  ((int32_t *)sc->col_data[c])[row_idx] = cell->value.as_bool; break;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID:
        default:
            ((char **)sc->col_data[c])[row_idx] = cell->value.as_text; break;
        }
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
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID:
        default:
            memcpy(cb->data.str, (char **)sc->col_data[tc] + start, nrows * sizeof(char *)); break;
        }
    }

    *cursor = end;
    return nrows;
}

/* Helper: copy a col_block value at index src_i to dst col_block at dst_i.
 * Handles all column types without triggering -Wswitch-enum. */
static inline void cb_copy_value(struct col_block *dst, uint32_t dst_i,
                                 const struct col_block *src, uint16_t src_i)
{
    dst->nulls[dst_i] = src->nulls[src_i];
    switch (src->type) {
    case COLUMN_TYPE_SMALLINT: dst->data.i16[dst_i] = src->data.i16[src_i]; break;
    case COLUMN_TYPE_BIGINT:   dst->data.i64[dst_i] = src->data.i64[src_i]; break;
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_NUMERIC:  dst->data.f64[dst_i] = src->data.f64[src_i]; break;
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_DATE:
    case COLUMN_TYPE_TIME:
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_TIMESTAMPTZ:
    case COLUMN_TYPE_INTERVAL:
    case COLUMN_TYPE_UUID:     dst->data.str[dst_i] = src->data.str[src_i]; break;
    case COLUMN_TYPE_INT:
    case COLUMN_TYPE_BOOLEAN:
    default:                   dst->data.i32[dst_i] = src->data.i32[src_i]; break;
    }
}

/* Helper: bulk copy count values from src col_block to dst col_block. */
static inline void cb_bulk_copy(struct col_block *dst,
                                const struct col_block *src, uint32_t count)
{
    memcpy(dst->nulls, src->nulls, count);
    switch (src->type) {
    case COLUMN_TYPE_SMALLINT: memcpy(dst->data.i16, src->data.i16, count * sizeof(int16_t)); break;
    case COLUMN_TYPE_BIGINT:   memcpy(dst->data.i64, src->data.i64, count * sizeof(int64_t)); break;
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_NUMERIC:  memcpy(dst->data.f64, src->data.f64, count * sizeof(double)); break;
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_DATE:
    case COLUMN_TYPE_TIME:
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_TIMESTAMPTZ:
    case COLUMN_TYPE_INTERVAL:
    case COLUMN_TYPE_UUID:     memcpy(dst->data.str, src->data.str, count * sizeof(char *)); break;
    case COLUMN_TYPE_INT:
    case COLUMN_TYPE_BOOLEAN:
    default:                   memcpy(dst->data.i32, src->data.i32, count * sizeof(int32_t)); break;
    }
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
    case COLUMN_TYPE_BOOLEAN:
    default:                   return (double)cb->data.i32[i];
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_DATE:
    case COLUMN_TYPE_TIME:
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_TIMESTAMPTZ:
    case COLUMN_TYPE_INTERVAL:
    case COLUMN_TYPE_UUID:     return 0.0;
    }
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
    case PLAN_NESTED_LOOP:
    case PLAN_SIMPLE_AGG:
        /* not yet implemented — fall through to child */
        return plan_node_ncols(arena, pn->left);
    }
    return plan_node_ncols(arena, pn->left);
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
        case COLUMN_TYPE_UUID:
        default: {
            /* Generic path for all other types */
            for (uint16_t r = 0; r < nrows; r++) {
                struct cell *cell = &t->rows.items[start + r].cells.items[tc];
                if (cell->is_null || cell->type != cb->type
                    || (column_type_is_text(cell->type) && !cell->value.as_text)) {
                    cb->nulls[r] = 1;
                    continue;
                }
                cb->nulls[r] = 0;
                switch (cell->type) {
                    case COLUMN_TYPE_SMALLINT:
                        cb->data.i16[r] = cell->value.as_smallint;
                        break;
                    case COLUMN_TYPE_INT:
                        cb->data.i32[r] = cell->value.as_int;
                        break;
                    case COLUMN_TYPE_BOOLEAN:
                        cb->data.i32[r] = cell->value.as_bool;
                        break;
                    case COLUMN_TYPE_BIGINT:
                        cb->data.i64[r] = cell->value.as_bigint;
                        break;
                    case COLUMN_TYPE_FLOAT:
                        cb->data.f64[r] = cell->value.as_float;
                        break;
                    case COLUMN_TYPE_NUMERIC:
                        cb->data.f64[r] = cell->value.as_numeric;
                        break;
                    case COLUMN_TYPE_TEXT:
                    case COLUMN_TYPE_ENUM:
                    case COLUMN_TYPE_DATE:
                    case COLUMN_TYPE_TIME:
                    case COLUMN_TYPE_TIMESTAMP:
                    case COLUMN_TYPE_TIMESTAMPTZ:
                    case COLUMN_TYPE_INTERVAL:
                    case COLUMN_TYPE_UUID:
                        cb->data.str[r] = cell->value.as_text;
                        break;
                }
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
                switch (cb->type) {
                    case COLUMN_TYPE_SMALLINT:
                        cell.value.as_smallint = cb->data.i16[ri];
                        break;
                    case COLUMN_TYPE_INT:
                        cell.value.as_int = cb->data.i32[ri];
                        break;
                    case COLUMN_TYPE_BOOLEAN:
                        cell.value.as_bool = cb->data.i32[ri];
                        break;
                    case COLUMN_TYPE_BIGINT:
                        cell.value.as_bigint = cb->data.i64[ri];
                        break;
                    case COLUMN_TYPE_FLOAT:
                        cell.value.as_float = cb->data.f64[ri];
                        break;
                    case COLUMN_TYPE_NUMERIC:
                        cell.value.as_numeric = cb->data.f64[ri];
                        break;
                    case COLUMN_TYPE_TEXT:
                    case COLUMN_TYPE_ENUM:
                    case COLUMN_TYPE_DATE:
                    case COLUMN_TYPE_TIME:
                    case COLUMN_TYPE_TIMESTAMP:
                    case COLUMN_TYPE_TIMESTAMPTZ:
                    case COLUMN_TYPE_INTERVAL:
                    case COLUMN_TYPE_UUID:
                        cell.value.as_text = cb->data.str[ri]
                            ? (rb ? bump_strdup(rb, cb->data.str[ri]) : strdup(cb->data.str[ri]))
                            : NULL;
                        break;
                }
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
                if (cell->type == COLUMN_TYPE_SMALLINT)
                    cb->data.i16[nrows] = cell->value.as_smallint;
                else if (cell->type == COLUMN_TYPE_INT)
                    cb->data.i32[nrows] = cell->value.as_int;
                else if (cell->type == COLUMN_TYPE_FLOAT)
                    cb->data.f64[nrows] = cell->value.as_float;
                else if (cell->type == COLUMN_TYPE_BIGINT)
                    cb->data.i64[nrows] = cell->value.as_bigint;
                else if (cell->type == COLUMN_TYPE_NUMERIC)
                    cb->data.f64[nrows] = cell->value.as_numeric;
                else if (cell->type == COLUMN_TYPE_BOOLEAN)
                    cb->data.i32[nrows] = cell->value.as_bool;
                else if (column_type_is_text(cell->type))
                    cb->data.str[nrows] = cell->value.as_text;
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

static int filter_next(struct plan_exec_ctx *ctx, uint32_t node_idx,
                       struct row_block *out)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);

    /* Pull block from child */
    int rc = plan_next_block(ctx, pn->left, out);
    if (rc != 0) return rc;

    /* Allocate selection vector */
    uint32_t *sel = (uint32_t *)bump_alloc(&ctx->arena->scratch,
                                           out->count * sizeof(uint32_t));
    uint16_t sel_count = 0;

    /* Fast path: simple numeric comparison on a single column */
    if (pn->filter.col_idx >= 0 && pn->filter.col_idx < out->ncols) {
        struct col_block *cb = &out->cols[pn->filter.col_idx];
        int op = pn->filter.cmp_op;

        if (cb->type == COLUMN_TYPE_SMALLINT) {
            int16_t cmp_val = (pn->filter.cmp_val.type == COLUMN_TYPE_SMALLINT)
                ? pn->filter.cmp_val.value.as_smallint : (int16_t)pn->filter.cmp_val.value.as_int;
            const int16_t *vals = cb->data.i16;
            const uint8_t *nulls = cb->nulls;
            uint16_t count = out->count;

            switch (op) {
                case CMP_EQ: for (uint16_t r = 0; r < count; r++) if (!nulls[r] && vals[r] == cmp_val) sel[sel_count++] = r; break;
                case CMP_NE: for (uint16_t r = 0; r < count; r++) if (!nulls[r] && vals[r] != cmp_val) sel[sel_count++] = r; break;
                case CMP_LT: for (uint16_t r = 0; r < count; r++) if (!nulls[r] && vals[r] <  cmp_val) sel[sel_count++] = r; break;
                case CMP_GT: for (uint16_t r = 0; r < count; r++) if (!nulls[r] && vals[r] >  cmp_val) sel[sel_count++] = r; break;
                case CMP_LE: for (uint16_t r = 0; r < count; r++) if (!nulls[r] && vals[r] <= cmp_val) sel[sel_count++] = r; break;
                case CMP_GE: for (uint16_t r = 0; r < count; r++) if (!nulls[r] && vals[r] >= cmp_val) sel[sel_count++] = r; break;
                default: break;
            }
        } else if (cb->type == COLUMN_TYPE_INT || cb->type == COLUMN_TYPE_BOOLEAN) {
            int32_t cmp_val = pn->filter.cmp_val.value.as_int;
            const int32_t *vals = cb->data.i32;
            const uint8_t *nulls = cb->nulls;
            uint16_t count = out->count;

            switch (op) {
                case 0: /* CMP_EQ */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] == cmp_val)
                            sel[sel_count++] = i;
                    break;
                case 1: /* CMP_NEQ */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] != cmp_val)
                            sel[sel_count++] = i;
                    break;
                case 2: /* CMP_LT */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] < cmp_val)
                            sel[sel_count++] = i;
                    break;
                case 3: /* CMP_GT */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] > cmp_val)
                            sel[sel_count++] = i;
                    break;
                case 4: /* CMP_LTE */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] <= cmp_val)
                            sel[sel_count++] = i;
                    break;
                case 5: /* CMP_GTE */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] >= cmp_val)
                            sel[sel_count++] = i;
                    break;
                default:
                    goto fallback;
            }
            goto done;
        }

        if (cb->type == COLUMN_TYPE_FLOAT || cb->type == COLUMN_TYPE_NUMERIC) {
            double cmp_val_f = pn->filter.cmp_val.type == COLUMN_TYPE_FLOAT
                ? pn->filter.cmp_val.value.as_float
                : (double)pn->filter.cmp_val.value.as_int;
            const double *vals = cb->data.f64;
            const uint8_t *nulls = cb->nulls;
            uint16_t count = out->count;

            switch (op) {
                case 0: /* CMP_EQ */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] == cmp_val_f)
                            sel[sel_count++] = i;
                    break;
                case 1: /* CMP_NEQ */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] != cmp_val_f)
                            sel[sel_count++] = i;
                    break;
                case 2: /* CMP_LT */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] < cmp_val_f)
                            sel[sel_count++] = i;
                    break;
                case 3: /* CMP_GT */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] > cmp_val_f)
                            sel[sel_count++] = i;
                    break;
                case 4: /* CMP_LTE */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] <= cmp_val_f)
                            sel[sel_count++] = i;
                    break;
                case 5: /* CMP_GTE */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] >= cmp_val_f)
                            sel[sel_count++] = i;
                    break;
                default:
                    goto fallback;
            }
            goto done;
        }

        if (column_type_is_text(cb->type)) {
            const char *cmp_str = pn->filter.cmp_val.value.as_text;
            char * const *vals = cb->data.str;
            const uint8_t *nulls = cb->nulls;
            uint16_t count = out->count;

            if (!cmp_str) cmp_str = "";

            switch (op) {
                case 0: /* CMP_EQ */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] && strcmp(vals[i], cmp_str) == 0)
                            sel[sel_count++] = i;
                    break;
                case 1: /* CMP_NEQ */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] && strcmp(vals[i], cmp_str) != 0)
                            sel[sel_count++] = i;
                    break;
                case 2: /* CMP_LT */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] && strcmp(vals[i], cmp_str) < 0)
                            sel[sel_count++] = i;
                    break;
                case 3: /* CMP_GT */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] && strcmp(vals[i], cmp_str) > 0)
                            sel[sel_count++] = i;
                    break;
                case 4: /* CMP_LTE */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] && strcmp(vals[i], cmp_str) <= 0)
                            sel[sel_count++] = i;
                    break;
                case 5: /* CMP_GTE */
                    for (uint16_t i = 0; i < count; i++)
                        if (!nulls[i] && vals[i] && strcmp(vals[i], cmp_str) >= 0)
                            sel[sel_count++] = i;
                    break;
                default:
                    goto fallback;
            }
            goto done;
        }
    }

fallback:
    /* Slow path: use eval_condition per row */
    if (pn->filter.cond_idx != IDX_NONE) {
        struct plan_node *parent = &PLAN_NODE(ctx->arena, pn->left);
        struct table *t = NULL;
        if (parent->op == PLAN_SEQ_SCAN)
            t = parent->seq_scan.table;

        if (t) {
            /* We need to find the original row for eval_condition.
             * The scan state tells us which rows were scanned. */
            struct scan_state *sst = (struct scan_state *)ctx->node_states[pn->left];
            size_t base = sst ? (sst->cursor - out->count) : 0;
            for (uint16_t i = 0; i < out->count; i++) {
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
                if (cb->type == COLUMN_TYPE_SMALLINT)
                    cell->value.as_smallint = cb->data.i16[ri];
                else if (cb->type == COLUMN_TYPE_INT || cb->type == COLUMN_TYPE_BOOLEAN)
                    cell->value.as_int = cb->data.i32[ri];
                else if (cb->type == COLUMN_TYPE_BIGINT)
                    cell->value.as_bigint = cb->data.i64[ri];
                else if (cb->type == COLUMN_TYPE_FLOAT || cb->type == COLUMN_TYPE_NUMERIC)
                    cell->value.as_float = cb->data.f64[ri];
                else
                    cell->value.as_text = cb->data.str[ri];
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
                } else {
                    /* Text-like types */
                    if (column_type_is_text(result.type) || result.type == COLUMN_TYPE_DATE ||
                        result.type == COLUMN_TYPE_TIMESTAMP || result.type == COLUMN_TYPE_TIMESTAMPTZ ||
                        result.type == COLUMN_TYPE_INTERVAL || result.type == COLUMN_TYPE_UUID ||
                        result.type == COLUMN_TYPE_ENUM || result.type == COLUMN_TYPE_TIME) {
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

/* Helper: element size for a column type in the join cache */
static size_t jc_elem_size(enum column_type ct)
{
    if (ct == COLUMN_TYPE_SMALLINT) return sizeof(int16_t);
    if (ct == COLUMN_TYPE_BIGINT)   return sizeof(int64_t);
    if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC) return sizeof(double);
    if (column_type_is_text(ct))    return sizeof(char *);
    return sizeof(int32_t); /* INT, BOOLEAN, etc. */
}

/* Helper: copy data from heap cache array into col_block's fixed union array */
static void jc_copy_to_colblock(struct col_block *dst, void *src_data,
                                 uint8_t *src_nulls, enum column_type ct,
                                 uint32_t nrows)
{
    dst->type = ct;
    dst->count = (uint16_t)(nrows < BLOCK_CAPACITY ? nrows : BLOCK_CAPACITY);
    size_t esz = jc_elem_size(ct);
    memcpy(dst->nulls, src_nulls, nrows);
    /* Copy into the union — the col_block is bump-allocated with enough space
     * because hash_join_build already uses indices > BLOCK_CAPACITY */
    if (column_type_is_text(ct))
        memcpy(dst->data.str, src_data, esz * nrows);
    else if (ct == COLUMN_TYPE_SMALLINT)
        memcpy(dst->data.i16, src_data, esz * nrows);
    else if (ct == COLUMN_TYPE_BIGINT)
        memcpy(dst->data.i64, src_data, esz * nrows);
    else if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC)
        memcpy(dst->data.f64, src_data, esz * nrows);
    else
        memcpy(dst->data.i32, src_data, esz * nrows);
}

/* Helper: copy data from col_block's union array to heap cache array */
static void *jc_copy_from_colblock(const struct col_block *src, uint32_t nrows)
{
    size_t esz = jc_elem_size(src->type);
    void *dst = malloc(esz * nrows);
    if (!dst) return NULL;
    if (column_type_is_text(src->type))
        memcpy(dst, src->data.str, esz * nrows);
    else if (src->type == COLUMN_TYPE_SMALLINT)
        memcpy(dst, src->data.i16, esz * nrows);
    else if (src->type == COLUMN_TYPE_BIGINT)
        memcpy(dst, src->data.i64, esz * nrows);
    else if (src->type == COLUMN_TYPE_FLOAT || src->type == COLUMN_TYPE_NUMERIC)
        memcpy(dst, src->data.f64, esz * nrows);
    else
        memcpy(dst, src->data.i32, esz * nrows);
    return dst;
}

/* Restore hash join state from a table's join_cache into bump-allocated state */
static int hash_join_restore_from_cache(struct plan_exec_ctx *ctx,
                                        struct hash_join_state *st,
                                        struct join_cache *jc)
{
    st->build_ncols = jc->ncols;
    st->build_count = jc->nrows;
    st->build_cap = jc->nrows;
    st->build_cols = (struct col_block *)bump_calloc(&ctx->arena->scratch,
                                                     jc->ncols, sizeof(struct col_block));
    for (uint16_t c = 0; c < jc->ncols; c++)
        jc_copy_to_colblock(&st->build_cols[c], jc->col_data[c], jc->col_nulls[c],
                            jc->col_types[c], jc->nrows);

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
        jc->col_data[c] = jc_copy_from_colblock(&st->build_cols[c], st->build_count);
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
    uint16_t inner_ncols = 0;
    if (inner->op == PLAN_SEQ_SCAN) inner_ncols = inner->seq_scan.ncols;
    else inner_ncols = 16; /* fallback estimate */

    /* Collect all rows from inner side into col_blocks */
    uint32_t cap = 1024;
    st->build_ncols = inner_ncols;
    st->build_cols = (struct col_block *)bump_calloc(&ctx->arena->scratch,
                                                     inner_ncols, sizeof(struct col_block));
    st->build_cap = cap;
    st->build_count = 0;

    struct row_block inner_block;
    row_block_alloc(&inner_block, inner_ncols, &ctx->arena->scratch);

    while (plan_next_block(ctx, pn->right, &inner_block) == 0) {
        uint16_t active = row_block_active_count(&inner_block);
        /* Grow build col_blocks if needed */
        while (st->build_count + active > st->build_cap) {
            uint32_t new_cap = st->build_cap * 2;
            struct col_block *new_cols = (struct col_block *)bump_calloc(
                &ctx->arena->scratch, inner_ncols, sizeof(struct col_block));
            /* Copy existing data */
            for (uint16_t c = 0; c < inner_ncols; c++) {
                new_cols[c].type = st->build_cols[c].type;
                new_cols[c].count = (uint16_t)st->build_count;
                cb_bulk_copy(&new_cols[c], &st->build_cols[c], st->build_count);
            }
            st->build_cols = new_cols;
            st->build_cap = new_cap;
        }

        /* Append rows from this block */
        for (uint16_t i = 0; i < active; i++) {
            uint16_t ri = row_block_row_idx(&inner_block, i);
            uint32_t di = st->build_count++;
            for (uint16_t c = 0; c < inner_ncols; c++) {
                struct col_block *src_cb = &inner_block.cols[c];
                struct col_block *dst_cb = &st->build_cols[c];
                if (di == 0) dst_cb->type = src_cb->type;
                cb_copy_value(dst_cb, di, src_cb, ri);
                dst_cb->count = (uint16_t)(di + 1);
            }
        }
        row_block_reset(&inner_block);
    }

    /* Build hash table on the join key column */
    block_ht_init(&st->ht, st->build_count > 0 ? st->build_count : 1,
                  &ctx->arena->scratch);

    struct col_block *key_cb = &st->build_cols[key_col];
    for (uint32_t i = 0; i < st->build_count; i++) {
        uint32_t h = block_hash_cell(key_cb, (uint16_t)i);
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

    /* Determine outer (left child) column count */
    struct plan_node *outer = &PLAN_NODE(ctx->arena, pn->left);
    uint16_t outer_ncols = 0;
    if (outer->op == PLAN_SEQ_SCAN) outer_ncols = outer->seq_scan.ncols;
    else outer_ncols = (uint16_t)(out->ncols - st->build_ncols);

    struct row_block outer_block;
    row_block_alloc(&outer_block, outer_ncols, &ctx->arena->scratch);

    int rc = plan_next_block(ctx, pn->left, &outer_block);
    if (rc != 0) return rc;

    /* Probe: for each outer row, look up in hash table */
    int outer_key = pn->hash_join.outer_key_col;
    int inner_key = pn->hash_join.inner_key_col;
    struct col_block *outer_key_cb = &outer_block.cols[outer_key];
    struct col_block *inner_key_cb = &st->build_cols[inner_key];

    row_block_reset(out);
    uint16_t out_count = 0;
    uint16_t active = row_block_active_count(&outer_block);

    for (uint16_t i = 0; i < active && out_count < BLOCK_CAPACITY; i++) {
        uint16_t oi = row_block_row_idx(&outer_block, i);
        if (outer_key_cb->nulls[oi]) continue;

        uint32_t h = block_hash_cell(outer_key_cb, oi);
        uint32_t bucket = h & (st->ht.nbuckets - 1);
        uint32_t entry = st->ht.buckets[bucket];

        while (entry != IDX_NONE && entry != 0xFFFFFFFF && out_count < BLOCK_CAPACITY) {
            if (st->ht.hashes[entry] == h &&
                block_cell_eq(outer_key_cb, oi, inner_key_cb, (uint16_t)entry)) {
                /* Match: emit combined row [outer cols | inner cols] */
                for (uint16_t c = 0; c < outer_ncols; c++) {
                    out->cols[c].type = outer_block.cols[c].type;
                    cb_copy_value(&out->cols[c], out_count, &outer_block.cols[c], oi);
                }
                for (uint16_t c = 0; c < st->build_ncols; c++) {
                    out->cols[outer_ncols + c].type = st->build_cols[c].type;
                    cb_copy_value(&out->cols[outer_ncols + c], out_count,
                                  &st->build_cols[c], (uint16_t)entry);
                }
                out_count++;
            }
            entry = st->ht.nexts[entry];
        }
    }

    if (out_count == 0) return -1;

    out->count = out_count;
    for (uint16_t c = 0; c < out->ncols; c++)
        out->cols[c].count = out_count;

    return 0;
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
        st->nonnull = (size_t *)bump_calloc(&ctx->arena->scratch, agg_n * max_groups, sizeof(size_t));
        st->grp_counts = (size_t *)bump_calloc(&ctx->arena->scratch, max_groups, sizeof(size_t));
        st->minmax_init = (int *)bump_calloc(&ctx->arena->scratch, agg_n * max_groups, sizeof(int));

        block_ht_init(&st->ht, max_groups, &ctx->arena->scratch);
        ctx->node_states[node_idx] = st;
    }

    /* Phase 1: consume all input blocks */
    if (!st->input_done) {
        struct plan_node *child = &PLAN_NODE(ctx->arena, pn->left);
        uint16_t child_ncols = 0;
        if (child->op == PLAN_SEQ_SCAN) child_ncols = child->seq_scan.ncols;
        else child_ncols = pn->hash_agg.ngroup_cols; /* minimum */

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
                    struct agg_expr *ae = &ctx->arena->aggregates.items[pn->hash_agg.agg_start + a];
                    int ac = -1;
                    if (!sv_eq_cstr(ae->column, "*")) {
                        /* Find column index by name */
                        struct plan_node *scan = &PLAN_NODE(ctx->arena, pn->left);
                        if (scan->op == PLAN_SEQ_SCAN) {
                            struct table *t = scan->seq_scan.table;
                            ac = table_find_column_sv(t, ae->column);
                        }
                    }
                    if (ac < 0) continue;

                    struct col_block *acb = &input.cols[ac];
                    if (acb->nulls[ri]) continue;

                    size_t idx = a * st->group_cap + group_idx;
                    st->nonnull[idx]++;
                    double v = cb_to_double(acb, ri);
                    st->sums[idx] += v;
                    if (!st->minmax_init[idx] || v < st->mins[idx])
                        st->mins[idx] = v;
                    if (!st->minmax_init[idx] || v > st->maxs[idx])
                        st->maxs[idx] = v;
                    st->minmax_init[idx] = 1;
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
                    dst->data.i32[out_count] = (sv_eq_cstr(ae->column, "*"))
                        ? (int32_t)st->grp_counts[g]
                        : (int32_t)st->nonnull[idx];
                    break;
                case AGG_SUM:
                    if (st->nonnull[idx] == 0) {
                        dst->type = COLUMN_TYPE_BIGINT;
                        dst->nulls[out_count] = 1;
                    } else {
                        double sv = st->sums[idx];
                        if (sv == (double)(int32_t)sv && sv >= -2147483648.0 && sv <= 2147483647.0) {
                            dst->type = COLUMN_TYPE_INT;
                            dst->nulls[out_count] = 0;
                            dst->data.i32[out_count] = (int32_t)sv;
                        } else {
                            dst->type = COLUMN_TYPE_BIGINT;
                            dst->nulls[out_count] = 0;
                            dst->data.i64[out_count] = (int64_t)sv;
                        }
                    }
                    break;
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
                case AGG_MAX:
                    if (st->nonnull[idx] == 0) {
                        dst->type = COLUMN_TYPE_INT;
                        dst->nulls[out_count] = 1;
                    } else {
                        dst->type = COLUMN_TYPE_INT;
                        dst->nulls[out_count] = 0;
                        dst->data.i32[out_count] = (int32_t)(ae->func == AGG_MIN
                            ? st->mins[idx] : st->maxs[idx]);
                    }
                    break;
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

/* ---- Sort ---- */

struct block_sort_ctx {
    struct col_block *all_cols;
    uint16_t          ncols;
    uint32_t          rows_per_block;
    int              *sort_cols;
    int              *sort_descs;
    int              *sort_nulls_first; /* per-key: -1=default, 0=NULLS LAST, 1=NULLS FIRST */
    uint16_t          nsort_cols;
    /* Flat arrays for fast comparator (one per sort key) */
    void             *flat_keys[32];   /* contiguous typed array per sort key */
    uint8_t          *flat_nulls[32];  /* contiguous null bitmap per sort key */
    enum column_type  key_types[32];
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
        } else if (kt == COLUMN_TYPE_BIGINT) {
            int64_t va = ((const int64_t *)_bsort_ctx.flat_keys[k])[ia];
            int64_t vb = ((const int64_t *)_bsort_ctx.flat_keys[k])[ib];
            cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
        } else if (kt == COLUMN_TYPE_FLOAT || kt == COLUMN_TYPE_NUMERIC) {
            double va = ((const double *)_bsort_ctx.flat_keys[k])[ia];
            double vb = ((const double *)_bsort_ctx.flat_keys[k])[ib];
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

            size_t elem_sz;
            if (kt == COLUMN_TYPE_SMALLINT)
                elem_sz = sizeof(int16_t);
            else if (kt == COLUMN_TYPE_INT || kt == COLUMN_TYPE_BOOLEAN)
                elem_sz = sizeof(int32_t);
            else if (kt == COLUMN_TYPE_BIGINT)
                elem_sz = sizeof(int64_t);
            else if (kt == COLUMN_TYPE_FLOAT || kt == COLUMN_TYPE_NUMERIC)
                elem_sz = sizeof(double);
            else
                elem_sz = sizeof(char *);

            _bsort_ctx.flat_col_data[ci] = bump_alloc(&ctx->arena->scratch,
                                                       (total ? total : 1) * elem_sz);
            _bsort_ctx.flat_col_nulls[ci] = (uint8_t *)bump_alloc(&ctx->arena->scratch,
                                                                   (total ? total : 1));

            uint32_t fi = 0;
            for (uint32_t b = 0; b < st->nblocks; b++) {
                struct col_block *src = &_bsort_ctx.all_cols[b * child_ncols + ci];
                uint16_t cnt = st->collected[b].count;
                memcpy(_bsort_ctx.flat_col_nulls[ci] + fi, src->nulls, cnt);
                if (kt == COLUMN_TYPE_SMALLINT)
                    memcpy((int16_t *)_bsort_ctx.flat_col_data[ci] + fi, src->data.i16, cnt * sizeof(int16_t));
                else if (kt == COLUMN_TYPE_INT || kt == COLUMN_TYPE_BOOLEAN)
                    memcpy((int32_t *)_bsort_ctx.flat_col_data[ci] + fi, src->data.i32, cnt * sizeof(int32_t));
                else if (kt == COLUMN_TYPE_BIGINT)
                    memcpy((int64_t *)_bsort_ctx.flat_col_data[ci] + fi, src->data.i64, cnt * sizeof(int64_t));
                else if (kt == COLUMN_TYPE_FLOAT || kt == COLUMN_TYPE_NUMERIC)
                    memcpy((double *)_bsort_ctx.flat_col_data[ci] + fi, src->data.f64, cnt * sizeof(double));
                else
                    memcpy((char **)_bsort_ctx.flat_col_data[ci] + fi, src->data.str, cnt * sizeof(char *));
                fi += cnt;
            }
        }

        /* Point sort key flat arrays into the all-column flat arrays */
        uint16_t nsk = pn->sort.nsort_cols < 32 ? pn->sort.nsort_cols : 32;
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
    uint16_t out_count = 0;

    /* Emit sorted rows directly from flat arrays — no block remap needed */
    for (uint16_t c = 0; c < child_ncols; c++)
        out->cols[c].type = _bsort_ctx.flat_col_types[c];

    while (st->emit_cursor < st->sorted_count && out_count < BLOCK_CAPACITY) {
        uint32_t fi = st->sorted_indices[st->emit_cursor++];

        for (uint16_t c = 0; c < child_ncols; c++) {
            out->cols[c].nulls[out_count] = _bsort_ctx.flat_col_nulls[c][fi];
            enum column_type ct = _bsort_ctx.flat_col_types[c];
            if (ct == COLUMN_TYPE_SMALLINT)
                out->cols[c].data.i16[out_count] = ((const int16_t *)_bsort_ctx.flat_col_data[c])[fi];
            else if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN)
                out->cols[c].data.i32[out_count] = ((const int32_t *)_bsort_ctx.flat_col_data[c])[fi];
            else if (ct == COLUMN_TYPE_BIGINT)
                out->cols[c].data.i64[out_count] = ((const int64_t *)_bsort_ctx.flat_col_data[c])[fi];
            else if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC)
                out->cols[c].data.f64[out_count] = ((const double *)_bsort_ctx.flat_col_data[c])[fi];
            else
                out->cols[c].data.str[out_count] = ((char **)_bsort_ctx.flat_col_data[c])[fi];
        }
        out_count++;
    }

    out->count = out_count;
    for (uint16_t c = 0; c < child_ncols; c++)
        out->cols[c].count = out_count;

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
            } else if (ot == COLUMN_TYPE_BIGINT) {
                int64_t va = ((int64_t *)_wsort_ctx.ord_data)[ia];
                int64_t vb = ((int64_t *)_wsort_ctx.ord_data)[ib];
                cmp = (va > vb) - (va < vb);
            } else if (ot == COLUMN_TYPE_FLOAT || ot == COLUMN_TYPE_NUMERIC) {
                double va = ((double *)_wsort_ctx.ord_data)[ia];
                double vb = ((double *)_wsort_ctx.ord_data)[ib];
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
    if (ct == COLUMN_TYPE_BIGINT) {
        int64_t va = ((int64_t *)data)[a], vb = ((int64_t *)data)[b];
        return (va > vb) - (va < vb);
    }
    if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC) {
        double va = ((double *)data)[a], vb = ((double *)data)[b];
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
            size_t esz;
            if (kt == COLUMN_TYPE_SMALLINT) esz = sizeof(int16_t);
            else if (kt == COLUMN_TYPE_INT || kt == COLUMN_TYPE_BOOLEAN) esz = sizeof(int32_t);
            else if (kt == COLUMN_TYPE_BIGINT) esz = sizeof(int64_t);
            else if (kt == COLUMN_TYPE_FLOAT || kt == COLUMN_TYPE_NUMERIC) esz = sizeof(double);
            else esz = sizeof(char *);

            st->flat_data[ci] = bump_alloc(&ctx->arena->scratch, total * esz);
            st->flat_nulls[ci] = (uint8_t *)bump_alloc(&ctx->arena->scratch, total);

            uint32_t fi = 0;
            for (uint32_t b = 0; b < nblocks; b++) {
                struct col_block *src = &collected[b].cols[ci];
                uint16_t cnt = collected[b].count;
                memcpy(st->flat_nulls[ci] + fi, src->nulls, cnt);
                if (kt == COLUMN_TYPE_SMALLINT)
                    memcpy((int16_t *)st->flat_data[ci] + fi, src->data.i16, cnt * sizeof(int16_t));
                else if (kt == COLUMN_TYPE_INT || kt == COLUMN_TYPE_BOOLEAN)
                    memcpy((int32_t *)st->flat_data[ci] + fi, src->data.i32, cnt * sizeof(int32_t));
                else if (kt == COLUMN_TYPE_BIGINT)
                    memcpy((int64_t *)st->flat_data[ci] + fi, src->data.i64, cnt * sizeof(int64_t));
                else if (kt == COLUMN_TYPE_FLOAT || kt == COLUMN_TYPE_NUMERIC)
                    memcpy((double *)st->flat_data[ci] + fi, src->data.f64, cnt * sizeof(double));
                else
                    memcpy((char **)st->flat_data[ci] + fi, src->data.str, cnt * sizeof(char *));
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

        for (uint16_t w = 0; w < nw; w++) {
            int wf = pn->window.win_func[w];
            int oc = pn->window.win_ord_col[w];
            int ac = pn->window.win_arg_col[w];

            for (uint32_t p = 0; p < st->nparts; p++) {
                uint32_t ps = st->part_starts[p];
                uint32_t pe = st->part_starts[p + 1];
                uint32_t psize = pe - ps;

                switch (wf) {
                case WIN_ROW_NUMBER:
                    for (uint32_t i = ps; i < pe; i++)
                        st->win_i32[i * nw + w] = (int32_t)(i - ps + 1);
                    break;
                case WIN_RANK: {
                    int32_t rank = 1;
                    for (uint32_t i = ps; i < pe; i++) {
                        if (i > ps && oc >= 0 &&
                            flat_col_ord_cmp(st->flat_data[oc], st->flat_types[oc], st->flat_nulls[oc],
                                             st->sorted[i], st->sorted[i-1]) != 0)
                            rank = (int32_t)(i - ps + 1);
                        st->win_i32[i * nw + w] = rank;
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
                        st->win_i32[i * nw + w] = rank;
                    }
                    break;
                }
                case WIN_NTILE: {
                    int nb = pn->window.win_offset[w] > 0 ? pn->window.win_offset[w] : 1;
                    for (uint32_t i = ps; i < pe; i++)
                        st->win_i32[i * nw + w] = (int32_t)(((i - ps) * (uint32_t)nb) / psize) + 1;
                    break;
                }
                case WIN_PERCENT_RANK: {
                    st->win_is_dbl[w] = 1;
                    if (psize <= 1) {
                        for (uint32_t i = ps; i < pe; i++) st->win_f64[i * nw + w] = 0.0;
                    } else {
                        int32_t rank = 1;
                        for (uint32_t i = ps; i < pe; i++) {
                            if (i > ps && oc >= 0 &&
                                flat_col_ord_cmp(st->flat_data[oc], st->flat_types[oc], st->flat_nulls[oc],
                                                 st->sorted[i], st->sorted[i-1]) != 0)
                                rank = (int32_t)(i - ps + 1);
                            st->win_f64[i * nw + w] = (double)(rank - 1) / (double)(psize - 1);
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
                            for (uint32_t k = i; k < j; k++) st->win_f64[k * nw + w] = cd;
                            i = j;
                        }
                    } else {
                        for (uint32_t i = ps; i < pe; i++) st->win_f64[i * nw + w] = 1.0;
                    }
                    break;
                }
                case WIN_LAG:
                case WIN_LEAD: {
                    int offset = pn->window.win_offset[w];
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
                        if (in_range && ac >= 0) {
                            uint32_t si = st->sorted[target];
                            if (st->flat_nulls[ac][si]) {
                                st->win_null[i * nw + w] = 1;
                            } else {
                                st->win_f64[i * nw + w] = flat_col_to_double(st->flat_data[ac], st->flat_nulls[ac], st->flat_types[ac], si);
                                st->win_is_dbl[w] = 1;
                            }
                        } else {
                            st->win_null[i * nw + w] = 1;
                        }
                    }
                    break;
                }
                case WIN_FIRST_VALUE:
                case WIN_LAST_VALUE: {
                    for (uint32_t i = ps; i < pe; i++) {
                        uint32_t target = (wf == WIN_FIRST_VALUE) ? ps : (pe - 1);
                        if (ac >= 0) {
                            uint32_t si = st->sorted[target];
                            if (st->flat_nulls[ac][si]) {
                                st->win_null[i * nw + w] = 1;
                            } else {
                                st->win_f64[i * nw + w] = flat_col_to_double(st->flat_data[ac], st->flat_nulls[ac], st->flat_types[ac], si);
                                st->win_is_dbl[w] = 1;
                            }
                        } else {
                            st->win_null[i * nw + w] = 1;
                        }
                    }
                    break;
                }
                case WIN_NTH_VALUE: {
                    int nth = pn->window.win_offset[w];
                    for (uint32_t i = ps; i < pe; i++) {
                        if (nth >= 1 && (uint32_t)nth <= psize && ac >= 0) {
                            uint32_t si = st->sorted[ps + (uint32_t)(nth - 1)];
                            if (st->flat_nulls[ac][si]) {
                                st->win_null[i * nw + w] = 1;
                            } else {
                                st->win_f64[i * nw + w] = flat_col_to_double(st->flat_data[ac], st->flat_nulls[ac], st->flat_types[ac], si);
                                st->win_is_dbl[w] = 1;
                            }
                        } else {
                            st->win_null[i * nw + w] = 1;
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
                            if (wf == WIN_SUM) {
                                if (ac >= 0 && (st->flat_types[ac] == COLUMN_TYPE_FLOAT || st->flat_types[ac] == COLUMN_TYPE_NUMERIC)) {
                                    st->win_is_dbl[w] = 1;
                                    st->win_f64[i * nw + w] = part_sum;
                                } else {
                                    st->win_i32[i * nw + w] = (int32_t)part_sum;
                                }
                            } else if (wf == WIN_COUNT) {
                                st->win_i32[i * nw + w] = (ac >= 0) ? part_nn : (int32_t)psize;
                            } else {
                                st->win_is_dbl[w] = 1;
                                if (part_nn > 0) st->win_f64[i * nw + w] = part_sum / (double)part_nn;
                                else st->win_null[i * nw + w] = 1;
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
                            if (wf == WIN_SUM) {
                                if (ac >= 0 && (st->flat_types[ac] == COLUMN_TYPE_FLOAT || st->flat_types[ac] == COLUMN_TYPE_NUMERIC)) {
                                    st->win_is_dbl[w] = 1;
                                    st->win_f64[i * nw + w] = running_sum;
                                } else {
                                    st->win_i32[i * nw + w] = (int32_t)running_sum;
                                }
                            } else if (wf == WIN_COUNT) {
                                st->win_i32[i * nw + w] = (ac >= 0) ? running_nn : running_count;
                            } else {
                                st->win_is_dbl[w] = 1;
                                if (running_nn > 0) st->win_f64[i * nw + w] = running_sum / (double)running_nn;
                                else st->win_null[i * nw + w] = 1;
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
                            if (wf == WIN_SUM) {
                                if (ac >= 0 && (st->flat_types[ac] == COLUMN_TYPE_FLOAT || st->flat_types[ac] == COLUMN_TYPE_NUMERIC)) {
                                    st->win_is_dbl[w] = 1; st->win_f64[i * nw + w] = frame_sum;
                                } else st->win_i32[i * nw + w] = (int32_t)frame_sum;
                            } else if (wf == WIN_COUNT) {
                                st->win_i32[i * nw + w] = (ac >= 0) ? frame_nn : frame_count;
                            } else {
                                st->win_is_dbl[w] = 1;
                                if (frame_nn > 0) st->win_f64[i * nw + w] = frame_sum / (double)frame_nn;
                                else st->win_null[i * nw + w] = 1;
                            }
                        }
                    }
                    break;
                }
                } /* switch */
            } /* partitions */
        } /* window exprs */

        st->input_done = 1;
        st->emit_cursor = 0;
    }

    /* Emit phase: output blocks with passthrough + window columns */
    if (st->emit_cursor >= st->total_rows) return -1;

    uint16_t out_ncols = pn->window.out_ncols;
    uint16_t n_pass = pn->window.n_pass;
    uint16_t nw = pn->window.n_win;
    row_block_reset(out);
    uint16_t out_count = 0;

    /* Set output column types */
    for (uint16_t c = 0; c < n_pass; c++)
        out->cols[c].type = st->flat_types[pn->window.pass_cols[c]];
    for (uint16_t w = 0; w < nw; w++)
        out->cols[n_pass + w].type = st->win_is_dbl[w] ? COLUMN_TYPE_FLOAT : COLUMN_TYPE_INT;

    while (st->emit_cursor < st->total_rows && out_count < BLOCK_CAPACITY) {
        uint32_t fi = st->sorted[st->emit_cursor++];

        /* Passthrough columns */
        for (uint16_t c = 0; c < n_pass; c++) {
            int sci = pn->window.pass_cols[c];
            out->cols[c].nulls[out_count] = st->flat_nulls[sci][fi];
            enum column_type ct = st->flat_types[sci];
            if (ct == COLUMN_TYPE_SMALLINT)
                out->cols[c].data.i16[out_count] = ((int16_t *)st->flat_data[sci])[fi];
            else if (ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN)
                out->cols[c].data.i32[out_count] = ((int32_t *)st->flat_data[sci])[fi];
            else if (ct == COLUMN_TYPE_BIGINT)
                out->cols[c].data.i64[out_count] = ((int64_t *)st->flat_data[sci])[fi];
            else if (ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC)
                out->cols[c].data.f64[out_count] = ((double *)st->flat_data[sci])[fi];
            else
                out->cols[c].data.str[out_count] = ((char **)st->flat_data[sci])[fi];
        }

        /* Window result columns */
        uint32_t si = st->emit_cursor - 1; /* sorted position */
        for (uint16_t w = 0; w < nw; w++) {
            uint16_t oc = n_pass + w;
            if (st->win_null[si * nw + w]) {
                out->cols[oc].nulls[out_count] = 1;
                if (st->win_is_dbl[w])
                    out->cols[oc].data.f64[out_count] = 0.0;
                else
                    out->cols[oc].data.i32[out_count] = 0;
            } else if (st->win_is_dbl[w]) {
                out->cols[oc].nulls[out_count] = 0;
                out->cols[oc].data.f64[out_count] = st->win_f64[si * nw + w];
            } else {
                out->cols[oc].nulls[out_count] = 0;
                out->cols[oc].data.i32[out_count] = st->win_i32[si * nw + w];
            }
        }
        out_count++;
    }

    out->count = out_count;
    for (uint16_t c = 0; c < out_ncols; c++)
        out->cols[c].count = out_count;

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
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
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
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
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
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
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
                case COLUMN_TYPE_TEXT:
                case COLUMN_TYPE_ENUM:
                case COLUMN_TYPE_DATE:
                case COLUMN_TYPE_TIME:
                case COLUMN_TYPE_TIMESTAMP:
                case COLUMN_TYPE_TIMESTAMPTZ:
                case COLUMN_TYPE_INTERVAL:
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
                    /* INTERSECT / INTERSECT ALL: mark ALL LHS rows that match this RHS row.
                     * Legacy behavior: keeps all LHS rows with any match in RHS. */
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
                            if (eq) st->matched[entry] = 1;
                        }
                        entry = st->ht.nexts[entry];
                    }
                } else {
                    /* EXCEPT / EXCEPT ALL: mark ALL matching LHS rows to remove.
                     * Legacy behavior: removes all LHS rows with any match in RHS. */
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
                            if (eq) st->matched[entry] = 1;
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
    case PLAN_NESTED_LOOP:
    case PLAN_SIMPLE_AGG:
        /* not yet implemented */
        return -1;
    }
    return -1;
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
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_DATE:
    case COLUMN_TYPE_TIME:
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_TIMESTAMPTZ:
    case COLUMN_TYPE_INTERVAL:
    case COLUMN_TYPE_UUID:
        if (c->value.as_text)
            return snprintf(buf, buflen, "'%s'", c->value.as_text);
        return snprintf(buf, buflen, "?");
    }
    return snprintf(buf, buflen, "?");
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
    case PLAN_INDEX_SCAN: {
        /* Try to show the filter condition */
        const char *tname = pn->index_scan.table ? pn->index_scan.table->name : "?";
        if (pn->index_scan.cond_idx != IDX_NONE) {
            struct condition *cond = &arena->conditions.items[pn->index_scan.cond_idx];
            const char *op_str = cond->op == CMP_EQ ? "=" : cond->op == CMP_NE ? "!=" :
                cond->op == CMP_LT ? "<" : cond->op == CMP_GT ? ">" :
                cond->op == CMP_LE ? "<=" : cond->op == CMP_GE ? ">=" : "?";
            char vbuf[64] = "";
            cell_value_to_str(&cond->value, vbuf, sizeof(vbuf));
            n = snprintf(buf + written, buflen - written,
                         "Index Scan on %s (" SV_FMT " %s %s)\n",
                         tname, (int)cond->column.len, cond->column.data, op_str, vbuf);
        } else {
            n = snprintf(buf + written, buflen - written, "Index Scan on %s\n", tname);
        }
        if (n > 0) written += n;
        break;
    }
    case PLAN_FILTER: {
        /* Show filter condition */
        if (pn->filter.cond_idx != IDX_NONE) {
            struct condition *cond = &arena->conditions.items[pn->filter.cond_idx];
            const char *op_str = cond->op == CMP_EQ ? "=" : cond->op == CMP_NE ? "!=" :
                cond->op == CMP_LT ? "<" : cond->op == CMP_GT ? ">" :
                cond->op == CMP_LE ? "<=" : cond->op == CMP_GE ? ">=" : "?";
            char vbuf[64] = "";
            cell_value_to_str(&cond->value, vbuf, sizeof(vbuf));
            n = snprintf(buf + written, buflen - written,
                         "Filter: (" SV_FMT " %s %s)\n",
                         (int)cond->column.len, cond->column.data, op_str, vbuf);
        } else {
            n = snprintf(buf + written, buflen - written, "Filter\n");
        }
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        break;
    }
    case PLAN_PROJECT:
        n = snprintf(buf + written, buflen - written, "Project\n");
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        break;
    case PLAN_EXPR_PROJECT:
        n = snprintf(buf + written, buflen - written, "Project (expressions)\n");
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        break;
    case PLAN_SORT: {
        n = snprintf(buf + written, buflen - written, "Sort");
        if (n > 0) written += n;
        /* show sort columns */
        if (pn->sort.nsort_cols > 0 && pn->left != IDX_NONE) {
            /* try to get table from child seq_scan */
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
        break;
    }
    case PLAN_HASH_JOIN:
        n = snprintf(buf + written, buflen - written, "Hash Join\n");
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->right, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        break;
    case PLAN_HASH_SEMI_JOIN:
        n = snprintf(buf + written, buflen - written, "Hash Semi Join\n");
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->right, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        break;
    case PLAN_LIMIT: {
        if (pn->limit.has_limit && pn->limit.has_offset)
            n = snprintf(buf + written, buflen - written, "Limit (%zu, offset %zu)\n",
                         pn->limit.limit, pn->limit.offset);
        else if (pn->limit.has_limit)
            n = snprintf(buf + written, buflen - written, "Limit (%zu)\n", pn->limit.limit);
        else
            n = snprintf(buf + written, buflen - written, "Limit\n");
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        break;
    }
    case PLAN_DISTINCT:
        n = snprintf(buf + written, buflen - written, "HashAggregate (DISTINCT)\n");
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        break;
    case PLAN_HASH_AGG:
        n = snprintf(buf + written, buflen - written, "HashAggregate\n");
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        break;
    case PLAN_SIMPLE_AGG:
        n = snprintf(buf + written, buflen - written, "Aggregate\n");
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        break;
    case PLAN_SET_OP: {
        const char *opname = "SetOp";
        if (pn->set_op.set_op == 0) opname = pn->set_op.set_all ? "Append (UNION ALL)" : "HashSetOp Union";
        else if (pn->set_op.set_op == 1) opname = "HashSetOp Intersect";
        else if (pn->set_op.set_op == 2) opname = "HashSetOp Except";
        n = snprintf(buf + written, buflen - written, "%s\n", opname);
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->right, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        break;
    }
    case PLAN_WINDOW:
        n = snprintf(buf + written, buflen - written, "WindowAgg\n");
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        break;
    case PLAN_GENERATE_SERIES:
        n = snprintf(buf + written, buflen - written, "Function Scan on generate_series\n");
        if (n > 0) written += n;
        break;
    case PLAN_NESTED_LOOP:
        n = snprintf(buf + written, buflen - written, "Nested Loop\n");
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->left, buf + written, buflen - written, depth + 1);
        if (n > 0) written += n;
        n = plan_explain_node(arena, pn->right, buf + written, buflen - written, depth + 1);
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

/* ---- Plan builder ---- */

/* Try to build a block-oriented plan for a simple single-table SELECT.
 * Returns root plan node index, or IDX_NONE if the query is too complex
 * and should fall back to the legacy executor. */
uint32_t plan_build_select(struct table *t, struct query_select *s,
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
            return IDX_NONE;
        if (!sv_eq_cstr(s->columns, "*"))
            return IDX_NONE;

        struct cell c_start = eval_expr(s->gs_start_expr, arena, NULL, NULL, db, NULL);
        struct cell c_stop  = eval_expr(s->gs_stop_expr, arena, NULL, NULL, db, NULL);

        /* Bail out for timestamp/date series — keep on legacy path */
        if (c_start.type == COLUMN_TYPE_DATE || c_start.type == COLUMN_TYPE_TIMESTAMP ||
            c_start.type == COLUMN_TYPE_TIMESTAMPTZ)
            return IDX_NONE;
        if (c_start.type == COLUMN_TYPE_INTERVAL || c_stop.type == COLUMN_TYPE_INTERVAL)
            return IDX_NONE;

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
                return IDX_NONE; /* timestamp step — bail to legacy */
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

        /* Add LIMIT/OFFSET if present */
        if (s->has_limit || s->has_offset) {
            uint32_t limit_idx = plan_alloc_node(arena, PLAN_LIMIT);
            PLAN_NODE(arena, limit_idx).left = current;
            PLAN_NODE(arena, limit_idx).limit.has_limit = s->has_limit;
            PLAN_NODE(arena, limit_idx).limit.limit = s->has_limit ? (size_t)s->limit_count : 0;
            PLAN_NODE(arena, limit_idx).limit.has_offset = s->has_offset;
            PLAN_NODE(arena, limit_idx).limit.offset = s->has_offset ? (size_t)s->offset_count : 0;
            current = limit_idx;
        }

        return current;
    }

    /* ---- Single equi-join fast path ---- */
    if (s->has_join && s->joins_count == 1 && db) {
        struct join_info *ji = &arena->joins.items[s->joins_start];
        /* Only handle: INNER JOIN, equi-join (CMP_EQ), not LATERAL, not NATURAL, not USING */
        if (ji->join_type != 0) return IDX_NONE;       /* not INNER */
        if (ji->is_lateral)     return IDX_NONE;
        if (ji->is_natural)     return IDX_NONE;
        if (ji->has_using)      return IDX_NONE;
        /* Bail out for complex queries on joined results */
        if (s->select_exprs_count > 0) return IDX_NONE; /* window functions */
        if (s->has_group_by)        return IDX_NONE;
        if (s->aggregates_count > 0) return IDX_NONE;
        if (s->has_set_op)          return IDX_NONE;
        if (s->ctes_count > 0)     return IDX_NONE;
        if (s->cte_sql != IDX_NONE) return IDX_NONE;
        if (s->from_subquery_sql != IDX_NONE) return IDX_NONE;
        if (s->has_recursive_cte)   return IDX_NONE;
        if (s->has_distinct)        return IDX_NONE;
        if (s->has_distinct_on)     return IDX_NONE;
        if (s->where.has_where)     return IDX_NONE;    /* WHERE on joined result — complex */
        if (s->has_order_by)        return IDX_NONE;    /* ORDER BY on joined result — complex */
        if (s->insert_rows_count > 0) return IDX_NONE;

        struct table *t1 = t;
        if (!t1) return IDX_NONE;
        struct table *t2 = db_find_table_sv(db, ji->join_table);
        if (!t2) return IDX_NONE;
        if (t2->view_sql) return IDX_NONE; /* view — need materialization */

        /* Extract join key columns — need simple equi-join on two column refs */
        int t1_key = -1, t2_key = -1;

        if (ji->join_on_cond != IDX_NONE) {
            struct condition *cond = &COND(arena, ji->join_on_cond);
            if (cond->type != COND_COMPARE || cond->op != CMP_EQ)
                return IDX_NONE;
            if (cond->lhs_expr != IDX_NONE) return IDX_NONE;
            if (cond->column.len == 0 || cond->rhs_column.len == 0)
                return IDX_NONE;
            /* Try both orderings: col might be in t1 or t2 */
            int lhs_in_t1 = table_find_column_sv(t1, cond->column);
            int lhs_in_t2 = table_find_column_sv(t2, cond->column);
            int rhs_in_t1 = table_find_column_sv(t1, cond->rhs_column);
            int rhs_in_t2 = table_find_column_sv(t2, cond->rhs_column);
            if (lhs_in_t1 >= 0 && rhs_in_t2 >= 0) {
                t1_key = lhs_in_t1; t2_key = rhs_in_t2;
            } else if (lhs_in_t2 >= 0 && rhs_in_t1 >= 0) {
                t1_key = rhs_in_t1; t2_key = lhs_in_t2;
            } else {
                return IDX_NONE;
            }
        } else if (ji->join_left_col.len > 0 && ji->join_right_col.len > 0 &&
                   ji->join_op == CMP_EQ) {
            int left_in_t1 = table_find_column_sv(t1, ji->join_left_col);
            int left_in_t2 = table_find_column_sv(t2, ji->join_left_col);
            int right_in_t1 = table_find_column_sv(t1, ji->join_right_col);
            int right_in_t2 = table_find_column_sv(t2, ji->join_right_col);
            if (left_in_t1 >= 0 && right_in_t2 >= 0) {
                t1_key = left_in_t1; t2_key = right_in_t2;
            } else if (left_in_t2 >= 0 && right_in_t1 >= 0) {
                t1_key = right_in_t1; t2_key = left_in_t2;
            } else {
                return IDX_NONE;
            }
        } else {
            return IDX_NONE;
        }

        /* Resolve projection columns across merged column space [t1 cols | t2 cols] */
        uint16_t t1_ncols = (uint16_t)t1->columns.count;
        uint16_t t2_ncols = (uint16_t)t2->columns.count;
        int select_all_join = sv_eq_cstr(s->columns, "*");
        int need_project_join = 0;
        int *proj_map_join = NULL;
        uint16_t proj_ncols_join = 0;

        if (select_all_join) {
            /* No projection — return all columns from both tables */
        } else if (s->parsed_columns_count > 0) {
            proj_ncols_join = (uint16_t)s->parsed_columns_count;
            proj_map_join = (int *)bump_alloc(&arena->scratch, proj_ncols_join * sizeof(int));
            for (uint32_t i = 0; i < s->parsed_columns_count; i++) {
                struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + i];
                if (sc->expr_idx == IDX_NONE) return IDX_NONE;
                struct expr *e = &EXPR(arena, sc->expr_idx);
                if (e->type != EXPR_COLUMN_REF) return IDX_NONE;
                /* Try to find in t1 first, then t2 (with offset) */
                int ci = table_find_column_sv(t1, e->column_ref.column);
                if (ci >= 0) {
                    proj_map_join[i] = ci;
                } else {
                    ci = table_find_column_sv(t2, e->column_ref.column);
                    if (ci >= 0) {
                        proj_map_join[i] = t1_ncols + ci;
                    } else {
                        return IDX_NONE;
                    }
                }
            }
            need_project_join = 1;
        } else {
            return IDX_NONE; /* legacy text-based column list */
        }

        /* Bail out if either table has mixed cell types */
        if (t1->rows.count > 0) {
            struct row *first = &t1->rows.items[0];
            for (size_t c = 0; c < first->cells.count && c < t1->columns.count; c++) {
                if (!first->cells.items[c].is_null &&
                    first->cells.items[c].type != t1->columns.items[c].type)
                    return IDX_NONE;
            }
        }
        if (t2->rows.count > 0) {
            struct row *first = &t2->rows.items[0];
            for (size_t c = 0; c < first->cells.count && c < t2->columns.count; c++) {
                if (!first->cells.items[c].is_null &&
                    first->cells.items[c].type != t2->columns.items[c].type)
                    return IDX_NONE;
            }
        }

        /* --- All validation passed, build plan nodes --- */

        /* SEQ_SCAN for t1 (outer / left / probe side) */
        int *col_map1 = (int *)bump_alloc(&arena->scratch, t1_ncols * sizeof(int));
        for (uint16_t i = 0; i < t1_ncols; i++) col_map1[i] = (int)i;
        uint32_t scan1 = plan_alloc_node(arena, PLAN_SEQ_SCAN);
        PLAN_NODE(arena, scan1).seq_scan.table = t1;
        PLAN_NODE(arena, scan1).seq_scan.ncols = t1_ncols;
        PLAN_NODE(arena, scan1).seq_scan.col_map = col_map1;
        PLAN_NODE(arena, scan1).est_rows = (double)t1->rows.count;

        /* SEQ_SCAN for t2 (inner / right / build side) */
        int *col_map2 = (int *)bump_alloc(&arena->scratch, t2_ncols * sizeof(int));
        for (uint16_t i = 0; i < t2_ncols; i++) col_map2[i] = (int)i;
        uint32_t scan2 = plan_alloc_node(arena, PLAN_SEQ_SCAN);
        PLAN_NODE(arena, scan2).seq_scan.table = t2;
        PLAN_NODE(arena, scan2).seq_scan.ncols = t2_ncols;
        PLAN_NODE(arena, scan2).seq_scan.col_map = col_map2;
        PLAN_NODE(arena, scan2).est_rows = (double)t2->rows.count;

        /* HASH_JOIN node */
        uint32_t join_idx = plan_alloc_node(arena, PLAN_HASH_JOIN);
        PLAN_NODE(arena, join_idx).left = scan1;   /* outer (probe) */
        PLAN_NODE(arena, join_idx).right = scan2;  /* inner (build) */
        PLAN_NODE(arena, join_idx).hash_join.outer_key_col = t1_key;
        PLAN_NODE(arena, join_idx).hash_join.inner_key_col = t2_key;
        PLAN_NODE(arena, join_idx).hash_join.join_type = 0; /* INNER */

        uint32_t current = join_idx;

        /* PROJECT node if specific columns selected */
        if (need_project_join) {
            uint32_t proj_idx = plan_alloc_node(arena, PLAN_PROJECT);
            PLAN_NODE(arena, proj_idx).left = current;
            PLAN_NODE(arena, proj_idx).project.ncols = proj_ncols_join;
            PLAN_NODE(arena, proj_idx).project.col_map = proj_map_join;
            current = proj_idx;
        }

        /* LIMIT/OFFSET node if present */
        if (s->has_limit || s->has_offset) {
            uint32_t limit_idx = plan_alloc_node(arena, PLAN_LIMIT);
            PLAN_NODE(arena, limit_idx).left = current;
            PLAN_NODE(arena, limit_idx).limit.has_limit = s->has_limit;
            PLAN_NODE(arena, limit_idx).limit.limit = s->has_limit ? (size_t)s->limit_count : 0;
            PLAN_NODE(arena, limit_idx).limit.has_offset = s->has_offset;
            PLAN_NODE(arena, limit_idx).limit.offset = s->has_offset ? (size_t)s->offset_count : 0;
            current = limit_idx;
        }

        return current;
    }

    /* ---- Window function fast path ---- */
    if (s->select_exprs_count > 0 && !s->has_join && !s->has_group_by &&
        s->aggregates_count == 0 && !s->has_set_op && s->ctes_count == 0 &&
        s->cte_sql == IDX_NONE && s->from_subquery_sql == IDX_NONE &&
        !s->has_recursive_cte && !s->has_distinct && !s->has_distinct_on &&
        t && s->insert_rows_count == 0) {

        size_t nexprs = s->select_exprs_count;
        /* Count passthrough and window expressions, resolve column indices */
        uint16_t n_pass = 0, n_win = 0;
        for (size_t e = 0; e < nexprs; e++) {
            struct select_expr *se = &arena->select_exprs.items[s->select_exprs_start + e];
            if (se->kind == SEL_COLUMN) n_pass++;
            else n_win++;
        }
        if (n_win == 0) return IDX_NONE; /* shouldn't happen */

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
                if (ci < 0) return IDX_NONE;
                pass_cols[pi++] = ci;
            } else {
                int pc = -1, oc = -1, ac = -1;
                if (se->win.has_partition) {
                    pc = table_find_column_sv(t, se->win.partition_col);
                    if (pc < 0) return IDX_NONE;
                    if (global_part < 0) global_part = pc;
                }
                if (se->win.has_order) {
                    oc = table_find_column_sv(t, se->win.order_col);
                    if (oc < 0) return IDX_NONE;
                    if (global_ord < 0) { global_ord = oc; global_ord_desc = se->win.order_desc; }
                }
                if (se->win.arg_column.len > 0 && !sv_eq_cstr(se->win.arg_column, "*")) {
                    ac = table_find_column_sv(t, se->win.arg_column);
                    if (ac < 0) return IDX_NONE;
                }
                /* For LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE with text arg columns,
                 * bail out — plan executor uses double arrays, can't handle text */
                if (ac >= 0 && column_type_is_text(t->columns.items[ac].type) &&
                    (se->win.func == WIN_LAG || se->win.func == WIN_LEAD ||
                     se->win.func == WIN_FIRST_VALUE || se->win.func == WIN_LAST_VALUE ||
                     se->win.func == WIN_NTH_VALUE))
                    return IDX_NONE;
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
        if (t->rows.count > 0) {
            struct row *first = &t->rows.items[0];
            for (size_t c = 0; c < first->cells.count && c < t->columns.count; c++) {
                if (!first->cells.items[c].is_null &&
                    first->cells.items[c].type != t->columns.items[c].type)
                    return IDX_NONE;
            }
        }

        /* Build scan → (filter →) window (→ sort) plan */
        uint16_t scan_ncols = (uint16_t)t->columns.count;
        int *col_map = (int *)bump_alloc(&arena->scratch, scan_ncols * sizeof(int));
        for (uint16_t i = 0; i < scan_ncols; i++) col_map[i] = (int)i;

        uint32_t scan_idx = plan_alloc_node(arena, PLAN_SEQ_SCAN);
        PLAN_NODE(arena, scan_idx).seq_scan.table = t;
        PLAN_NODE(arena, scan_idx).seq_scan.ncols = scan_ncols;
        PLAN_NODE(arena, scan_idx).seq_scan.col_map = col_map;
        PLAN_NODE(arena, scan_idx).est_rows = (double)t->rows.count;
        uint32_t current = scan_idx;

        /* Add filter if WHERE is simple enough */
        if (s->where.has_where && s->where.where_cond != IDX_NONE) {
            struct condition *cond = &COND(arena, s->where.where_cond);
            if (cond->type == COND_COMPARE && cond->lhs_expr == IDX_NONE &&
                cond->rhs_column.len == 0 &&
                cond->scalar_subquery_sql == IDX_NONE &&
                cond->subquery_sql == IDX_NONE &&
                cond->in_values_count == 0 && cond->array_values_count == 0 &&
                !cond->is_any && !cond->is_all && cond->op <= CMP_GE) {
                int fc = table_find_column_sv(t, cond->column);
                if (fc >= 0) {
                    enum column_type ct = t->columns.items[fc].type;
                    if ((ct == COLUMN_TYPE_INT || ct == COLUMN_TYPE_BOOLEAN ||
                         ct == COLUMN_TYPE_FLOAT || ct == COLUMN_TYPE_NUMERIC ||
                         ct == COLUMN_TYPE_BIGINT) &&
                        (cond->value.is_null || cond->value.type == ct ||
                         (ct == COLUMN_TYPE_FLOAT && cond->value.type == COLUMN_TYPE_INT))) {
                        if (!(ct == COLUMN_TYPE_INT && cond->value.type == COLUMN_TYPE_FLOAT)) {
                            uint32_t filter_idx = plan_alloc_node(arena, PLAN_FILTER);
                            PLAN_NODE(arena, filter_idx).left = current;
                            PLAN_NODE(arena, filter_idx).filter.cond_idx = s->where.where_cond;
                            PLAN_NODE(arena, filter_idx).filter.col_idx = fc;
                            PLAN_NODE(arena, filter_idx).filter.cmp_op = (int)cond->op;
                            PLAN_NODE(arena, filter_idx).filter.cmp_val = cond->value;
                            current = filter_idx;
                        }
                    }
                }
            }
        }

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
            int sort_cols_buf[32];
            int sort_descs_buf[32];
            uint16_t sort_nord = s->order_by_count < 32 ? (uint16_t)s->order_by_count : 32;
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
            if (sort_ok && sort_nord > 0) {
                int *sort_cols = (int *)bump_alloc(&arena->scratch, sort_nord * sizeof(int));
                int *sort_descs = (int *)bump_alloc(&arena->scratch, sort_nord * sizeof(int));
                memcpy(sort_cols, sort_cols_buf, sort_nord * sizeof(int));
                memcpy(sort_descs, sort_descs_buf, sort_nord * sizeof(int));
                uint32_t sort_idx = plan_alloc_node(arena, PLAN_SORT);
                PLAN_NODE(arena, sort_idx).left = current;
                PLAN_NODE(arena, sort_idx).sort.sort_cols = sort_cols;
                PLAN_NODE(arena, sort_idx).sort.sort_descs = sort_descs;
                PLAN_NODE(arena, sort_idx).sort.nsort_cols = sort_nord;
                current = sort_idx;
            }
        }

        /* Add LIMIT/OFFSET if present */
        if (s->has_limit || s->has_offset) {
            uint32_t limit_idx = plan_alloc_node(arena, PLAN_LIMIT);
            PLAN_NODE(arena, limit_idx).left = current;
            PLAN_NODE(arena, limit_idx).limit.has_limit = s->has_limit;
            PLAN_NODE(arena, limit_idx).limit.limit = s->has_limit ? (size_t)s->limit_count : 0;
            PLAN_NODE(arena, limit_idx).limit.has_offset = s->has_offset;
            PLAN_NODE(arena, limit_idx).limit.offset = s->has_offset ? (size_t)s->offset_count : 0;
            current = limit_idx;
        }

        return current;
    }

    /* ---- Set operations fast path (UNION / INTERSECT / EXCEPT) ---- */
    if (s->has_set_op && s->set_rhs_sql != IDX_NONE && !s->has_join &&
        !s->has_group_by && s->aggregates_count == 0 && s->ctes_count == 0 &&
        s->cte_sql == IDX_NONE && s->from_subquery_sql == IDX_NONE &&
        !s->has_recursive_cte && s->select_exprs_count == 0 &&
        !s->where.has_where &&
        t && db && s->insert_rows_count == 0) {

        /* Parse the RHS SQL */
        const char *rhs_sql = ASTRING(arena, s->set_rhs_sql);
        struct query rhs_q = {0};
        if (query_parse(rhs_sql, &rhs_q) != 0) return IDX_NONE;
        if (rhs_q.query_type != QUERY_TYPE_SELECT) { query_free(&rhs_q); return IDX_NONE; }

        struct query_select *rs = &rhs_q.select;
        /* Only handle simple single-table RHS SELECTs */
        if (rs->has_join || rs->has_group_by || rs->aggregates_count > 0 ||
            rs->has_set_op || rs->ctes_count > 0 || rs->has_distinct ||
            rs->from_subquery_sql != IDX_NONE || rs->has_generate_series ||
            rs->select_exprs_count > 0) {
            query_free(&rhs_q);
            return IDX_NONE;
        }

        struct table *t2 = db_find_table_sv(db, rs->table);
        if (!t2 || t2->view_sql) { query_free(&rhs_q); return IDX_NONE; }

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
                if (sc->expr_idx == IDX_NONE) { query_free(&rhs_q); return IDX_NONE; }
                struct expr *e = &EXPR(arena, sc->expr_idx);
                if (e->type != EXPR_COLUMN_REF) { query_free(&rhs_q); return IDX_NONE; }
                int ci = table_find_column_sv(t, e->column_ref.column);
                if (ci < 0) { query_free(&rhs_q); return IDX_NONE; }
                lhs_col_map[i] = ci;
            }
        } else {
            query_free(&rhs_q);
            return IDX_NONE;
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
                if (sc->expr_idx == IDX_NONE) { query_free(&rhs_q); return IDX_NONE; }
                struct expr *e = &EXPR(&rhs_q.arena, sc->expr_idx);
                if (e->type != EXPR_COLUMN_REF) { query_free(&rhs_q); return IDX_NONE; }
                int ci = table_find_column_sv(t2, e->column_ref.column);
                if (ci < 0) { query_free(&rhs_q); return IDX_NONE; }
                rhs_col_map[i] = ci;
            }
        } else {
            /* Try legacy text-based column resolution */
            if (rs->columns.len > 0 && !sv_eq_cstr(rs->columns, "*")) {
                /* Parse comma-separated column list from raw text */
                query_free(&rhs_q);
                return IDX_NONE;
            }
            query_free(&rhs_q);
            return IDX_NONE;
        }

        /* Column counts must match */
        if (lhs_ncols != rhs_ncols) { query_free(&rhs_q); return IDX_NONE; }

        /* Check column type compatibility between LHS and RHS */
        for (uint16_t i = 0; i < lhs_ncols; i++) {
            enum column_type lt = t->columns.items[lhs_col_map[i]].type;
            enum column_type rt = t2->columns.items[rhs_col_map[i]].type;
            if (lt != rt) { query_free(&rhs_q); return IDX_NONE; }
        }

        /* Check for mixed cell types in both tables */
        if (t->rows.count > 0) {
            struct row *first = &t->rows.items[0];
            for (size_t c = 0; c < first->cells.count && c < t->columns.count; c++) {
                if (!first->cells.items[c].is_null &&
                    first->cells.items[c].type != t->columns.items[c].type) {
                    query_free(&rhs_q);
                    return IDX_NONE;
                }
            }
        }
        if (t2->rows.count > 0) {
            struct row *first = &t2->rows.items[0];
            for (size_t c = 0; c < first->cells.count && c < t2->columns.count; c++) {
                if (!first->cells.items[c].is_null &&
                    first->cells.items[c].type != t2->columns.items[c].type) {
                    query_free(&rhs_q);
                    return IDX_NONE;
                }
            }
        }

        /* Build LHS plan: SEQ_SCAN (→ PROJECT if not SELECT *) */
        uint16_t lhs_scan_ncols = (uint16_t)t->columns.count;
        int *lhs_scan_map = (int *)bump_alloc(&arena->scratch, lhs_scan_ncols * sizeof(int));
        for (uint16_t i = 0; i < lhs_scan_ncols; i++) lhs_scan_map[i] = (int)i;

        uint32_t lhs_scan = plan_alloc_node(arena, PLAN_SEQ_SCAN);
        PLAN_NODE(arena, lhs_scan).seq_scan.table = t;
        PLAN_NODE(arena, lhs_scan).seq_scan.ncols = lhs_scan_ncols;
        PLAN_NODE(arena, lhs_scan).seq_scan.col_map = lhs_scan_map;
        PLAN_NODE(arena, lhs_scan).est_rows = (double)t->rows.count;
        uint32_t lhs_current = lhs_scan;

        if (!select_all_lhs) {
            uint32_t lhs_proj = plan_alloc_node(arena, PLAN_PROJECT);
            PLAN_NODE(arena, lhs_proj).left = lhs_current;
            PLAN_NODE(arena, lhs_proj).project.ncols = lhs_ncols;
            PLAN_NODE(arena, lhs_proj).project.col_map = lhs_col_map;
            lhs_current = lhs_proj;
        }

        /* Build RHS plan: SEQ_SCAN (→ FILTER) (→ PROJECT if not SELECT *) */
        uint16_t rhs_scan_ncols = (uint16_t)t2->columns.count;
        int *rhs_scan_map = (int *)bump_alloc(&arena->scratch, rhs_scan_ncols * sizeof(int));
        for (uint16_t i = 0; i < rhs_scan_ncols; i++) rhs_scan_map[i] = (int)i;

        uint32_t rhs_scan = plan_alloc_node(arena, PLAN_SEQ_SCAN);
        PLAN_NODE(arena, rhs_scan).seq_scan.table = t2;
        PLAN_NODE(arena, rhs_scan).seq_scan.ncols = rhs_scan_ncols;
        PLAN_NODE(arena, rhs_scan).seq_scan.col_map = rhs_scan_map;
        PLAN_NODE(arena, rhs_scan).est_rows = (double)t2->rows.count;
        uint32_t rhs_current = rhs_scan;

        /* Add filter on RHS if it has a simple WHERE */
        if (rs->where.has_where && rs->where.where_cond != IDX_NONE) {
            struct condition *rcond = &COND(&rhs_q.arena, rs->where.where_cond);
            if (rcond->type == COND_COMPARE && rcond->lhs_expr == IDX_NONE &&
                rcond->rhs_column.len == 0 && rcond->subquery_sql == IDX_NONE &&
                rcond->scalar_subquery_sql == IDX_NONE &&
                rcond->in_values_count == 0 && rcond->array_values_count == 0 &&
                !rcond->is_any && !rcond->is_all && rcond->op <= CMP_GE) {
                int fc = table_find_column_sv(t2, rcond->column);
                if (fc >= 0) {
                    enum column_type fct = t2->columns.items[fc].type;
                    if (fct == COLUMN_TYPE_INT || fct == COLUMN_TYPE_BIGINT ||
                        fct == COLUMN_TYPE_FLOAT || fct == COLUMN_TYPE_NUMERIC ||
                        fct == COLUMN_TYPE_BOOLEAN) {
                        uint32_t rhs_filter = plan_alloc_node(arena, PLAN_FILTER);
                        PLAN_NODE(arena, rhs_filter).left = rhs_current;
                        PLAN_NODE(arena, rhs_filter).filter.col_idx = fc;
                        PLAN_NODE(arena, rhs_filter).filter.cmp_op = (int)rcond->op;
                        PLAN_NODE(arena, rhs_filter).filter.cmp_val = rcond->value;
                        rhs_current = rhs_filter;
                    }
                }
            }
        }

        if (!select_all_rhs) {
            uint32_t rhs_proj = plan_alloc_node(arena, PLAN_PROJECT);
            PLAN_NODE(arena, rhs_proj).left = rhs_current;
            PLAN_NODE(arena, rhs_proj).project.ncols = rhs_ncols;
            PLAN_NODE(arena, rhs_proj).project.col_map = rhs_col_map;
            rhs_current = rhs_proj;
        }

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

            int sort_cols_buf[32];
            int sort_descs_buf[32];
            uint16_t sort_nord = 0;
            int sort_ok = 1;

            while (*p && sort_nord < 32) {
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

            if (sort_ok && sort_nord > 0) {
                int *sort_cols = (int *)bump_alloc(&arena->scratch, sort_nord * sizeof(int));
                int *sort_descs = (int *)bump_alloc(&arena->scratch, sort_nord * sizeof(int));
                memcpy(sort_cols, sort_cols_buf, sort_nord * sizeof(int));
                memcpy(sort_descs, sort_descs_buf, sort_nord * sizeof(int));
                uint32_t sort_idx = plan_alloc_node(arena, PLAN_SORT);
                PLAN_NODE(arena, sort_idx).left = current;
                PLAN_NODE(arena, sort_idx).sort.sort_cols = sort_cols;
                PLAN_NODE(arena, sort_idx).sort.sort_descs = sort_descs;
                PLAN_NODE(arena, sort_idx).sort.nsort_cols = sort_nord;
                current = sort_idx;
            }
        }

        /* Also handle ORDER BY from the main query (s->has_order_by) */
        if (s->has_order_by && s->order_by_count > 0) {
            uint16_t sort_nord = s->order_by_count < 32 ? (uint16_t)s->order_by_count : 32;
            int sort_cols_buf2[32];
            int sort_descs_buf2[32];
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
            if (sort_ok && sort_nord > 0) {
                int *sort_cols = (int *)bump_alloc(&arena->scratch, sort_nord * sizeof(int));
                int *sort_descs = (int *)bump_alloc(&arena->scratch, sort_nord * sizeof(int));
                memcpy(sort_cols, sort_cols_buf2, sort_nord * sizeof(int));
                memcpy(sort_descs, sort_descs_buf2, sort_nord * sizeof(int));
                uint32_t sort_idx = plan_alloc_node(arena, PLAN_SORT);
                PLAN_NODE(arena, sort_idx).left = current;
                PLAN_NODE(arena, sort_idx).sort.sort_cols = sort_cols;
                PLAN_NODE(arena, sort_idx).sort.sort_descs = sort_descs;
                PLAN_NODE(arena, sort_idx).sort.nsort_cols = sort_nord;
                current = sort_idx;
            }
        }

        /* Add LIMIT/OFFSET if present */
        if (s->has_limit || s->has_offset) {
            uint32_t limit_idx = plan_alloc_node(arena, PLAN_LIMIT);
            PLAN_NODE(arena, limit_idx).left = current;
            PLAN_NODE(arena, limit_idx).limit.has_limit = s->has_limit;
            PLAN_NODE(arena, limit_idx).limit.limit = s->has_limit ? (size_t)s->limit_count : 0;
            PLAN_NODE(arena, limit_idx).limit.has_offset = s->has_offset;
            PLAN_NODE(arena, limit_idx).limit.offset = s->has_offset ? (size_t)s->offset_count : 0;
            current = limit_idx;
        }

        query_free(&rhs_q);
        return current;
    }

    /* ---- Single-table GROUP BY + aggregates fast path ---- */
    if (s->has_group_by && s->aggregates_count > 0 && t && !s->has_join &&
        !s->has_set_op && s->ctes_count == 0 && s->cte_sql == IDX_NONE &&
        s->from_subquery_sql == IDX_NONE && !s->has_recursive_cte &&
        s->select_exprs_count == 0 && s->insert_rows_count == 0 &&
        !s->has_distinct && !s->has_distinct_on &&
        !s->group_by_rollup && !s->group_by_cube &&
        !s->has_having && !s->has_order_by) {

        /* Validate: all aggregates must be simple SUM/COUNT/AVG/MIN/MAX on column refs */
        int agg_ok = 1;
        for (uint32_t a = 0; a < s->aggregates_count; a++) {
            struct agg_expr *ae = &arena->aggregates.items[s->aggregates_start + a];
            if (ae->func == AGG_STRING_AGG || ae->func == AGG_ARRAY_AGG || ae->func == AGG_NONE)
                { agg_ok = 0; break; }
            if (ae->expr_idx != IDX_NONE) { agg_ok = 0; break; }
            if (ae->has_distinct) { agg_ok = 0; break; }
            if (!sv_eq_cstr(ae->column, "*")) {
                int ci = table_find_column_sv(t, ae->column);
                if (ci < 0) { agg_ok = 0; break; }
            }
        }

        /* Validate: GROUP BY columns are resolvable */
        int *grp_col_idxs = NULL;
        if (agg_ok && s->group_by_count > 0) {
            grp_col_idxs = (int *)bump_alloc(&arena->scratch, s->group_by_count * sizeof(int));
            for (uint32_t g = 0; g < s->group_by_count; g++) {
                sv gcol = arena->svs.items[s->group_by_start + g];
                grp_col_idxs[g] = table_find_column_sv(t, gcol);
                if (grp_col_idxs[g] < 0) { agg_ok = 0; break; }
            }
        }

        if (agg_ok) {
            /* Build: SEQ_SCAN → HASH_AGG */
            uint16_t scan_ncols = (uint16_t)t->columns.count;
            int *scan_map = (int *)bump_alloc(&arena->scratch, scan_ncols * sizeof(int));
            for (uint16_t i = 0; i < scan_ncols; i++) scan_map[i] = (int)i;

            uint32_t scan_idx = plan_alloc_node(arena, PLAN_SEQ_SCAN);
            PLAN_NODE(arena, scan_idx).seq_scan.table = t;
            PLAN_NODE(arena, scan_idx).seq_scan.ncols = scan_ncols;
            PLAN_NODE(arena, scan_idx).seq_scan.col_map = scan_map;

            uint32_t agg_idx = plan_alloc_node(arena, PLAN_HASH_AGG);
            PLAN_NODE(arena, agg_idx).left = scan_idx;
            PLAN_NODE(arena, agg_idx).hash_agg.ngroup_cols = (uint16_t)s->group_by_count;
            PLAN_NODE(arena, agg_idx).hash_agg.group_cols = grp_col_idxs;
            PLAN_NODE(arena, agg_idx).hash_agg.agg_start = s->aggregates_start;
            PLAN_NODE(arena, agg_idx).hash_agg.agg_count = s->aggregates_count;
            PLAN_NODE(arena, agg_idx).hash_agg.agg_before_cols = s->agg_before_cols;

            uint32_t current = agg_idx;

            /* Add ORDER BY if present */
            if (s->has_order_by && s->order_by_count > 0) {
                /* For now, bail to legacy if ORDER BY is present on aggregates */
                /* (would need to map ORDER BY columns to agg output columns) */
            }

            /* Add LIMIT/OFFSET if present */
            if (s->has_limit || s->has_offset) {
                uint32_t limit_idx = plan_alloc_node(arena, PLAN_LIMIT);
                PLAN_NODE(arena, limit_idx).left = current;
                PLAN_NODE(arena, limit_idx).limit.has_limit = s->has_limit;
                PLAN_NODE(arena, limit_idx).limit.limit = s->has_limit ? (size_t)s->limit_count : 0;
                PLAN_NODE(arena, limit_idx).limit.has_offset = s->has_offset;
                PLAN_NODE(arena, limit_idx).limit.offset = s->has_offset ? (size_t)s->offset_count : 0;
                current = limit_idx;
            }

            return current;
        }
    }

    /* ---- Single-table path (original) ---- */

    /* Bail out for queries we don't handle yet */
    if (s->has_join)            return IDX_NONE;
    if (s->select_exprs_count > 0) return IDX_NONE; /* window functions — handled above */
    if (s->has_group_by)        return IDX_NONE;
    if (s->aggregates_count > 0) return IDX_NONE;
    if (s->has_set_op)          return IDX_NONE;
    if (s->ctes_count > 0)     return IDX_NONE;
    if (s->cte_sql != IDX_NONE) return IDX_NONE;
    if (s->from_subquery_sql != IDX_NONE) return IDX_NONE;
    if (s->has_recursive_cte)   return IDX_NONE;
    if (s->has_distinct_on)     return IDX_NONE;
    if (!t)                     return IDX_NONE;
    if (s->insert_rows_count > 0) return IDX_NONE; /* literal SELECT */

    int select_all = sv_eq_cstr(s->columns, "*");

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
            if (!expr_ok) return IDX_NONE;
            need_expr_project = 1;
        }
    } else {
        /* Legacy text-based column list — can't handle */
        return IDX_NONE;
    }

    /* Expression projection can't handle ORDER BY or DISTINCT yet — bail to legacy */
    if (need_expr_project && s->has_order_by) return IDX_NONE;
    if (need_expr_project && s->has_distinct) return IDX_NONE;

    /* Pre-validate ORDER BY columns BEFORE allocating plan nodes */
    int  sort_cols_buf[32];
    int  sort_descs_buf[32];
    int  sort_nf_buf[32];
    uint16_t sort_nord = 0;
    if (s->has_order_by && s->order_by_count > 0) {
        sort_nord = s->order_by_count < 32 ? (uint16_t)s->order_by_count : 32;
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
            if (sort_cols_buf[k] < 0) return IDX_NONE;
        }
    }

    /* Pre-validate WHERE clause BEFORE allocating plan nodes */
    int filter_col = -1;
    int filter_ok = 0;
    int semi_join_ok = 0;       /* set to 1 if we can use hash semi-join */
    int semi_outer_key = -1;    /* outer table column index for semi-join key */
    struct table *semi_inner_t = NULL;
    int semi_inner_key = -1;    /* inner table column index for semi-join key */
    int semi_inner_filter_col = -1;
    int semi_inner_filter_op = -1;
    struct cell semi_inner_filter_val = {0};
    struct query semi_sq = {0}; /* parsed subquery — freed after plan build */

    if (s->where.has_where) {
        if (s->where.where_cond == IDX_NONE)
            return IDX_NONE;
        struct condition *cond = &COND(arena, s->where.where_cond);
        if (cond->type != COND_COMPARE)
            return IDX_NONE;

        /* ---- IN-subquery → hash semi-join ---- */
        if ((cond->op == CMP_IN || cond->op == CMP_NOT_IN) &&
            cond->subquery_sql != IDX_NONE && db &&
            cond->op == CMP_IN /* NOT IN is more complex — skip for now */) {

            /* Find outer key column */
            semi_outer_key = table_find_column_sv(t, cond->column);
            if (semi_outer_key < 0) goto semi_bail;

            /* Parse the subquery */
            const char *sq_sql = ASTRING(arena, cond->subquery_sql);
            if (query_parse(sq_sql, &semi_sq) != 0) goto semi_bail;
            if (semi_sq.query_type != QUERY_TYPE_SELECT) goto semi_bail;

            struct query_select *isq = &semi_sq.select;
            /* Must be a simple single-table SELECT with one column */
            if (isq->has_join || isq->has_group_by || isq->aggregates_count > 0 ||
                isq->has_set_op || isq->ctes_count > 0 || isq->has_distinct ||
                isq->from_subquery_sql != IDX_NONE || isq->has_generate_series)
                goto semi_bail;

            /* Find inner table */
            semi_inner_t = db_find_table_sv(db, isq->table);
            if (!semi_inner_t) goto semi_bail;
            if (semi_inner_t->view_sql) goto semi_bail;

            /* Resolve inner SELECT column — must be a single column ref */
            if (isq->parsed_columns_count == 1) {
                struct select_column *sc = &semi_sq.arena.select_cols.items[isq->parsed_columns_start];
                if (sc->expr_idx != IDX_NONE) {
                    struct expr *e = &EXPR(&semi_sq.arena, sc->expr_idx);
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
            if (semi_inner_key < 0) goto semi_bail;

            /* Validate inner WHERE (optional simple numeric filter) */
            if (isq->where.has_where && isq->where.where_cond != IDX_NONE) {
                struct condition *icond = &COND(&semi_sq.arena, isq->where.where_cond);
                if (icond->type == COND_COMPARE && icond->lhs_expr == IDX_NONE &&
                    icond->rhs_column.len == 0 && icond->subquery_sql == IDX_NONE &&
                    icond->scalar_subquery_sql == IDX_NONE &&
                    icond->in_values_count == 0 && !icond->is_any && !icond->is_all) {
                    int fc = table_find_column_sv(semi_inner_t, icond->column);
                    if (fc >= 0 && icond->op <= CMP_GE) {
                        enum column_type fct = semi_inner_t->columns.items[fc].type;
                        if (fct == COLUMN_TYPE_INT || fct == COLUMN_TYPE_BIGINT ||
                            fct == COLUMN_TYPE_FLOAT || fct == COLUMN_TYPE_NUMERIC ||
                            fct == COLUMN_TYPE_BOOLEAN) {
                            semi_inner_filter_col = fc;
                            semi_inner_filter_op = (int)icond->op;
                            semi_inner_filter_val = icond->value;
                        } else {
                            goto semi_bail; /* text WHERE — can't handle */
                        }
                    } else {
                        goto semi_bail;
                    }
                } else {
                    goto semi_bail; /* complex inner WHERE */
                }
            }

            /* Check for mixed cell types in inner table */
            if (semi_inner_t->rows.count > 0) {
                struct row *first = &semi_inner_t->rows.items[0];
                for (size_t c = 0; c < first->cells.count && c < semi_inner_t->columns.count; c++) {
                    if (!first->cells.items[c].is_null &&
                        first->cells.items[c].type != semi_inner_t->columns.items[c].type)
                        goto semi_bail;
                }
            }

            semi_join_ok = 1;
            goto where_done;

        semi_bail:
            query_free(&semi_sq);
            memset(&semi_sq, 0, sizeof(semi_sq));
            semi_join_ok = 0;
            return IDX_NONE; /* fall back to legacy */
        }

        /* ---- Standard simple comparison filter ---- */
        if (cond->lhs_expr != IDX_NONE || cond->rhs_column.len != 0)
            return IDX_NONE;
        if (cond->scalar_subquery_sql != IDX_NONE) return IDX_NONE;
        if (cond->subquery_sql != IDX_NONE) return IDX_NONE;
        if (cond->in_values_count > 0) return IDX_NONE;
        if (cond->array_values_count > 0) return IDX_NONE;
        if (cond->is_any || cond->is_all) return IDX_NONE;
        filter_col = table_find_column_sv(t, cond->column);
        if (filter_col < 0) return IDX_NONE;
        enum column_type ct = t->columns.items[filter_col].type;
        if (ct != COLUMN_TYPE_INT && ct != COLUMN_TYPE_BOOLEAN &&
            ct != COLUMN_TYPE_FLOAT && ct != COLUMN_TYPE_NUMERIC &&
            ct != COLUMN_TYPE_BIGINT && !column_type_is_text(ct))
            return IDX_NONE;
        if (cond->op > CMP_GE) return IDX_NONE;
        /* For text columns, accept text comparison values */
        if (column_type_is_text(ct)) {
            if (!cond->value.is_null && !column_type_is_text(cond->value.type))
                return IDX_NONE;
        } else {
            /* Reject cross-type comparisons (e.g. INT col vs FLOAT value from subquery) */
            if (!cond->value.is_null && cond->value.type != ct &&
                !(ct == COLUMN_TYPE_FLOAT && cond->value.type == COLUMN_TYPE_INT) &&
                !(ct == COLUMN_TYPE_INT && cond->value.type == COLUMN_TYPE_FLOAT))
                return IDX_NONE;
            /* If INT column but FLOAT value, reject — fast path can't handle */
            if (ct == COLUMN_TYPE_INT && cond->value.type == COLUMN_TYPE_FLOAT)
                return IDX_NONE;
        }
        filter_ok = 1;
    }
where_done:

    /* Bail out if outer table has mixed cell types (e.g. after ALTER COLUMN TYPE) */
    if (t->rows.count > 0) {
        struct row *first = &t->rows.items[0];
        for (size_t c = 0; c < first->cells.count && c < t->columns.count; c++) {
            if (!first->cells.items[c].is_null &&
                first->cells.items[c].type != t->columns.items[c].type) {
                if (semi_join_ok) query_free(&semi_sq);
                return IDX_NONE;
            }
        }
    }

    /* --- All validation passed, now allocate plan nodes --- */

    uint16_t scan_ncols = (uint16_t)t->columns.count;
    int *col_map = (int *)bump_alloc(&arena->scratch, scan_ncols * sizeof(int));
    for (uint16_t i = 0; i < scan_ncols; i++)
        col_map[i] = (int)i;

    uint32_t current;

    /* ---- Hash semi-join path ---- */
    if (semi_join_ok) {
        /* Build outer scan (left / probe side) */
        uint32_t outer_scan = plan_alloc_node(arena, PLAN_SEQ_SCAN);
        PLAN_NODE(arena, outer_scan).seq_scan.table = t;
        PLAN_NODE(arena, outer_scan).seq_scan.ncols = scan_ncols;
        PLAN_NODE(arena, outer_scan).seq_scan.col_map = col_map;
        PLAN_NODE(arena, outer_scan).est_rows = (double)t->rows.count;

        /* Build inner scan (right / build side) */
        uint16_t inner_ncols = (uint16_t)semi_inner_t->columns.count;
        int *inner_col_map = (int *)bump_alloc(&arena->scratch, inner_ncols * sizeof(int));
        for (uint16_t i = 0; i < inner_ncols; i++) inner_col_map[i] = (int)i;

        uint32_t inner_scan = plan_alloc_node(arena, PLAN_SEQ_SCAN);
        PLAN_NODE(arena, inner_scan).seq_scan.table = semi_inner_t;
        PLAN_NODE(arena, inner_scan).seq_scan.ncols = inner_ncols;
        PLAN_NODE(arena, inner_scan).seq_scan.col_map = inner_col_map;
        PLAN_NODE(arena, inner_scan).est_rows = (double)semi_inner_t->rows.count;

        uint32_t inner_current = inner_scan;

        /* Add filter on inner side if subquery has WHERE */
        if (semi_inner_filter_col >= 0) {
            uint32_t inner_filter = plan_alloc_node(arena, PLAN_FILTER);
            PLAN_NODE(arena, inner_filter).left = inner_current;
            PLAN_NODE(arena, inner_filter).filter.col_idx = semi_inner_filter_col;
            PLAN_NODE(arena, inner_filter).filter.cmp_op = semi_inner_filter_op;
            PLAN_NODE(arena, inner_filter).filter.cmp_val = semi_inner_filter_val;
            inner_current = inner_filter;
        }

        /* If inner SELECT is a single column (not SELECT *), add projection */
        if (semi_inner_key != 0 || inner_ncols > 1) {
            /* Project to just the key column */
            int *inner_proj = (int *)bump_alloc(&arena->scratch, sizeof(int));
            inner_proj[0] = semi_inner_key;
            uint32_t inner_proj_node = plan_alloc_node(arena, PLAN_PROJECT);
            PLAN_NODE(arena, inner_proj_node).left = inner_current;
            PLAN_NODE(arena, inner_proj_node).project.ncols = 1;
            PLAN_NODE(arena, inner_proj_node).project.col_map = inner_proj;
            inner_current = inner_proj_node;
            semi_inner_key = 0; /* after projection, key is column 0 */
        }

        /* Build HASH_SEMI_JOIN node */
        uint32_t semi_idx = plan_alloc_node(arena, PLAN_HASH_SEMI_JOIN);
        PLAN_NODE(arena, semi_idx).left = outer_scan;
        PLAN_NODE(arena, semi_idx).right = inner_current;
        PLAN_NODE(arena, semi_idx).hash_semi_join.outer_key_col = semi_outer_key;
        PLAN_NODE(arena, semi_idx).hash_semi_join.inner_key_col = semi_inner_key;

        current = semi_idx;

        query_free(&semi_sq);
        goto append_sort_project_limit;
    }

    /* Try index scan for equality WHERE on an indexed column */
    int used_index = 0;
    if (filter_ok && filter_col >= 0) {
        struct condition *cond = &COND(arena, s->where.where_cond);
        if (cond->op == CMP_EQ) {
            for (size_t ix = 0; ix < t->indexes.count; ix++) {
                if (strcmp(t->indexes.items[ix].column_name,
                           t->columns.items[filter_col].name) == 0) {
                    uint32_t idx_node = plan_alloc_node(arena, PLAN_INDEX_SCAN);
                    PLAN_NODE(arena, idx_node).index_scan.table = t;
                    PLAN_NODE(arena, idx_node).index_scan.idx = &t->indexes.items[ix];
                    PLAN_NODE(arena, idx_node).index_scan.cond_idx = s->where.where_cond;
                    PLAN_NODE(arena, idx_node).index_scan.ncols = scan_ncols;
                    PLAN_NODE(arena, idx_node).index_scan.col_map = col_map;
                    PLAN_NODE(arena, idx_node).est_rows = 1.0;
                    current = idx_node;
                    used_index = 1;
                    break;
                }
            }
        }
    }

    if (!used_index) {
        uint32_t scan_idx = plan_alloc_node(arena, PLAN_SEQ_SCAN);
        PLAN_NODE(arena, scan_idx).seq_scan.table = t;
        PLAN_NODE(arena, scan_idx).seq_scan.ncols = scan_ncols;
        PLAN_NODE(arena, scan_idx).seq_scan.col_map = col_map;
        PLAN_NODE(arena, scan_idx).est_rows = (double)t->rows.count;
        current = scan_idx;

        /* Add filter node if WHERE clause was validated */
        if (filter_ok) {
            struct condition *cond = &COND(arena, s->where.where_cond);
            uint32_t filter_idx = plan_alloc_node(arena, PLAN_FILTER);
            PLAN_NODE(arena, filter_idx).left = current;
            PLAN_NODE(arena, filter_idx).filter.cond_idx = s->where.where_cond;
            PLAN_NODE(arena, filter_idx).filter.col_idx = filter_col;
            PLAN_NODE(arena, filter_idx).filter.cmp_op = (int)cond->op;
            PLAN_NODE(arena, filter_idx).filter.cmp_val = cond->value;
            current = filter_idx;
        }
    }

append_sort_project_limit:

    /* Add SORT node if ORDER BY was validated */
    if (sort_nord > 0) {
        int *sort_cols = (int *)bump_alloc(&arena->scratch, sort_nord * sizeof(int));
        int *sort_descs = (int *)bump_alloc(&arena->scratch, sort_nord * sizeof(int));
        int *sort_nf = (int *)bump_alloc(&arena->scratch, sort_nord * sizeof(int));
        memcpy(sort_cols, sort_cols_buf, sort_nord * sizeof(int));
        memcpy(sort_descs, sort_descs_buf, sort_nord * sizeof(int));
        memcpy(sort_nf, sort_nf_buf, sort_nord * sizeof(int));

        uint32_t sort_idx = plan_alloc_node(arena, PLAN_SORT);
        PLAN_NODE(arena, sort_idx).left = current;
        PLAN_NODE(arena, sort_idx).sort.sort_cols = sort_cols;
        PLAN_NODE(arena, sort_idx).sort.sort_descs = sort_descs;
        PLAN_NODE(arena, sort_idx).sort.sort_nulls_first = sort_nf;
        PLAN_NODE(arena, sort_idx).sort.nsort_cols = sort_nord;
        current = sort_idx;
    }

    /* Add projection node if specific columns are selected */
    if (need_project) {
        uint32_t proj_idx = plan_alloc_node(arena, PLAN_PROJECT);
        PLAN_NODE(arena, proj_idx).left = current;
        PLAN_NODE(arena, proj_idx).project.ncols = proj_ncols;
        PLAN_NODE(arena, proj_idx).project.col_map = proj_map;
        current = proj_idx;
    }

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

    /* Add LIMIT/OFFSET node if present */
    if (s->has_limit || s->has_offset) {
        uint32_t limit_idx = plan_alloc_node(arena, PLAN_LIMIT);
        PLAN_NODE(arena, limit_idx).left = current;
        PLAN_NODE(arena, limit_idx).limit.has_limit = s->has_limit;
        PLAN_NODE(arena, limit_idx).limit.limit = s->has_limit ? (size_t)s->limit_count : 0;
        PLAN_NODE(arena, limit_idx).limit.has_offset = s->has_offset;
        PLAN_NODE(arena, limit_idx).limit.offset = s->has_offset ? (size_t)s->offset_count : 0;
        current = limit_idx;
    }

    return current;
}
