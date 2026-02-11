#include "plan.h"
#include "arena_helpers.h"
#include <stdio.h>
#include <string.h>

/* Helper: copy a col_block value at index src_i to dst col_block at dst_i.
 * Handles all column types without triggering -Wswitch-enum. */
static inline void cb_copy_value(struct col_block *dst, uint32_t dst_i,
                                 const struct col_block *src, uint16_t src_i)
{
    dst->nulls[dst_i] = src->nulls[src_i];
    if (column_type_is_text(src->type)) {
        dst->data.str[dst_i] = src->data.str[src_i];
    } else if (src->type == COLUMN_TYPE_BIGINT) {
        dst->data.i64[dst_i] = src->data.i64[src_i];
    } else if (src->type == COLUMN_TYPE_FLOAT || src->type == COLUMN_TYPE_NUMERIC) {
        dst->data.f64[dst_i] = src->data.f64[src_i];
    } else {
        dst->data.i32[dst_i] = src->data.i32[src_i];
    }
}

/* Helper: bulk copy count values from src col_block to dst col_block. */
static inline void cb_bulk_copy(struct col_block *dst,
                                const struct col_block *src, uint32_t count)
{
    memcpy(dst->nulls, src->nulls, count);
    if (column_type_is_text(src->type)) {
        memcpy(dst->data.str, src->data.str, count * sizeof(char *));
    } else if (src->type == COLUMN_TYPE_BIGINT) {
        memcpy(dst->data.i64, src->data.i64, count * sizeof(int64_t));
    } else if (src->type == COLUMN_TYPE_FLOAT || src->type == COLUMN_TYPE_NUMERIC) {
        memcpy(dst->data.f64, src->data.f64, count * sizeof(double));
    } else {
        memcpy(dst->data.i32, src->data.i32, count * sizeof(int32_t));
    }
}

/* Helper: get a double value from a col_block at index i. */
static inline double cb_to_double(const struct col_block *cb, uint16_t i)
{
    if (cb->type == COLUMN_TYPE_FLOAT || cb->type == COLUMN_TYPE_NUMERIC)
        return cb->data.f64[i];
    if (cb->type == COLUMN_TYPE_BIGINT)
        return (double)cb->data.i64[i];
    return (double)cb->data.i32[i];
}

/* Helper: get output column count for a plan node. */
static uint16_t plan_node_ncols(struct query_arena *arena, uint32_t node_idx)
{
    if (node_idx == IDX_NONE) return 0;
    struct plan_node *pn = &PLAN_NODE(arena, node_idx);
    if (pn->op == PLAN_SEQ_SCAN) return pn->seq_scan.ncols;
    if (pn->op == PLAN_PROJECT) return pn->project.ncols;
    if (pn->op == PLAN_HASH_AGG)
        return pn->hash_agg.ngroup_cols + (uint16_t)pn->hash_agg.agg_count;
    if (pn->op == PLAN_SORT)
        return plan_node_ncols(arena, pn->left);
    /* For pass-through nodes, recurse to child */
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
        cb->type = t->columns.items[tc].type;
        for (uint16_t r = 0; r < nrows; r++) {
            struct cell *cell = &t->rows.items[start + r].cells.items[tc];
            if (!cell->is_null) { cb->type = cell->type; break; }
        }
        cb->count = nrows;

        for (uint16_t r = 0; r < nrows; r++) {
            struct cell *cell = &t->rows.items[start + r].cells.items[tc];
            if (cell->is_null || cell->type != cb->type
                || (column_type_is_text(cell->type) && !cell->value.as_text)) {
                cb->nulls[r] = 1;
                continue;
            }
            cb->nulls[r] = 0;
            switch (cell->type) {
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
    }

    *cursor = end;
    return nrows;
}

/* Convert a row_block back to struct rows for final output.
 * Text values are strdup'd (caller owns the result rows). */
void block_to_rows(const struct row_block *rb, struct rows *result)
{
    uint16_t active = row_block_active_count(rb);
    for (uint16_t i = 0; i < active; i++) {
        uint16_t ri = row_block_row_idx(rb, i);
        struct row dst = {0};
        da_init(&dst.cells);
        for (uint16_t c = 0; c < rb->ncols; c++) {
            const struct col_block *cb = &rb->cols[c];
            struct cell cell = {0};
            cell.type = cb->type;
            if (cb->nulls[ri]) {
                cell.is_null = 1;
            } else {
                switch (cb->type) {
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
                        cell.value.as_text = cb->data.str[ri] ? strdup(cb->data.str[ri]) : NULL;
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
    uint16_t n = scan_table_block(pn->seq_scan.table, &st->cursor,
                                  out, pn->seq_scan.col_map,
                                  pn->seq_scan.ncols, &ctx->arena->scratch);
    if (n == 0) return -1;
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

        if (cb->type == COLUMN_TYPE_INT || cb->type == COLUMN_TYPE_BOOLEAN) {
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

static void hash_join_build(struct plan_exec_ctx *ctx, uint32_t node_idx)
{
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);
    struct hash_join_state *st = (struct hash_join_state *)ctx->node_states[node_idx];

    /* Determine inner (right child) column count */
    struct plan_node *inner = &PLAN_NODE(ctx->arena, pn->right);
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
    int key_col = pn->hash_join.inner_key_col;
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
                        /* Check key equality */
                        int eq = 1;
                        for (uint16_t g = 0; g < ngrp; g++) {
                            int gc = pn->hash_agg.group_cols[g];
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
                        dst->type = COLUMN_TYPE_INT;
                        dst->nulls[out_count] = 1;
                    } else {
                        dst->type = COLUMN_TYPE_INT;
                        dst->nulls[out_count] = 0;
                        dst->data.i32[out_count] = (int32_t)st->sums[idx];
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
    uint16_t          nsort_cols;
};
static struct block_sort_ctx _bsort_ctx;

static int cb_compare(const struct col_block *a, uint16_t ai,
                      const struct col_block *b, uint16_t bi)
{
    if (a->nulls[ai] && b->nulls[bi]) return 0;
    if (a->nulls[ai]) return 1;
    if (b->nulls[bi]) return -1;
    if (a->type == COLUMN_TYPE_INT || a->type == COLUMN_TYPE_BOOLEAN) {
        int32_t va = a->data.i32[ai], vb = b->data.i32[bi];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    if (a->type == COLUMN_TYPE_BIGINT) {
        int64_t va = a->data.i64[ai], vb = b->data.i64[bi];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    if (a->type == COLUMN_TYPE_FLOAT || a->type == COLUMN_TYPE_NUMERIC) {
        double va = a->data.f64[ai], vb = b->data.f64[bi];
        return (va < vb) ? -1 : (va > vb) ? 1 : 0;
    }
    const char *sa = a->data.str[ai], *sb = b->data.str[bi];
    if (!sa && !sb) return 0;
    if (!sa) return -1;
    if (!sb) return 1;
    int cmp = strcmp(sa, sb);
    return (cmp < 0) ? -1 : (cmp > 0) ? 1 : 0;
}

static int sort_index_cmp(const void *a, const void *b)
{
    uint32_t ia = *(const uint32_t *)a;
    uint32_t ib = *(const uint32_t *)b;
    uint32_t block_a = ia / _bsort_ctx.rows_per_block;
    uint16_t row_a = (uint16_t)(ia % _bsort_ctx.rows_per_block);
    uint32_t block_b = ib / _bsort_ctx.rows_per_block;
    uint16_t row_b = (uint16_t)(ib % _bsort_ctx.rows_per_block);

    for (uint16_t k = 0; k < _bsort_ctx.nsort_cols; k++) {
        int ci = _bsort_ctx.sort_cols[k];
        struct col_block *ca = &_bsort_ctx.all_cols[block_a * _bsort_ctx.ncols + ci];
        struct col_block *cblk = &_bsort_ctx.all_cols[block_b * _bsort_ctx.ncols + ci];
        int cmp = cb_compare(ca, row_a, cblk, row_b);
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
        _bsort_ctx.nsort_cols = pn->sort.nsort_cols;

        _bsort_ctx.all_cols = (struct col_block *)bump_alloc(&ctx->arena->scratch,
                                (st->nblocks ? st->nblocks : 1) * child_ncols * sizeof(struct col_block));
        for (uint32_t b = 0; b < st->nblocks; b++)
            memcpy(&_bsort_ctx.all_cols[b * child_ncols],
                   st->collected[b].cols,
                   child_ncols * sizeof(struct col_block));

        if (total > 1)
            qsort(st->sorted_indices, total, sizeof(uint32_t), sort_index_cmp);

        st->input_done = 1;
        st->emit_cursor = 0;
    }

    if (st->emit_cursor >= st->sorted_count) return -1;

    uint16_t child_ncols = _bsort_ctx.ncols;
    row_block_reset(out);
    uint16_t out_count = 0;

    while (st->emit_cursor < st->sorted_count && out_count < BLOCK_CAPACITY) {
        uint32_t encoded = st->sorted_indices[st->emit_cursor++];
        uint32_t block_idx = encoded / BLOCK_CAPACITY;
        uint16_t row_idx = (uint16_t)(encoded % BLOCK_CAPACITY);

        for (uint16_t c = 0; c < child_ncols; c++) {
            struct col_block *src = &_bsort_ctx.all_cols[block_idx * child_ncols + c];
            out->cols[c].type = src->type;
            cb_copy_value(&out->cols[c], out_count, src, row_idx);
        }
        out_count++;
    }

    out->count = out_count;
    for (uint16_t c = 0; c < child_ncols; c++)
        out->cols[c].count = out_count;

    return 0;
}

/* ---- Dispatcher ---- */

int plan_next_block(struct plan_exec_ctx *ctx, uint32_t node_idx,
                    struct row_block *out)
{
    if (node_idx == IDX_NONE) return -1;
    struct plan_node *pn = &PLAN_NODE(ctx->arena, node_idx);

    if (pn->op == PLAN_SEQ_SCAN)    return seq_scan_next(ctx, node_idx, out);
    if (pn->op == PLAN_FILTER)       return filter_next(ctx, node_idx, out);
    if (pn->op == PLAN_PROJECT)      return project_next(ctx, node_idx, out);
    if (pn->op == PLAN_LIMIT)        return limit_next(ctx, node_idx, out);
    if (pn->op == PLAN_HASH_JOIN)    return hash_join_next(ctx, node_idx, out);
    if (pn->op == PLAN_HASH_AGG)     return hash_agg_next(ctx, node_idx, out);
    if (pn->op == PLAN_SORT)         return sort_next(ctx, node_idx, out);
    /* Not yet implemented */
    return -1;
}

/* ---- Full plan execution to rows ---- */

int plan_exec_to_rows(struct plan_exec_ctx *ctx, uint32_t root_node,
                      struct rows *result)
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
        block_to_rows(&block, result);
        row_block_reset(&block);
    }

    return 0;
}

/* ---- Plan builder ---- */

/* Try to build a block-oriented plan for a simple single-table SELECT.
 * Returns root plan node index, or IDX_NONE if the query is too complex
 * and should fall back to the legacy executor. */
uint32_t plan_build_select(struct table *t, struct query_select *s,
                           struct query_arena *arena, struct database *db)
{
    (void)db;

    /* Bail out for queries we don't handle yet */
    if (s->has_join)            return IDX_NONE;
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
    if (!t)                     return IDX_NONE;
    if (s->insert_rows_count > 0) return IDX_NONE; /* literal SELECT */

    int select_all = sv_eq_cstr(s->columns, "*");

    /* Determine projection: either SELECT * or all parsed_columns are simple column refs */
    int need_project = 0;
    int *proj_map = NULL;
    uint16_t proj_ncols = 0;

    if (select_all) {
        /* No projection needed — return all columns */
    } else if (s->parsed_columns_count > 0) {
        /* Check that every parsed column is a simple EXPR_COLUMN_REF */
        proj_ncols = (uint16_t)s->parsed_columns_count;
        proj_map = (int *)bump_alloc(&arena->scratch, proj_ncols * sizeof(int));
        for (uint32_t i = 0; i < s->parsed_columns_count; i++) {
            struct select_column *sc = &arena->select_cols.items[s->parsed_columns_start + i];
            if (sc->expr_idx == IDX_NONE) return IDX_NONE;
            struct expr *e = &EXPR(arena, sc->expr_idx);
            if (e->type != EXPR_COLUMN_REF) return IDX_NONE;
            int ci = table_find_column_sv(t, e->column_ref.column);
            if (ci < 0) return IDX_NONE;
            proj_map[i] = ci;
        }
        need_project = 1;
    } else {
        /* Legacy text-based column list — can't handle */
        return IDX_NONE;
    }

    /* Pre-validate ORDER BY columns BEFORE allocating plan nodes */
    int  sort_cols_buf[32];
    int  sort_descs_buf[32];
    uint16_t sort_nord = 0;
    if (s->has_order_by && s->order_by_count > 0) {
        sort_nord = s->order_by_count < 32 ? (uint16_t)s->order_by_count : 32;
        for (uint16_t k = 0; k < sort_nord; k++) {
            struct order_by_item *obi = &arena->order_items.items[s->order_by_start + k];
            sort_descs_buf[k] = obi->desc;
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
    if (s->where.has_where) {
        if (s->where.where_cond == IDX_NONE)
            return IDX_NONE;
        struct condition *cond = &COND(arena, s->where.where_cond);
        if (cond->type != COND_COMPARE || cond->lhs_expr != IDX_NONE
            || cond->rhs_column.len != 0)
            return IDX_NONE;
        /* Reject subqueries, IN lists, ANY/ALL arrays — plan executor can't handle */
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
            ct != COLUMN_TYPE_BIGINT)
            return IDX_NONE;
        if (cond->op > CMP_GE) return IDX_NONE;
        /* Reject cross-type comparisons (e.g. INT col vs FLOAT value from subquery) */
        if (!cond->value.is_null && cond->value.type != ct &&
            !(ct == COLUMN_TYPE_FLOAT && cond->value.type == COLUMN_TYPE_INT) &&
            !(ct == COLUMN_TYPE_INT && cond->value.type == COLUMN_TYPE_FLOAT))
            return IDX_NONE;
        /* If INT column but FLOAT value, reject — fast path can't handle */
        if (ct == COLUMN_TYPE_INT && cond->value.type == COLUMN_TYPE_FLOAT)
            return IDX_NONE;
        filter_ok = 1;
    }

    /* Bail out if table has mixed cell types (e.g. after ALTER COLUMN TYPE) */
    if (t->rows.count > 0) {
        struct row *first = &t->rows.items[0];
        for (size_t c = 0; c < first->cells.count && c < t->columns.count; c++) {
            if (!first->cells.items[c].is_null &&
                first->cells.items[c].type != t->columns.items[c].type)
                return IDX_NONE;
        }
    }

    /* --- All validation passed, now allocate plan nodes --- */

    uint16_t scan_ncols = (uint16_t)t->columns.count;
    int *col_map = (int *)bump_alloc(&arena->scratch, scan_ncols * sizeof(int));
    for (uint16_t i = 0; i < scan_ncols; i++)
        col_map[i] = (int)i;

    uint32_t scan_idx = plan_alloc_node(arena, PLAN_SEQ_SCAN);
    PLAN_NODE(arena, scan_idx).seq_scan.table = t;
    PLAN_NODE(arena, scan_idx).seq_scan.ncols = scan_ncols;
    PLAN_NODE(arena, scan_idx).seq_scan.col_map = col_map;
    PLAN_NODE(arena, scan_idx).est_rows = (double)t->rows.count;

    uint32_t current = scan_idx;

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

    /* Add SORT node if ORDER BY was validated */
    if (sort_nord > 0) {
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

    /* Add projection node if specific columns are selected */
    if (need_project) {
        uint32_t proj_idx = plan_alloc_node(arena, PLAN_PROJECT);
        PLAN_NODE(arena, proj_idx).left = current;
        PLAN_NODE(arena, proj_idx).project.ncols = proj_ncols;
        PLAN_NODE(arena, proj_idx).project.col_map = proj_map;
        current = proj_idx;
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
