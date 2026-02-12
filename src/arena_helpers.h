#ifndef ARENA_HELPERS_H
#define ARENA_HELPERS_H

/* This header provides arena helper functions that require complete type
 * definitions from query.h.  It must be included AFTER query.h defines
 * struct expr, struct condition, struct case_when_branch, etc. */

#include "arena.h"
#include <string.h> /* memset */

/* allocate an expr slot, return its index.
 * Caller MUST set the type and fill in the appropriate union member,
 * including setting any index fields to IDX_NONE if unused. */
static inline uint32_t arena_alloc_expr(struct query_arena *a)
{
    struct expr e;
    memset(&e, 0, sizeof(e));
    da_push(&a->exprs, e);
    return (uint32_t)(a->exprs.count - 1);
}

/* allocate a condition slot, return its index */
static inline uint32_t arena_alloc_cond(struct query_arena *a)
{
    struct condition c;
    memset(&c, 0, sizeof(c));
    c.left = IDX_NONE;
    c.right = IDX_NONE;
    c.lhs_expr = IDX_NONE;
    c.subquery_sql = IDX_NONE;
    c.scalar_subquery_sql = IDX_NONE;
    da_push(&a->conditions, c);
    return (uint32_t)(a->conditions.count - 1);
}

/* push a case_when_branch, return its index */
static inline uint32_t arena_push_branch(struct query_arena *a, struct case_when_branch b)
{
    da_push(&a->branches, b);
    return (uint32_t)(a->branches.count - 1);
}

/* push a join_info, return its index */
static inline uint32_t arena_push_join(struct query_arena *a, struct join_info j)
{
    da_push(&a->joins, j);
    return (uint32_t)(a->joins.count - 1);
}

/* push a cte_def, return its index */
static inline uint32_t arena_push_cte(struct query_arena *a, struct cte_def c)
{
    da_push(&a->ctes, c);
    return (uint32_t)(a->ctes.count - 1);
}

/* push a set_clause, return its index */
static inline uint32_t arena_push_set_clause(struct query_arena *a, struct set_clause sc)
{
    da_push(&a->set_clauses, sc);
    return (uint32_t)(a->set_clauses.count - 1);
}

/* push an order_by_item, return its index */
static inline uint32_t arena_push_order_item(struct query_arena *a, struct order_by_item o)
{
    da_push(&a->order_items, o);
    return (uint32_t)(a->order_items.count - 1);
}

/* push a select_column, return its index */
static inline uint32_t arena_push_select_col(struct query_arena *a, struct select_column sc)
{
    da_push(&a->select_cols, sc);
    return (uint32_t)(a->select_cols.count - 1);
}

/* push a select_expr, return its index */
static inline uint32_t arena_push_select_expr(struct query_arena *a, struct select_expr se)
{
    da_push(&a->select_exprs, se);
    return (uint32_t)(a->select_exprs.count - 1);
}

/* push an agg_expr, return its index */
static inline uint32_t arena_push_agg(struct query_arena *a, struct agg_expr ae)
{
    da_push(&a->aggregates, ae);
    return (uint32_t)(a->aggregates.count - 1);
}

/* Free column default_value structs (calloc'd).
 * The text inside default_value cells is in the bump slab — do NOT free it. */
static inline void arena_free_column_defaults(struct query_arena *a)
{
    for (size_t i = 0; i < a->columns.count; i++) {
        if (a->columns.items[i].default_value) {
            free(a->columns.items[i].default_value);
            a->columns.items[i].default_value = NULL;
        }
    }
}

/* Free execution-time result rows.
 * When arena_owns_text is set, text lives in result_text bump — just free
 * the DA backing arrays and reset the bump in one shot.
 * Otherwise fall back to per-cell free (heap-allocated text). */
static inline void arena_free_result_rows(struct query_arena *a)
{
    if (a->result.arena_owns_text) {
        for (size_t i = 0; i < a->result.count; i++)
            da_free(&a->result.data[i].cells);
        bump_reset(&a->result_text);
    } else {
        for (size_t i = 0; i < a->result.count; i++)
            row_free(&a->result.data[i]);
    }
    a->result.count = 0;
    /* keep data/capacity for reuse */
}

/* Free per-row cells arrays in arena.rows (INSERT tuples).
 * Cell text is in the bump slab (bump_strndup in parse_value_tuple),
 * so only the DA backing array needs freeing. */
static inline void arena_free_row_cell_arrays(struct query_arena *a)
{
    for (size_t i = 0; i < a->rows.count; i++)
        da_free(&a->rows.items[i].cells);
}

/* Reset the arena: set all counts to 0, reset bump slabs.
 * Keeps all backing memory allocated for reuse.
 * All text (parser strings, cell text, subquery-resolved values) lives in
 * the bump slab and is released by bump_reset — no per-item free needed. */
static inline void query_arena_reset(struct query_arena *a)
{
    arena_free_column_defaults(a);
    arena_free_result_rows(a);
    arena_free_row_cell_arrays(a);

    da_reset(&a->exprs);
    da_reset(&a->conditions);
    da_reset(&a->cells);
    da_reset(&a->strings);
    da_reset(&a->branches);
    da_reset(&a->joins);
    da_reset(&a->ctes);
    da_reset(&a->set_clauses);
    da_reset(&a->order_items);
    da_reset(&a->select_cols);
    da_reset(&a->select_exprs);
    da_reset(&a->aggregates);
    da_reset(&a->rows);
    da_reset(&a->svs);
    da_reset(&a->columns);
    da_reset(&a->arg_indices);
    da_reset(&a->plan_nodes);
    bump_reset(&a->bump);
    bump_reset(&a->result_text);
    bump_reset(&a->scratch);
    a->errmsg[0] = '\0';
    a->sqlstate[0] = '\0';
}

/* destroy the arena: free all backing memory.
 * All text (parser strings, cell text, subquery-resolved values) lives in
 * the bump slab — no per-item free needed.
 * Column default_value and row cell arrays still need individual cleanup. */
static inline void query_arena_destroy(struct query_arena *a)
{
    arena_free_column_defaults(a);
    arena_free_row_cell_arrays(a);

    da_free(&a->strings);
    da_free(&a->cells);
    da_free(&a->exprs);
    da_free(&a->conditions);
    da_free(&a->rows);
    da_free(&a->columns);
    da_free(&a->branches);
    da_free(&a->joins);
    da_free(&a->ctes);
    da_free(&a->set_clauses);
    da_free(&a->order_items);
    da_free(&a->select_cols);
    da_free(&a->select_exprs);
    da_free(&a->aggregates);
    da_free(&a->svs);
    da_free(&a->arg_indices);
    da_free(&a->plan_nodes);
    bump_destroy(&a->bump);
    bump_destroy(&a->result_text);
    bump_destroy(&a->scratch);

    /* free result rows */
    if (a->result.arena_owns_text) {
        for (size_t i = 0; i < a->result.count; i++)
            da_free(&a->result.data[i].cells);
    } else {
        for (size_t i = 0; i < a->result.count; i++)
            row_free(&a->result.data[i]);
    }
    free(a->result.data);
    a->result.data = NULL;
    a->result.count = 0;
    a->result.capacity = 0;
    a->result.arena_owns_text = 0;
}

#endif
