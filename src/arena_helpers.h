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

/* destroy the arena: free all owned strings, text cells, rows, columns, then arrays */
static inline void query_arena_destroy(struct query_arena *a)
{
    /* free owned strings */
    for (size_t i = 0; i < a->strings.count; i++)
        free(a->strings.items[i]);
    da_free(&a->strings);

    /* free text in literal cells */
    for (size_t i = 0; i < a->cells.count; i++)
        cell_free_text(&a->cells.items[i]);
    da_free(&a->cells);

    /* free text in expr literals */
    for (size_t i = 0; i < a->exprs.count; i++) {
        if (a->exprs.items[i].type == EXPR_LITERAL) {
            cell_free_text(&a->exprs.items[i].literal);
        }
    }
    da_free(&a->exprs);

    /* free text in condition values */
    for (size_t i = 0; i < a->conditions.count; i++) {
        cell_free_text(&a->conditions.items[i].value);
        cell_free_text(&a->conditions.items[i].between_high);
    }
    da_free(&a->conditions);

    /* free rows (each row has its own cells dynamic array) */
    for (size_t i = 0; i < a->rows.count; i++) {
        for (size_t j = 0; j < a->rows.items[i].cells.count; j++)
            cell_free_text(&a->rows.items[i].cells.items[j]);
        da_free(&a->rows.items[i].cells);
    }
    da_free(&a->rows);

    /* free columns (name, enum_type_name, default_value) */
    for (size_t i = 0; i < a->columns.count; i++)
        column_free(&a->columns.items[i]);
    da_free(&a->columns);

    /* free remaining flat arrays */
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
}

#endif
