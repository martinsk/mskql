#ifndef ARENA_H
#define ARENA_H

#include <stdint.h>
#include "dynamic_array.h"
#include "row.h"
#include "column.h"
#include "stringview.h"

#define IDX_NONE ((uint32_t)0xFFFFFFFF)

/* forward declarations — full definitions in query.h */
struct expr;
struct condition;
struct case_when_branch;
struct set_clause;
struct select_column;
struct select_expr;
struct agg_expr;
struct order_by_item;
struct join_info;
struct cte_def;

/* Pool-based arena: each parsed type gets its own flat dynamic array.
 * Structures reference items by uint32_t index instead of pointers.
 * Freeing the entire query is a single query_arena_destroy() call. */
struct query_arena {
    DYNAMIC_ARRAY(struct expr)             exprs;
    DYNAMIC_ARRAY(struct condition)        conditions;
    DYNAMIC_ARRAY(struct cell)             cells;       /* literal values, IN lists, etc. */
    DYNAMIC_ARRAY(char *)                  strings;     /* owned NUL-terminated strings */
    DYNAMIC_ARRAY(struct case_when_branch) branches;
    DYNAMIC_ARRAY(struct join_info)        joins;
    DYNAMIC_ARRAY(struct cte_def)          ctes;
    DYNAMIC_ARRAY(struct set_clause)       set_clauses;
    DYNAMIC_ARRAY(struct order_by_item)    order_items;
    DYNAMIC_ARRAY(struct select_column)    select_cols;
    DYNAMIC_ARRAY(struct select_expr)      select_exprs;
    DYNAMIC_ARRAY(struct agg_expr)         aggregates;
    DYNAMIC_ARRAY(struct row)              rows;        /* INSERT value tuples, literal SELECT */
    DYNAMIC_ARRAY(sv)                      svs;         /* sv values (multi_columns, group_by) */
    DYNAMIC_ARRAY(struct column)           columns;     /* CREATE TABLE columns */
    DYNAMIC_ARRAY(uint32_t)                arg_indices; /* func call arg expr indices */
};

static inline void query_arena_init(struct query_arena *a)
{
    da_init(&a->exprs);
    da_init(&a->conditions);
    da_init(&a->cells);
    da_init(&a->strings);
    da_init(&a->branches);
    da_init(&a->joins);
    da_init(&a->ctes);
    da_init(&a->set_clauses);
    da_init(&a->order_items);
    da_init(&a->select_cols);
    da_init(&a->select_exprs);
    da_init(&a->aggregates);
    da_init(&a->rows);
    da_init(&a->svs);
    da_init(&a->columns);
    da_init(&a->arg_indices);
}

/* helpers that only need cell / char* / sv / row / column (complete here) */

static inline uint32_t arena_push_cell(struct query_arena *a, struct cell c)
{
    da_push(&a->cells, c);
    return (uint32_t)(a->cells.count - 1);
}

static inline uint32_t arena_store_string(struct query_arena *a, const char *s, size_t len)
{
    char *copy = malloc(len + 1);
    memcpy(copy, s, len);
    copy[len] = '\0';
    da_push(&a->strings, copy);
    return (uint32_t)(a->strings.count - 1);
}

static inline uint32_t arena_own_string(struct query_arena *a, char *s)
{
    da_push(&a->strings, s);
    return (uint32_t)(a->strings.count - 1);
}

static inline uint32_t arena_push_sv(struct query_arena *a, sv s)
{
    da_push(&a->svs, s);
    return (uint32_t)(a->svs.count - 1);
}

static inline uint32_t arena_push_row(struct query_arena *a, struct row r)
{
    da_push(&a->rows, r);
    return (uint32_t)(a->rows.count - 1);
}

static inline uint32_t arena_push_column(struct query_arena *a, struct column col)
{
    da_push(&a->columns, col);
    return (uint32_t)(a->columns.count - 1);
}

/* accessor macros for dereferencing arena indices */
#define EXPR(arena, idx)      ((arena)->exprs.items[(idx)])
#define COND(arena, idx)      ((arena)->conditions.items[(idx)])
#define ACELL(arena, idx)     ((arena)->cells.items[(idx)])
#define ASTRING(arena, idx)   ((arena)->strings.items[(idx)])
#define ABRANCH(arena, idx)   ((arena)->branches.items[(idx)])
#define ASV(arena, idx)       ((arena)->svs.items[(idx)])
#define FUNC_ARG(arena, start, i) ((arena)->arg_indices.items[(start) + (i)])

#endif
