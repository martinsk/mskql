#ifndef LOGICAL_H
#define LOGICAL_H

#include "arena.h"
#include "query.h"

/* ---- Logical IR node types ----
 *
 * A canonical 9-node intermediate representation sitting between the parser
 * (query_select AST) and the physical plan builder (plan.c).
 *
 * The parser emits a rich query_select with ~40 flags.  logical_build()
 * desugars that into a minimal tree of logical_node using only these ops.
 * logical_normalize() then applies semantics-preserving tree rewrites.
 * logical_to_physical() (in plan.c) pattern-matches the result to physical
 * plan_node types.
 *
 * Key invariants:
 *   - No physical knowledge (no INDEX_SCAN, TOP_N, HASH_JOIN, etc.)
 *   - Sugar is fully lowered: no HAVING, no NATURAL JOIN, no USING,
 *     no BETWEEN, no IN-list, no DISTINCT keyword
 *   - All nodes arena-allocated; children referenced by uint32_t index
 *   - IDX_NONE (0xFFFFFFFF) is the null sentinel for optional children
 */

enum logical_op {
    L_SCAN,        /* full table scan: table name + optional column list */
    L_FILTER,      /* child + predicate (condition index in arena) */
    L_PROJECT,     /* child + expression list (select columns) */
    L_JOIN,        /* left + right children + explicit ON condition */
    L_AGGREGATE,   /* child + group keys + agg exprs; HAVING becomes outer L_FILTER */
    L_SORT,        /* child + sort keys */
    L_LIMIT,       /* child + count + offset */
    L_SUBQUERY,    /* inline subquery / CTE: child_root is root of inner logical tree */
    L_DISTINCT_ON,    /* keep first row per key after sort: desugared from DISTINCT ON */
    L_WINDOW,         /* window functions (SELECT ... OVER (...)) */
    L_SET_OP,         /* UNION / INTERSECT / EXCEPT */
    L_GENERATE_SERIES, /* generate_series(start, stop[, step]) virtual table */
};

/* L_SCAN payload */
struct logical_scan {
    sv   table;           /* table name */
    sv   alias;           /* table alias, may be empty */
};

/* L_FILTER payload */
struct logical_filter {
    uint32_t cond_idx;    /* index into arena->conditions */
};

/* L_PROJECT payload — mirrors query_select column/expr lists */
struct logical_project {
    sv       columns;                /* "*" or empty if using parsed_columns */
    uint32_t parsed_columns_start;   /* index into arena->select_cols */
    uint32_t parsed_columns_count;
    uint32_t select_exprs_start;     /* index into arena->select_exprs */
    uint32_t select_exprs_count;
    uint32_t aggregates_start;       /* index into arena->aggregates */
    uint32_t aggregates_count;
    int      agg_before_cols;
    int      has_expr_aggs;
    int      has_distinct_on;
    uint32_t distinct_on_start;      /* index into arena->svs */
    uint32_t distinct_on_count;
};

/* L_JOIN payload */
struct logical_join {
    enum join_type join_type;
    uint32_t joins_start; /* index into arena->joins (all joins as resolved join_info) */
    uint32_t joins_count;
};

/* L_AGGREGATE payload */
struct logical_aggregate {
    uint32_t group_by_start;       /* index into arena->svs */
    uint32_t group_by_count;
    uint32_t group_by_exprs_start; /* index into arena->arg_indices */
    int      group_by_rollup;
    int      group_by_cube;
    /* agg expressions are carried in L_PROJECT above this node */
};

/* L_SORT payload */
struct logical_sort {
    uint32_t order_by_start; /* index into arena->order_items */
    uint32_t order_by_count;
};

/* L_LIMIT payload */
struct logical_limit {
    int has_limit;
    int64_t limit_count;
    int has_offset;
    int64_t offset_count;
};

/* L_SUBQUERY payload — represents an inline subquery or non-recursive CTE.
 * sql_idx is an index into arena->strings pointing to the inner SQL text.
 * The physical builder (build_subquery in plan.c) calls query_parse on it
 * and builds a sub-plan, just like build_set_op does for UNION/INTERSECT.
 * The outer query treats the output of this node as if it were an L_SCAN. */
struct logical_subquery {
    uint32_t sql_idx;  /* index into arena->strings for inner SQL text */
    sv       alias;    /* table alias (CTE name or FROM subquery alias) */
};

/* L_WINDOW payload — window function query.
 * All relevant fields are carried in the outer query_select; this node is
 * a marker that logical_to_physical routes to build_window. */
struct logical_window {
    int dummy; /* no extra payload needed — build_window reads query_select directly */
};

/* L_SET_OP payload — UNION / INTERSECT / EXCEPT.
 * rhs_sql_idx is an index into arena->strings for the RHS SQL text. */
struct logical_set_op {
    enum set_op_kind set_op;
    int      set_all;     /* 1 for UNION ALL / INTERSECT ALL / EXCEPT ALL */
    uint32_t rhs_sql_idx; /* index into arena->strings for RHS SQL */
};

/* L_GENERATE_SERIES payload — generate_series(start, stop[, step]).
 * Expression indices point into the outer query_select arena. */
struct logical_gen_series {
    uint32_t start_expr; /* index into arena->exprs */
    uint32_t stop_expr;  /* index into arena->exprs */
    uint32_t step_expr;  /* index into arena->exprs, or IDX_NONE */
};

/* L_DISTINCT_ON payload — keep first row per (key_start..key_start+key_count).
 * Desugared from DISTINCT ON (col1, col2): logical_build emits L_SORT on the
 * DISTINCT ON keys first, then L_DISTINCT_ON to strip duplicates. */
struct logical_distinct_on {
    uint32_t key_start;  /* index into arena->svs */
    uint32_t key_count;  /* number of key columns */
};

/* A logical plan node.  Arena-allocated in arena->bump.
 * Children referenced by index into the logical_nodes array (also bump). */
struct logical_node {
    enum logical_op op;
    uint32_t        child;   /* primary child index, or IDX_NONE */
    uint32_t        right;   /* second child (L_JOIN only), or IDX_NONE */
    union {
        struct logical_scan        scan;
        struct logical_filter      filter;
        struct logical_project     project;
        struct logical_join        join;
        struct logical_aggregate   aggregate;
        struct logical_sort        sort;
        struct logical_limit       limit;
        struct logical_subquery    subquery;
        struct logical_distinct_on distinct_on;
        struct logical_window      window;
        struct logical_set_op      set_op;
        struct logical_gen_series  gen_series;
    };
};

/* ---- Public API ---- */

/* Build a logical tree from a parsed query_select.
 * Returns the root node index (into arena->logical_nodes via bump).
 * Returns IDX_NONE and sets arena error on failure.
 *
 * Desugaring performed:
 *   HAVING cond        → L_FILTER wrapping L_AGGREGATE
 *   DISTINCT           → L_AGGREGATE (all projected cols as group keys)
 *   NATURAL JOIN       → L_JOIN with explicit ON condition (via join_info)
 *   JOIN ... USING(c)  → L_JOIN with explicit ON condition (via join_info)
 *   ORDER BY + LIMIT   → L_LIMIT wrapping L_SORT (TOP_N candidate for matcher)
 *   Non-recursive CTE  → L_SUBQUERY (inline; no temp table materialization)
 *   FROM subquery      → L_SUBQUERY (inline; no temp table materialization)
 *   DISTINCT ON (cols) → L_SORT(cols) + L_DISTINCT_ON(cols)
 *
 * logical_build always succeeds (no validation) — inner parse errors for
 * subquery SQL must be detected before calling logical_build.
 *
 * Returns IDX_NONE only if the arena is out of memory (fatal).
 */
uint32_t logical_build(struct query_select *s, struct query_arena *arena,
                       struct database *db);

/* Apply semantics-preserving tree rewrites to the logical tree.
 * Modifies nodes in place (arena-allocated, safe to update).
 * Rules applied bottom-up in a single pass:
 *   - Filter merge:       L_FILTER(L_FILTER(x,p1),p2) → L_FILTER(x, p1 AND p2)
 *   - Predicate pushdown: L_FILTER(L_JOIN(a,b), pred on a) → L_JOIN(L_FILTER(a,p),b)
 *   - Constant fold:      L_FILTER(x, always-true) → x
 *   - Dead project elim:  L_PROJECT(L_PROJECT(x,c1),c2) → L_PROJECT(x,c2)
 *
 * Returns the (possibly new) root index after rewrites. */
uint32_t logical_normalize(uint32_t root, struct query_arena *arena,
                           struct database *db);

/* Retrieve a logical_node by index (bump-allocated array). */
struct logical_node *logical_node_get(struct query_arena *arena, uint32_t idx);

/* Allocate a new logical_node in the arena, return its index. */
uint32_t logical_alloc_node(struct query_arena *arena, enum logical_op op);

/* Pretty-print the logical plan tree rooted at root into buf (buflen bytes).
 * Returns the number of bytes written (excluding null terminator).
 * Output uses 2-space indentation per depth level. */
int logical_explain(struct query_arena *arena, uint32_t root,
                    char *buf, int buflen);

#endif
