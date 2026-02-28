#ifndef LOGICAL_H
#define LOGICAL_H

#include "arena.h"
#include "query.h"

/* ---- Logical IR node types ----
 *
 * A canonical 7-node intermediate representation sitting between the parser
 * (query_select AST) and the physical plan builder (plan.c).
 *
 * The parser emits a rich query_select with ~40 flags.  logical_build()
 * desugars that into a minimal tree of logical_node using only these 7 ops.
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
    L_SCAN,       /* full table scan: table name + optional column list */
    L_FILTER,     /* child + predicate (condition index in arena) */
    L_PROJECT,    /* child + expression list (select columns) */
    L_JOIN,       /* left + right children + explicit ON condition */
    L_AGGREGATE,  /* child + group keys + agg exprs; HAVING becomes outer L_FILTER */
    L_SORT,       /* child + sort keys */
    L_LIMIT,      /* child + count + offset */
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
    int      join_type;   /* 0=INNER, 1=LEFT, 2=RIGHT, 3=FULL, 4=CROSS */
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
    int limit_count;
    int has_offset;
    int offset_count;
};

/* A logical plan node.  Arena-allocated in arena->bump.
 * Children referenced by index into the logical_nodes array (also bump). */
struct logical_node {
    enum logical_op op;
    uint32_t        child;   /* primary child index, or IDX_NONE */
    uint32_t        right;   /* second child (L_JOIN only), or IDX_NONE */
    union {
        struct logical_scan      scan;
        struct logical_filter    filter;
        struct logical_project   project;
        struct logical_join      join;
        struct logical_aggregate aggregate;
        struct logical_sort      sort;
        struct logical_limit     limit;
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

#endif
