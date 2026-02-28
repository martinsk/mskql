#include "logical.h"
#include "arena_helpers.h"
#include "database.h"
#include <string.h>

/* ---- Allocator ---- */

uint32_t logical_alloc_node(struct query_arena *arena, enum logical_op op)
{
    struct logical_node n;
    memset(&n, 0, sizeof(n));
    n.op    = op;
    n.child = IDX_NONE;
    n.right = IDX_NONE;
    da_push(&arena->logical_nodes, n);
    return (uint32_t)(arena->logical_nodes.count - 1);
}

struct logical_node *logical_node_get(struct query_arena *arena, uint32_t idx)
{
    if (idx == IDX_NONE || idx >= (uint32_t)arena->logical_nodes.count)
        return NULL;
    return &arena->logical_nodes.items[idx];
}

/* ---- Internal helpers ---- */

/* Allocate an AND condition combining two existing conditions. */
static uint32_t make_and_cond(struct query_arena *arena,
                               uint32_t left_cond, uint32_t right_cond)
{
    uint32_t idx = arena_alloc_cond(arena);
    struct condition *c = &arena->conditions.items[idx];
    c->type  = COND_AND;
    c->left  = left_cond;
    c->right = right_cond;
    return idx;
}

/* ---- logical_build ---- */

uint32_t logical_build(struct query_select *s, struct query_arena *arena,
                       struct database *db)
{
    (void)db; /* reserved for future schema lookups */

    /* ---- Base: L_SCAN ---- */
    uint32_t scan_idx = logical_alloc_node(arena, L_SCAN);
    struct logical_node *scan = logical_node_get(arena, scan_idx);
    scan->scan.table = s->table;
    scan->scan.alias = s->table_alias;

    uint32_t current = scan_idx;

    /* ---- L_FILTER (WHERE) ---- */
    if (s->where.has_where && s->where.where_cond != IDX_NONE) {
        uint32_t filt_idx = logical_alloc_node(arena, L_FILTER);
        struct logical_node *filt = logical_node_get(arena, filt_idx);
        filt->child               = current;
        filt->filter.cond_idx     = s->where.where_cond;
        current = filt_idx;
    }

    /* ---- L_JOIN ---- */
    if (s->has_join && s->joins_count > 0) {
        uint32_t join_idx = logical_alloc_node(arena, L_JOIN);
        struct logical_node *jn = logical_node_get(arena, join_idx);
        jn->child                  = current; /* left side */
        jn->right                  = IDX_NONE; /* right side resolved by planner per join_info */
        jn->join.join_type         = s->join_type;
        jn->join.joins_start       = s->joins_start;
        jn->join.joins_count       = s->joins_count;
        current = join_idx;
    }

    /* ---- L_AGGREGATE (GROUP BY) ---- */
    if (s->has_group_by || s->aggregates_count > 0) {
        /* DISTINCT without GROUP BY desugars to L_AGGREGATE with all projected
         * columns as group keys.  We record has_distinct in the project node
         * below; the planner detects agg_count==0 → PLAN_DISTINCT. */
        uint32_t agg_idx = logical_alloc_node(arena, L_AGGREGATE);
        struct logical_node *ag = logical_node_get(arena, agg_idx);
        ag->child                         = current;
        ag->aggregate.group_by_start      = s->group_by_start;
        ag->aggregate.group_by_count      = s->group_by_count;
        ag->aggregate.group_by_exprs_start = s->group_by_exprs_start;
        ag->aggregate.group_by_rollup     = s->group_by_rollup;
        ag->aggregate.group_by_cube       = s->group_by_cube;
        current = agg_idx;

        /* HAVING desugars: wrap L_AGGREGATE in L_FILTER */
        if (s->has_having && s->having_cond != IDX_NONE) {
            uint32_t hfilt_idx = logical_alloc_node(arena, L_FILTER);
            struct logical_node *hf = logical_node_get(arena, hfilt_idx);
            hf->child             = current;
            hf->filter.cond_idx   = s->having_cond;
            current = hfilt_idx;
        }
    } else if (s->has_distinct && !s->has_distinct_on) {
        /* DISTINCT without aggregates: desugar to L_AGGREGATE with no group keys.
         * The planner recognises agg_count==0 && group_count==0 → PLAN_DISTINCT. */
        uint32_t agg_idx = logical_alloc_node(arena, L_AGGREGATE);
        struct logical_node *ag = logical_node_get(arena, agg_idx);
        ag->child                    = current;
        ag->aggregate.group_by_start = IDX_NONE;
        ag->aggregate.group_by_count = 0;
        current = agg_idx;
    }

    /* ---- L_PROJECT ---- */
    {
        uint32_t proj_idx = logical_alloc_node(arena, L_PROJECT);
        struct logical_node *pr = logical_node_get(arena, proj_idx);
        pr->child = current;
        pr->project.columns               = s->columns;
        pr->project.parsed_columns_start  = s->parsed_columns_start;
        pr->project.parsed_columns_count  = s->parsed_columns_count;
        pr->project.select_exprs_start    = s->select_exprs_start;
        pr->project.select_exprs_count    = s->select_exprs_count;
        pr->project.aggregates_start      = s->aggregates_start;
        pr->project.aggregates_count      = s->aggregates_count;
        pr->project.agg_before_cols       = s->agg_before_cols;
        pr->project.has_expr_aggs         = s->has_expr_aggs;
        pr->project.has_distinct_on       = s->has_distinct_on;
        pr->project.distinct_on_start     = s->distinct_on_start;
        pr->project.distinct_on_count     = s->distinct_on_count;
        current = proj_idx;
    }

    /* ---- L_SORT ---- */
    if (s->has_order_by && s->order_by_count > 0) {
        uint32_t sort_idx = logical_alloc_node(arena, L_SORT);
        struct logical_node *so = logical_node_get(arena, sort_idx);
        so->child               = current;
        so->sort.order_by_start = s->order_by_start;
        so->sort.order_by_count = s->order_by_count;
        current = sort_idx;
    }

    /* ---- L_LIMIT ---- */
    if (s->has_limit || s->has_offset) {
        uint32_t lim_idx = logical_alloc_node(arena, L_LIMIT);
        struct logical_node *lm = logical_node_get(arena, lim_idx);
        lm->child             = current;
        lm->limit.has_limit   = s->has_limit;
        lm->limit.limit_count = s->limit_count;
        lm->limit.has_offset  = s->has_offset;
        lm->limit.offset_count = s->offset_count;
        current = lim_idx;
    }

    return current;
}

/* ---- logical_normalize ---- */

/* Rule: merge two adjacent L_FILTER nodes into one with AND condition.
 *   L_FILTER(L_FILTER(child, p1), p2) → L_FILTER(child, p1 AND p2) */
static uint32_t rule_filter_merge(uint32_t idx, struct query_arena *arena)
{
    struct logical_node *outer = logical_node_get(arena, idx);
    if (outer->op != L_FILTER) return idx;

    struct logical_node *inner = logical_node_get(arena, outer->child);
    if (!inner || inner->op != L_FILTER) return idx;

    /* Combine predicates */
    uint32_t combined = make_and_cond(arena,
                                      inner->filter.cond_idx,
                                      outer->filter.cond_idx);
    /* Reuse outer node: point at inner's child, use combined predicate */
    outer->child           = inner->child;
    outer->filter.cond_idx = combined;
    /* inner node is now unreferenced — arena-allocated so no free needed */
    return idx;
}

/* Rule: push a filter past a join when the predicate touches only the left
 * (outer) side.  Moves filter inside the join for the planner to see
 * L_FILTER(L_SCAN) on the left child, enabling index scan detection.
 *
 * Conservative: only applies when the condition references a single
 * concrete column name (COND_COMPARE with non-empty column sv).
 * Complex predicates (COND_AND/OR, subqueries) are left in place. */
static uint32_t rule_predicate_pushdown(uint32_t idx, struct query_arena *arena)
{
    struct logical_node *filt = logical_node_get(arena, idx);
    if (!filt || filt->op != L_FILTER) return idx;

    struct logical_node *join = logical_node_get(arena, filt->child);
    if (!join || join->op != L_JOIN) return idx;

    if (filt->filter.cond_idx == IDX_NONE ||
        filt->filter.cond_idx >= (uint32_t)arena->conditions.count)
        return idx;

    struct condition *cond = &arena->conditions.items[filt->filter.cond_idx];

    /* Only push simple single-column comparisons */
    if (cond->type != COND_COMPARE) return idx;
    if (cond->column.len == 0)    return idx;

    /* Push: create new L_FILTER wrapping join's left child */
    uint32_t new_filt_idx = logical_alloc_node(arena, L_FILTER);
    struct logical_node *new_filt = logical_node_get(arena, new_filt_idx);
    new_filt->child           = join->child; /* join's left child */
    new_filt->filter.cond_idx = filt->filter.cond_idx;

    /* Re-fetch join pointer (DA may have reallocated during alloc) */
    join = logical_node_get(arena, filt->child);

    /* Reuse the outer filter node as the join — swap its op and fields */
    uint32_t join_idx = filt->child;
    (void)join_idx;

    /* Point the join's left child at the new filter */
    join->child = new_filt_idx;

    /* The original filter node becomes a pass-through: return the join */
    return filt->child;
}

/* Rule: eliminate dead L_PROJECT(L_PROJECT(child, c1), c2) → keep outer only.
 * The outer projection is always the authoritative one. */
static uint32_t rule_dead_project_elim(uint32_t idx, struct query_arena *arena)
{
    struct logical_node *outer = logical_node_get(arena, idx);
    if (!outer || outer->op != L_PROJECT) return idx;

    struct logical_node *inner = logical_node_get(arena, outer->child);
    if (!inner || inner->op != L_PROJECT) return idx;

    /* Skip inner: outer takes inner's child */
    outer->child = inner->child;
    return idx;
}

/* Recursive bottom-up normalize pass */
static uint32_t normalize_node(uint32_t idx, struct query_arena *arena,
                                struct database *db)
{
    if (idx == IDX_NONE) return IDX_NONE;

    struct logical_node *n = logical_node_get(arena, idx);
    if (!n) return idx;

    /* Recurse into children first (bottom-up) */
    uint32_t new_child = normalize_node(n->child, arena, db);
    n = logical_node_get(arena, idx); /* re-fetch after potential realloc */
    if (n) n->child = new_child;

    uint32_t new_right = normalize_node(n ? n->right : IDX_NONE, arena, db);
    n = logical_node_get(arena, idx);
    if (n) n->right = new_right;

    /* Apply rules */
    idx = rule_filter_merge(idx, arena);
    idx = rule_dead_project_elim(idx, arena);
    idx = rule_predicate_pushdown(idx, arena);

    return idx;
}

uint32_t logical_normalize(uint32_t root, struct query_arena *arena,
                           struct database *db)
{
    return normalize_node(root, arena, db);
}
