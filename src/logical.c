#include "logical.h"
#include "arena_helpers.h"
#include "database.h"
#include <string.h>
#include <stdio.h>

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

    uint32_t current;

    /* ---- generate_series fast path: emit L_GENERATE_SERIES leaf ---- */
    if (s->has_generate_series && s->gs_start_expr != IDX_NONE &&
        s->gs_stop_expr != IDX_NONE) {
        uint32_t gs_idx = logical_alloc_node(arena, L_GENERATE_SERIES);
        struct logical_node *gs = logical_node_get(arena, gs_idx);
        gs->gen_series.start_expr = s->gs_start_expr;
        gs->gen_series.stop_expr  = s->gs_stop_expr;
        gs->gen_series.step_expr  = s->gs_step_expr;
        current = gs_idx;
        /* generate_series does not support WHERE/JOIN/GROUP BY/etc in this path */
        return current;
    }

    /* ---- Window function path: emit L_WINDOW leaf ---- */
    if (s->select_exprs_count > 0) {
        uint32_t win_idx = logical_alloc_node(arena, L_WINDOW);
        /* no payload needed — build_window reads query_select directly */
        (void)logical_node_get(arena, win_idx);
        current = win_idx;
        return current;
    }

    /* ---- Set operation path: emit L_SET_OP leaf ---- */
    if (s->has_set_op && s->set_rhs_sql != IDX_NONE) {
        uint32_t sop_idx = logical_alloc_node(arena, L_SET_OP);
        struct logical_node *sop = logical_node_get(arena, sop_idx);
        sop->set_op.set_op      = s->set_op;
        sop->set_op.set_all     = s->set_all;
        sop->set_op.rhs_sql_idx = s->set_rhs_sql;
        current = sop_idx;
        return current;
    }

    /* ---- Base: L_SUBQUERY (FROM subquery) or L_SCAN ---- */

    if (s->from_subquery_sql != IDX_NONE) {
        /* FROM (SELECT ...) AS alias: desugar to L_SUBQUERY */
        uint32_t sq_idx = logical_alloc_node(arena, L_SUBQUERY);
        struct logical_node *sq = logical_node_get(arena, sq_idx);
        sq->subquery.sql_idx = s->from_subquery_sql;
        sq->subquery.alias   = s->from_subquery_alias;
        current = sq_idx;
    } else {
        /* Plain table scan.
         * NOTE: Non-recursive CTEs are pre-materialised by plan_build_select
         * (and by try_plan_send in pgwire.c) into real temp tables before
         * logical_build is called, so L_SCAN(cte_name) is correct here —
         * the table already exists by the time the physical builder runs. */
        uint32_t scan_idx = logical_alloc_node(arena, L_SCAN);
        struct logical_node *scan = logical_node_get(arena, scan_idx);
        scan->scan.table = s->table;
        scan->scan.alias = s->table_alias;
        current = scan_idx;
    }

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

    /* ---- L_DISTINCT_ON: desugar from DISTINCT ON (cols) ---- */
    if (s->has_distinct_on && s->distinct_on_count > 0) {
        /* First: sort by the DISTINCT ON key columns so duplicates are adjacent */
        uint32_t sort_idx = logical_alloc_node(arena, L_SORT);
        struct logical_node *so = logical_node_get(arena, sort_idx);
        so->child               = current;
        /* Re-use the distinct_on svs as the sort key (same columns).
         * Store their sv start index in sort.order_by_start as a negative
         * sentinel that the physical builder recognises as "sv-based sort". */
        so->sort.order_by_start = s->distinct_on_start;
        so->sort.order_by_count = s->distinct_on_count;
        current = sort_idx;

        /* Then: emit only the first row per key group */
        uint32_t don_idx = logical_alloc_node(arena, L_DISTINCT_ON);
        struct logical_node *don = logical_node_get(arena, don_idx);
        don->child                   = current;
        don->distinct_on.key_start   = s->distinct_on_start;
        don->distinct_on.key_count   = s->distinct_on_count;
        current = don_idx;
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

    /* Leaf-like nodes: no children to recurse into. */
    if (n->op == L_SUBQUERY || n->op == L_WINDOW ||
        n->op == L_SET_OP   || n->op == L_GENERATE_SERIES) return idx;

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

/* ---- logical_explain ---- */

static const char *lcmp_op_str(enum cmp_op op)
{
    switch (op) {
    case CMP_EQ:  return "=";  case CMP_NE: return "!=";
    case CMP_LT:  return "<";  case CMP_GT: return ">";
    case CMP_LE:  return "<="; case CMP_GE: return ">=";
    case CMP_IS_NULL:     return "IS NULL";
    case CMP_IS_NOT_NULL: return "IS NOT NULL";
    case CMP_BETWEEN:     return "BETWEEN";
    case CMP_LIKE:        return "LIKE";
    case CMP_ILIKE:       return "ILIKE";
    case CMP_IN:          return "IN";
    case CMP_NOT_IN:      return "NOT IN";
    case CMP_IS_DISTINCT: return "IS DISTINCT FROM";
    case CMP_IS_NOT_DISTINCT: return "IS NOT DISTINCT FROM";
    case CMP_EXISTS:      return "EXISTS";
    case CMP_NOT_EXISTS:  return "NOT EXISTS";
    case CMP_REGEX_MATCH: return "~";
    case CMP_REGEX_NOT_MATCH: return "!~";
    case CMP_REGEX_ICASE_MATCH: return "~*";
    case CMP_REGEX_ICASE_NOT_MATCH: return "!~*";
    case CMP_IS_NOT_TRUE:  return "IS NOT TRUE";
    case CMP_IS_NOT_FALSE: return "IS NOT FALSE";
    case CMP_SIMILAR_TO:   return "SIMILAR TO";
    case CMP_NOT_SIMILAR_TO: return "NOT SIMILAR TO";
    }
    return "?";
}

/* Print a condition as a human-readable predicate into buf. Returns bytes written. */
static int lcond_to_str(struct query_arena *arena, uint32_t cond_idx,
                        char *buf, int buflen)
{
    if (cond_idx == IDX_NONE || cond_idx >= (uint32_t)arena->conditions.count)
        return snprintf(buf, buflen, "(?)");
    struct condition *c = &arena->conditions.items[cond_idx];
    switch (c->type) {
    case COND_AND: {
        int w = lcond_to_str(arena, c->left, buf, buflen);
        if (w < buflen - 5) w += snprintf(buf+w, buflen-w, " AND ");
        w += lcond_to_str(arena, c->right, buf+w, buflen-w);
        return w;
    }
    case COND_OR: {
        int w = snprintf(buf, buflen, "(");
        w += lcond_to_str(arena, c->left, buf+w, buflen-w);
        if (w < buflen - 5) w += snprintf(buf+w, buflen-w, " OR ");
        w += lcond_to_str(arena, c->right, buf+w, buflen-w);
        if (w < buflen - 2) w += snprintf(buf+w, buflen-w, ")");
        return w;
    }
    case COND_NOT: {
        int w = snprintf(buf, buflen, "NOT ");
        w += lcond_to_str(arena, c->left, buf+w, buflen-w);
        return w;
    }
    case COND_COMPARE: {
        const char *op = lcmp_op_str(c->op);
        if (c->rhs_column.len > 0)
            return snprintf(buf, buflen, SV_FMT " %s " SV_FMT,
                            (int)c->column.len, c->column.data,
                            op,
                            (int)c->rhs_column.len, c->rhs_column.data);
        if (c->op == CMP_IS_NULL || c->op == CMP_IS_NOT_NULL)
            return snprintf(buf, buflen, SV_FMT " %s",
                            (int)c->column.len, c->column.data, op);
        /* simple literal comparison */
        if (c->value.is_null)
            return snprintf(buf, buflen, SV_FMT " %s NULL",
                            (int)c->column.len, c->column.data, op);
        switch (c->value.type) {
        case COLUMN_TYPE_INT:
            return snprintf(buf, buflen, SV_FMT " %s %d",
                            (int)c->column.len, c->column.data, op, c->value.value.as_int);
        case COLUMN_TYPE_BIGINT:
            return snprintf(buf, buflen, SV_FMT " %s %lld",
                            (int)c->column.len, c->column.data, op,
                            (long long)c->value.value.as_bigint);
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:
            return snprintf(buf, buflen, SV_FMT " %s %.15g",
                            (int)c->column.len, c->column.data, op, c->value.value.as_float);
        case COLUMN_TYPE_BOOLEAN:
            return snprintf(buf, buflen, SV_FMT " %s %s",
                            (int)c->column.len, c->column.data, op,
                            c->value.value.as_bool ? "true" : "false");
        case COLUMN_TYPE_TEXT:
            return snprintf(buf, buflen, SV_FMT " %s '%s'",
                            (int)c->column.len, c->column.data, op,
                            c->value.value.as_text ? c->value.value.as_text : "");
        case COLUMN_TYPE_SMALLINT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID:
        case COLUMN_TYPE_VECTOR:
            return snprintf(buf, buflen, SV_FMT " %s ?",
                            (int)c->column.len, c->column.data, op);
        }
        return snprintf(buf, buflen, SV_FMT " %s ?",
                        (int)c->column.len, c->column.data, op);
    }
    case COND_MULTI_IN:
        return snprintf(buf, buflen, "(multi-col) IN (...)");
    }
    return snprintf(buf, buflen, "(?)");
}

static const char *ljoin_type_str(int jt)
{
    switch (jt) {
    case 0: return "INNER";
    case 1: return "LEFT";
    case 2: return "RIGHT";
    case 3: return "FULL";
    case 4: return "CROSS";
    default: return "?";
    }
}

static int logical_explain_node(struct query_arena *arena, uint32_t idx,
                                 char *buf, int buflen, int depth);

static int logical_explain_node(struct query_arena *arena, uint32_t idx,
                                 char *buf, int buflen, int depth)
{
    if (idx == IDX_NONE || buflen <= 2) return 0;
    struct logical_node *n = logical_node_get(arena, idx);
    if (!n) return 0;

    int written = 0, r;
    /* indent */
    for (int i = 0; i < depth * 2 && written < buflen - 1; i++)
        buf[written++] = ' ';

    switch (n->op) {
    case L_SCAN:
        if (n->scan.alias.len > 0)
            r = snprintf(buf+written, buflen-written, "Scan on " SV_FMT " [alias: " SV_FMT "]\n",
                         (int)n->scan.table.len, n->scan.table.data,
                         (int)n->scan.alias.len, n->scan.alias.data);
        else
            r = snprintf(buf+written, buflen-written, "Scan on " SV_FMT "\n",
                         (int)n->scan.table.len, n->scan.table.data);
        if (r > 0) written += r;
        break;

    case L_FILTER: {
        char pbuf[256] = "?";
        lcond_to_str(arena, n->filter.cond_idx, pbuf, sizeof(pbuf));
        r = snprintf(buf+written, buflen-written, "Filter: (%s)\n", pbuf);
        if (r > 0) written += r;
        r = logical_explain_node(arena, n->child, buf+written, buflen-written, depth+1);
        if (r > 0) written += r;
        break;
    }

    case L_PROJECT: {
        if (n->project.columns.len > 0 &&
            !(n->project.columns.len == 1 && n->project.columns.data[0] == '*'))
            r = snprintf(buf+written, buflen-written, "Project [" SV_FMT "]\n",
                         (int)n->project.columns.len, n->project.columns.data);
        else if (n->project.parsed_columns_count > 0)
            r = snprintf(buf+written, buflen-written, "Project [%u cols]\n",
                         n->project.parsed_columns_count);
        else if (n->project.aggregates_count > 0)
            r = snprintf(buf+written, buflen-written, "Project [%u aggs]\n",
                         n->project.aggregates_count);
        else
            r = snprintf(buf+written, buflen-written, "Project [*]\n");
        if (r > 0) written += r;
        r = logical_explain_node(arena, n->child, buf+written, buflen-written, depth+1);
        if (r > 0) written += r;
        break;
    }

    case L_JOIN: {
        const char *jtype = ljoin_type_str(n->join.join_type);
        /* Print each join_info entry */
        for (uint32_t j = 0; j < n->join.joins_count && written < buflen - 4; j++) {
            struct join_info *ji = &arena->joins.items[n->join.joins_start + j];
            /* re-indent after first */
            if (j > 0) {
                for (int i = 0; i < depth * 2 && written < buflen - 1; i++)
                    buf[written++] = ' ';
            }
            if (ji->join_left_col.len > 0 && ji->join_right_col.len > 0)
                r = snprintf(buf+written, buflen-written,
                             "Join %s on " SV_FMT " = " SV_FMT " [" SV_FMT "]\n",
                             jtype,
                             (int)ji->join_left_col.len, ji->join_left_col.data,
                             (int)ji->join_right_col.len, ji->join_right_col.data,
                             (int)ji->join_table.len, ji->join_table.data);
            else
                r = snprintf(buf+written, buflen-written, "Join %s [" SV_FMT "]\n",
                             jtype,
                             (int)ji->join_table.len, ji->join_table.data);
            if (r > 0) written += r;
        }
        r = logical_explain_node(arena, n->child, buf+written, buflen-written, depth+1);
        if (r > 0) written += r;
        break;
    }

    case L_AGGREGATE: {
        if (n->aggregate.group_by_count > 0) {
            int w = snprintf(buf+written, buflen-written, "Aggregate GROUP BY [");
            if (w > 0) written += w;
            for (uint32_t k = 0; k < n->aggregate.group_by_count && written < buflen-4; k++) {
                sv col = arena->svs.items[n->aggregate.group_by_start + k];
                if (k > 0) { r = snprintf(buf+written, buflen-written, ", "); if (r>0) written+=r; }
                r = snprintf(buf+written, buflen-written, SV_FMT, (int)col.len, col.data);
                if (r > 0) written += r;
            }
            r = snprintf(buf+written, buflen-written, "]\n");
            if (r > 0) written += r;
        } else {
            r = snprintf(buf+written, buflen-written, "Aggregate (DISTINCT)\n");
            if (r > 0) written += r;
        }
        r = logical_explain_node(arena, n->child, buf+written, buflen-written, depth+1);
        if (r > 0) written += r;
        break;
    }

    case L_SORT: {
        int w = snprintf(buf+written, buflen-written, "Sort [");
        if (w > 0) written += w;
        for (uint32_t k = 0; k < n->sort.order_by_count && written < buflen-4; k++) {
            struct order_by_item *ob = &arena->order_items.items[n->sort.order_by_start + k];
            if (k > 0) { r = snprintf(buf+written, buflen-written, ", "); if (r>0) written+=r; }
            r = snprintf(buf+written, buflen-written, SV_FMT "%s",
                         (int)ob->column.len, ob->column.data,
                         ob->desc ? " DESC" : "");
            if (r > 0) written += r;
        }
        r = snprintf(buf+written, buflen-written, "]\n");
        if (r > 0) written += r;
        r = logical_explain_node(arena, n->child, buf+written, buflen-written, depth+1);
        if (r > 0) written += r;
        break;
    }

    case L_LIMIT: {
        if (n->limit.has_limit && n->limit.has_offset)
            r = snprintf(buf+written, buflen-written, "Limit (%d offset %d)\n",
                         n->limit.limit_count, n->limit.offset_count);
        else if (n->limit.has_limit)
            r = snprintf(buf+written, buflen-written, "Limit (%d)\n", n->limit.limit_count);
        else
            r = snprintf(buf+written, buflen-written, "Offset (%d)\n", n->limit.offset_count);
        if (r > 0) written += r;
        r = logical_explain_node(arena, n->child, buf+written, buflen-written, depth+1);
        if (r > 0) written += r;
        break;
    }

    case L_SUBQUERY:
        r = snprintf(buf+written, buflen-written, "Subquery [alias: " SV_FMT "]\n",
                     (int)n->subquery.alias.len, n->subquery.alias.data);
        if (r > 0) written += r;
        break;

    case L_WINDOW:
        r = snprintf(buf+written, buflen-written, "Window\n");
        if (r > 0) written += r;
        break;

    case L_SET_OP: {
        const char *opname = "Union";
        if (n->set_op.set_op == 1) opname = "Intersect";
        else if (n->set_op.set_op == 2) opname = "Except";
        r = snprintf(buf+written, buflen-written, "SetOp %s%s\n",
                     opname, n->set_op.set_all ? " ALL" : "");
        if (r > 0) written += r;
        break;
    }

    case L_GENERATE_SERIES:
        r = snprintf(buf+written, buflen-written, "GenerateSeries\n");
        if (r > 0) written += r;
        break;

    case L_DISTINCT_ON: {
        int w = snprintf(buf+written, buflen-written, "DistinctOn [");
        if (w > 0) written += w;
        for (uint32_t k = 0; k < n->distinct_on.key_count && written < buflen-4; k++) {
            sv col = arena->svs.items[n->distinct_on.key_start + k];
            if (k > 0) { r = snprintf(buf+written, buflen-written, ", "); if (r>0) written+=r; }
            r = snprintf(buf+written, buflen-written, SV_FMT, (int)col.len, col.data);
            if (r > 0) written += r;
        }
        r = snprintf(buf+written, buflen-written, "]\n");
        if (r > 0) written += r;
        r = logical_explain_node(arena, n->child, buf+written, buflen-written, depth+1);
        if (r > 0) written += r;
        break;
    }
    }
    return written;
}

int logical_explain(struct query_arena *arena, uint32_t root,
                    char *buf, int buflen)
{
    int written = logical_explain_node(arena, root, buf, buflen, 0);
    /* strip trailing newline */
    if (written > 0 && buf[written - 1] == '\n')
        buf[--written] = '\0';
    else if (written < buflen)
        buf[written] = '\0';
    return written;
}
