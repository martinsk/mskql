#ifndef PLAN_H
#define PLAN_H

#include "arena.h"
#include "block.h"
#include "table.h"
#include "database.h"

/* ---- Plan node types ---- */

enum plan_op {
    PLAN_SEQ_SCAN,       /* full table scan → row_block */
    PLAN_INDEX_SCAN,     /* B-tree lookup/range → row_block */
    PLAN_FILTER,         /* apply predicate, produce selection vector */
    PLAN_PROJECT,        /* column selection/expression evaluation */
    PLAN_HASH_JOIN,      /* build hash table on inner, probe with outer */
    PLAN_NESTED_LOOP,    /* fallback for small tables / complex ON */
    PLAN_SORT,           /* block-aware merge sort */
    PLAN_HASH_AGG,       /* hash-based GROUP BY + aggregates */
    PLAN_SIMPLE_AGG,     /* aggregates without GROUP BY */
    PLAN_LIMIT,          /* LIMIT/OFFSET — early termination */
    PLAN_DISTINCT,       /* hash-based dedup */
    PLAN_SET_OP,         /* UNION/INTERSECT/EXCEPT via hashing */
    PLAN_WINDOW,         /* window functions */
};

/* Plan node: arena-allocated in query_arena.plan_nodes DA.
 * Children referenced by uint32_t index (IDX_NONE = no child). */
struct plan_node {
    enum plan_op op;
    uint32_t left;       /* child node index, or IDX_NONE */
    uint32_t right;      /* second child (joins, set ops), or IDX_NONE */
    double est_rows;
    double est_cost;
    union {
        struct {
            struct table *table;
            uint16_t     ncols;      /* number of columns to scan */
            int         *col_map;    /* bump-allocated: col_map[i] = table column index for output col i */
        } seq_scan;
        struct {
            struct table *table;
            struct index *idx;
            uint32_t     cond_idx;   /* condition index in arena */
            uint16_t     ncols;
            int         *col_map;
        } index_scan;
        struct {
            uint32_t cond_idx;       /* condition index in arena for predicate */
            int      col_idx;        /* column index for simple comparisons (-1 = complex) */
            int      cmp_op;         /* CMP_EQ, CMP_GT, etc. for simple path */
            struct cell cmp_val;     /* comparison value for simple path */
        } filter;
        struct {
            uint16_t ncols;          /* number of output columns */
            int     *col_map;        /* bump-allocated: which input columns to keep */
        } project;
        struct {
            int inner_key_col;       /* join key column index in inner (right child) */
            int outer_key_col;       /* join key column index in outer (left child) */
            int join_type;           /* 0=INNER, 1=LEFT, 2=RIGHT, 3=FULL */
        } hash_join;
        struct {
            uint32_t cond_idx;       /* join condition */
            int      join_type;
        } nested_loop;
        struct {
            int     *sort_cols;      /* bump-allocated: column indices to sort by */
            int     *sort_descs;     /* bump-allocated: 1=DESC, 0=ASC per column */
            uint16_t nsort_cols;
        } sort;
        struct {
            int      *group_cols;    /* bump-allocated: column indices for GROUP BY */
            uint16_t  ngroup_cols;
            uint32_t  agg_start;     /* index into arena->aggregates */
            uint32_t  agg_count;
            int       agg_before_cols; /* layout flag */
        } hash_agg;
        struct {
            uint32_t agg_start;
            uint32_t agg_count;
        } simple_agg;
        struct {
            size_t offset;
            size_t limit;
            int    has_offset;
            int    has_limit;
        } limit;
    };
};

/* ---- Per-node execution state ---- */

struct scan_state {
    size_t cursor;   /* next row index in table */
};

struct filter_state {
    /* stateless — operates on input block */
    int dummy;
};

struct hash_join_state {
    struct block_hash_table ht;
    /* build-side rows stored as col_blocks */
    struct col_block *build_cols;
    uint16_t          build_ncols;
    uint32_t          build_count;   /* total rows in build side */
    uint32_t          build_cap;     /* capacity of build col_blocks */
    /* probe state */
    int               build_done;
    size_t            probe_cursor;  /* for nested-loop fallback */
};

struct hash_agg_state {
    struct block_hash_table ht;
    /* per-group accumulators stored in parallel arrays */
    double   *sums;        /* bump-allocated: [agg_count * ngroups] */
    double   *mins;
    double   *maxs;
    size_t   *nonnull;     /* non-null count per agg per group */
    size_t   *grp_counts;  /* row count per group */
    int      *minmax_init;
    /* group key values stored in col_blocks */
    struct col_block *group_keys;
    uint32_t  ngroups;
    uint32_t  group_cap;
    int       input_done;
    uint32_t  emit_cursor;
};

struct sort_state {
    struct row_block *collected; /* bump-allocated array of blocks */
    uint32_t nblocks;
    uint32_t block_cap;
    int      input_done;
    uint32_t emit_cursor;       /* current block being emitted */
    uint32_t emit_row;          /* current row within block */
    /* merged result */
    uint32_t *sorted_indices;   /* bump-allocated */
    uint32_t  sorted_count;
};

struct limit_state {
    size_t emitted;
    size_t skipped;
};

/* Execution context: holds arena, database, and per-node state. */
struct plan_exec_ctx {
    struct query_arena *arena;
    struct database    *db;
    void              **node_states;  /* bump-allocated array of per-node state pointers */
    uint32_t            nnodes;
};

/* ---- Public API ---- */

/* Allocate a plan node in the arena, return its index. */
uint32_t plan_alloc_node(struct query_arena *arena, enum plan_op op);

/* Try to build a block-oriented plan for a SELECT query.
 * Returns the root plan node index, or IDX_NONE if the query
 * cannot be handled by the planner (falls back to legacy path). */
uint32_t plan_build_select(struct table *t, struct query_select *s,
                           struct query_arena *arena, struct database *db);

/* Initialize execution context for a plan tree. */
void plan_exec_init(struct plan_exec_ctx *ctx, struct query_arena *arena,
                    struct database *db, uint32_t root_node);

/* Pull the next block from a plan node. Returns 0 on success, -1 on end-of-data. */
int plan_next_block(struct plan_exec_ctx *ctx, uint32_t node_idx,
                    struct row_block *out);

/* Execute a full plan tree, collecting all results into struct rows.
 * This is the main entry point for the plan executor. */
int plan_exec_to_rows(struct plan_exec_ctx *ctx, uint32_t root_node,
                      struct rows *result, struct bump_alloc *rb);

/* ---- Block utility functions ---- */

/* Initialize a row_block with ncols columns, bump-allocated from scratch. */
void row_block_alloc(struct row_block *rb, uint16_t ncols,
                     struct bump_alloc *scratch);

/* Scan up to BLOCK_CAPACITY rows from a table starting at cursor.
 * Returns number of rows scanned. Updates *cursor. */
uint16_t scan_table_block(struct table *t, size_t *cursor,
                          struct row_block *out, int *col_map, uint16_t ncols,
                          struct bump_alloc *scratch);

/* Convert a row_block back to struct rows (for final output). */
void block_to_rows(const struct row_block *blk, struct rows *result, struct bump_alloc *rb);

#endif
