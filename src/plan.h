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
    PLAN_HASH_SEMI_JOIN, /* hash semi-join for IN (SELECT ...) */
    PLAN_GENERATE_SERIES, /* virtual table: generate_series(start, stop, step) */
    PLAN_EXPR_PROJECT,   /* expression evaluation: UPPER(x), ABS(y), etc. */
    PLAN_PARQUET_SCAN,   /* read Parquet file directly into col_blocks */
    PLAN_VEC_PROJECT,    /* vectorized columnar expression eval (no per-row overhead) */
    PLAN_TOP_N,          /* fused SORT + LIMIT: heap-based top-N selection */
};

/* ---- Plan builder result ---- */

enum plan_status {
    PLAN_OK,       /* plan built successfully */
    PLAN_NOTIMPL,  /* feature not yet implemented — fall back to legacy */
    PLAN_ERROR     /* real error (bad column, type mismatch, etc.) */
};

struct plan_result {
    uint32_t         node;   /* valid only when status == PLAN_OK */
    enum plan_status status;
};

#define PLAN_RES_OK(n)   ((struct plan_result){ .node = (n), .status = PLAN_OK })
#define PLAN_RES_NOTIMPL ((struct plan_result){ .node = IDX_NONE, .status = PLAN_NOTIMPL })
#define PLAN_RES_ERR     ((struct plan_result){ .node = IDX_NONE, .status = PLAN_ERROR })

/* Vectorized projection operation — one per output column */
enum vec_op_kind {
    VEC_PASSTHROUGH,   /* copy input column directly */
    VEC_COL_OP_LIT,    /* col OP literal */
    VEC_COL_OP_COL,    /* col OP col */
    VEC_FUNC_UPPER,    /* UPPER(col) → TEXT */
    VEC_FUNC_LOWER,    /* LOWER(col) → TEXT */
    VEC_FUNC_LENGTH,   /* LENGTH(col) → INT */
    VEC_FUNC_ABS_I32,  /* ABS(col) → INT */
    VEC_FUNC_ABS_I64,  /* ABS(col) → BIGINT */
    VEC_FUNC_ABS_F64,  /* ABS(col) → FLOAT/NUMERIC */
    VEC_FUNC_ROUND,    /* ROUND(col, N) → NUMERIC */
    VEC_FUNC_CAST_INT_TO_F64, /* col::numeric / col::float → NUMERIC */
};

struct vec_project_op {
    enum vec_op_kind kind;
    uint16_t left_col;         /* input column index */
    uint16_t right_col;        /* for VEC_COL_OP_COL */
    int      op;               /* enum expr_op (OP_ADD, OP_SUB, etc.) */
    int64_t  lit_i64;          /* literal value for integer ops */
    double   lit_f64;          /* literal value for float ops */
    enum column_type out_type; /* output column type */
    int      func_precision;   /* decimal places for VEC_FUNC_ROUND */
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
            uint32_t     cond_idx;   /* condition index in arena (single-col compat) */
            uint32_t     cond_indices[MAX_INDEX_COLS]; /* one cond_idx per index column */
            int          nkeys;      /* number of index columns matched */
            uint16_t     ncols;
            int         *col_map;
        } index_scan;
        struct {
            uint32_t cond_idx;       /* condition index in arena for predicate */
            int      col_idx;        /* column index for simple comparisons (-1 = complex) */
            int      cmp_op;         /* CMP_EQ, CMP_GT, etc. for simple path */
            struct cell cmp_val;     /* comparison value for simple path */
            /* Extended filter fields */
            struct cell between_high;  /* high bound for CMP_BETWEEN */
            struct cell *in_values;    /* bump-allocated array for CMP_IN literal list */
            uint32_t    in_count;      /* number of values in in_values */
            const char *like_pattern;  /* bump-allocated pattern for CMP_LIKE/CMP_ILIKE */
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
            int     *sort_nulls_first; /* bump-allocated: -1=default, 0=NULLS LAST, 1=NULLS FIRST */
            uint16_t nsort_cols;
        } sort;
        struct {
            int      *group_cols;    /* bump-allocated: column indices for GROUP BY */
            uint16_t  ngroup_cols;
            uint32_t  agg_start;     /* index into arena->aggregates */
            uint32_t  agg_count;
            int       agg_before_cols; /* layout flag */
            int      *agg_col_indices; /* bump-allocated: pre-resolved column index per aggregate (-1=COUNT(*), -2=expr, -3=vec binop) */
            int      *agg_vec_col_a;  /* bump: left col index for vectorized binop aggs */
            int      *agg_vec_col_b;  /* bump: right col index (or -1 if literal) */
            int      *agg_vec_op;     /* bump: OP_ADD/SUB/MUL/DIV for vectorized binop aggs */
            double   *agg_vec_lit;    /* bump: literal value when col_b == -1 */
            struct table *table;     /* source table — needed for eval_expr on expression aggregates */
        } hash_agg;
        struct {
            uint32_t agg_start;
            uint32_t agg_count;
            int      *agg_col_indices; /* pre-resolved: -1=COUNT(*), -2=expr, -3=vec binop */
            int      *agg_vec_col_a;  /* bump: left col index for vectorized binop aggs */
            int      *agg_vec_col_b;  /* bump: right col index (or -1 if literal) */
            int      *agg_vec_op;     /* bump: OP_ADD/SUB/MUL/DIV */
            double   *agg_vec_lit;    /* bump: literal value when col_b == -1 */
            struct table *table;       /* for eval_expr on expression aggs */
        } simple_agg;
        struct {
            size_t offset;
            size_t limit;
            int    has_offset;
            int    has_limit;
        } limit;
        struct {
            int inner_key_col;       /* join key column index in inner (right child) */
            int outer_key_col;       /* join key column index in outer (left child) */
        } hash_semi_join;
        struct {
            int      set_op;         /* 0=UNION, 1=INTERSECT, 2=EXCEPT */
            int      set_all;        /* 1 for UNION ALL etc. */
            uint16_t ncols;          /* number of output columns */
        } set_op;
        struct {
            long long start;
            long long stop;
            long long step;
            int       use_bigint;    /* 1 = BIGINT, 0 = INT */
        } gen_series;
        struct {
            uint16_t ncols;           /* number of output columns */
            uint32_t *expr_indices;   /* bump-allocated: arena expr index per output col */
            struct table *table;      /* source table (for eval_expr column lookups) */
        } expr_project;
        struct {
            struct table *table;      /* foreign table (has parquet_path) */
            uint16_t     ncols;       /* number of columns to read */
            int         *col_map;     /* bump-allocated: col_map[i] = parquet column index */
        } parquet_scan;
        struct {
            uint16_t ncols;           /* number of output columns */
            struct vec_project_op *ops; /* bump-allocated: [ncols] */
        } vec_project;
        struct {
            int     *sort_cols;        /* bump: column indices to sort by */
            int     *sort_descs;       /* bump: 1=DESC, 0=ASC per column */
            int     *sort_nulls_first; /* bump: -1=default, 0=NULLS LAST, 1=NULLS FIRST */
            uint16_t nsort_cols;
            size_t   limit;            /* N = number of rows to keep */
            size_t   offset;           /* skip first M rows from sorted result */
        } top_n;
        struct {
            uint16_t out_ncols;       /* total output columns (passthrough + window) */
            uint16_t n_pass;          /* number of passthrough columns */
            int     *pass_cols;       /* bump: table column indices for passthrough */
            uint16_t n_win;           /* number of window expressions */
            int     *win_part_col;    /* bump: partition column index per win expr (-1 = none) */
            int     *win_ord_col;     /* bump: order column index per win expr (-1 = none) */
            int     *win_ord_desc;    /* bump: 1=DESC per win expr */
            int     *win_arg_col;     /* bump: arg column index per win expr (-1 = none) */
            int     *win_func;        /* bump: enum win_func per win expr */
            int     *win_offset;      /* bump: offset/ntile per win expr */
            int     *win_has_default; /* bump: 1 if LAG/LEAD has default value */
            double  *win_default_dbl; /* bump: default value as double */
            int     *win_has_frame;   /* bump: has_frame per win expr */
            int     *win_frame_start; /* bump: enum frame_bound */
            int     *win_frame_end;   /* bump: enum frame_bound */
            int     *win_frame_start_n;
            int     *win_frame_end_n;
            int      sort_part_col;   /* global partition col for sort (-1 = none) */
            int      sort_ord_col;    /* global order col for sort (-1 = none) */
            int      sort_ord_desc;   /* global order direction */
        } window;
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

struct parquet_scan_state {
    void *reader;         /* carquet_reader_t* — opaque to avoid header dep */
    void *batch_reader;   /* carquet_batch_reader_t* */
    int   done;
    /* cache-read state */
    size_t cache_cursor;  /* next row in pq_cache */
    int    using_cache;   /* 1 = serving from pq_cache */
};

/* Flat column storage for hash join build side — no BLOCK_CAPACITY limit.
 * Data arrays are bump-allocated and can grow via realloc-style copy. */
struct flat_col {
    enum column_type type;
    uint8_t         *nulls;   /* bump: [cap] */
    void            *data;    /* bump: int32_t[cap] / int64_t[cap] / double[cap] / char*[cap] */
};

struct hash_join_state {
    struct block_hash_table ht;
    /* build-side rows stored as flat columns (no BLOCK_CAPACITY limit) */
    struct flat_col  *build_cols;
    uint16_t          build_ncols;
    uint32_t          build_count;   /* total rows in build side */
    uint32_t          build_cap;     /* capacity of flat col arrays */
    /* probe state */
    int               build_done;
    size_t            probe_cursor;  /* for nested-loop fallback */
    /* outer join state */
    uint8_t          *matched;       /* bitmap: matched[i]=1 if inner row i was matched (RIGHT/FULL) */
    int               outer_done;    /* 1 when probe phase is complete */
    uint32_t          right_emit_cursor; /* cursor for emitting unmatched inner rows */
};

/* Open-addressing hash set for COUNT(DISTINCT) */
struct distinct_set {
    uint64_t *slots;   /* bump-allocated hash slots (0 = empty) */
    uint32_t  cap;     /* must be power of 2 */
    uint32_t  count;   /* number of distinct values inserted */
};

struct simple_agg_state {
    double   *sums;        /* bump: [agg_count] — FLOAT/NUMERIC sums */
    int64_t  *i64_sums;    /* bump: [agg_count] — INT/SMALLINT/BIGINT sums */
    double   *mins;
    double   *maxs;
    const char **text_mins; /* bump: [agg_count] for TEXT MIN/MAX */
    const char **text_maxs;
    size_t   *nonnull;     /* non-null count per agg */
    size_t    total_rows;
    int      *minmax_init;
    struct distinct_set *distinct_sets; /* bump: [agg_count], only used for DISTINCT aggs */
    int       input_done;
    int       emit_done;
    struct row tmp_row;
    int       tmp_row_inited;
};

struct hash_agg_state {
    struct block_hash_table ht;
    /* per-group accumulators stored in parallel arrays */
    double   *sums;        /* bump: [agg_count * ngroups] — FLOAT/NUMERIC sums */
    int64_t  *i64_sums;    /* bump: [agg_count * ngroups] — INT/SMALLINT/BIGINT sums */
    double   *mins;
    double   *maxs;
    const char **text_mins; /* bump: [agg_count * ngroups] for TEXT MIN/MAX */
    const char **text_maxs;
    size_t   *nonnull;     /* non-null count per agg per group */
    size_t   *grp_counts;  /* row count per group */
    int      *minmax_init;
    struct distinct_set *distinct_sets; /* bump: [agg_count * group_cap], only for DISTINCT aggs */
    /* STRING_AGG / ARRAY_AGG accumulators */
    char   **str_accum;    /* bump: [agg_count * group_cap] concatenated strings */
    size_t  *str_accum_len; /* bump: [agg_count * group_cap] current length */
    size_t  *str_accum_cap; /* bump: [agg_count * group_cap] allocated capacity */
    /* group key values stored in col_blocks */
    struct col_block *group_keys;
    uint32_t  ngroups;
    uint32_t  group_cap;
    int       input_done;
    uint32_t  emit_cursor;
    /* temp row for expression-based aggregates */
    struct row tmp_row;
    int       tmp_row_inited;
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

struct window_state {
    int       input_done;
    uint32_t  emit_cursor;
    uint32_t  total_rows;
    /* flat columnar arrays for ALL input columns */
    void    **flat_data;        /* bump: [ncols] */
    uint8_t **flat_nulls;       /* bump: [ncols] */
    enum column_type *flat_types; /* bump: [ncols] */
    uint16_t  input_ncols;
    /* sorted index + partition boundaries */
    uint32_t *sorted;           /* bump: [total_rows] */
    uint32_t *part_starts;      /* bump: [nparts+1] */
    uint32_t  nparts;
    /* pre-computed window result columns */
    int32_t  *win_i32;          /* bump: [n_win * total_rows] */
    int64_t  *win_i64;          /* bump: [n_win * total_rows] — integer SUM results */
    double   *win_f64;          /* bump: [n_win * total_rows] */
    uint8_t  *win_null;         /* bump: [n_win * total_rows] */
    int      *win_is_dbl;       /* bump: [n_win] — 1 if result is double */
    int      *win_is_i64;       /* bump: [n_win] — 1 if result is int64 */
    char    **win_str;          /* bump: [n_win * total_rows] — text results */
    int      *win_is_str;       /* bump: [n_win] — 1 if result is text */
};

struct hash_semi_join_state {
    struct block_hash_table ht;
    /* build-side key values — flat bump-allocated arrays (no BLOCK_CAPACITY limit) */
    enum column_type       key_type;
    void                  *key_data;     /* bump: int32_t[] / int64_t[] / double[] / char*[] */
    uint8_t               *key_nulls;    /* bump: [build_count] */
    uint32_t               build_count;
    uint32_t               build_cap;
    int                    build_done;
};

struct set_op_state {
    struct block_hash_table ht;
    /* flat columnar arrays for collected unique rows */
    void    **col_data;        /* bump: [ncols] typed arrays */
    uint8_t **col_nulls;       /* bump: [ncols] null bitmaps */
    enum column_type *col_types; /* bump: [ncols] */
    uint32_t  row_count;       /* total rows collected */
    uint32_t  row_cap;         /* capacity of flat arrays */
    uint16_t  ncols;
    int       phase;           /* 0=collect-left, 1=collect-right, 2=emit */
    uint32_t  emit_cursor;
    /* for INTERSECT: per-row flag indicating if matched by RHS */
    uint8_t  *matched;         /* bump: [row_cap] */
};

struct limit_state {
    size_t emitted;
    size_t skipped;
};

struct top_n_state {
    int       input_done;
    uint32_t  emit_cursor;
    /* flat columnar arrays for all collected rows */
    void    **flat_data;       /* bump: [ncols] typed arrays */
    uint8_t **flat_nulls;      /* bump: [ncols] null bitmaps */
    enum column_type *flat_types; /* bump: [ncols] */
    uint16_t  ncols;
    uint32_t  heap_size;       /* current entries in heap */
    uint32_t  heap_cap;        /* = limit + offset */
    uint32_t *heap;            /* bump: indices into flat arrays, heap-ordered */
    uint32_t  total_rows;      /* total rows appended to flat arrays */
    uint32_t  flat_cap;        /* capacity of flat arrays */
    uint32_t *sorted;          /* bump: final sorted indices for emit */
    uint32_t  sorted_count;    /* number of entries to emit (after offset) */
};

struct gen_series_state {
    long long current;   /* next value to emit */
    int       done;      /* 1 = exhausted */
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
 * Returns a plan_result: PLAN_OK with a valid node index on success,
 * PLAN_NOTIMPL for unimplemented features (fall back to legacy),
 * or PLAN_ERROR for real errors (bad column, type mismatch, etc.). */
struct plan_result plan_build_select(struct table *t, struct query_select *s,
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

/* Get output column count for a plan node. */
uint16_t plan_node_ncols(struct query_arena *arena, uint32_t node_idx);

/* Generate EXPLAIN text for a plan tree. Writes into buf, returns bytes written. */
int plan_explain(struct query_arena *arena, uint32_t node_idx, char *buf, int buflen);

/* Patch a single row in the scan cache after UPDATE (avoids full rebuild). */
void scan_cache_update_row(struct table *t, size_t row_idx);

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
