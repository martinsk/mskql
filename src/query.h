#ifndef QUERY_H
#define QUERY_H

#include <stdint.h>
#include "row.h"
#include "table.h"
#include "stringview.h"
#include "arena.h"

struct database;

enum agg_func {
    AGG_NONE,
    AGG_SUM,
    AGG_COUNT,
    AGG_AVG,
    AGG_MIN,
    AGG_MAX
};

struct agg_expr {
    enum agg_func func;
    sv column;
    sv alias; /* optional AS alias name */
    int has_distinct; /* COUNT(DISTINCT col) */
};

enum win_func {
    WIN_ROW_NUMBER,
    WIN_RANK,
    WIN_DENSE_RANK,
    WIN_NTILE,
    WIN_PERCENT_RANK,
    WIN_CUME_DIST,
    WIN_LAG,
    WIN_LEAD,
    WIN_FIRST_VALUE,
    WIN_LAST_VALUE,
    WIN_NTH_VALUE,
    WIN_SUM,
    WIN_COUNT,
    WIN_AVG
};

/* window frame boundary types */
enum frame_bound {
    FRAME_UNBOUNDED_PRECEDING,
    FRAME_N_PRECEDING,
    FRAME_CURRENT_ROW,
    FRAME_N_FOLLOWING,
    FRAME_UNBOUNDED_FOLLOWING
};

struct win_expr {
    enum win_func func;
    sv arg_column;       /* column arg for SUM/COUNT/AVG/LAG/LEAD/etc, empty for ROW_NUMBER/RANK */
    int has_partition;
    sv partition_col;
    int has_order;
    sv order_col;
    int order_desc; /* 1 = DESC, 0 = ASC (default) */
    int offset;     /* for LAG/LEAD (default 1), NTH_VALUE (n), NTILE (buckets) */
    /* window frame */
    int has_frame;
    enum frame_bound frame_start;
    enum frame_bound frame_end;
    int frame_start_n; /* for N PRECEDING/FOLLOWING */
    int frame_end_n;
};

enum select_expr_kind {
    SEL_COLUMN,
    SEL_WINDOW
};

struct select_expr {
    enum select_expr_kind kind;
    sv column;           /* for SEL_COLUMN */
    struct win_expr win; /* for SEL_WINDOW */
    sv alias;            /* optional AS alias */
};

/* A parsed SELECT column: expression + optional alias. */
struct select_column {
    uint32_t expr_idx;   /* index into arena.exprs, or IDX_NONE */
    sv alias;            /* optional AS alias (empty if none) */
};

enum cmp_op {
    CMP_EQ,
    CMP_NE,
    CMP_LT,
    CMP_GT,
    CMP_LE,
    CMP_GE,
    CMP_IS_NULL,
    CMP_IS_NOT_NULL,
    CMP_IN,
    CMP_NOT_IN,
    CMP_BETWEEN,
    CMP_LIKE,
    CMP_ILIKE,
    CMP_IS_DISTINCT,
    CMP_IS_NOT_DISTINCT,
    CMP_EXISTS,
    CMP_NOT_EXISTS
};

enum cond_type {
    COND_COMPARE,
    COND_AND,
    COND_OR,
    COND_NOT,
    COND_MULTI_IN
};

struct condition {
    enum cond_type type;
    /* for COND_COMPARE */
    sv column;
    enum cmp_op op;
    struct cell value;
    sv rhs_column;               /* if set, RHS is a column ref (JOIN ON) */
    uint32_t lhs_expr;           /* index into arena.exprs, or IDX_NONE */
    /* for CMP_IN / CMP_NOT_IN: range in arena.cells */
    uint32_t in_values_start;    /* index into arena.cells */
    uint32_t in_values_count;
    /* for CMP_IN subquery: index into arena.strings, or IDX_NONE */
    uint32_t subquery_sql;
    /* for scalar subquery comparison: index into arena.strings, or IDX_NONE */
    uint32_t scalar_subquery_sql;
    /* for CMP_BETWEEN: low (in value) and high */
    struct cell between_high;
    /* for COND_AND / COND_OR / COND_NOT */
    uint32_t left;               /* index into arena.conditions, or IDX_NONE */
    uint32_t right;              /* index into arena.conditions, or IDX_NONE */
    /* for COND_MULTI_IN: WHERE (a, b) IN ((1,2), (3,4)) */
    uint32_t multi_columns_start; /* index into arena.svs */
    uint32_t multi_columns_count;
    int multi_tuple_width;
    uint32_t multi_values_start;  /* index into arena.cells */
    uint32_t multi_values_count;
    /* for ANY/ALL/SOME: col op ANY(ARRAY[...]) */
    int is_any;
    int is_all;
    uint32_t array_values_start;  /* index into arena.cells */
    uint32_t array_values_count;
};

int eval_condition(uint32_t cond_idx, struct query_arena *arena,
                   struct row *row, struct table *t,
                   struct database *db);

/* ---------------------------------------------------------------------------
 * Expression AST — tagged union for all SQL expressions.
 * All child references are uint32_t indices into the query arena pools.
 * ------------------------------------------------------------------------- */

enum expr_type {
    EXPR_LITERAL,       /* integer, float, string, boolean, or NULL */
    EXPR_COLUMN_REF,    /* column reference: optional table.column */
    EXPR_BINARY_OP,     /* a op b  (arithmetic, concat) */
    EXPR_UNARY_OP,      /* -a */
    EXPR_FUNC_CALL,     /* UPPER(x), COALESCE(a,b), etc. */
    EXPR_CASE_WHEN,     /* CASE WHEN ... THEN ... ELSE ... END */
    EXPR_SUBQUERY,      /* (SELECT ...) */
    EXPR_CAST,          /* CAST(expr AS type) or expr::type */
    EXPR_IS_NULL        /* expr IS NULL / expr IS NOT NULL */
};

enum expr_op {
    OP_ADD,             /* + */
    OP_SUB,             /* - */
    OP_MUL,             /* * */
    OP_DIV,             /* / */
    OP_MOD,             /* % */
    OP_CONCAT,          /* || */
    OP_NEG              /* unary minus */
};

enum expr_func {
    FUNC_COALESCE,
    FUNC_NULLIF,
    FUNC_GREATEST,
    FUNC_LEAST,
    FUNC_UPPER,
    FUNC_LOWER,
    FUNC_LENGTH,
    FUNC_TRIM,
    FUNC_SUBSTRING,
    FUNC_NEXTVAL,
    FUNC_CURRVAL,
    FUNC_GEN_RANDOM_UUID,
    FUNC_NOW,
    FUNC_CURRENT_TIMESTAMP,
    FUNC_CURRENT_DATE,
    FUNC_EXTRACT,
    FUNC_DATE_TRUNC,
    FUNC_DATE_PART,
    FUNC_AGE,
    FUNC_TO_CHAR,
    /* math functions */
    FUNC_ABS,
    FUNC_CEIL,
    FUNC_FLOOR,
    FUNC_ROUND,
    FUNC_POWER,
    FUNC_SQRT,
    FUNC_MOD,
    FUNC_SIGN,
    FUNC_RANDOM,
    /* string functions */
    FUNC_REPLACE,
    FUNC_LPAD,
    FUNC_RPAD,
    FUNC_CONCAT,
    FUNC_CONCAT_WS,
    FUNC_POSITION,
    FUNC_SPLIT_PART,
    FUNC_LEFT,
    FUNC_RIGHT,
    FUNC_REPEAT,
    FUNC_REVERSE,
    FUNC_INITCAP
};

struct case_when_branch {
    uint32_t cond_idx;       /* index into arena.conditions */
    uint32_t then_expr_idx;  /* index into arena.exprs */
};

struct expr {
    enum expr_type type;
    union {
        /* EXPR_LITERAL */
        struct cell literal;

        /* EXPR_COLUMN_REF */
        struct {
            sv table;       /* empty if unqualified */
            sv column;
        } column_ref;

        /* EXPR_BINARY_OP */
        struct {
            enum expr_op op;
            uint32_t left;   /* index into arena.exprs */
            uint32_t right;  /* index into arena.exprs */
        } binary;

        /* EXPR_UNARY_OP */
        struct {
            enum expr_op op;
            uint32_t operand; /* index into arena.exprs */
        } unary;

        /* EXPR_FUNC_CALL */
        struct {
            enum expr_func func;
            uint32_t args_start; /* index into arena.arg_indices (consecutive) */
            uint32_t args_count;
        } func_call;

        /* EXPR_CASE_WHEN */
        struct {
            uint32_t branches_start; /* index into arena.branches (consecutive) */
            uint32_t branches_count;
            uint32_t else_expr;      /* index into arena.exprs, or IDX_NONE */
        } case_when;

        /* EXPR_SUBQUERY */
        struct {
            uint32_t sql_idx; /* index into arena.strings */
        } subquery;

        /* EXPR_IS_NULL */
        struct {
            uint32_t operand_is;       /* index into arena.exprs */
            int negate;                /* 1 for IS NOT NULL */
        } is_null;

        /* EXPR_CAST */
        struct {
            uint32_t operand;          /* index into arena.exprs */
            enum column_type target;   /* target type */
        } cast;
    };

    sv alias; /* optional AS alias */
};

struct cell eval_expr(uint32_t expr_idx, struct query_arena *arena,
                      struct table *t, struct row *row,
                      struct database *db, struct bump_alloc *rb);

struct set_clause {
    sv column;
    struct cell value;     /* literal value (used when expr_idx is IDX_NONE) */
    uint32_t expr_idx;     /* index into arena.exprs, or IDX_NONE */
};

enum query_type {
    QUERY_TYPE_CREATE,
    QUERY_TYPE_DROP,
    QUERY_TYPE_SELECT,
    QUERY_TYPE_INSERT,
    QUERY_TYPE_DELETE,
    QUERY_TYPE_UPDATE,
    QUERY_TYPE_CREATE_INDEX,
    QUERY_TYPE_DROP_INDEX,
    QUERY_TYPE_CREATE_TYPE,
    QUERY_TYPE_DROP_TYPE,
    QUERY_TYPE_ALTER,
    QUERY_TYPE_BEGIN,
    QUERY_TYPE_COMMIT,
    QUERY_TYPE_ROLLBACK,
    QUERY_TYPE_CREATE_SEQUENCE,
    QUERY_TYPE_DROP_SEQUENCE,
    QUERY_TYPE_CREATE_VIEW,
    QUERY_TYPE_DROP_VIEW,
    QUERY_TYPE_TRUNCATE
};

enum alter_action {
    ALTER_ADD_COLUMN,
    ALTER_DROP_COLUMN,
    ALTER_RENAME_COLUMN,
    ALTER_COLUMN_TYPE
};

struct join_info {
    int join_type; /* 0=INNER, 1=LEFT, 2=RIGHT, 3=FULL, 4=CROSS */
    sv join_table;
    sv join_alias;
    sv join_left_col;
    sv join_right_col;
    enum cmp_op join_op; /* comparison operator in ON clause (default CMP_EQ) */
    int has_using;
    sv using_col;
    int is_natural;
    int is_lateral;
    uint32_t lateral_subquery_sql; /* index into arena.strings, or IDX_NONE */
    uint32_t join_on_cond; /* full ON condition tree, or IDX_NONE */
};

struct cte_def {
    uint32_t name_idx;   /* index into arena.strings */
    uint32_t sql_idx;    /* index into arena.strings */
    int is_recursive;
};

struct order_by_item {
    sv column;
    int desc;
    uint32_t expr_idx;  /* index into arena.exprs, or IDX_NONE for simple column */
};

/* WHERE clause fields shared by SELECT, UPDATE, DELETE */
struct where_clause {
    int has_where;
    sv where_column;
    struct cell where_value;
    uint32_t where_cond;  /* index into arena.conditions, or IDX_NONE */
};

struct query_select {
    sv table;
    sv table_alias;
    sv columns;
    uint32_t parsed_columns_start; /* index into arena.select_cols (consecutive) */
    uint32_t parsed_columns_count;
    int has_distinct;
    /* DISTINCT ON */
    int has_distinct_on;
    uint32_t distinct_on_start; /* index into arena.svs (consecutive column names) */
    uint32_t distinct_on_count;
    /* WHERE */
    struct where_clause where;
    /* JOIN */
    int has_join;
    int join_type; /* 0=INNER, 1=LEFT, 2=RIGHT, 3=FULL */
    sv join_table;
    sv join_left_col;
    sv join_right_col;
    uint32_t joins_start;  /* index into arena.joins (consecutive) */
    uint32_t joins_count;
    /* GROUP BY */
    int has_group_by;
    sv group_by_col;
    uint32_t group_by_start; /* index into arena.svs (consecutive) */
    uint32_t group_by_count;
    int group_by_rollup;  /* 1 if GROUP BY ROLLUP(...) */
    int group_by_cube;    /* 1 if GROUP BY CUBE(...) */
    /* HAVING */
    int has_having;
    uint32_t having_cond;  /* index into arena.conditions, or IDX_NONE */
    /* ORDER BY */
    int has_order_by;
    sv order_by_col;
    int order_desc;
    uint32_t order_by_start; /* index into arena.order_items (consecutive) */
    uint32_t order_by_count;
    /* LIMIT / OFFSET */
    int has_limit;
    int limit_count;
    int has_offset;
    int offset_count;
    /* aggregates & window functions */
    uint32_t aggregates_start; /* index into arena.aggregates (consecutive) */
    uint32_t aggregates_count;
    int agg_before_cols; /* 1 if aggregates listed before plain columns in SELECT */
    uint32_t select_exprs_start; /* index into arena.select_exprs (consecutive) */
    uint32_t select_exprs_count;
    /* set operations: UNION / INTERSECT / EXCEPT */
    int has_set_op;
    int set_op;       /* 0=UNION, 1=INTERSECT, 2=EXCEPT */
    int set_all;      /* UNION ALL etc. */
    uint32_t set_rhs_sql;   /* index into arena.strings, or IDX_NONE */
    uint32_t set_order_by;  /* index into arena.strings, or IDX_NONE */
    /* CTE support (legacy single CTE) */
    uint32_t cte_name;      /* index into arena.strings, or IDX_NONE */
    uint32_t cte_sql;       /* index into arena.strings, or IDX_NONE */
    /* multiple / recursive CTEs */
    uint32_t ctes_start;    /* index into arena.ctes (consecutive) */
    uint32_t ctes_count;
    int has_recursive_cte;
    /* FROM subquery: SELECT * FROM (SELECT ...) AS alias */
    uint32_t from_subquery_sql; /* index into arena.strings, or IDX_NONE */
    sv from_subquery_alias;
    /* literal SELECT (no table): rows in arena.rows */
    uint32_t insert_rows_start; /* index into arena.rows (consecutive) */
    uint32_t insert_rows_count;
};

struct query_insert {
    sv table;
    sv columns;
    uint32_t insert_columns_start; /* index into arena.svs (consecutive column names) */
    uint32_t insert_columns_count;
    uint32_t insert_rows_start; /* index into arena.rows (consecutive) */
    uint32_t insert_rows_count;
    /* RETURNING */
    int has_returning;
    sv returning_columns;
    /* INSERT ... SELECT */
    uint32_t insert_select_sql; /* index into arena.strings, or IDX_NONE */
    /* ON CONFLICT */
    int has_on_conflict;
    int on_conflict_do_nothing;
    sv conflict_column;
    /* ON CONFLICT DO UPDATE SET */
    int on_conflict_do_update;
    uint32_t conflict_set_start; /* index into arena.set_clauses (consecutive) */
    uint32_t conflict_set_count;
    /* CTE support for WITH ... INSERT INTO ... SELECT */
    uint32_t cte_name;      /* index into arena.strings, or IDX_NONE */
    uint32_t cte_sql;       /* index into arena.strings, or IDX_NONE */
    uint32_t ctes_start;    /* index into arena.ctes (consecutive) */
    uint32_t ctes_count;
};

struct query_update {
    sv table;
    uint32_t set_clauses_start; /* index into arena.set_clauses (consecutive) */
    uint32_t set_clauses_count;
    /* WHERE */
    struct where_clause where;
    /* RETURNING */
    int has_returning;
    sv returning_columns;
    /* UPDATE ... FROM */
    int has_update_from;
    sv update_from_table;
    sv update_from_join_left;
    sv update_from_join_right;
};

struct query_delete {
    sv table;
    /* WHERE */
    struct where_clause where;
    /* RETURNING */
    int has_returning;
    sv returning_columns;
};

struct query_create_table {
    sv table;
    uint32_t columns_start; /* index into arena.columns (consecutive) */
    uint32_t columns_count;
};

struct query_drop_table {
    sv table;
    int if_exists;
};

struct query_alter {
    sv table;
    enum alter_action alter_action;
    sv alter_column;
    sv alter_new_name;
    struct column alter_new_col;
};

struct query_create_index {
    sv table;
    sv index_name;
    sv index_column;
};

struct query_drop_index {
    sv index_name;
};

struct query_create_type {
    sv type_name;
    uint32_t enum_values_start; /* index into arena.strings (consecutive) */
    uint32_t enum_values_count;
};

struct query_drop_type {
    sv type_name;
};

struct query_create_sequence {
    sv name;
    long long start_value;  /* START WITH (default 1) */
    long long increment;    /* INCREMENT BY (default 1) */
    long long min_value;
    long long max_value;
};

struct query_drop_sequence {
    sv name;
};

struct query_create_view {
    sv name;
    uint32_t sql_idx;  /* index into arena.strings — the SELECT body */
};

struct query_drop_view {
    sv name;
};

struct query {
    enum query_type query_type;
    struct query_arena arena;
    union {
        struct query_select select;
        struct query_insert insert;
        struct query_update update;
        struct query_delete del;
        struct query_create_table create_table;
        struct query_drop_table drop_table;
        struct query_alter alter;
        struct query_create_index create_index;
        struct query_drop_index drop_index;
        struct query_create_type create_type;
        struct query_drop_type drop_type;
        struct query_create_sequence create_seq;
        struct query_drop_sequence drop_seq;
        struct query_create_view create_view;
        struct query_drop_view drop_view;
    };
};

int query_exec(struct table *t, struct query *q, struct rows *result, struct database *db, struct bump_alloc *rb);
int query_aggregate(struct table *t, struct query_select *s, struct query_arena *arena, struct rows *result);
int query_group_by(struct table *t, struct query_select *s, struct query_arena *arena, struct rows *result);

/* arena helpers that need complete type definitions — must come after all structs */
#include "arena_helpers.h"

#endif
