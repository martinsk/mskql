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
};

enum win_func {
    WIN_ROW_NUMBER,
    WIN_RANK,
    WIN_SUM,
    WIN_COUNT,
    WIN_AVG
};

struct win_expr {
    enum win_func func;
    sv arg_column;       /* column arg for SUM/COUNT/AVG, empty for ROW_NUMBER/RANK */
    int has_partition;
    sv partition_col;
    int has_order;
    sv order_col;
    int order_desc; /* 1 = DESC, 0 = ASC (default) */
};

enum select_expr_kind {
    SEL_COLUMN,
    SEL_WINDOW
};

struct select_expr {
    enum select_expr_kind kind;
    sv column;           /* for SEL_COLUMN */
    struct win_expr win; /* for SEL_WINDOW */
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
                   struct row *row, struct table *t);

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
    EXPR_SUBQUERY       /* (SELECT ...) */
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
    FUNC_SUBSTRING
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
    };

    sv alias; /* optional AS alias */
};

struct cell eval_expr(uint32_t expr_idx, struct query_arena *arena,
                      struct table *t, struct row *row,
                      struct database *db);

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
    QUERY_TYPE_ROLLBACK
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
};

struct cte_def {
    uint32_t name_idx;   /* index into arena.strings */
    uint32_t sql_idx;    /* index into arena.strings */
    int is_recursive;
};

struct order_by_item {
    sv column;
    int desc;
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
    };
};

int query_exec(struct table *t, struct query *q, struct rows *result, struct database *db);
int query_aggregate(struct table *t, struct query_select *s, struct query_arena *arena, struct rows *result);
int query_group_by(struct table *t, struct query_select *s, struct query_arena *arena, struct rows *result);

/* arena helpers that need complete type definitions — must come after all structs */
#include "arena_helpers.h"

#endif
