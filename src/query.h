#ifndef QUERY_H
#define QUERY_H

#include "row.h"
#include "table.h"
#include "stringview.h"

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
    /* for CMP_IN / CMP_NOT_IN: list of values */
    DYNAMIC_ARRAY(struct cell) in_values;
    /* for CMP_IN subquery: raw SQL text to be resolved before execution */
    char *subquery_sql;
    /* for scalar subquery comparison: WHERE x > (SELECT ...) */
    char *scalar_subquery_sql;
    /* for CMP_BETWEEN: low and high */
    struct cell between_high;
    /* for COND_AND / COND_OR */
    struct condition *left;
    struct condition *right;
    /* for COND_MULTI_IN: WHERE (a, b) IN ((1,2), (3,4)) */
    DYNAMIC_ARRAY(sv) multi_columns;        /* column names */
    int multi_tuple_width;                   /* number of columns per tuple */
    DYNAMIC_ARRAY(struct cell) multi_values; /* flat array: tuples concatenated */
    /* for ANY/ALL/SOME: col op ANY(ARRAY[...]) */
    int is_any;  /* 1 = ANY/SOME, 0 = not */
    int is_all;  /* 1 = ALL, 0 = not */
    DYNAMIC_ARRAY(struct cell) array_values; /* values for ANY/ALL */
};

void condition_free(struct condition *c);
int eval_condition(struct condition *cond, struct row *row, struct table *t);

struct set_clause {
    sv column;
    struct cell value;
    sv value_expr; /* raw expression text for evaluation (e.g. "score + 10") */
    int has_expr;  /* 1 if value_expr should be evaluated per-row */
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
    char *lateral_subquery_sql; /* for LATERAL (SELECT ...) */
};

struct cte_def {
    char *name;
    char *sql;
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
    struct condition *where_cond;
};

struct query_select {
    sv table;
    sv table_alias;
    sv columns;
    int has_distinct;
    /* WHERE */
    struct where_clause where;
    /* JOIN */
    int has_join;
    int join_type; /* 0=INNER, 1=LEFT, 2=RIGHT, 3=FULL */
    sv join_table;
    sv join_left_col;
    sv join_right_col;
    DYNAMIC_ARRAY(struct join_info) joins;
    /* GROUP BY */
    int has_group_by;
    sv group_by_col;
    DYNAMIC_ARRAY(sv) group_by_cols;
    /* HAVING */
    int has_having;
    struct condition *having_cond;
    /* ORDER BY */
    int has_order_by;
    sv order_by_col;
    int order_desc;
    DYNAMIC_ARRAY(struct order_by_item) order_by_items;
    /* LIMIT / OFFSET */
    int has_limit;
    int limit_count;
    int has_offset;
    int offset_count;
    /* aggregates & window functions */
    DYNAMIC_ARRAY(struct agg_expr) aggregates;
    DYNAMIC_ARRAY(struct select_expr) select_exprs;
    /* set operations: UNION / INTERSECT / EXCEPT */
    int has_set_op;
    int set_op;       /* 0=UNION, 1=INTERSECT, 2=EXCEPT */
    int set_all;      /* UNION ALL etc. */
    char *set_rhs_sql;
    char *set_order_by;
    /* CTE support (legacy single CTE) */
    char *cte_name;
    char *cte_sql;
    /* multiple / recursive CTEs */
    DYNAMIC_ARRAY(struct cte_def) ctes;
    int has_recursive_cte;
    /* FROM subquery: SELECT * FROM (SELECT ...) AS alias */
    char *from_subquery_sql;
    sv from_subquery_alias;
    /* literal SELECT (no table): reuse insert_row for literal values */
    DYNAMIC_ARRAY(struct row) insert_rows;
    struct row *insert_row;
};

struct query_insert {
    sv table;
    sv columns;
    struct row *insert_row; /* alias into insert_rows (never independently owned) */
    DYNAMIC_ARRAY(struct row) insert_rows;
    /* RETURNING */
    int has_returning;
    sv returning_columns;
    /* INSERT ... SELECT */
    char *insert_select_sql;
    /* ON CONFLICT */
    int has_on_conflict;
    int on_conflict_do_nothing;
    sv conflict_column;
};

struct query_update {
    sv table;
    DYNAMIC_ARRAY(struct set_clause) set_clauses;
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
    DYNAMIC_ARRAY(struct column) create_columns;
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
    DYNAMIC_ARRAY(char *) enum_values;
};

struct query_drop_type {
    sv type_name;
};

struct query {
    enum query_type query_type;
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
void query_free(struct query *q);

#endif
