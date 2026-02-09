#ifndef QUERY_H
#define QUERY_H

#include "row.h"
#include "table.h"
#include "stringview.h"

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
    COND_NOT
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
    /* for CMP_BETWEEN: low and high */
    struct cell between_high;
    /* for COND_AND / COND_OR */
    struct condition *left;
    struct condition *right;
};

void condition_free(struct condition *c);
int eval_condition(struct condition *cond, struct row *row, struct table *t);

struct set_clause {
    sv column;
    struct cell value;
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
    int has_using;
    sv using_col;
    int is_natural;
};

struct order_by_item {
    sv column;
    int desc;
};

struct query {
    enum query_type query_type;
    int has_distinct;
    sv columns;
    sv table;
    sv table_alias;
    struct row *insert_row; /* always an alias into insert_rows (never independently owned) */
    DYNAMIC_ARRAY(struct row) insert_rows;
    sv returning_columns;
    DYNAMIC_ARRAY(struct column) create_columns;
    int has_join;
    int join_type; /* 0=INNER, 1=LEFT, 2=RIGHT, 3=FULL */
    sv join_table;
    sv join_left_col;
    sv join_right_col;
    DYNAMIC_ARRAY(struct join_info) joins;
    int has_where;
    sv where_column;
    struct cell where_value;
    struct condition *where_cond;
    DYNAMIC_ARRAY(struct set_clause) set_clauses;
    int has_group_by;
    sv group_by_col;
    DYNAMIC_ARRAY(sv) group_by_cols;
    int has_having;
    struct condition *having_cond;
    int has_order_by;
    sv order_by_col;
    int order_desc;
    DYNAMIC_ARRAY(struct order_by_item) order_by_items;
    int has_limit;
    int limit_count;
    int has_offset;
    int offset_count;
    sv index_name;
    sv index_column;
    sv type_name;
    DYNAMIC_ARRAY(char *) enum_values;
    DYNAMIC_ARRAY(struct agg_expr) aggregates;
    DYNAMIC_ARRAY(struct select_expr) select_exprs;
    enum alter_action alter_action;
    sv alter_column;
    sv alter_new_name;
    struct column alter_new_col;
    /* set operations: UNION / INTERSECT / EXCEPT */
    int has_set_op;
    int set_op;       /* 0=UNION, 1=INTERSECT, 2=EXCEPT */
    int set_all;      /* UNION ALL etc. */
    // TODO: OWNERSHIP VIOLATION (JPL): set_rhs_sql, set_order_by, cte_name, cte_sql,
    // and insert_select_sql are all malloc'd in parser.c but freed in query_free (query.c).
    // The allocator and deallocator live in different files. Consider moving the free logic
    // into a parser_query_free helper in parser.c, or documenting the cross-file contract.
    // TODO: STRINGVIEW OPPORTUNITY: these five char* fields hold substrings of the original
    // SQL input; they could be sv references into the input string if the caller guaranteed
    // its lifetime, avoiding the malloc+memcpy+free round-trip entirely.
    char *set_rhs_sql; /* right-hand SELECT as raw SQL */
    char *set_order_by; /* ORDER BY clause for combined set result */
    /* CTE support */
    char *cte_name;
    char *cte_sql;
    /* INSERT ... SELECT */
    char *insert_select_sql;
    /* ON CONFLICT */
    int has_on_conflict;
    int on_conflict_do_nothing;
    sv conflict_column;
    /* RETURNING on UPDATE/DELETE */
    int has_returning;
    /* UPDATE ... FROM */
    int has_update_from;
    sv update_from_table;
    sv update_from_join_left;
    sv update_from_join_right;
};

int query_exec(struct table *t, struct query *q, struct rows *result);
void query_free(struct query *q);

#endif
