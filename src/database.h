#ifndef DATABASE_H
#define DATABASE_H

#include "dynamic_array.h"
#include "table.h"
#include "query.h"

struct sequence {
    char *name;
    long long current_value;
    long long increment;
    long long min_value;
    long long max_value;
    int has_been_called; /* 0 until first nextval() */
};

struct db_snapshot {
    DYNAMIC_ARRAY(struct table) tables;
    DYNAMIC_ARRAY(struct enum_type) types;
    DYNAMIC_ARRAY(struct sequence) sequences;
};

struct database {
    // TODO: STRINGVIEW OPPORTUNITY: name is strdup'd from a string literal in main.c;
    // could be sv if the caller guaranteed lifetime.
    char *name;
    DYNAMIC_ARRAY(struct table) tables;
    DYNAMIC_ARRAY(struct enum_type) types;
    DYNAMIC_ARRAY(struct sequence) sequences;
    int in_transaction;
    struct db_snapshot *snapshot;
};

void db_init(struct database *db, const char *name);
struct enum_type *db_find_type(struct database *db, const char *name);
int  db_create_table(struct database *db, const char *name, struct column *cols);
struct table *db_find_table(struct database *db, const char *name);
struct table *db_find_table_sv(struct database *db, sv name);
int  db_table_exec_query(struct database *db, sv table_name,
                         struct query *q, struct rows *result, struct bump_alloc *rb);
int  db_exec(struct database *db, struct query *q, struct rows *result, struct bump_alloc *rb);
int  db_exec_sql(struct database *db, const char *sql, struct rows *result);
void db_free(struct database *db);

/* Materialize a subquery into a temporary table added to db->tables.
 * Returns a pointer to the new table, or NULL on failure.
 * The caller is responsible for calling remove_temp_table when done. */
struct table *materialize_subquery(struct database *db, const char *sql,
                                   const char *table_name);

/* Remove a temporary table from the database by pointer */
void remove_temp_table(struct database *db, struct table *t);

#endif
