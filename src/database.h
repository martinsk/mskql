#ifndef DATABASE_H
#define DATABASE_H

#include "dynamic_array.h"
#include "table.h"
#include "query.h"

struct db_snapshot {
    DYNAMIC_ARRAY(struct table) tables;
    DYNAMIC_ARRAY(struct enum_type) types;
};

struct database {
    char *name;
    DYNAMIC_ARRAY(struct table) tables;
    DYNAMIC_ARRAY(struct enum_type) types;
    int in_transaction;
    struct db_snapshot *snapshot;
};

void db_init(struct database *db, const char *name);
struct enum_type *db_find_type(struct database *db, const char *name);
int  db_create_table(struct database *db, const char *name, struct column *cols);
struct table *db_find_table(struct database *db, const char *name);
struct table *db_find_table_sv(struct database *db, sv name);
int  db_table_exec_query(struct database *db, sv table_name,
                         struct query *q, struct rows *result);
int  db_exec(struct database *db, struct query *q, struct rows *result);
int  db_exec_sql(struct database *db, const char *sql, struct rows *result);
void db_free(struct database *db);

#endif
