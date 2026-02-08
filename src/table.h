#ifndef TABLE_H
#define TABLE_H

#include "dynamic_array.h"
#include "column.h"
#include "row.h"
#include "index.h"

struct table {
    // TODO: STRINGVIEW OPPORTUNITY: name is strdup'd from sv-originated strings in most
    // paths (db_exec CREATE TABLE). Could be sv if the schema had a persistent backing store.
    char *name;
    DYNAMIC_ARRAY(struct column) columns;
    DYNAMIC_ARRAY(struct row) rows;
    DYNAMIC_ARRAY(struct index) indexes;
};

void table_init(struct table *t, const char *name);
void table_add_column(struct table *t, struct column *col);
void table_free(struct table *t);
void table_deep_copy(struct table *dst, const struct table *src);

#endif
