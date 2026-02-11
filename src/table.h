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
    char *view_sql;  /* non-NULL if this is a view (stores the SELECT body) */
    DYNAMIC_ARRAY(struct column) columns;
    DYNAMIC_ARRAY(struct row) rows;
    DYNAMIC_ARRAY(struct index) indexes;
};

void table_init(struct table *t, const char *name);
void table_init_own(struct table *t, char *name); /* takes ownership of name */
void table_add_column(struct table *t, struct column *col);
void table_free(struct table *t);
void table_deep_copy(struct table *dst, const struct table *src);

/* column lookup — exact match first, then strips "table." prefix and retries */
#include "stringview.h"
int table_find_column_sv(struct table *t, sv name);
int table_find_column(struct table *t, const char *name);

/* resolve an ORDER BY name that might be a SELECT alias (e.g. "price AS cost")
 * by scanning the raw column-list text for "col AS alias" patterns */
int resolve_alias_to_column(struct table *t, sv columns, sv alias);

#endif
