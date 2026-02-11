#ifndef TABLE_H
#define TABLE_H

#include <stdint.h>
#include "dynamic_array.h"
#include "column.h"
#include "row.h"
#include "index.h"

/* Cached columnar representation of a table for fast repeated scans.
 * Invalidated when table->generation changes. */
struct scan_cache {
    uint64_t generation;    /* generation when cache was built */
    uint16_t ncols;         /* number of cached columns */
    size_t   nrows;         /* total rows cached */
    void   **col_data;      /* [ncols] heap-allocated typed arrays */
    uint8_t **col_nulls;    /* [ncols] heap-allocated null bitmaps */
    enum column_type *col_types; /* [ncols] column types */
};

struct table {
    // TODO: STRINGVIEW OPPORTUNITY: name is strdup'd from sv-originated strings in most
    // paths (db_exec CREATE TABLE). Could be sv if the schema had a persistent backing store.
    char *name;
    char *view_sql;  /* non-NULL if this is a view (stores the SELECT body) */
    DYNAMIC_ARRAY(struct column) columns;
    DYNAMIC_ARRAY(struct row) rows;
    DYNAMIC_ARRAY(struct index) indexes;
    uint64_t generation;  /* bumped on every INSERT/UPDATE/DELETE for scan cache invalidation */
    struct scan_cache scan_cache; /* cached columnar representation */
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
