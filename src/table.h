#ifndef TABLE_H
#define TABLE_H

#include <stdint.h>
#include "dynamic_array.h"
#include "column.h"
#include "row.h"
#include "index.h"
#include "block.h"

/* Cached hash join build result for a specific join key column.
 * Invalidated when table->generation changes.
 * Uses struct flat_table for the columnar data arrays. */
struct join_cache {
    uint64_t         generation; /* generation when cache was built */
    int              key_col;    /* inner key column index */
    struct flat_table ft;        /* columnar data */
    uint32_t        *hashes;     /* [ft.nrows] hash values */
    uint32_t        *nexts;      /* [ft.nrows] next pointers */
    uint32_t        *buckets;    /* [nbuckets] bucket heads */
    uint32_t         nbuckets;   /* number of hash buckets */
    int              valid;
};

/* Cached columnar representation of a parquet foreign table.
 * Built on first scan, reused for all subsequent queries. */
struct parquet_cache {
    uint16_t ncols;
    size_t   nrows;
    void   **col_data;           /* [ncols] heap-allocated typed arrays */
    uint8_t **col_nulls;         /* [ncols] heap-allocated null bitmaps (1 byte per row) */
    enum column_type *col_types; /* [ncols] */
    uint32_t **col_str_lens;     /* [ncols] TEXT only: strlen of each entry, or NULL */
    int      valid;
};

enum table_kind {
    TABLE_MEMORY,   /* in-memory row-store + columnar flat storage */
    TABLE_VIEW,     /* virtual — stores SELECT SQL, no data */
    TABLE_PARQUET,  /* read-only Parquet foreign table */
};

static inline int table_is_writable(enum table_kind kind)
{
    switch (kind) {
    case TABLE_MEMORY:  return 1;
    case TABLE_VIEW:    return 0;
    case TABLE_PARQUET: return 0;
    }
    __builtin_unreachable();
}

struct table {
    enum table_kind kind;
    // TODO: STRINGVIEW OPPORTUNITY: name is strdup'd from sv-originated strings in most
    // paths (db_exec CREATE TABLE). Could be sv if the schema had a persistent backing store.
    char *name;
    DYNAMIC_ARRAY(struct column) columns; /* schema — shared by all non-view kinds */
    uint64_t generation;       /* bumped on every INSERT/UPDATE/DELETE for scan cache invalidation */

    /* Row/columnar storage — used by TABLE_MEMORY natively and by TABLE_PARQUET
     * after legacy-executor materialization (parquet_materialize). */
    DYNAMIC_ARRAY(struct row) rows;
    DYNAMIC_ARRAY(struct index) indexes;
    struct flat_table flat;    /* primary columnar storage */
    struct join_cache join_cache;

    union {
        struct {
            char *sql;  /* the SELECT body */
        } view;
        struct {
            char *path;
            struct parquet_cache pq_cache;
        } parquet;
    };
};

void table_init(struct table *t, const char *name);
void table_init_own(struct table *t, char *name); /* takes ownership of name */
void table_add_column(struct table *t, struct column *col);
void table_free(struct table *t);
void table_deep_copy(struct table *dst, const struct table *src);

/* Initialize t->memory.flat columnar arrays from t->columns schema.
 * Must be called after all columns have been added via table_add_column.
 * Safe to call multiple times — frees existing flat before reinitializing. */
void table_flat_init_schema(struct table *t);

/* Append one row to t->flat. Grows arrays as needed.
 * row must have cells.count == t->columns.count.
 * Text pointers are stored by reference (same ownership as row-store). */
void table_flat_append_row(struct table *t, const struct row *row);

/* Patch one row in t->flat after an UPDATE (row_idx must be < t->flat.nrows). */
void table_flat_update_row(struct table *t, size_t row_idx, const struct row *row);

/* Remove one row from t->flat by shifting rows [row_idx+1..nrows) left by one. */
void table_flat_delete_row(struct table *t, size_t row_idx);

/* Append multiple rows to t->flat in a single batch (pre-grows once).
 * rows[0..count) must each have cells.count >= t->columns.count. */
void table_flat_append_rows_bulk(struct table *t, struct row *rows, size_t count);

/* Rebuild t->flat entirely from t->rows (used after schema changes like ALTER). */
void table_flat_rebuild_from_rows(struct table *t);

/* column lookup — exact match first, then strips "table." prefix and retries */
#include "stringview.h"
int table_find_column_sv(struct table *t, sv name);
int table_find_column(struct table *t, const char *name);

/* resolve an ORDER BY name that might be a SELECT alias (e.g. "price AS cost")
 * by scanning the raw column-list text for "col AS alias" patterns */
int resolve_alias_to_column(struct table *t, sv columns, sv alias);

#endif
