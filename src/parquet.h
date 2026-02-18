#ifndef PARQUET_H
#define PARQUET_H

#ifndef MSKQL_WASM

#include "column.h"
#include <stdint.h>

struct parquet_table_info {
    uint16_t ncols;
    char **col_names;              /* heap-allocated array of strdup'd names */
    enum column_type *col_types;   /* mapped from Parquet schema */
    int64_t total_rows;
    int32_t num_row_groups;
};

/* Open a Parquet file and read its schema metadata (no data read).
 * Returns 0 on success, -1 on error. Caller must call parquet_info_free(). */
int parquet_open_metadata(const char *path, struct parquet_table_info *info);

/* Free resources allocated by parquet_open_metadata(). */
void parquet_info_free(struct parquet_table_info *info);

struct table;
/* Materialize all rows from a Parquet file into t->rows (legacy fallback).
 * Returns 0 on success, -1 on error. Idempotent — skips if rows already loaded. */
int parquet_materialize(struct table *t);

#endif /* MSKQL_WASM */
#endif
