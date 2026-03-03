#ifndef MSKQL_WASM

#include "parquet.h"
#include "pq_reader.h"
#include "table.h"
#include "row.h"
#include "datetime.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Parquet uses Unix epoch (1970-01-01), mskql uses PG epoch (2000-01-01).
 * Difference: 10957 days, or 10957 * 86400 * 1000000 microseconds. */
#define PARQUET_PG_DATE_OFFSET   10957
#define PARQUET_PG_USEC_OFFSET   (PARQUET_PG_DATE_OFFSET * 86400LL * 1000000LL)

int parquet_open_metadata(const char *path, struct parquet_table_info *info)
{
    memset(info, 0, sizeof(*info));

    pq_reader_t *r = pq_open(path);
    if (!r) return -1;

    int32_t ncols = pq_num_columns(r);
    info->ncols = (uint16_t)ncols;
    info->total_rows = pq_num_rows(r);
    info->num_row_groups = pq_num_row_groups(r);
    info->col_names = (char **)calloc((size_t)ncols, sizeof(char *));
    info->col_types = (enum column_type *)calloc((size_t)ncols, sizeof(enum column_type));
    if (!info->col_names || !info->col_types) { pq_close(r); return -1; }

    for (int32_t i = 0; i < ncols; i++) {
        const char *name = pq_column_name(r, i);
        info->col_names[i] = strdup(name ? name : "?");
        info->col_types[i] = pq_column_type(r, i);
    }

    pq_close(r);
    return 0;
}

void parquet_info_free(struct parquet_table_info *info)
{
    if (info->col_names) {
        for (uint16_t i = 0; i < info->ncols; i++)
            free(info->col_names[i]);
        free(info->col_names);
    }
    free(info->col_types);
    memset(info, 0, sizeof(*info));
}

int parquet_materialize(struct table *t)
{
    if (!t || t->kind != TABLE_PARQUET) return -1;
    /* Idempotent: skip if rows already loaded */
    if (t->rows.count > 0) return 0;

    pq_reader_t *r = pq_open(t->parquet.path);
    if (!r) return -1;

    uint16_t ncols = (uint16_t)t->columns.count;
    int32_t nrg = pq_num_row_groups(r);

    struct pq_column *cols = (struct pq_column *)calloc((size_t)ncols, sizeof(struct pq_column));
    if (!cols) { pq_close(r); return -1; }

    for (int32_t rg = 0; rg < nrg; rg++) {
        if (pq_read_row_group(r, rg, cols) < 0) continue;

        int64_t nrows = cols[0].nrows;
        for (int64_t row_idx = 0; row_idx < nrows; row_idx++) {
            struct row row = {0};
            da_init(&row.cells);

            for (uint16_t c = 0; c < ncols; c++) {
                struct cell cell = {0};
                cell.type = t->columns.items[c].type;

                int is_null = cols[c].nulls ? cols[c].nulls[row_idx] : 0;
                if (is_null) {
                    cell.is_null = 1;
                    da_push(&row.cells, cell);
                    continue;
                }

                switch (cell.type) {
                case COLUMN_TYPE_BOOLEAN: {
                    const uint8_t *v = (const uint8_t *)cols[c].data;
                    cell.value.as_int = v[row_idx] ? 1 : 0;
                    break;
                }
                case COLUMN_TYPE_INT:
                case COLUMN_TYPE_SMALLINT:
                case COLUMN_TYPE_ENUM: {
                    const int32_t *v = (const int32_t *)cols[c].data;
                    cell.value.as_int = v[row_idx];
                    break;
                }
                case COLUMN_TYPE_BIGINT: {
                    const int64_t *v = (const int64_t *)cols[c].data;
                    cell.value.as_bigint = v[row_idx];
                    break;
                }
                case COLUMN_TYPE_DATE: {
                    const int32_t *v = (const int32_t *)cols[c].data;
                    cell.value.as_date = v[row_idx] - PARQUET_PG_DATE_OFFSET;
                    break;
                }
                case COLUMN_TYPE_TIME: {
                    const int64_t *v = (const int64_t *)cols[c].data;
                    cell.value.as_time = v[row_idx];
                    break;
                }
                case COLUMN_TYPE_TIMESTAMP:
                case COLUMN_TYPE_TIMESTAMPTZ: {
                    const int64_t *v = (const int64_t *)cols[c].data;
                    cell.value.as_timestamp = v[row_idx] - PARQUET_PG_USEC_OFFSET;
                    break;
                }
                case COLUMN_TYPE_FLOAT:
                case COLUMN_TYPE_NUMERIC: {
                    if (cols[c].phys == PQ_PHYS_FLOAT) {
                        const float *v = (const float *)cols[c].data;
                        cell.value.as_float = (double)v[row_idx];
                    } else {
                        const double *v = (const double *)cols[c].data;
                        cell.value.as_float = v[row_idx];
                    }
                    break;
                }
                case COLUMN_TYPE_INTERVAL:
                    cell.value.as_interval = (struct interval){0, 0, 0};
                    break;
                case COLUMN_TYPE_TEXT:
                case COLUMN_TYPE_UUID: {
                    char **strs = (char **)cols[c].data;
                    char *s = strs[row_idx];
                    cell.value.as_text = s ? strdup(s) : NULL;
                    break;
                }
                case COLUMN_TYPE_VECTOR:
                    cell.is_null = 1;
                    break;
                }
                da_push(&row.cells, cell);
            }
            da_push(&t->rows, row);
        }

        /* Free decoded column data for this row group */
        for (uint16_t c = 0; c < ncols; c++) {
            if (cols[c].phys == PQ_PHYS_BYTE_ARRAY || cols[c].phys == PQ_PHYS_FIXED_LEN_BYTE_ARRAY) {
                char **strs = (char **)cols[c].data;
                if (strs) {
                    for (int64_t i = 0; i < cols[c].nrows; i++) free(strs[i]);
                }
            }
            free(cols[c].data); cols[c].data = NULL;
            free(cols[c].nulls); cols[c].nulls = NULL;
        }
    }

    free(cols);
    pq_close(r);
    t->generation++;
    return 0;
}

#endif /* MSKQL_WASM */
