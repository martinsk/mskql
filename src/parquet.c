#ifndef MSKQL_WASM

#include "parquet.h"
#include "table.h"
#include "row.h"
#include "datetime.h"
#include <carquet/carquet.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Parquet uses Unix epoch (1970-01-01), mskql uses PG epoch (2000-01-01).
 * Difference: 10957 days, or 10957 * 86400 * 1000000 microseconds. */
#define PARQUET_PG_DATE_OFFSET   10957
#define PARQUET_PG_USEC_OFFSET   (PARQUET_PG_DATE_OFFSET * 86400LL * 1000000LL)

/* Map Carquet physical+logical type to mskql column_type. */
static enum column_type parquet_map_type(carquet_physical_type_t phys,
                                          const carquet_logical_type_t *logical)
{
    if (logical && logical->id != CARQUET_LOGICAL_UNKNOWN) {
        switch (logical->id) {
        case CARQUET_LOGICAL_UNKNOWN:   break;
        case CARQUET_LOGICAL_DATE:      return COLUMN_TYPE_DATE;
        case CARQUET_LOGICAL_TIME:      return COLUMN_TYPE_TIME;
        case CARQUET_LOGICAL_TIMESTAMP: return COLUMN_TYPE_TIMESTAMP;
        case CARQUET_LOGICAL_DECIMAL:   return COLUMN_TYPE_NUMERIC;
        case CARQUET_LOGICAL_UUID:      return COLUMN_TYPE_UUID;
        case CARQUET_LOGICAL_STRING:    return COLUMN_TYPE_TEXT;
        case CARQUET_LOGICAL_JSON:      return COLUMN_TYPE_TEXT;
        case CARQUET_LOGICAL_MAP:       return COLUMN_TYPE_TEXT;
        case CARQUET_LOGICAL_LIST:      return COLUMN_TYPE_TEXT;
        case CARQUET_LOGICAL_ENUM:      return COLUMN_TYPE_TEXT;
        case CARQUET_LOGICAL_BSON:      return COLUMN_TYPE_TEXT;
        case CARQUET_LOGICAL_NULL:      return COLUMN_TYPE_TEXT;
        case CARQUET_LOGICAL_INTEGER:
            if (phys == CARQUET_PHYSICAL_INT64) return COLUMN_TYPE_BIGINT;
            return COLUMN_TYPE_INT;
        case CARQUET_LOGICAL_FLOAT16:   return COLUMN_TYPE_FLOAT;
        }
    }
    switch (phys) {
    case CARQUET_PHYSICAL_BOOLEAN:               return COLUMN_TYPE_BOOLEAN;
    case CARQUET_PHYSICAL_INT32:                  return COLUMN_TYPE_INT;
    case CARQUET_PHYSICAL_INT64:                  return COLUMN_TYPE_BIGINT;
    case CARQUET_PHYSICAL_INT96:                  return COLUMN_TYPE_TIMESTAMP;
    case CARQUET_PHYSICAL_FLOAT:                  return COLUMN_TYPE_FLOAT;
    case CARQUET_PHYSICAL_DOUBLE:                 return COLUMN_TYPE_FLOAT;
    case CARQUET_PHYSICAL_BYTE_ARRAY:             return COLUMN_TYPE_TEXT;
    case CARQUET_PHYSICAL_FIXED_LEN_BYTE_ARRAY:   return COLUMN_TYPE_TEXT;
    }
    return COLUMN_TYPE_TEXT;
}

int parquet_open_metadata(const char *path, struct parquet_table_info *info)
{
    memset(info, 0, sizeof(*info));

    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_reader_t *reader = carquet_reader_open(path, NULL, &err);
    if (!reader) return -1;

    const carquet_schema_t *schema = carquet_reader_schema(reader);
    int32_t ncols = carquet_reader_num_columns(reader);

    info->ncols = (uint16_t)ncols;
    info->total_rows = carquet_reader_num_rows(reader);
    info->num_row_groups = carquet_reader_num_row_groups(reader);
    info->col_names = (char **)calloc(ncols, sizeof(char *));
    info->col_types = (enum column_type *)calloc(ncols, sizeof(enum column_type));

    /* Iterate schema elements, collecting only leaf (column) nodes.
     * Element 0 is the root group, so leaves start at index >= 1. */
    int32_t num_elements = carquet_schema_num_elements(schema);
    int32_t col_idx = 0;
    for (int32_t i = 0; i < num_elements && col_idx < ncols; i++) {
        const carquet_schema_node_t *node = carquet_schema_get_element(schema, i);
        if (!node || !carquet_schema_node_is_leaf(node)) continue;

        const char *name = carquet_schema_node_name(node);
        info->col_names[col_idx] = strdup(name ? name : "?");

        carquet_physical_type_t phys = carquet_schema_node_physical_type(node);
        const carquet_logical_type_t *logical = carquet_schema_node_logical_type(node);
        info->col_types[col_idx] = parquet_map_type(phys, logical);
        col_idx++;
    }

    carquet_reader_close(reader);
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
    if (!t || !t->parquet_path) return -1;
    /* Idempotent: skip if rows already loaded */
    if (t->rows.count > 0) return 0;

    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_reader_t *reader = carquet_reader_open(t->parquet_path, NULL, &err);
    if (!reader) return -1;

    carquet_batch_reader_config_t cfg;
    carquet_batch_reader_config_init(&cfg);

    carquet_batch_reader_t *br = carquet_batch_reader_create(reader, &cfg, &err);
    if (!br) { carquet_reader_close(reader); return -1; }

    /* Determine physical types for float/double distinction */
    const carquet_schema_t *schema = carquet_reader_schema(reader);
    int32_t num_elements = carquet_schema_num_elements(schema);
    uint16_t ncols = (uint16_t)t->columns.count;

    carquet_physical_type_t *phys_types = calloc(ncols, sizeof(carquet_physical_type_t));
    {
        int leaf = 0;
        for (int32_t ei = 0; ei < num_elements && leaf < ncols; ei++) {
            const carquet_schema_node_t *node = carquet_schema_get_element(schema, ei);
            if (!node || !carquet_schema_node_is_leaf(node)) continue;
            phys_types[leaf] = carquet_schema_node_physical_type(node);
            leaf++;
        }
    }

    carquet_row_batch_t *batch = NULL;
    while (carquet_batch_reader_next(br, &batch) == CARQUET_OK && batch) {
        int64_t nrows = carquet_row_batch_num_rows(batch);

        /* Get column data pointers */
        const void **col_data = calloc(ncols, sizeof(void *));
        const uint8_t **col_nulls = calloc(ncols, sizeof(uint8_t *));
        int64_t *col_nvals = calloc(ncols, sizeof(int64_t));
        for (uint16_t c = 0; c < ncols; c++)
            (void)carquet_row_batch_column(batch, c, &col_data[c], &col_nulls[c], &col_nvals[c]);

        /* Track running data indices per column for compacted nullable data */
        uint16_t *di = calloc(ncols, sizeof(uint16_t));

        for (int64_t r = 0; r < nrows; r++) {
            struct row row = {0};
            da_init(&row.cells);
            for (uint16_t c = 0; c < ncols; c++) {
                struct cell cell = {0};
                cell.type = t->columns.items[c].type;

                /* Carquet: bit set = NULL, all-zero = all valid */
                int is_null = 0;
                if (col_nulls[c])
                    is_null = (col_nulls[c][r / 8] >> (r % 8)) & 1;
                if (is_null) {
                    cell.is_null = 1;
                    da_push(&row.cells, cell);
                    continue;
                }

                uint16_t d = di[c]++;

                switch (cell.type) {
                case COLUMN_TYPE_BOOLEAN: {
                    const uint8_t *v = (const uint8_t *)col_data[c];
                    cell.value.as_int = v[d] ? 1 : 0;
                    break;
                }
                case COLUMN_TYPE_INT:
                case COLUMN_TYPE_SMALLINT:
                case COLUMN_TYPE_ENUM: {
                    const int32_t *v = (const int32_t *)col_data[c];
                    cell.value.as_int = v[d];
                    break;
                }
                case COLUMN_TYPE_BIGINT: {
                    const int64_t *v = (const int64_t *)col_data[c];
                    cell.value.as_bigint = v[d];
                    break;
                }
                case COLUMN_TYPE_DATE: {
                    const int32_t *v = (const int32_t *)col_data[c];
                    cell.value.as_date = v[d] - PARQUET_PG_DATE_OFFSET;
                    break;
                }
                case COLUMN_TYPE_TIME: {
                    const int64_t *v = (const int64_t *)col_data[c];
                    cell.value.as_time = v[d];
                    break;
                }
                case COLUMN_TYPE_TIMESTAMP:
                case COLUMN_TYPE_TIMESTAMPTZ: {
                    const int64_t *v = (const int64_t *)col_data[c];
                    cell.value.as_timestamp = v[d] - PARQUET_PG_USEC_OFFSET;
                    break;
                }
                case COLUMN_TYPE_FLOAT: {
                    if (phys_types[c] == CARQUET_PHYSICAL_FLOAT) {
                        const float *v = (const float *)col_data[c];
                        cell.value.as_float = (double)v[d];
                    } else {
                        const double *v = (const double *)col_data[c];
                        cell.value.as_float = v[d];
                    }
                    break;
                }
                case COLUMN_TYPE_NUMERIC: {
                    if (phys_types[c] == CARQUET_PHYSICAL_FLOAT) {
                        const float *v = (const float *)col_data[c];
                        cell.value.as_float = (double)v[d];
                    } else if (phys_types[c] == CARQUET_PHYSICAL_DOUBLE) {
                        const double *v = (const double *)col_data[c];
                        cell.value.as_float = v[d];
                    } else {
                        const carquet_byte_array_t *arr = (const carquet_byte_array_t *)col_data[c];
                        char *s = malloc(arr[d].length + 1);
                        memcpy(s, arr[d].data, arr[d].length);
                        s[arr[d].length] = '\0';
                        cell.value.as_float = atof(s);
                        free(s);
                    }
                    break;
                }
                case COLUMN_TYPE_INTERVAL: {
                    cell.value.as_interval = (struct interval){0, 0, 0};
                    break;
                }
                case COLUMN_TYPE_TEXT:
                case COLUMN_TYPE_UUID:
                default: {
                    const carquet_byte_array_t *arr = (const carquet_byte_array_t *)col_data[c];
                    char *s = malloc(arr[d].length + 1);
                    memcpy(s, arr[d].data, arr[d].length);
                    s[arr[d].length] = '\0';
                    cell.value.as_text = s;
                    break;
                }
                }
                da_push(&row.cells, cell);
            }
            da_push(&t->rows, row);
        }
        free(di);

        free(col_data);
        free(col_nulls);
        free(col_nvals);
        carquet_row_batch_free(batch);
        batch = NULL;
    }

    free(phys_types);
    carquet_batch_reader_free(br);
    carquet_reader_close(reader);
    t->generation++;
    return 0;
}

#endif /* MSKQL_WASM */
