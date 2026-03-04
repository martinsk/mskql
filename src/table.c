#include "table.h"
#include "block.h"
#include "row.h"
#include "stringview.h"
#include <string.h>
#include <strings.h>
#include <stdlib.h>

void table_init(struct table *t, const char *name)
{
    memset(t, 0, sizeof(*t));
    t->kind = TABLE_MEMORY;
    t->name = strdup(name);
    da_init(&t->columns);
    t->generation = 0;
    da_init(&t->indexes);
    memset(&t->flat, 0, sizeof(t->flat));
    memset(&t->join_cache, 0, sizeof(t->join_cache));
}

void table_init_own(struct table *t, char *name)
{
    memset(t, 0, sizeof(*t));
    t->kind = TABLE_MEMORY;
    t->name = name;
    da_init(&t->columns);
    t->generation = 0;
    da_init(&t->indexes);
    memset(&t->flat, 0, sizeof(t->flat));
    memset(&t->join_cache, 0, sizeof(t->join_cache));
}

void table_add_column(struct table *t, struct column *col)
{
    struct column c = {
        .name = strdup(col->name),
        .type = col->type,
        .enum_type_name = col->enum_type_name ? strdup(col->enum_type_name) : NULL,
        .not_null = col->not_null,
        .has_default = col->has_default,
        .default_value = NULL,
        .is_unique = col->is_unique,
        .is_primary_key = col->is_primary_key,
        .is_serial = col->is_serial,
        .serial_next = col->serial_next,
        .fk_table = col->fk_table ? strdup(col->fk_table) : NULL,
        .fk_column = col->fk_column ? strdup(col->fk_column) : NULL,
        .fk_on_delete = col->fk_on_delete,
        .fk_on_update = col->fk_on_update,
        .check_expr_sql = col->check_expr_sql ? strdup(col->check_expr_sql) : NULL,
        .vector_dim = col->vector_dim
    };
    if (col->has_default && col->default_value) {
        c.default_value = calloc(1, sizeof(struct cell));
        if (!c.default_value) { fprintf(stderr, "OOM: table_add_column\n"); abort(); }
        c.default_value->type = col->default_value->type;
        c.default_value->is_null = col->default_value->is_null;
        if (column_type_is_text(col->default_value->type) && col->default_value->value.as_text)
            c.default_value->value.as_text = strdup(col->default_value->value.as_text);
        else
            c.default_value->value = col->default_value->value;
    }
    da_push(&t->columns, c);
}

void table_deep_copy(struct table *dst, const struct table *src)
{
    memset(dst, 0, sizeof(*dst));
    dst->kind = src->kind;
    dst->name = strdup(src->name);
    da_init(&dst->columns);
    dst->generation = src->generation;

    /* deep-copy columns */
    for (size_t i = 0; i < src->columns.count; i++) {
        struct column *sc = &src->columns.items[i];
        struct column c = {
            .name = strdup(sc->name),
            .type = sc->type,
            .enum_type_name = sc->enum_type_name ? strdup(sc->enum_type_name) : NULL,
            .not_null = sc->not_null,
            .has_default = sc->has_default,
            .default_value = NULL,
            .is_unique = sc->is_unique,
            .is_primary_key = sc->is_primary_key,
            .is_serial = sc->is_serial,
            .serial_next = sc->serial_next,
            .fk_table = sc->fk_table ? strdup(sc->fk_table) : NULL,
            .fk_column = sc->fk_column ? strdup(sc->fk_column) : NULL,
            .fk_on_delete = sc->fk_on_delete,
            .fk_on_update = sc->fk_on_update,
            .check_expr_sql = sc->check_expr_sql ? strdup(sc->check_expr_sql) : NULL,
            .vector_dim = sc->vector_dim
        };
        if (sc->has_default && sc->default_value) {
            c.default_value = calloc(1, sizeof(struct cell));
            if (!c.default_value) { fprintf(stderr, "OOM: table_deep_copy\n"); abort(); }
            c.default_value->type = sc->default_value->type;
            c.default_value->is_null = sc->default_value->is_null;
            if (column_type_is_text(sc->default_value->type) && sc->default_value->value.as_text)
                c.default_value->value.as_text = strdup(sc->default_value->value.as_text);
            else
                c.default_value->value = sc->default_value->value;
        }
        da_push(&dst->columns, c);
    }

    /* deep-copy flat columnar storage */
    da_init(&dst->indexes);
    memset(&dst->flat, 0, sizeof(dst->flat));
    memset(&dst->join_cache, 0, sizeof(dst->join_cache));
    if (src->flat.nrows > 0 && src->flat.ncols > 0) {
        table_flat_init_schema(dst);
        for (size_t ri = 0; ri < src->flat.nrows; ri++) {
            struct row r = {0};
            da_init(&r.cells);
            for (uint16_t c = 0; c < src->flat.ncols; c++) {
                struct cell cv = flat_cell_at_pub(&src->flat, c, ri);
                struct cell owned = cv;
                if (column_type_is_text(cv.type) && cv.value.as_text)
                    owned.value.as_text = strdup(cv.value.as_text);
                else if (cv.type == COLUMN_TYPE_VECTOR && cv.value.as_vector && !cv.is_null) {
                    uint16_t dim = src->columns.items[c].vector_dim;
                    owned.value.as_vector = (float *)malloc(dim * sizeof(float));
                    memcpy(owned.value.as_vector, cv.value.as_vector, dim * sizeof(float));
                }
                da_push(&r.cells, owned);
            }
            table_flat_append_row(dst, &r);
            row_free(&r);
        }
    }
    /* skip indexes — they will be rebuilt if needed */

    /* deep-copy kind-specific union fields */
    switch (src->kind) {
    case TABLE_MEMORY:
        break;
    case TABLE_VIEW:
        dst->view.sql = src->view.sql ? strdup(src->view.sql) : NULL;
        break;
    case TABLE_PARQUET:
        dst->parquet.path = src->parquet.path ? strdup(src->parquet.path) : NULL;
        memset(&dst->parquet.pq_cache, 0, sizeof(dst->parquet.pq_cache));
        break;
    case TABLE_DISK:
        dst->disk.dir_path = src->disk.dir_path ? strdup(src->disk.dir_path) : NULL;
        memset(&dst->disk.meta, 0, sizeof(dst->disk.meta));
        dst->disk.cache_valid = 0;
        dst->disk.wal_bytes = src->disk.wal_bytes;
        dst->disk.wal_dirty = src->disk.wal_dirty;
        break;
    }
}

int table_find_column_sv(struct table *t, sv name)
{
    /* exact match first */
    for (size_t i = 0; i < t->columns.count; i++) {
        if (sv_eq_cstr(name, t->columns.items[i].name))
            return (int)i;
    }
    /* case-insensitive exact match (SQL identifiers are case-insensitive) */
    for (size_t i = 0; i < t->columns.count; i++) {
        if (sv_eq_ignorecase_cstr(name, t->columns.items[i].name))
            return (int)i;
    }
    /* strip "table." prefix and retry */
    sv col = name;
    for (size_t k = 0; k < name.len; k++) {
        if (name.data[k] == '.') {
            col = sv_from(name.data + k + 1, name.len - k - 1);
            break;
        }
    }
    if (col.data != name.data) {
        for (size_t i = 0; i < t->columns.count; i++) {
            if (sv_eq_cstr(col, t->columns.items[i].name))
                return (int)i;
        }
    }
    /* try matching bare name against suffix of "table.col" stored names */
    for (size_t i = 0; i < t->columns.count; i++) {
        const char *cname = t->columns.items[i].name;
        const char *dot = strchr(cname, '.');
        if (dot) {
            const char *suffix = dot + 1;
            if (sv_eq_cstr(name, suffix))
                return (int)i;
        }
    }
    return -1;
}

int table_find_column(struct table *t, const char *name)
{
    return table_find_column_sv(t, sv_from(name, strlen(name)));
}

int resolve_alias_to_column(struct table *t, sv columns, sv alias)
{
    const char *p = columns.data;
    const char *end = columns.data + columns.len;
    while (p < end) {
        while (p < end && (*p == ' ' || *p == '\t' || *p == '\n')) p++;
        const char *col_start = p;
        while (p < end && *p != ' ' && *p != ',' && *p != '\t' && *p != '\n') p++;
        sv col_name = sv_from(col_start, (size_t)(p - col_start));
        while (p < end && (*p == ' ' || *p == '\t')) p++;
        if (p + 2 < end && (p[0] == 'A' || p[0] == 'a') &&
            (p[1] == 'S' || p[1] == 's') && (p[2] == ' ' || p[2] == '\t')) {
            p += 2;
            while (p < end && (*p == ' ' || *p == '\t')) p++;
            const char *alias_start = p;
            while (p < end && *p != ' ' && *p != ',' && *p != '\t' && *p != '\n' && *p != ';') p++;
            sv alias_name = sv_from(alias_start, (size_t)(p - alias_start));
            if (sv_eq_ignorecase(alias, alias_name))
                return table_find_column_sv(t, col_name);
        }
        while (p < end && *p != ',') p++;
        if (p < end) p++;
    }
    return -1;
}

void table_flat_init_schema(struct table *t)
{
    flat_table_free(&t->flat);
    uint16_t ncols = (uint16_t)t->columns.count;
    if (ncols == 0) return;
    flat_table_init(&t->flat, ncols, 16);
    for (uint16_t c = 0; c < ncols; c++) {
        t->flat.col_types[c] = t->columns.items[c].type;
        t->flat.col_vec_dims[c] = t->columns.items[c].vector_dim;
    }
    flat_table_alloc_cols(&t->flat);
}

void table_flat_append_row(struct table *t, const struct row *row)
{
    /* Lazy init: initialize flat storage from schema on first append */
    if (t->flat.ncols == 0 && t->columns.count > 0)
        table_flat_init_schema(t);
    uint16_t ncols = t->flat.ncols;
    if (ncols == 0) return;

    /* Grow if needed */
    if (t->flat.nrows >= t->flat.cap) {
        size_t new_cap = t->flat.cap * 2;
        if (new_cap < 16) new_cap = 16;
        flat_table_grow(&t->flat, new_cap);
    }

    size_t r = t->flat.nrows;
    for (uint16_t c = 0; c < ncols && c < (uint16_t)row->cells.count; c++) {
        const struct cell *cell = &row->cells.items[c];
        enum column_type ct = t->flat.col_types[c];
        if (cell->is_null) {
            t->flat.col_nulls[c][r] = 1;
            continue;
        }
        t->flat.col_nulls[c][r] = 0;
        /* Coerce TEXT "true"/"false" → BOOLEAN when schema expects BOOLEAN */
        int32_t coerced_bool = 0;
        if (ct == COLUMN_TYPE_BOOLEAN && cell->type == COLUMN_TYPE_TEXT && cell->value.as_text) {
            const char *tv = cell->value.as_text;
            coerced_bool = (strcasecmp(tv, "true") == 0 || strcasecmp(tv, "t") == 0 ||
                            strcmp(tv, "1") == 0) ? 1 : 0;
        }
        switch (ct) {
        case COLUMN_TYPE_SMALLINT:  ((int16_t *)t->flat.col_data[c])[r] = cell->value.as_smallint; break;
        case COLUMN_TYPE_INT:       ((int32_t *)t->flat.col_data[c])[r] = cell->value.as_int; break;
        case COLUMN_TYPE_BOOLEAN:
            ((int32_t *)t->flat.col_data[c])[r] = (cell->type == COLUMN_TYPE_BOOLEAN)
                ? cell->value.as_bool : coerced_bool; break;
        case COLUMN_TYPE_DATE:      ((int32_t *)t->flat.col_data[c])[r] = cell->value.as_date; break;
        case COLUMN_TYPE_BIGINT:    ((int64_t *)t->flat.col_data[c])[r] = cell->value.as_bigint; break;
        case COLUMN_TYPE_TIME:      ((int64_t *)t->flat.col_data[c])[r] = cell->value.as_time; break;
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ: ((int64_t *)t->flat.col_data[c])[r] = cell->value.as_timestamp; break;
        case COLUMN_TYPE_FLOAT:     ((double *)t->flat.col_data[c])[r] = cell->value.as_float; break;
        case COLUMN_TYPE_NUMERIC:   ((double *)t->flat.col_data[c])[r] = cell->value.as_numeric; break;
        case COLUMN_TYPE_INTERVAL:  ((struct interval *)t->flat.col_data[c])[r] = cell->value.as_interval; break;
        case COLUMN_TYPE_TEXT: {
            const char *prev = ((const char **)t->flat.col_data[c])[r];
            free((char *)prev);
            const char *dup = cell->value.as_text ? strdup(cell->value.as_text) : NULL;
            ((const char **)t->flat.col_data[c])[r] = dup;
            if (t->flat.col_str_lens && t->flat.col_str_lens[c] && dup)
                t->flat.col_str_lens[c][r] = (uint32_t)strlen(dup);
            break;
        }
        case COLUMN_TYPE_ENUM:      ((int32_t *)t->flat.col_data[c])[r] = cell->value.as_enum; break;
        case COLUMN_TYPE_UUID:      ((struct uuid_val *)t->flat.col_data[c])[r] = cell->value.as_uuid; break;
        case COLUMN_TYPE_VECTOR: {
            uint16_t dim = t->flat.col_vec_dims[c];
            if (cell->value.as_vector)
                memcpy(&((float *)t->flat.col_data[c])[r * dim], cell->value.as_vector, dim * sizeof(float));
            break;
        }
        }
    }
    t->flat.nrows++;
}

void table_flat_update_row(struct table *t, size_t row_idx, const struct row *row)
{
    if (row_idx >= t->flat.nrows || t->flat.ncols == 0) return;
    uint16_t ncols = t->flat.ncols;
    for (uint16_t c = 0; c < ncols && c < (uint16_t)row->cells.count; c++) {
        const struct cell *cell = &row->cells.items[c];
        enum column_type ct = t->flat.col_types[c];
        if (cell->is_null) { t->flat.col_nulls[c][row_idx] = 1; continue; }
        t->flat.col_nulls[c][row_idx] = 0;
        int32_t coerced_bool2 = 0;
        if (ct == COLUMN_TYPE_BOOLEAN && cell->type == COLUMN_TYPE_TEXT && cell->value.as_text) {
            const char *tv = cell->value.as_text;
            coerced_bool2 = (strcasecmp(tv, "true") == 0 || strcasecmp(tv, "t") == 0 ||
                             strcmp(tv, "1") == 0) ? 1 : 0;
        }
        switch (ct) {
        case COLUMN_TYPE_SMALLINT:  ((int16_t *)t->flat.col_data[c])[row_idx] = cell->value.as_smallint; break;
        case COLUMN_TYPE_INT:       ((int32_t *)t->flat.col_data[c])[row_idx] = cell->value.as_int; break;
        case COLUMN_TYPE_BOOLEAN:
            ((int32_t *)t->flat.col_data[c])[row_idx] = (cell->type == COLUMN_TYPE_BOOLEAN)
                ? cell->value.as_bool : coerced_bool2; break;
        case COLUMN_TYPE_DATE:      ((int32_t *)t->flat.col_data[c])[row_idx] = cell->value.as_date; break;
        case COLUMN_TYPE_BIGINT:    ((int64_t *)t->flat.col_data[c])[row_idx] = cell->value.as_bigint; break;
        case COLUMN_TYPE_TIME:      ((int64_t *)t->flat.col_data[c])[row_idx] = cell->value.as_time; break;
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ: ((int64_t *)t->flat.col_data[c])[row_idx] = cell->value.as_timestamp; break;
        case COLUMN_TYPE_FLOAT:     ((double *)t->flat.col_data[c])[row_idx] = cell->value.as_float; break;
        case COLUMN_TYPE_NUMERIC:   ((double *)t->flat.col_data[c])[row_idx] = cell->value.as_numeric; break;
        case COLUMN_TYPE_INTERVAL:  ((struct interval *)t->flat.col_data[c])[row_idx] = cell->value.as_interval; break;
        case COLUMN_TYPE_TEXT: {
            const char *prev = ((const char **)t->flat.col_data[c])[row_idx];
            free((char *)prev);
            const char *dup = cell->value.as_text ? strdup(cell->value.as_text) : NULL;
            ((const char **)t->flat.col_data[c])[row_idx] = dup;
            if (t->flat.col_str_lens && t->flat.col_str_lens[c] && dup)
                t->flat.col_str_lens[c][row_idx] = (uint32_t)strlen(dup);
            break;
        }
        case COLUMN_TYPE_ENUM:      ((int32_t *)t->flat.col_data[c])[row_idx] = cell->value.as_enum; break;
        case COLUMN_TYPE_UUID:      ((struct uuid_val *)t->flat.col_data[c])[row_idx] = cell->value.as_uuid; break;
        case COLUMN_TYPE_VECTOR: {
            uint16_t dim = t->flat.col_vec_dims[c];
            if (cell->value.as_vector)
                memcpy(&((float *)t->flat.col_data[c])[row_idx * dim], cell->value.as_vector, dim * sizeof(float));
            break;
        }
        }
    }
}

void table_flat_delete_row(struct table *t, size_t row_idx)
{
    if (row_idx >= t->flat.nrows || t->flat.ncols == 0) return;
    uint16_t ncols = t->flat.ncols;
    /* free individually-owned text strings in the row being deleted */
    for (uint16_t c = 0; c < ncols; c++) {
        if (t->flat.col_types[c] == COLUMN_TYPE_TEXT && !t->flat.col_nulls[c][row_idx]) {
            const char *s = ((const char **)t->flat.col_data[c])[row_idx];
            free((char *)s);
            ((const char **)t->flat.col_data[c])[row_idx] = NULL;
        }
    }
    size_t tail = t->flat.nrows - row_idx - 1;
    if (tail > 0) {
        for (uint16_t c = 0; c < ncols; c++) {
            size_t esz = col_type_elem_size(t->flat.col_types[c]);
            size_t mul = (t->flat.col_types[c] == COLUMN_TYPE_VECTOR) ? t->flat.col_vec_dims[c] : 1;
            size_t row_sz = esz * mul;
            uint8_t *data = (uint8_t *)t->flat.col_data[c];
            memmove(data + row_idx * row_sz, data + (row_idx + 1) * row_sz, tail * row_sz);
            memmove(t->flat.col_nulls[c] + row_idx, t->flat.col_nulls[c] + row_idx + 1, tail);
            if (t->flat.col_str_lens && t->flat.col_str_lens[c])
                memmove(t->flat.col_str_lens[c] + row_idx, t->flat.col_str_lens[c] + row_idx + 1,
                        tail * sizeof(uint32_t));
        }
    }
    t->flat.nrows--;
}

void table_flat_append_rows_bulk(struct table *t, struct row *rows, size_t count)
{
    if (count == 0) return;
    /* Lazy init */
    if (t->flat.ncols == 0 && t->columns.count > 0)
        table_flat_init_schema(t);
    uint16_t ncols = t->flat.ncols;
    if (ncols == 0) return;

    /* Pre-grow once */
    size_t needed = t->flat.nrows + count;
    if (needed > t->flat.cap) {
        size_t new_cap = t->flat.cap ? t->flat.cap : 16;
        while (new_cap < needed) new_cap *= 2;
        flat_table_grow(&t->flat, new_cap);
    }

    /* Batch append per column — tight typed loops */
    size_t base = t->flat.nrows;
    for (uint16_t c = 0; c < ncols; c++) {
        uint8_t *nulls = t->flat.col_nulls[c] + base;
        enum column_type ct = t->flat.col_types[c];
        switch (ct) {
        case COLUMN_TYPE_SMALLINT: {
            int16_t *dst = (int16_t *)t->flat.col_data[c] + base;
            for (size_t r = 0; r < count; r++) {
                const struct cell *cell = &rows[r].cells.items[c];
                nulls[r] = cell->is_null;
                if (!cell->is_null) dst[r] = cell->value.as_smallint;
            }
            break;
        }
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_ENUM: {
            int32_t *dst = (int32_t *)t->flat.col_data[c] + base;
            for (size_t r = 0; r < count; r++) {
                const struct cell *cell = &rows[r].cells.items[c];
                nulls[r] = cell->is_null;
                if (!cell->is_null) {
                    switch (ct) {
                    case COLUMN_TYPE_INT:     dst[r] = cell->value.as_int; break;
                    case COLUMN_TYPE_BOOLEAN: dst[r] = cell->value.as_bool; break;
                    case COLUMN_TYPE_DATE:    dst[r] = cell->value.as_date; break;
                    case COLUMN_TYPE_ENUM:    dst[r] = cell->value.as_enum; break;
                    case COLUMN_TYPE_SMALLINT: case COLUMN_TYPE_BIGINT:
                    case COLUMN_TYPE_TIME: case COLUMN_TYPE_TIMESTAMP:
                    case COLUMN_TYPE_TIMESTAMPTZ: case COLUMN_TYPE_FLOAT:
                    case COLUMN_TYPE_NUMERIC: case COLUMN_TYPE_INTERVAL:
                    case COLUMN_TYPE_TEXT: case COLUMN_TYPE_UUID:
                    case COLUMN_TYPE_VECTOR:
                        break;
                    }
                }
            }
            break;
        }
        case COLUMN_TYPE_BIGINT:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ: {
            int64_t *dst = (int64_t *)t->flat.col_data[c] + base;
            for (size_t r = 0; r < count; r++) {
                const struct cell *cell = &rows[r].cells.items[c];
                nulls[r] = cell->is_null;
                if (!cell->is_null) {
                    switch (ct) {
                    case COLUMN_TYPE_BIGINT:      dst[r] = cell->value.as_bigint; break;
                    case COLUMN_TYPE_TIME:        dst[r] = cell->value.as_time; break;
                    case COLUMN_TYPE_TIMESTAMP:
                    case COLUMN_TYPE_TIMESTAMPTZ: dst[r] = cell->value.as_timestamp; break;
                    case COLUMN_TYPE_SMALLINT: case COLUMN_TYPE_INT:
                    case COLUMN_TYPE_BOOLEAN: case COLUMN_TYPE_DATE:
                    case COLUMN_TYPE_FLOAT: case COLUMN_TYPE_NUMERIC:
                    case COLUMN_TYPE_INTERVAL: case COLUMN_TYPE_TEXT:
                    case COLUMN_TYPE_ENUM: case COLUMN_TYPE_UUID:
                    case COLUMN_TYPE_VECTOR:
                        break;
                    }
                }
            }
            break;
        }
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC: {
            double *dst = (double *)t->flat.col_data[c] + base;
            for (size_t r = 0; r < count; r++) {
                const struct cell *cell = &rows[r].cells.items[c];
                nulls[r] = cell->is_null;
                if (!cell->is_null) {
                    switch (ct) {
                    case COLUMN_TYPE_FLOAT:   dst[r] = cell->value.as_float; break;
                    case COLUMN_TYPE_NUMERIC: dst[r] = cell->value.as_numeric; break;
                    case COLUMN_TYPE_SMALLINT: case COLUMN_TYPE_INT:
                    case COLUMN_TYPE_BOOLEAN: case COLUMN_TYPE_DATE:
                    case COLUMN_TYPE_BIGINT: case COLUMN_TYPE_TIME:
                    case COLUMN_TYPE_TIMESTAMP: case COLUMN_TYPE_TIMESTAMPTZ:
                    case COLUMN_TYPE_INTERVAL: case COLUMN_TYPE_TEXT:
                    case COLUMN_TYPE_ENUM: case COLUMN_TYPE_UUID:
                    case COLUMN_TYPE_VECTOR:
                        break;
                    }
                }
            }
            break;
        }
        case COLUMN_TYPE_INTERVAL: {
            struct interval *dst = (struct interval *)t->flat.col_data[c] + base;
            for (size_t r = 0; r < count; r++) {
                const struct cell *cell = &rows[r].cells.items[c];
                nulls[r] = cell->is_null;
                if (!cell->is_null) dst[r] = cell->value.as_interval;
            }
            break;
        }
        case COLUMN_TYPE_TEXT: {
            const char **dst = (const char **)t->flat.col_data[c] + base;
            uint32_t *lens = (t->flat.col_str_lens && t->flat.col_str_lens[c])
                           ? t->flat.col_str_lens[c] + base : NULL;
            for (size_t r = 0; r < count; r++) {
                const struct cell *cell = &rows[r].cells.items[c];
                nulls[r] = cell->is_null;
                if (!cell->is_null) {
                    const char *dup = cell->value.as_text ? strdup(cell->value.as_text) : NULL;
                    dst[r] = dup;
                    if (lens && dup)
                        lens[r] = (uint32_t)strlen(dup);
                }
            }
            break;
        }
        case COLUMN_TYPE_UUID: {
            struct uuid_val *dst = (struct uuid_val *)t->flat.col_data[c] + base;
            for (size_t r = 0; r < count; r++) {
                const struct cell *cell = &rows[r].cells.items[c];
                nulls[r] = cell->is_null;
                if (!cell->is_null) dst[r] = cell->value.as_uuid;
            }
            break;
        }
        case COLUMN_TYPE_VECTOR: {
            uint16_t dim = t->flat.col_vec_dims[c];
            float *dst = (float *)t->flat.col_data[c] + base * dim;
            for (size_t r = 0; r < count; r++) {
                const struct cell *cell = &rows[r].cells.items[c];
                nulls[r] = cell->is_null;
                if (!cell->is_null && cell->value.as_vector)
                    memcpy(dst + r * dim, cell->value.as_vector, dim * sizeof(float));
            }
            break;
        }
        }
    }
    t->flat.nrows += count;
}

struct cell flat_cell_at_pub(const struct flat_table *ft, uint16_t c, size_t ri)
{
    struct cell cell = {0};
    if (c >= ft->ncols || ri >= ft->nrows) return cell;
    cell.type = ft->col_types[c];
    if (ft->col_nulls[c][ri]) { cell.is_null = 1; return cell; }
    switch (ft->col_types[c]) {
    case COLUMN_TYPE_SMALLINT:  cell.value.as_smallint = ((int16_t *)ft->col_data[c])[ri]; break;
    case COLUMN_TYPE_INT:       cell.value.as_int      = ((int32_t *)ft->col_data[c])[ri]; break;
    case COLUMN_TYPE_BOOLEAN:   cell.value.as_bool     = ((int32_t *)ft->col_data[c])[ri]; break;
    case COLUMN_TYPE_DATE:      cell.value.as_date     = ((int32_t *)ft->col_data[c])[ri]; break;
    case COLUMN_TYPE_BIGINT:    cell.value.as_bigint   = ((int64_t *)ft->col_data[c])[ri]; break;
    case COLUMN_TYPE_TIME:      cell.value.as_time     = ((int64_t *)ft->col_data[c])[ri]; break;
    case COLUMN_TYPE_TIMESTAMP: case COLUMN_TYPE_TIMESTAMPTZ:
        cell.value.as_timestamp = ((int64_t *)ft->col_data[c])[ri]; break;
    case COLUMN_TYPE_FLOAT:     cell.value.as_float    = ((double *)ft->col_data[c])[ri]; break;
    case COLUMN_TYPE_NUMERIC:   cell.value.as_numeric  = ((double *)ft->col_data[c])[ri]; break;
    case COLUMN_TYPE_INTERVAL:  cell.value.as_interval = ((struct interval *)ft->col_data[c])[ri]; break;
    case COLUMN_TYPE_TEXT:      cell.value.as_text     = ((char **)ft->col_data[c])[ri]; break;
    case COLUMN_TYPE_ENUM:      cell.value.as_enum     = ((int32_t *)ft->col_data[c])[ri]; break;
    case COLUMN_TYPE_UUID:      cell.value.as_uuid     = ((struct uuid_val *)ft->col_data[c])[ri]; break;
    case COLUMN_TYPE_VECTOR: {
        uint16_t dim = ft->col_vec_dims ? ft->col_vec_dims[c] : 0;
        cell.value.as_vector = dim ? &((float *)ft->col_data[c])[ri * dim] : NULL; break;
    }
    }
    return cell;
}

void table_free(struct table *t)
{
    free(t->name);
    for (size_t i = 0; i < t->columns.count; i++) {
        free(t->columns.items[i].name);
        free(t->columns.items[i].enum_type_name);
        free(t->columns.items[i].fk_table);
        free(t->columns.items[i].fk_column);
        free(t->columns.items[i].check_expr_sql);
        if (t->columns.items[i].default_value) {
            if (column_type_is_text(t->columns.items[i].default_value->type)
                && t->columns.items[i].default_value->value.as_text)
                free(t->columns.items[i].default_value->value.as_text);
            free(t->columns.items[i].default_value);
        }
    }
    da_free(&t->columns);

    for (size_t i = 0; i < t->indexes.count; i++)
        index_free(&t->indexes.items[i]);
    da_free(&t->indexes);
    flat_table_free(&t->flat);
    if (t->join_cache.valid) {
        flat_table_free(&t->join_cache.ft);
        free(t->join_cache.hashes);
        free(t->join_cache.nexts);
        free(t->join_cache.buckets);
    }

    /* Free kind-specific union fields */
    switch (t->kind) {
    case TABLE_MEMORY:
        break;
    case TABLE_VIEW:
        free(t->view.sql);
        break;
    case TABLE_PARQUET:
        free(t->parquet.path);
        if (t->parquet.pq_cache.valid) {
            for (uint16_t i = 0; i < t->parquet.pq_cache.ncols; i++) {
                if (t->parquet.pq_cache.col_types[i] == COLUMN_TYPE_TEXT ||
                    t->parquet.pq_cache.col_types[i] == COLUMN_TYPE_UUID) {
                    char **strs = (char **)t->parquet.pq_cache.col_data[i];
                    for (size_t r = 0; r < t->parquet.pq_cache.nrows; r++)
                        free(strs[r]);
                }
                free(t->parquet.pq_cache.col_data[i]);
                free(t->parquet.pq_cache.col_nulls[i]);
                if (t->parquet.pq_cache.col_str_lens)
                    free(t->parquet.pq_cache.col_str_lens[i]);
            }
            free(t->parquet.pq_cache.col_data);
            free(t->parquet.pq_cache.col_nulls);
            free(t->parquet.pq_cache.col_types);
            free(t->parquet.pq_cache.col_str_lens);
        }
        break;
    case TABLE_DISK:
        free(t->disk.dir_path);
#ifndef MSKQL_WASM
        disk_meta_free(&t->disk.meta);
#endif /* MSKQL_WASM */
        /* flat_table cache is freed above (shared flat field) */
        break;
    }
}
