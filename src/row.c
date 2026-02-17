#include "row.h"
#include "arena.h"
#include "column.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---- shared cell helpers ---- */

void cell_free_text(struct cell *c)
{
    if (column_type_is_text(c->type) && c->value.as_text)
        free(c->value.as_text);
}

void cell_copy(struct cell *dst, const struct cell *src)
{
    dst->type = src->type;
    dst->is_null = src->is_null;
    if (column_type_is_text(src->type) && src->value.as_text) {
        dst->value.as_text = strdup(src->value.as_text);
    } else {
        dst->value = src->value;
    }
}

void cell_copy_bump(struct cell *dst, const struct cell *src, struct bump_alloc *b)
{
    dst->type = src->type;
    dst->is_null = src->is_null;
    if (column_type_is_text(src->type) && src->value.as_text) {
        dst->value.as_text = bump_strdup(b, src->value.as_text);
    } else {
        dst->value = src->value;
    }
}

int cell_compare(const struct cell *a, const struct cell *b)
{
    /* NULL handling: NULLs sort after all non-NULL values */
    int a_null = a->is_null || (column_type_is_text(a->type) && !a->value.as_text);
    int b_null = b->is_null || (column_type_is_text(b->type) && !b->value.as_text);
    if (a_null && b_null) return 0;
    if (a_null) return 1;  /* NULL sorts last */
    if (b_null) return -1;
    /* promote SMALLINT/INT/BIGINT <-> FLOAT for numeric comparison */
    int a_is_int = (a->type == COLUMN_TYPE_INT || a->type == COLUMN_TYPE_SMALLINT || a->type == COLUMN_TYPE_BIGINT);
    int b_is_int = (b->type == COLUMN_TYPE_INT || b->type == COLUMN_TYPE_SMALLINT || b->type == COLUMN_TYPE_BIGINT);
    if ((a_is_int && b->type == COLUMN_TYPE_FLOAT) ||
        (a->type == COLUMN_TYPE_FLOAT && b_is_int)) {
        double da = (a->type == COLUMN_TYPE_FLOAT) ? a->value.as_float :
                    (a->type == COLUMN_TYPE_BIGINT) ? (double)a->value.as_bigint :
                    (a->type == COLUMN_TYPE_SMALLINT) ? (double)a->value.as_smallint : (double)a->value.as_int;
        double db = (b->type == COLUMN_TYPE_FLOAT) ? b->value.as_float :
                    (b->type == COLUMN_TYPE_BIGINT) ? (double)b->value.as_bigint :
                    (b->type == COLUMN_TYPE_SMALLINT) ? (double)b->value.as_smallint : (double)b->value.as_int;
        if (da < db) return -1;
        if (da > db) return  1;
        return 0;
    }
    /* promote SMALLINT/INT <-> BIGINT */
    if (a_is_int && b_is_int && a->type != b->type) {
        long long va = (a->type == COLUMN_TYPE_BIGINT) ? (long long)a->value.as_bigint :
                       (a->type == COLUMN_TYPE_SMALLINT) ? (long long)a->value.as_smallint :
                       (long long)a->value.as_int;
        long long vb = (b->type == COLUMN_TYPE_BIGINT) ? (long long)b->value.as_bigint :
                       (b->type == COLUMN_TYPE_SMALLINT) ? (long long)b->value.as_smallint :
                       (long long)b->value.as_int;
        if (va < vb) return -1;
        if (va > vb) return  1;
        return 0;
    }
    /* allow cross-comparison between text-based types (TEXT, ENUM) */
    if (a->type != b->type) {
        if (column_type_is_text(a->type) && column_type_is_text(b->type)) {
            if (!a->value.as_text && !b->value.as_text) return 0;
            if (!a->value.as_text) return -1;
            if (!b->value.as_text) return  1;
            int cmp = strcmp(a->value.as_text, b->value.as_text);
            return (cmp < 0) ? -1 : (cmp > 0) ? 1 : 0;
        }
        /* DATE vs TIMESTAMP: promote date to midnight timestamp */
        if ((a->type == COLUMN_TYPE_DATE && (b->type == COLUMN_TYPE_TIMESTAMP || b->type == COLUMN_TYPE_TIMESTAMPTZ)) ||
            ((a->type == COLUMN_TYPE_TIMESTAMP || a->type == COLUMN_TYPE_TIMESTAMPTZ) && b->type == COLUMN_TYPE_DATE)) {
            int64_t va = (a->type == COLUMN_TYPE_DATE) ? (int64_t)a->value.as_date * USEC_PER_DAY : a->value.as_timestamp;
            int64_t vb = (b->type == COLUMN_TYPE_DATE) ? (int64_t)b->value.as_date * USEC_PER_DAY : b->value.as_timestamp;
            if (va < vb) return -1;
            if (va > vb) return  1;
            return 0;
        }
        /* Temporal vs TEXT: coerce text to temporal for comparison */
        if (column_type_is_temporal(a->type) && column_type_is_text(b->type) && b->value.as_text) {
            struct cell coerced = *b;
            const char *s = b->value.as_text;
            switch (a->type) {
            case COLUMN_TYPE_DATE:        coerced.type = a->type; coerced.value.as_date = date_from_str(s); break;
            case COLUMN_TYPE_TIME:        coerced.type = a->type; coerced.value.as_time = time_from_str(s); break;
            case COLUMN_TYPE_TIMESTAMP:
            case COLUMN_TYPE_TIMESTAMPTZ: coerced.type = a->type; coerced.value.as_timestamp = timestamp_from_str(s); break;
            case COLUMN_TYPE_INTERVAL:    coerced.type = a->type; coerced.value.as_interval = interval_from_str(s); break;
            case COLUMN_TYPE_SMALLINT: case COLUMN_TYPE_INT: case COLUMN_TYPE_BIGINT:
            case COLUMN_TYPE_FLOAT: case COLUMN_TYPE_NUMERIC: case COLUMN_TYPE_BOOLEAN:
            case COLUMN_TYPE_TEXT: case COLUMN_TYPE_ENUM: case COLUMN_TYPE_UUID: break;
            }
            return cell_compare(a, &coerced);
        }
        if (column_type_is_temporal(b->type) && column_type_is_text(a->type) && a->value.as_text) {
            struct cell coerced = *a;
            const char *s = a->value.as_text;
            switch (b->type) {
            case COLUMN_TYPE_DATE:        coerced.type = b->type; coerced.value.as_date = date_from_str(s); break;
            case COLUMN_TYPE_TIME:        coerced.type = b->type; coerced.value.as_time = time_from_str(s); break;
            case COLUMN_TYPE_TIMESTAMP:
            case COLUMN_TYPE_TIMESTAMPTZ: coerced.type = b->type; coerced.value.as_timestamp = timestamp_from_str(s); break;
            case COLUMN_TYPE_INTERVAL:    coerced.type = b->type; coerced.value.as_interval = interval_from_str(s); break;
            case COLUMN_TYPE_SMALLINT: case COLUMN_TYPE_INT: case COLUMN_TYPE_BIGINT:
            case COLUMN_TYPE_FLOAT: case COLUMN_TYPE_NUMERIC: case COLUMN_TYPE_BOOLEAN:
            case COLUMN_TYPE_TEXT: case COLUMN_TYPE_ENUM: case COLUMN_TYPE_UUID: break;
            }
            return cell_compare(&coerced, b);
        }
        /* INT/BIGINT vs TEXT: coerce text to number for comparison */
        int a_num = (a->type == COLUMN_TYPE_INT || a->type == COLUMN_TYPE_SMALLINT || a->type == COLUMN_TYPE_BIGINT);
        int b_num = (b->type == COLUMN_TYPE_INT || b->type == COLUMN_TYPE_SMALLINT || b->type == COLUMN_TYPE_BIGINT);
        if (a_num && column_type_is_text(b->type) && b->value.as_text) {
            long long va = (a->type == COLUMN_TYPE_BIGINT) ? (long long)a->value.as_bigint :
                           (a->type == COLUMN_TYPE_SMALLINT) ? (long long)a->value.as_smallint :
                           (long long)a->value.as_int;
            long long vb = atoll(b->value.as_text);
            if (va < vb) return -1;
            if (va > vb) return  1;
            return 0;
        }
        if (b_num && column_type_is_text(a->type) && a->value.as_text) {
            long long va = atoll(a->value.as_text);
            long long vb = (b->type == COLUMN_TYPE_BIGINT) ? (long long)b->value.as_bigint :
                           (b->type == COLUMN_TYPE_SMALLINT) ? (long long)b->value.as_smallint :
                           (long long)b->value.as_int;
            if (va < vb) return -1;
            if (va > vb) return  1;
            return 0;
        }
        return -2; /* incompatible types */
    }
    switch (a->type) {
        case COLUMN_TYPE_SMALLINT:
            if (a->value.as_smallint < b->value.as_smallint) return -1;
            if (a->value.as_smallint > b->value.as_smallint) return  1;
            return 0;
        case COLUMN_TYPE_INT:
            if (a->value.as_int < b->value.as_int) return -1;
            if (a->value.as_int > b->value.as_int) return  1;
            return 0;
        case COLUMN_TYPE_FLOAT:
            if (a->value.as_float < b->value.as_float) return -1;
            if (a->value.as_float > b->value.as_float) return  1;
            return 0;
        case COLUMN_TYPE_BOOLEAN:
            if (a->value.as_bool < b->value.as_bool) return -1;
            if (a->value.as_bool > b->value.as_bool) return  1;
            return 0;
        case COLUMN_TYPE_BIGINT:
            if (a->value.as_bigint < b->value.as_bigint) return -1;
            if (a->value.as_bigint > b->value.as_bigint) return  1;
            return 0;
        case COLUMN_TYPE_NUMERIC:
            if (a->value.as_numeric < b->value.as_numeric) return -1;
            if (a->value.as_numeric > b->value.as_numeric) return  1;
            return 0;
        case COLUMN_TYPE_DATE:
            if (a->value.as_date < b->value.as_date) return -1;
            if (a->value.as_date > b->value.as_date) return  1;
            return 0;
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
            if (a->value.as_timestamp < b->value.as_timestamp) return -1;
            if (a->value.as_timestamp > b->value.as_timestamp) return  1;
            return 0;
        case COLUMN_TYPE_INTERVAL:
            return interval_compare(a->value.as_interval, b->value.as_interval);
        case COLUMN_TYPE_TEXT:
            if (!a->value.as_text && !b->value.as_text) return 0;
            if (!a->value.as_text) return -1;
            if (!b->value.as_text) return  1;
            { int cmp = strcmp(a->value.as_text, b->value.as_text);
              return (cmp < 0) ? -1 : (cmp > 0) ? 1 : 0; }
        case COLUMN_TYPE_ENUM:
            if (a->value.as_enum < b->value.as_enum) return -1;
            if (a->value.as_enum > b->value.as_enum) return  1;
            return 0;
        case COLUMN_TYPE_UUID:
            return uuid_compare(a->value.as_uuid, b->value.as_uuid);
    }
    return -2;
}

int cell_equal(const struct cell *a, const struct cell *b)
{
    /* SQL standard: NULL is never equal to anything, including NULL */
    int a_null = a->is_null || (column_type_is_text(a->type) && !a->value.as_text);
    int b_null = b->is_null || (column_type_is_text(b->type) && !b->value.as_text);
    if (a_null || b_null) return 0;
    /* promote SMALLINT/INT <-> FLOAT */
    int a_is_int2 = (a->type == COLUMN_TYPE_INT || a->type == COLUMN_TYPE_SMALLINT);
    int b_is_int2 = (b->type == COLUMN_TYPE_INT || b->type == COLUMN_TYPE_SMALLINT);
    if ((a_is_int2 && b->type == COLUMN_TYPE_FLOAT) ||
        (a->type == COLUMN_TYPE_FLOAT && b_is_int2)) {
        double da = (a->type == COLUMN_TYPE_FLOAT) ? a->value.as_float :
                    (a->type == COLUMN_TYPE_SMALLINT) ? (double)a->value.as_smallint : (double)a->value.as_int;
        double db = (b->type == COLUMN_TYPE_FLOAT) ? b->value.as_float :
                    (b->type == COLUMN_TYPE_SMALLINT) ? (double)b->value.as_smallint : (double)b->value.as_int;
        return da == db;
    }
    /* promote SMALLINT <-> INT */
    if ((a->type == COLUMN_TYPE_SMALLINT && b->type == COLUMN_TYPE_INT) ||
        (a->type == COLUMN_TYPE_INT && b->type == COLUMN_TYPE_SMALLINT)) {
        int va = (a->type == COLUMN_TYPE_SMALLINT) ? (int)a->value.as_smallint : a->value.as_int;
        int vb = (b->type == COLUMN_TYPE_SMALLINT) ? (int)b->value.as_smallint : b->value.as_int;
        return va == vb;
    }
    if (a->type != b->type) return 0;
    switch (a->type) {
        case COLUMN_TYPE_SMALLINT: return a->value.as_smallint == b->value.as_smallint;
        case COLUMN_TYPE_INT:     return a->value.as_int == b->value.as_int;
        case COLUMN_TYPE_FLOAT:   return a->value.as_float == b->value.as_float;
        case COLUMN_TYPE_BOOLEAN: return a->value.as_bool == b->value.as_bool;
        case COLUMN_TYPE_BIGINT:  return a->value.as_bigint == b->value.as_bigint;
        case COLUMN_TYPE_NUMERIC: return a->value.as_numeric == b->value.as_numeric;
        case COLUMN_TYPE_DATE:
            return a->value.as_date == b->value.as_date;
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
            return a->value.as_timestamp == b->value.as_timestamp;
        case COLUMN_TYPE_INTERVAL:
            return a->value.as_interval.months == b->value.as_interval.months &&
                   a->value.as_interval.days == b->value.as_interval.days &&
                   a->value.as_interval.usec == b->value.as_interval.usec;
        case COLUMN_TYPE_TEXT:
            if (!a->value.as_text || !b->value.as_text) return a->value.as_text == b->value.as_text;
            return strcmp(a->value.as_text, b->value.as_text) == 0;
        case COLUMN_TYPE_ENUM:
            return a->value.as_enum == b->value.as_enum;
        case COLUMN_TYPE_UUID:
            return uuid_equal(a->value.as_uuid, b->value.as_uuid);
    }
    return 0;
}

int cell_equal_nullsafe(const struct cell *a, const struct cell *b)
{
    /* NULL-safe: two NULLs are considered equal */
    int a_null = a->is_null || (column_type_is_text(a->type) && !a->value.as_text);
    int b_null = b->is_null || (column_type_is_text(b->type) && !b->value.as_text);
    if (a_null && b_null) return 1;
    if (a_null || b_null) return 0;
    /* delegate to regular equality for non-NULL values */
    return cell_equal(a, b);
}

int row_equal(const struct row *a, const struct row *b)
{
    if (a->cells.count != b->cells.count) return 0;
    for (size_t i = 0; i < a->cells.count; i++) {
        if (!cell_equal(&a->cells.items[i], &b->cells.items[i]))
            return 0;
    }
    return 1;
}

int row_equal_nullsafe(const struct row *a, const struct row *b)
{
    if (a->cells.count != b->cells.count) return 0;
    for (size_t i = 0; i < a->cells.count; i++) {
        if (!cell_equal_nullsafe(&a->cells.items[i], &b->cells.items[i]))
            return 0;
    }
    return 1;
}

void rows_push(struct rows *rows, struct row row)
{
    if (rows->count >= rows->capacity) {
        rows->capacity = rows->capacity == 0 ? 8 : rows->capacity * 2;
        void *tmp = realloc(rows->data, rows->capacity * sizeof(struct row));
        if (!tmp) { fprintf(stderr, "rows_push: out of memory\n"); abort(); }
        rows->data = tmp;
    }
    rows->data[rows->count++] = row;
}

void row_free(struct row *row)
{
    for (size_t i = 0; i < row->cells.count; i++) {
        struct cell *c = &row->cells.items[i];
        if (column_type_is_text(c->type) && c->value.as_text) {
            free(c->value.as_text);
        }
    }
    da_free(&row->cells);
}

void rows_free(struct rows *rows)
{
    for (size_t i = 0; i < rows->count; i++) {
        row_free(&rows->data[i]);
    }
    free(rows->data);
    rows->data = NULL;
    rows->count = 0;
    rows->capacity = 0;
}
