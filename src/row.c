#include "row.h"
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

int cell_compare(const struct cell *a, const struct cell *b)
{
    /* NULL handling: NULLs sort after all non-NULL values */
    int a_null = a->is_null || (column_type_is_text(a->type) && !a->value.as_text);
    int b_null = b->is_null || (column_type_is_text(b->type) && !b->value.as_text);
    if (a_null && b_null) return 0;
    if (a_null) return 1;  /* NULL sorts last */
    if (b_null) return -1;
    /* promote INT <-> FLOAT for numeric comparison */
    if ((a->type == COLUMN_TYPE_INT && b->type == COLUMN_TYPE_FLOAT) ||
        (a->type == COLUMN_TYPE_FLOAT && b->type == COLUMN_TYPE_INT)) {
        double da = (a->type == COLUMN_TYPE_FLOAT) ? a->value.as_float : (double)a->value.as_int;
        double db = (b->type == COLUMN_TYPE_FLOAT) ? b->value.as_float : (double)b->value.as_int;
        if (da < db) return -1;
        if (da > db) return  1;
        return 0;
    }
    if (a->type != b->type) return -2; /* incompatible types */
    switch (a->type) {
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
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_UUID:
            if (!a->value.as_text && !b->value.as_text) return 0;
            if (!a->value.as_text) return -1;
            if (!b->value.as_text) return  1;
            return strcmp(a->value.as_text, b->value.as_text);
    }
    return -2;
}

int cell_equal(const struct cell *a, const struct cell *b)
{
    /* SQL standard: NULL is never equal to anything, including NULL */
    int a_null = a->is_null || (column_type_is_text(a->type) && !a->value.as_text);
    int b_null = b->is_null || (column_type_is_text(b->type) && !b->value.as_text);
    if (a_null || b_null) return 0;
    /* promote INT <-> FLOAT */
    if ((a->type == COLUMN_TYPE_INT && b->type == COLUMN_TYPE_FLOAT) ||
        (a->type == COLUMN_TYPE_FLOAT && b->type == COLUMN_TYPE_INT)) {
        double da = (a->type == COLUMN_TYPE_FLOAT) ? a->value.as_float : (double)a->value.as_int;
        double db = (b->type == COLUMN_TYPE_FLOAT) ? b->value.as_float : (double)b->value.as_int;
        return da == db;
    }
    if (a->type != b->type) return 0;
    switch (a->type) {
        case COLUMN_TYPE_INT:     return a->value.as_int == b->value.as_int;
        case COLUMN_TYPE_FLOAT:   return a->value.as_float == b->value.as_float;
        case COLUMN_TYPE_BOOLEAN: return a->value.as_bool == b->value.as_bool;
        case COLUMN_TYPE_BIGINT:  return a->value.as_bigint == b->value.as_bigint;
        case COLUMN_TYPE_NUMERIC: return a->value.as_numeric == b->value.as_numeric;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_UUID:
            if (!a->value.as_text || !b->value.as_text) return a->value.as_text == b->value.as_text;
            return strcmp(a->value.as_text, b->value.as_text) == 0;
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
