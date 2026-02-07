#include "row.h"
#include <string.h>

void rows_push(struct rows *rows, struct row row)
{
    if (rows->count >= rows->capacity) {
        rows->capacity = rows->capacity == 0 ? 8 : rows->capacity * 2;
        rows->data = realloc(rows->data, rows->capacity * sizeof(struct row));
    }
    rows->data[rows->count++] = row;
}

void row_free(struct row *row)
{
    for (size_t i = 0; i < row->cells.count; i++) {
        struct cell *c = &row->cells.items[i];
        if ((c->type == COLUMN_TYPE_TEXT || c->type == COLUMN_TYPE_ENUM)
            && c->value.as_text) {
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
