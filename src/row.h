#ifndef ROW_H
#define ROW_H

#include <stdlib.h>
#include "dynamic_array.h"
#include "column.h"

union cell_value {
    int as_int;
    double as_float;
    char *as_text;
    int as_bool;
    long long as_bigint;
    double as_numeric;
    char *as_date;      /* "YYYY-MM-DD" */
    char *as_timestamp; /* "YYYY-MM-DD HH:MM:SS" */
    char *as_uuid;      /* "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" */
};

struct cell {
    enum column_type type;
    int is_null;
    union cell_value value;
};

struct row {
    DYNAMIC_ARRAY(struct cell) cells;
};

struct rows {
    struct row *data;
    size_t count;
    size_t capacity;
};

void rows_push(struct rows *rows, struct row row);
void rows_free(struct rows *rows);
void row_free(struct row *row);

#endif
