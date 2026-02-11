#ifndef ROW_H
#define ROW_H

#include <stdlib.h>
#include "dynamic_array.h"
#include "column.h"

struct bump_alloc; /* forward declaration — defined in arena.h */

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
    int arena_owns_text; /* when set, text cells are bump-allocated — skip per-cell free */
};

void rows_push(struct rows *rows, struct row row);
void rows_free(struct rows *rows);
void row_free(struct row *row);

/* shared cell helpers — single canonical implementations */
int  cell_compare(const struct cell *a, const struct cell *b);
int  cell_equal(const struct cell *a, const struct cell *b);
int  cell_equal_nullsafe(const struct cell *a, const struct cell *b);
void cell_copy(struct cell *dst, const struct cell *src);
void cell_copy_bump(struct cell *dst, const struct cell *src, struct bump_alloc *b);
void cell_free_text(struct cell *c);

/* row-level equality (same cell count and all cells equal) */
int  row_equal(const struct row *a, const struct row *b);
int  row_equal_nullsafe(const struct row *a, const struct row *b);

#endif
