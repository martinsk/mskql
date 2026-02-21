#ifndef ROW_H
#define ROW_H

#include <stdlib.h>
#include <stdint.h>
#include "dynamic_array.h"
#include "column.h"
#include "datetime.h"
#include "uuid.h"

struct bump_alloc; /* forward declaration — defined in arena.h */

union cell_value {
    int16_t as_smallint;
    int as_int;
    double as_float;
    char *as_text;
    int as_bool;
    long long as_bigint;
    double as_numeric;
    int32_t as_date;            /* days since 2000-01-01 */
    int64_t as_timestamp;       /* microseconds since 2000-01-01 00:00:00 */
    int64_t as_time;            /* microseconds since midnight */
    struct interval as_interval;
    struct uuid_val as_uuid;
    int32_t as_enum;            /* 0-based ordinal into enum_type.values */
};

struct cell {
    enum column_type type;
    int is_null;
    union cell_value value;
    int8_t numeric_scale; /* for NUMERIC/ROUND: decimal places to display (-1 = auto) */
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
int  cells_compare(const struct cell *a, const struct cell *b, int ncols);
int  cell_equal(const struct cell *a, const struct cell *b);
int  cell_equal_nullsafe(const struct cell *a, const struct cell *b);
void cell_copy(struct cell *dst, const struct cell *src);
void cell_copy_bump(struct cell *dst, const struct cell *src, struct bump_alloc *b);
void cell_free_text(struct cell *c);

/* row-level equality (same cell count and all cells equal) */
int  row_equal(const struct row *a, const struct row *b);
int  row_equal_nullsafe(const struct row *a, const struct row *b);

#endif
