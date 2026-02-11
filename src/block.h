#ifndef BLOCK_H
#define BLOCK_H

#include <stdint.h>
#include <string.h>
#include "column.h"

/* Block capacity: 1024 rows per block.
 * 1024 × 8 bytes = 8 KB per numeric column — fits L1 cache.
 * A 4-column block is ~32 KB. */
#define BLOCK_CAPACITY 1024

/* Column block: a contiguous typed array for a single column.
 * All data is bump-allocated from arena->scratch — no per-block free. */
struct col_block {
    enum column_type type;
    uint16_t         count;                    /* 0..BLOCK_CAPACITY */
    uint8_t          nulls[BLOCK_CAPACITY];    /* 0=not-null, 1=null */
    union {
        int32_t   i32[BLOCK_CAPACITY];         /* INT, BOOLEAN */
        int64_t   i64[BLOCK_CAPACITY];         /* BIGINT */
        double    f64[BLOCK_CAPACITY];         /* FLOAT, NUMERIC */
        char     *str[BLOCK_CAPACITY];         /* TEXT types — pointers into table or bump slab */
    } data;
};

/* Row block: a set of column blocks representing a horizontal slice.
 * sel is an optional selection vector — when non-NULL, only sel[0..sel_count-1]
 * indices are "active". This avoids copying data during filtering. */
struct row_block {
    uint16_t          ncols;
    uint16_t          count;       /* total rows materialized in col_blocks */
    struct col_block *cols;        /* ncols col_blocks, bump-allocated */
    uint32_t         *sel;         /* selection vector (bump-allocated), or NULL */
    uint16_t          sel_count;   /* number of active entries in sel */
};

/* Arena-allocated hash table for joins, GROUP BY, DISTINCT.
 * All arrays are bump-allocated — no per-table free. */
struct block_hash_table {
    uint32_t  nbuckets;        /* power of 2 */
    uint32_t *buckets;         /* bump-allocated, nbuckets entries, IDX_NONE = empty */
    uint32_t *nexts;           /* bump-allocated, chain links per entry */
    uint32_t *hashes;          /* bump-allocated, cached hash per entry */
    uint32_t  capacity;        /* max entries before resize */
    uint32_t  count;           /* current number of entries */
};

/* ---- inline helpers ---- */

/* Reset a row_block for reuse (zero counts, keep allocated memory). */
static inline void row_block_reset(struct row_block *rb)
{
    rb->count = 0;
    for (uint16_t i = 0; i < rb->ncols; i++)
        rb->cols[i].count = 0;
    rb->sel = NULL;
    rb->sel_count = 0;
}

/* Get the effective row count (respects selection vector). */
static inline uint16_t row_block_active_count(const struct row_block *rb)
{
    return rb->sel ? rb->sel_count : rb->count;
}

/* Get the effective row index at position i (respects selection vector). */
static inline uint16_t row_block_row_idx(const struct row_block *rb, uint16_t i)
{
    return rb->sel ? (uint16_t)rb->sel[i] : i;
}

/* FNV-1a hash for int32 */
static inline uint32_t block_hash_i32(int32_t v)
{
    uint32_t h = 2166136261u;
    uint8_t *p = (uint8_t *)&v;
    for (int i = 0; i < 4; i++) {
        h ^= p[i];
        h *= 16777619u;
    }
    return h;
}

/* FNV-1a hash for int64 */
static inline uint32_t block_hash_i64(int64_t v)
{
    uint32_t h = 2166136261u;
    uint8_t *p = (uint8_t *)&v;
    for (int i = 0; i < 8; i++) {
        h ^= p[i];
        h *= 16777619u;
    }
    return h;
}

/* FNV-1a hash for double */
static inline uint32_t block_hash_f64(double v)
{
    uint32_t h = 2166136261u;
    uint8_t *p = (uint8_t *)&v;
    for (int i = 0; i < 8; i++) {
        h ^= p[i];
        h *= 16777619u;
    }
    return h;
}

/* FNV-1a hash for string */
static inline uint32_t block_hash_str(const char *s)
{
    uint32_t h = 2166136261u;
    if (!s) return h;
    while (*s) {
        h ^= (uint8_t)*s++;
        h *= 16777619u;
    }
    return h;
}

/* Hash a col_block value at index i */
static inline uint32_t block_hash_cell(const struct col_block *cb, uint16_t i)
{
    if (cb->nulls[i]) return 0;
    switch (cb->type) {
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:
            return block_hash_i32(cb->data.i32[i]);
        case COLUMN_TYPE_BIGINT:
            return block_hash_i64(cb->data.i64[i]);
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:
            return block_hash_f64(cb->data.f64[i]);
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID:
            return block_hash_str(cb->data.str[i]);
    }
    return 0;
}

/* Compare two col_block values: returns 1 if equal, 0 if not */
static inline int block_cell_eq(const struct col_block *a, uint16_t ai,
                                const struct col_block *b, uint16_t bi)
{
    if (a->nulls[ai] || b->nulls[bi]) return 0; /* NULL != anything */
    switch (a->type) {
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:
            return a->data.i32[ai] == b->data.i32[bi];
        case COLUMN_TYPE_BIGINT:
            return a->data.i64[ai] == b->data.i64[bi];
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:
            return a->data.f64[ai] == b->data.f64[bi];
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
        case COLUMN_TYPE_UUID:
            if (!a->data.str[ai] || !b->data.str[bi])
                return a->data.str[ai] == b->data.str[bi];
            return strcmp(a->data.str[ai], b->data.str[bi]) == 0;
    }
    return 0;
}

#endif
