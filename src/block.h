#ifndef BLOCK_H
#define BLOCK_H

#include <stdint.h>
#include <string.h>
#include "column.h"
#include "datetime.h"
#include "uuid.h"

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
        int16_t          i16[BLOCK_CAPACITY];         /* SMALLINT */
        int32_t          i32[BLOCK_CAPACITY];         /* INT, BOOLEAN, DATE */
        int64_t          i64[BLOCK_CAPACITY];         /* BIGINT, TIMESTAMP, TIMESTAMPTZ, TIME */
        double           f64[BLOCK_CAPACITY];         /* FLOAT, NUMERIC */
        char            *str[BLOCK_CAPACITY];         /* TEXT, ENUM — pointers into table or bump slab */
        struct interval  iv[BLOCK_CAPACITY];           /* INTERVAL */
        struct uuid_val  uuid[BLOCK_CAPACITY];          /* UUID — 16-byte binary */
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

/* Element size for a column type's data storage. */
static inline size_t col_type_elem_size(enum column_type ct)
{
    switch (column_type_storage(ct)) {
    case STORE_I16:   return sizeof(int16_t);
    case STORE_I32:   return sizeof(int32_t);
    case STORE_I64:   return sizeof(int64_t);
    case STORE_F64:   return sizeof(double);
    case STORE_STR:   return sizeof(char *);
    case STORE_IV:    return sizeof(struct interval);
    case STORE_UUID:  return sizeof(struct uuid_val);
    }
    __builtin_unreachable();
}

/* Pointer to the data element at index i in a col_block (cast to void*). */
static inline void *cb_data_ptr(const struct col_block *cb, uint32_t i)
{
    switch (column_type_storage(cb->type)) {
    case STORE_I16:   return (void *)&cb->data.i16[i];
    case STORE_I32:   return (void *)&cb->data.i32[i];
    case STORE_I64:   return (void *)&cb->data.i64[i];
    case STORE_F64:   return (void *)&cb->data.f64[i];
    case STORE_STR:   return (void *)&cb->data.str[i];
    case STORE_IV:    return (void *)&cb->data.iv[i];
    case STORE_UUID:  return (void *)&cb->data.uuid[i];
    }
    __builtin_unreachable();
}

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

#define FNV_OFFSET 2166136261u
#define FNV_PRIME  16777619u

/* FNV-1a hash for int32 */
static inline uint32_t block_hash_i32(int32_t v)
{
    uint32_t h = FNV_OFFSET;
    uint8_t *p = (uint8_t *)&v;
    for (int i = 0; i < (int)sizeof(int32_t); i++) {
        h ^= p[i];
        h *= FNV_PRIME;
    }
    return h;
}

/* FNV-1a hash for int64 */
static inline uint32_t block_hash_i64(int64_t v)
{
    uint32_t h = FNV_OFFSET;
    uint8_t *p = (uint8_t *)&v;
    for (int i = 0; i < (int)sizeof(int64_t); i++) {
        h ^= p[i];
        h *= FNV_PRIME;
    }
    return h;
}

/* FNV-1a hash for double */
static inline uint32_t block_hash_f64(double v)
{
    uint32_t h = FNV_OFFSET;
    uint8_t *p = (uint8_t *)&v;
    for (int i = 0; i < (int)sizeof(double); i++) {
        h ^= p[i];
        h *= FNV_PRIME;
    }
    return h;
}

/* FNV-1a hash for string */
static inline uint32_t block_hash_str(const char *s)
{
    uint32_t h = FNV_OFFSET;
    if (!s) return h;
    while (*s) {
        h ^= (uint8_t)*s++;
        h *= FNV_PRIME;
    }
    return h;
}

/* Hash a col_block value at index i */
static inline uint32_t block_hash_cell(const struct col_block *cb, uint16_t i)
{
    if (cb->nulls[i]) return 0;
    switch (cb->type) {
        case COLUMN_TYPE_SMALLINT:
            return block_hash_i32((int32_t)cb->data.i16[i]);
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:
        case COLUMN_TYPE_DATE:
            return block_hash_i32(cb->data.i32[i]);
        case COLUMN_TYPE_BIGINT:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_TIME:
            return block_hash_i64(cb->data.i64[i]);
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:
            return block_hash_f64(cb->data.f64[i]);
        case COLUMN_TYPE_TEXT:
            return block_hash_str(cb->data.str[i]);
        case COLUMN_TYPE_ENUM:
            return block_hash_i32(cb->data.i32[i]);
        case COLUMN_TYPE_UUID: {
            uint64_t uh = uuid_hash(cb->data.uuid[i]);
            return (uint32_t)(uh ^ (uh >> 32));
        }
        case COLUMN_TYPE_INTERVAL: {
            /* hash all 16 bytes of the interval struct */
            uint32_t h = FNV_OFFSET;
            uint8_t *p = (uint8_t *)&cb->data.iv[i];
            for (int j = 0; j < (int)sizeof(struct interval); j++) {
                h ^= p[j]; h *= FNV_PRIME;
            }
            return h;
        }
    }
    __builtin_unreachable();
}

/* Compare two col_block values: returns 1 if equal, 0 if not */
static inline int block_cell_eq(const struct col_block *a, uint16_t ai,
                                const struct col_block *b, uint16_t bi)
{
    if (a->nulls[ai] || b->nulls[bi]) return 0; /* NULL != anything */
    switch (a->type) {
        case COLUMN_TYPE_SMALLINT:
            return a->data.i16[ai] == b->data.i16[bi];
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOLEAN:
        case COLUMN_TYPE_DATE:
            return a->data.i32[ai] == b->data.i32[bi];
        case COLUMN_TYPE_BIGINT:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_TIME:
            return a->data.i64[ai] == b->data.i64[bi];
        case COLUMN_TYPE_FLOAT:
        case COLUMN_TYPE_NUMERIC:
            return a->data.f64[ai] == b->data.f64[bi];
        case COLUMN_TYPE_TEXT:
            if (!a->data.str[ai] || !b->data.str[bi])
                return a->data.str[ai] == b->data.str[bi];
            return strcmp(a->data.str[ai], b->data.str[bi]) == 0;
        case COLUMN_TYPE_ENUM:
            return a->data.i32[ai] == b->data.i32[bi];
        case COLUMN_TYPE_UUID:
            return uuid_equal(a->data.uuid[ai], b->data.uuid[bi]);
        case COLUMN_TYPE_INTERVAL:
            return a->data.iv[ai].months == b->data.iv[bi].months &&
                   a->data.iv[ai].days == b->data.iv[bi].days &&
                   a->data.iv[ai].usec == b->data.iv[bi].usec;
    }
    __builtin_unreachable();
}

#endif
