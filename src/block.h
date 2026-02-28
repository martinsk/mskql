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
 * All data is bump-allocated from arena->scratch — no per-block free.
 * str_lens: bump-allocated uint32_t[BLOCK_CAPACITY], non-NULL only for TEXT columns.
 *   str_lens[i] == strlen(data.str[i]) when data.str[i] != NULL.
 *   NULL means lengths are unknown — callers must fall back to strlen. */
struct col_block {
    enum column_type type;
    uint16_t         count;                    /* 0..BLOCK_CAPACITY */
    uint16_t         vec_dim;                  /* VECTOR only: dimension per element */
    uint8_t          nulls[BLOCK_CAPACITY];    /* 0=not-null, 1=null */
    uint32_t        *str_lens;                 /* TEXT only: bump-alloc'd lengths, or NULL */
    union {
        int16_t          i16[BLOCK_CAPACITY];         /* SMALLINT */
        int32_t          i32[BLOCK_CAPACITY];         /* INT, BOOLEAN, DATE */
        int64_t          i64[BLOCK_CAPACITY];         /* BIGINT, TIMESTAMP, TIMESTAMPTZ, TIME */
        double           f64[BLOCK_CAPACITY];         /* FLOAT, NUMERIC */
        char            *str[BLOCK_CAPACITY];         /* TEXT, ENUM — pointers into table or bump slab */
        struct interval  iv[BLOCK_CAPACITY];           /* INTERVAL */
        struct uuid_val  uuid[BLOCK_CAPACITY];          /* UUID — 16-byte binary */
        float           *vec;                          /* VECTOR — bump-alloc'd float[dim * BLOCK_CAPACITY] */
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

/* Element size for a column type's data storage.
 * For VECTOR, returns sizeof(float) — caller must multiply by dim for per-row size. */
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
    case STORE_VEC:   return sizeof(float);
    }
    __builtin_unreachable();
}

/* Per-row element size for a col_block, accounting for VECTOR dimension. */
static inline size_t cb_elem_size(const struct col_block *cb)
{
    if (cb->type == COLUMN_TYPE_VECTOR)
        return (size_t)cb->vec_dim * sizeof(float);
    return col_type_elem_size(cb->type);
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
    case STORE_VEC:   return (void *)&cb->data.vec[i * cb->vec_dim];
    }
    __builtin_unreachable();
}

/* Reset a row_block for reuse (zero counts, keep allocated memory).
 * Clears str_lens to prevent stale pointers across plan_next_block calls. */
static inline void row_block_reset(struct row_block *rb)
{
    rb->count = 0;
    for (uint16_t i = 0; i < rb->ncols; i++) {
        rb->cols[i].count = 0;
        rb->cols[i].str_lens = NULL;
    }
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

/* FNV-1a hash for string (null-terminated, length unknown) */
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

/* FNV-1a hash for string with known length — avoids null scan */
static inline uint32_t block_hash_str_n(const char *s, uint32_t len)
{
    uint32_t h = FNV_OFFSET;
    if (!s) return h;
    for (uint32_t i = 0; i < len; i++) {
        h ^= (uint8_t)s[i];
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
            if (cb->str_lens)
                return block_hash_str_n(cb->data.str[i], cb->str_lens[i]);
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
        case COLUMN_TYPE_VECTOR: {
            uint32_t h = FNV_OFFSET;
            uint8_t *p = (uint8_t *)&cb->data.vec[i * cb->vec_dim];
            for (int j = 0; j < (int)(cb->vec_dim * sizeof(float)); j++) {
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
            if (a->str_lens && b->str_lens) {
                if (a->str_lens[ai] != b->str_lens[bi]) return 0;
                return memcmp(a->data.str[ai], b->data.str[bi], a->str_lens[ai]) == 0;
            }
            return strcmp(a->data.str[ai], b->data.str[bi]) == 0;
        case COLUMN_TYPE_ENUM:
            return a->data.i32[ai] == b->data.i32[bi];
        case COLUMN_TYPE_UUID:
            return uuid_equal(a->data.uuid[ai], b->data.uuid[bi]);
        case COLUMN_TYPE_INTERVAL:
            return a->data.iv[ai].months == b->data.iv[bi].months &&
                   a->data.iv[ai].days == b->data.iv[bi].days &&
                   a->data.iv[ai].usec == b->data.iv[bi].usec;
        case COLUMN_TYPE_VECTOR:
            return a->vec_dim == b->vec_dim &&
                   memcmp(&a->data.vec[ai * a->vec_dim],
                          &b->data.vec[bi * b->vec_dim],
                          a->vec_dim * sizeof(float)) == 0;
    }
    __builtin_unreachable();
}

/* ---- Flat table: heap-allocated columnar storage for N rows, M columns ----
 *
 * Used as the unified representation for:
 *   - table.flat  (table.h) — primary columnar storage, maintained on every mutation
 *   - join_cache  (table.h) — hash join build side cached per inner table
 *   - hash_join_state build side (plan.h) — scratch build side during query
 *   - set_op_state, top_n_state, window_state (plan.h) — scratch columnar buffers
 *
 * Ownership: the module that calls flat_table_init() owns the arrays and must
 * call flat_table_free() when done. Cross-module ownership is not permitted.
 *
 * str_lens[c] is non-NULL only for TEXT columns; stores strlen of each entry. */
struct flat_table {
    uint16_t          ncols;
    size_t            nrows;    /* number of valid rows */
    size_t            cap;      /* allocated capacity (rows) */
    void            **col_data;      /* [ncols] heap-allocated typed arrays */
    uint8_t         **col_nulls;     /* [ncols] heap-allocated null bitmaps */
    enum column_type *col_types;     /* [ncols] */
    uint32_t        **col_str_lens;  /* [ncols] non-NULL only for TEXT cols */
    uint16_t         *col_vec_dims;  /* [ncols] VECTOR dims (0 for non-vector cols) */
};

/* Allocate the per-column pointer arrays for a flat_table.
 * Call this first, then set ft->col_types[c] for each column,
 * then call flat_table_alloc_cols() to allocate the typed data arrays. */
static inline void flat_table_init(struct flat_table *ft, uint16_t ncols, size_t cap)
{
    ft->ncols = ncols;
    ft->nrows = 0;
    ft->cap   = cap;
    ft->col_data     = (void **)calloc(ncols, sizeof(void *));
    ft->col_nulls    = (uint8_t **)calloc(ncols, sizeof(uint8_t *));
    ft->col_types    = (enum column_type *)calloc(ncols, sizeof(enum column_type));
    ft->col_str_lens = (uint32_t **)calloc(ncols, sizeof(uint32_t *));
    ft->col_vec_dims = (uint16_t *)calloc(ncols, sizeof(uint16_t));
}

/* Allocate typed data arrays after col_types[] have been set.
 * Must be called exactly once after flat_table_init + setting col_types. */
static inline void flat_table_alloc_cols(struct flat_table *ft)
{
    for (uint16_t c = 0; c < ft->ncols; c++) {
        size_t esz = col_type_elem_size(ft->col_types[c]);
        size_t mul = (ft->col_types[c] == COLUMN_TYPE_VECTOR) ? ft->col_vec_dims[c] : 1;
        ft->col_data[c]  = calloc(ft->cap ? ft->cap : 1, esz * mul);
        ft->col_nulls[c] = (uint8_t *)calloc(ft->cap ? ft->cap : 1, 1);
    }
}

/* Free all heap arrays owned by ft. Does not free ft itself. */
static inline void flat_table_free(struct flat_table *ft)
{
    if (!ft->col_data) return;
    for (uint16_t c = 0; c < ft->ncols; c++) {
        free(ft->col_data[c]);
        free(ft->col_nulls[c]);
        if (ft->col_str_lens) free(ft->col_str_lens[c]);
    }
    free(ft->col_data);
    free(ft->col_nulls);
    free(ft->col_types);
    free(ft->col_str_lens);
    free(ft->col_vec_dims);
    ft->col_data = NULL;
    ft->col_nulls = NULL;
    ft->col_types = NULL;
    ft->col_str_lens = NULL;
    ft->col_vec_dims = NULL;
    ft->ncols = 0;
    ft->nrows = 0;
    ft->cap   = 0;
}


/* Grow all column arrays to new_cap. Caller must ensure new_cap > ft->cap.
 * Existing data is preserved; new slots are zero-initialized. */
static inline void flat_table_grow(struct flat_table *ft, size_t new_cap)
{
    for (uint16_t c = 0; c < ft->ncols; c++) {
        size_t esz = col_type_elem_size(ft->col_types[c]);
        size_t mul = (ft->col_types[c] == COLUMN_TYPE_VECTOR) ? ft->col_vec_dims[c] : 1;
        size_t row_sz = esz * mul;
        void *nd = realloc(ft->col_data[c], new_cap * row_sz);
        if (nd) {
            memset((char *)nd + ft->cap * row_sz, 0, (new_cap - ft->cap) * row_sz);
            ft->col_data[c] = nd;
        }
        uint8_t *nn = (uint8_t *)realloc(ft->col_nulls[c], new_cap);
        if (nn) {
            memset(nn + ft->cap, 0, new_cap - ft->cap);
            ft->col_nulls[c] = nn;
        }
        if (ft->col_str_lens && ft->col_str_lens[c]) {
            uint32_t *nl = (uint32_t *)realloc(ft->col_str_lens[c], new_cap * sizeof(uint32_t));
            if (nl) {
                memset(nl + ft->cap, 0, (new_cap - ft->cap) * sizeof(uint32_t));
                ft->col_str_lens[c] = nl;
            }
        }
    }
    ft->cap = new_cap;
}

/* Hash a single value in flat_table column c at row r. */
static inline uint32_t flat_table_hash_cell(const struct flat_table *ft, uint16_t c, size_t r)
{
    if (ft->col_nulls[c][r]) return 0;
    switch (ft->col_types[c]) {
    case COLUMN_TYPE_SMALLINT:
        return block_hash_i32((int32_t)((int16_t *)ft->col_data[c])[r]);
    case COLUMN_TYPE_INT:
    case COLUMN_TYPE_BOOLEAN:
    case COLUMN_TYPE_DATE:
    case COLUMN_TYPE_ENUM:
        return block_hash_i32(((int32_t *)ft->col_data[c])[r]);
    case COLUMN_TYPE_BIGINT:
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_TIMESTAMPTZ:
    case COLUMN_TYPE_TIME:
        return block_hash_i64(((int64_t *)ft->col_data[c])[r]);
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_NUMERIC:
        return block_hash_f64(((double *)ft->col_data[c])[r]);
    case COLUMN_TYPE_TEXT: {
        const char *s = ((const char **)ft->col_data[c])[r];
        if (ft->col_str_lens && ft->col_str_lens[c])
            return block_hash_str_n(s, ft->col_str_lens[c][r]);
        return block_hash_str(s);
    }
    case COLUMN_TYPE_UUID: {
        struct uuid_val u = ((struct uuid_val *)ft->col_data[c])[r];
        uint64_t uh = uuid_hash(u);
        return (uint32_t)(uh ^ (uh >> 32));
    }
    case COLUMN_TYPE_INTERVAL: {
        struct interval iv = ((struct interval *)ft->col_data[c])[r];
        uint32_t h = FNV_OFFSET;
        uint8_t *p = (uint8_t *)&iv;
        for (int j = 0; j < (int)sizeof(struct interval); j++) { h ^= p[j]; h *= FNV_PRIME; }
        return h;
    }
    case COLUMN_TYPE_VECTOR: {
        uint16_t dim = ft->col_vec_dims[c];
        uint32_t h = FNV_OFFSET;
        uint8_t *p = (uint8_t *)&((float *)ft->col_data[c])[r * dim];
        for (int j = 0; j < (int)(dim * sizeof(float)); j++) { h ^= p[j]; h *= FNV_PRIME; }
        return h;
    }
    }
    __builtin_unreachable();
}

/* Compare two cells in the same flat_table column: returns 1 if equal, 0 if not. */
static inline int flat_table_eq_cell(const struct flat_table *ft, uint16_t c,
                                     size_t ra, size_t rb)
{
    if (ft->col_nulls[c][ra] || ft->col_nulls[c][rb]) return 0;
    switch (ft->col_types[c]) {
    case COLUMN_TYPE_SMALLINT:
        return ((int16_t *)ft->col_data[c])[ra] == ((int16_t *)ft->col_data[c])[rb];
    case COLUMN_TYPE_INT:
    case COLUMN_TYPE_BOOLEAN:
    case COLUMN_TYPE_DATE:
    case COLUMN_TYPE_ENUM:
        return ((int32_t *)ft->col_data[c])[ra] == ((int32_t *)ft->col_data[c])[rb];
    case COLUMN_TYPE_BIGINT:
    case COLUMN_TYPE_TIMESTAMP:
    case COLUMN_TYPE_TIMESTAMPTZ:
    case COLUMN_TYPE_TIME:
        return ((int64_t *)ft->col_data[c])[ra] == ((int64_t *)ft->col_data[c])[rb];
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_NUMERIC:
        return ((double *)ft->col_data[c])[ra] == ((double *)ft->col_data[c])[rb];
    case COLUMN_TYPE_TEXT: {
        const char *sa = ((const char **)ft->col_data[c])[ra];
        const char *sb = ((const char **)ft->col_data[c])[rb];
        if (!sa || !sb) return sa == sb;
        if (ft->col_str_lens && ft->col_str_lens[c]) {
            if (ft->col_str_lens[c][ra] != ft->col_str_lens[c][rb]) return 0;
            return memcmp(sa, sb, ft->col_str_lens[c][ra]) == 0;
        }
        return strcmp(sa, sb) == 0;
    }
    case COLUMN_TYPE_UUID: {
        struct uuid_val ua = ((struct uuid_val *)ft->col_data[c])[ra];
        struct uuid_val ub = ((struct uuid_val *)ft->col_data[c])[rb];
        return uuid_equal(ua, ub);
    }
    case COLUMN_TYPE_INTERVAL: {
        struct interval ia = ((struct interval *)ft->col_data[c])[ra];
        struct interval ib = ((struct interval *)ft->col_data[c])[rb];
        return ia.months == ib.months && ia.days == ib.days && ia.usec == ib.usec;
    }
    case COLUMN_TYPE_VECTOR: {
        uint16_t dim = ft->col_vec_dims[c];
        return memcmp(&((float *)ft->col_data[c])[ra * dim],
                      &((float *)ft->col_data[c])[rb * dim],
                      dim * sizeof(float)) == 0;
    }
    }
    __builtin_unreachable();
}

/* Hash the first ncols columns of flat_table row r (FNV-1a). */
static inline uint32_t flat_table_hash_row(const struct flat_table *ft, size_t r, uint16_t ncols)
{
    uint32_t h = FNV_OFFSET;
    for (uint16_t c = 0; c < ncols; c++) {
        h ^= flat_table_hash_cell(ft, c, r);
        h *= FNV_PRIME;
    }
    return h;
}

#endif
