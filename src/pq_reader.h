#ifndef PQ_READER_H
#define PQ_READER_H

#ifndef MSKQL_WASM

#include "column.h"
#include <stdint.h>
#include <stddef.h>

/* ---- Opaque reader handle ---- */

typedef struct pq_reader pq_reader_t;

/* ---- Physical type tag (subset needed for float/double distinction) ---- */

enum pq_phys_type {
    PQ_PHYS_BOOLEAN,
    PQ_PHYS_INT32,
    PQ_PHYS_INT64,
    PQ_PHYS_INT96,
    PQ_PHYS_FLOAT,
    PQ_PHYS_DOUBLE,
    PQ_PHYS_BYTE_ARRAY,
    PQ_PHYS_FIXED_LEN_BYTE_ARRAY,
};

/* ---- Per-column decoded data from one row group ---- */

struct pq_column {
    void    *data;       /* flat array: int32_t/int64_t/double/char**  */
    uint8_t *nulls;      /* per-row: 1=null, 0=valid (expanded from def-levels) */
    int64_t  nrows;      /* number of rows in this chunk */
    enum pq_phys_type phys; /* physical storage type in the file */
};

/* ---- API ---- */

/* Open a Parquet file and read its footer metadata.
 * Returns handle on success, NULL on error. */
pq_reader_t *pq_open(const char *path);

/* Close reader and free all associated memory. */
void pq_close(pq_reader_t *r);

/* Schema accessors. */
int             pq_num_columns(const pq_reader_t *r);
int64_t         pq_num_rows(const pq_reader_t *r);
int32_t         pq_num_row_groups(const pq_reader_t *r);
const char     *pq_column_name(const pq_reader_t *r, int col);
enum column_type pq_column_type(const pq_reader_t *r, int col);
enum pq_phys_type pq_column_phys_type(const pq_reader_t *r, int col);

/* Decode one row group into caller-supplied pq_column array.
 * cols[] must have pq_num_columns(r) entries.
 * On success returns 0 and fills cols[].data / cols[].nulls / cols[].nrows.
 * Caller must free each cols[c].data and cols[c].nulls via free().
 * Returns -1 on error. */
int pq_read_row_group(pq_reader_t *r, int rg, struct pq_column *cols);

#endif /* MSKQL_WASM */
#endif /* PQ_READER_H */
