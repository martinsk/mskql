#ifndef DISKIO_H
#define DISKIO_H

#include <stdint.h>
#include <stddef.h>
#include "column.h"
#include "block.h"

struct table; /* forward — defined in table.h */
struct row;   /* forward — defined in row.h */

/* ---- .mskd file format constants ---- */

#define MSKD_MAGIC      "MSKD"
#define MSKD_MAGIC_LEN  4
#define MSKD_VERSION     1
#define MSKD_HEADER_SIZE 32

/* Parsed column descriptor from a .mskd file header */
struct disk_col_desc {
    enum column_type type;
    char            *name;       /* heap-allocated */
    uint16_t         vec_dim;
    uint8_t          not_null;
    uint64_t         data_offset;
    uint64_t         null_offset;
    uint64_t         data_size;
};

/* Parsed file header — stored in struct table.disk.meta */
struct disk_meta {
    uint16_t              ncols;
    uint64_t              nrows;
    struct disk_col_desc *cols;   /* heap-allocated array [ncols] */
    uint64_t              file_size; /* total size of the base .mskd file */
};

/* Write a table's schema + flat data to a .mskd file.
 * If the table has no rows, writes header + column descriptors only. */
int disk_write_table(const char *path, struct table *t);

/* Read only the header + column descriptors from a .mskd file.
 * Populates meta->ncols, meta->nrows, meta->cols. */
int disk_read_schema(const char *path, struct disk_meta *meta);

/* Load all column data from a .mskd file into a flat_table.
 * ft must be zeroed; meta must already be populated via disk_read_schema.
 * Caller owns the resulting flat_table and must flat_table_free it. */
int disk_load_cache(const char *path, struct disk_meta *meta,
                    struct flat_table *ft);

/* Free the heap-allocated parts of a disk_meta. */
void disk_meta_free(struct disk_meta *meta);

/* ---- .mskd.wal delta log ---- */

enum wal_entry_type {
    WAL_INSERT = 1,
    WAL_DELETE = 2,
    WAL_UPDATE = 3,
};

/* Append INSERT entries to the WAL file. Returns bytes written, or -1 on error. */
int disk_wal_append_insert(const char *dir_path, const struct row *rows,
                           size_t count, const struct column *cols,
                           uint16_t ncols);

/* Append a DELETE entry. Returns bytes written, or -1 on error. */
int disk_wal_append_delete(const char *dir_path, uint64_t row_id);

/* Append an UPDATE entry. Returns bytes written, or -1 on error. */
int disk_wal_append_update(const char *dir_path, uint64_t row_id,
                           const uint8_t *col_mask, const struct cell *new_vals,
                           uint16_t ncols, const enum column_type *col_types);

/* Replay the WAL on top of an already-loaded flat_table.
 * Applies INSERTs, DELETEs (via deletion bitmap), UPDATEs in order. */
int disk_wal_replay(const char *dir_path, struct flat_table *ft,
                    struct disk_meta *meta);

/* Compact: merge base .mskd + WAL into a new .mskd, truncate WAL.
 * Returns 0 on success, -1 on error. */
int disk_compact(const char *dir_path, struct flat_table *ft,
                 struct disk_meta *meta);

/* Build the full path to the .mskd or .mskd.wal file.
 * buf must be large enough (PATH_MAX recommended). */
void disk_path_base(const char *dir_path, char *buf, size_t bufsz);
void disk_path_wal(const char *dir_path, char *buf, size_t bufsz);

/* ---- Disk catalog (persists disk table metadata across restarts) ---- */

struct database; /* forward — defined in database.h */

/* Save all TABLE_DISK entries to a binary catalog file.
 * Returns 0 on success, -1 on error. */
int disk_catalog_save(const char *catalog_path, struct database *db);

/* Load disk table entries from a catalog file and add them to db->tables.
 * Each table gets TABLE_DISK kind with cache_valid=0 (lazy load).
 * Returns number of tables loaded, or -1 on error. */
int disk_catalog_load(const char *catalog_path, struct database *db);

#endif
