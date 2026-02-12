/*
 * wasm_api.c — thin C API for the WASM build of mskql.
 * Exposes: mskql_open, mskql_exec, mskql_close, mskql_alloc, mskql_free
 * No pgwire, no sockets, no signals.
 */
#ifndef MSKQL_WASM
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#endif

#include "database.h"
#include "row.h"
#include "column.h"

/* ── exported API ───────────────────────────────────────────────── */

#define WASM_EXPORT __attribute__((visibility("default")))

WASM_EXPORT
void *mskql_open(void) {
    struct database *db = (struct database *)malloc(sizeof(struct database));
    if (!db) return NULL;
    memset(db, 0, sizeof(*db));
    db_init(db, "mskql");
    return db;
}

/*
 * Execute SQL and write results into out_buf as tab-separated text.
 * Format:
 *   - First line: column names (tab-separated), or "OK" for non-SELECT
 *   - Subsequent lines: row values (tab-separated), NULL shown as "NULL"
 * Returns: number of bytes written (excluding NUL), or -1 on error.
 * On error, the error message is written to out_buf.
 */
WASM_EXPORT
int mskql_exec(void *db_ptr, const char *sql, char *out_buf, int buf_len) {
    struct database *db = (struct database *)db_ptr;
    if (!db || !sql || !out_buf || buf_len <= 0) return -1;

    struct rows result = {0};
    int rc = db_exec_sql(db, sql, &result);

    int pos = 0;

    if (rc != 0) {
        /* error — write a message */
        const char *msg = "ERROR: query execution failed";
        int mlen = (int)strlen(msg);
        if (mlen >= buf_len) mlen = buf_len - 1;
        memcpy(out_buf, msg, mlen);
        out_buf[mlen] = '\0';
        rows_free(&result);
        return -1;
    }

    if (result.count == 0) {
        /* non-SELECT or empty result */
        const char *msg = "OK";
        int mlen = (int)strlen(msg);
        memcpy(out_buf, msg, mlen);
        out_buf[mlen] = '\0';
        rows_free(&result);
        return mlen;
    }

    /* format result rows as tab-separated text */
    /* we don't have column names from db_exec_sql, so just output data rows */
    for (size_t r = 0; r < result.count; r++) {
        struct row *row = &result.data[r];
        for (size_t c = 0; c < row->cells.count; c++) {
            if (c > 0 && pos < buf_len - 1) out_buf[pos++] = '\t';

            struct cell *cell = &row->cells.items[c];
            if (cell->is_null) {
                const char *s = "NULL";
                for (int i = 0; s[i] && pos < buf_len - 1; i++)
                    out_buf[pos++] = s[i];
            } else {
                char tmp[256];
                const char *text = NULL;
                switch (cell->type) {
                case COLUMN_TYPE_INT:
                    snprintf(tmp, sizeof(tmp), "%d", cell->value.as_int);
                    text = tmp;
                    break;
                case COLUMN_TYPE_FLOAT:
                    snprintf(tmp, sizeof(tmp), "%g", cell->value.as_float);
                    text = tmp;
                    break;
                case COLUMN_TYPE_BIGINT:
                    snprintf(tmp, sizeof(tmp), "%lld", cell->value.as_bigint);
                    text = tmp;
                    break;
                case COLUMN_TYPE_NUMERIC:
                    snprintf(tmp, sizeof(tmp), "%g", cell->value.as_numeric);
                    text = tmp;
                    break;
                case COLUMN_TYPE_BOOLEAN:
                    text = cell->value.as_bool ? "t" : "f";
                    break;
                case COLUMN_TYPE_TEXT:
                case COLUMN_TYPE_ENUM:
                case COLUMN_TYPE_DATE:
                case COLUMN_TYPE_TIME:
                case COLUMN_TYPE_TIMESTAMP:
                case COLUMN_TYPE_TIMESTAMPTZ:
                case COLUMN_TYPE_INTERVAL:
                case COLUMN_TYPE_UUID:
                    text = cell->value.as_text ? cell->value.as_text : "NULL";
                    break;
                }
                if (text) {
                    for (int i = 0; text[i] && pos < buf_len - 1; i++)
                        out_buf[pos++] = text[i];
                }
            }
        }
        if (pos < buf_len - 1) out_buf[pos++] = '\n';
    }

    out_buf[pos] = '\0';
    rows_free(&result);
    return pos;
}

WASM_EXPORT
void mskql_close(void *db_ptr) {
    struct database *db = (struct database *)db_ptr;
    if (!db) return;
    db_free(db);
    free(db);
}

WASM_EXPORT
void *mskql_alloc(int size) {
    return malloc((size_t)size);
}

WASM_EXPORT
void mskql_dealloc(void *ptr) {
    free(ptr);
}
