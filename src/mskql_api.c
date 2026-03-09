/*
 * mskql_api.c — implementation of the public embeddable C API (mskql.h)
 *
 * Thin wrapper around the internal database.h functions.
 * Compiled into libmskql.a for linking by external applications.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mskql.h"
#include "database.h"
#include "row.h"
#include "column.h"
#include "datetime.h"
#include "uuid.h"

/* ── opaque types ─────────────────────────────────────────────────── */

struct mskql_db {
    struct database db;
};

struct mskql_result {
    int nrows;
    int ncols;
    char **values;  /* nrows * ncols pointers into buf */
    char *buf;      /* single allocation holding all formatted strings */
};

/* ── format a cell to a malloc'd string (NULL for SQL NULL) ───────── */

static char *format_cell_alloc(const struct cell *c)
{
    if (c->is_null)
        return NULL;

    char tmp[256];
    const char *text = NULL;

    switch (c->type) {
    case COLUMN_TYPE_INT:
        snprintf(tmp, sizeof(tmp), "%d", c->value.as_int);
        text = tmp;
        break;
    case COLUMN_TYPE_FLOAT:
        snprintf(tmp, sizeof(tmp), "%g", c->value.as_float);
        text = tmp;
        break;
    case COLUMN_TYPE_BIGINT:
        snprintf(tmp, sizeof(tmp), "%lld", c->value.as_bigint);
        text = tmp;
        break;
    case COLUMN_TYPE_SMALLINT:
        snprintf(tmp, sizeof(tmp), "%d", (int)c->value.as_smallint);
        text = tmp;
        break;
    case COLUMN_TYPE_NUMERIC:
        snprintf(tmp, sizeof(tmp), "%g", c->value.as_numeric);
        text = tmp;
        break;
    case COLUMN_TYPE_BOOLEAN:
        text = c->value.as_bool ? "t" : "f";
        break;
    case COLUMN_TYPE_DATE:
        date_to_str(c->value.as_date, tmp, sizeof(tmp));
        text = tmp;
        break;
    case COLUMN_TYPE_TIME:
        time_to_str(c->value.as_time, tmp, sizeof(tmp));
        text = tmp;
        break;
    case COLUMN_TYPE_TIMESTAMP:
        timestamp_to_str(c->value.as_timestamp, tmp, sizeof(tmp));
        text = tmp;
        break;
    case COLUMN_TYPE_TIMESTAMPTZ:
        timestamptz_to_str(c->value.as_timestamp, tmp, sizeof(tmp));
        text = tmp;
        break;
    case COLUMN_TYPE_INTERVAL:
        interval_to_str(c->value.as_interval, tmp, sizeof(tmp));
        text = tmp;
        break;
    case COLUMN_TYPE_TEXT:
        text = c->value.as_text ? c->value.as_text : "";
        break;
    case COLUMN_TYPE_ENUM:
        snprintf(tmp, sizeof(tmp), "%d", c->value.as_enum);
        text = tmp;
        break;
    case COLUMN_TYPE_UUID:
        uuid_format(&c->value.as_uuid, tmp);
        text = tmp;
        break;
    case COLUMN_TYPE_VECTOR:
        /* vectors not exposed via simple text API */
        text = "<vector>";
        break;
    }

    return text ? strdup(text) : NULL;
}

/* ── public API ───────────────────────────────────────────────────── */

mskql_db *mskql_open(const char *name)
{
    mskql_db *h = (mskql_db *)calloc(1, sizeof(*h));
    if (!h) return NULL;
    db_init(&h->db, name ? name : "mskql");
    return h;
}

int mskql_exec(mskql_db *db, const char *sql)
{
    if (!db || !sql) return -1;
    struct rows r = {0};
    int rc = db_exec_sql(&db->db, sql, &r);
    rows_free(&r);
    return rc;
}

int mskql_exec_discard(mskql_db *db, const char *sql)
{
    if (!db || !sql) return -1;
    return db_exec_sql_discard(&db->db, sql);
}

int mskql_query(mskql_db *db, const char *sql, mskql_result **out)
{
    if (!out) return -1;
    *out = NULL;
    if (!db || !sql) return -1;

    struct rows r = {0};
    int rc = db_exec_sql(&db->db, sql, &r);
    if (rc < 0) {
        rows_free(&r);
        return rc;
    }

    int nrows = (int)r.count;
    int ncols = 0;
    if (nrows > 0)
        ncols = (int)r.data[0].cells.count;

    mskql_result *res = (mskql_result *)calloc(1, sizeof(*res));
    if (!res) {
        rows_free(&r);
        return -1;
    }
    res->nrows = nrows;
    res->ncols = ncols;

    if (nrows > 0 && ncols > 0) {
        /* First pass: format all cells and compute total buffer size */
        int total = nrows * ncols;
        char **strs = (char **)calloc((size_t)total, sizeof(char *));
        if (!strs) {
            free(res);
            rows_free(&r);
            return -1;
        }

        size_t buf_size = 0;
        for (int i = 0; i < nrows; i++) {
            struct row *row = &r.data[i];
            for (int j = 0; j < ncols && (size_t)j < row->cells.count; j++) {
                char *s = format_cell_alloc(&row->cells.items[j]);
                strs[i * ncols + j] = s;
                if (s) buf_size += strlen(s) + 1;
            }
        }

        /* Second pass: pack into a single buffer */
        char *buf = (char *)malloc(buf_size > 0 ? buf_size : 1);
        char **values = (char **)calloc((size_t)total, sizeof(char *));
        if (!buf || !values) {
            for (int i = 0; i < total; i++) free(strs[i]);
            free(strs);
            free(buf);
            free(values);
            free(res);
            rows_free(&r);
            return -1;
        }

        char *p = buf;
        for (int i = 0; i < total; i++) {
            if (strs[i]) {
                size_t len = strlen(strs[i]);
                memcpy(p, strs[i], len + 1);
                values[i] = p;
                p += len + 1;
                free(strs[i]);
            } else {
                values[i] = NULL;
            }
        }
        free(strs);

        res->values = values;
        res->buf = buf;
    }

    rows_free(&r);
    *out = res;
    return 0;
}

int mskql_result_nrows(const mskql_result *r)
{
    return r ? r->nrows : 0;
}

int mskql_result_ncols(const mskql_result *r)
{
    return r ? r->ncols : 0;
}

const char *mskql_result_value(const mskql_result *r, int row, int col)
{
    if (!r || row < 0 || row >= r->nrows || col < 0 || col >= r->ncols)
        return NULL;
    return r->values[row * r->ncols + col];
}

void mskql_result_free(mskql_result *r)
{
    if (!r) return;
    free(r->values);
    free(r->buf);
    free(r);
}

void mskql_close(mskql_db *db)
{
    if (!db) return;
    db_free(&db->db);
    free(db);
}

void mskql_reset(mskql_db *db)
{
    if (!db) return;
    db_reset(&db->db);
}
