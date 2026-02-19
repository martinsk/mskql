/*
 * wasm_api.c — thin C API for the WASM build of mskql.
 * Exposes: mskql_open, mskql_exec, mskql_close, mskql_alloc, mskql_dealloc
 * No pgwire, no sockets, no signals.
 */
#ifndef MSKQL_WASM
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#endif

#include "database.h"
#include "parser.h"
#include "row.h"
#include "column.h"
#include "arena.h"

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

/* helper: append a C string to the output buffer */
static int buf_puts(char *buf, int buf_len, int pos, const char *s) {
    while (*s && pos < buf_len - 1) buf[pos++] = *s++;
    return pos;
}

/* helper: append n bytes from an sv to the output buffer */
static int buf_putsv(char *buf, int buf_len, int pos, const char *data, size_t len) {
    for (size_t i = 0; i < len && pos < buf_len - 1; i++) buf[pos++] = data[i];
    return pos;
}

/* Resolve column name for column index i from the parsed query + table.
 * Mirrors the logic in pgwire.c send_row_description. */
static const char *resolve_col_name(struct query *q, struct database *db,
                                    struct table *t, int i, char *tmp, int tmp_sz)
{
    struct query_select *s = &q->select;

    /* 1. parsed_columns path (expression-based SELECT) */
    if (s->parsed_columns_count > 0 && (uint32_t)i < s->parsed_columns_count) {
        struct select_column *sc = &q->arena.select_cols.items[s->parsed_columns_start + i];
        if (sc->alias.len > 0) {
            int n = (int)(sc->alias.len < (size_t)(tmp_sz - 1) ? sc->alias.len : (size_t)(tmp_sz - 1));
            memcpy(tmp, sc->alias.data, n);
            tmp[n] = '\0';
            return tmp;
        }
        if (sc->expr_idx != IDX_NONE) {
            struct expr *e = &EXPR(&q->arena, sc->expr_idx);
            if (e->type == EXPR_COLUMN_REF) {
                if (t) {
                    int ci = table_find_column_sv(t, e->column_ref.column);
                    if (ci >= 0) return t->columns.items[ci].name;
                }
                /* no table match — use the raw column ref text */
                int n = (int)(e->column_ref.column.len < (size_t)(tmp_sz - 1)
                              ? e->column_ref.column.len : (size_t)(tmp_sz - 1));
                memcpy(tmp, e->column_ref.column.data, n);
                tmp[n] = '\0';
                return tmp;
            }
            if (e->type == EXPR_FUNC_CALL) {
                switch (e->func_call.func) {
                case FUNC_COALESCE:   return "coalesce";
                case FUNC_NULLIF:     return "nullif";
                case FUNC_UPPER:      return "upper";
                case FUNC_LOWER:      return "lower";
                case FUNC_LENGTH:     return "length";
                case FUNC_TRIM:       return "trim";
                case FUNC_SUBSTRING:  return "substring";
                case FUNC_ABS:        return "abs";
                case FUNC_CEIL:       return "ceil";
                case FUNC_FLOOR:      return "floor";
                case FUNC_ROUND:      return "round";
                case FUNC_POWER:      return "power";
                case FUNC_SQRT:       return "sqrt";
                case FUNC_MOD:        return "mod";
                case FUNC_NOW:        return "now";
                case FUNC_EXTRACT:    return "extract";
                case FUNC_DATE_TRUNC: return "date_trunc";
                case FUNC_AGE:        return "age";
                case FUNC_TO_CHAR:    return "to_char";
                case FUNC_GREATEST: case FUNC_LEAST: case FUNC_NEXTVAL:
                case FUNC_CURRVAL: case FUNC_GEN_RANDOM_UUID:
                case FUNC_CURRENT_TIMESTAMP: case FUNC_CURRENT_DATE:
                case FUNC_DATE_PART: case FUNC_SIGN: case FUNC_RANDOM:
                case FUNC_REPLACE: case FUNC_LPAD: case FUNC_RPAD:
                case FUNC_CONCAT: case FUNC_CONCAT_WS: case FUNC_POSITION:
                case FUNC_SPLIT_PART: case FUNC_LEFT: case FUNC_RIGHT:
                case FUNC_REPEAT: case FUNC_REVERSE: case FUNC_INITCAP:
                case FUNC_PG_GET_USERBYID: case FUNC_PG_TABLE_IS_VISIBLE:
                case FUNC_FORMAT_TYPE: case FUNC_PG_GET_EXPR:
                case FUNC_OBJ_DESCRIPTION: case FUNC_COL_DESCRIPTION:
                case FUNC_PG_ENCODING_TO_CHAR: case FUNC_SHOBJ_DESCRIPTION:
                case FUNC_HAS_TABLE_PRIVILEGE: case FUNC_HAS_DATABASE_PRIVILEGE:
                case FUNC_PG_GET_CONSTRAINTDEF: case FUNC_PG_GET_INDEXDEF:
                case FUNC_ARRAY_TO_STRING: case FUNC_CURRENT_SCHEMA:
                case FUNC_CURRENT_SCHEMAS: case FUNC_PG_IS_IN_RECOVERY:
                case FUNC_AGG_SUM: case FUNC_AGG_COUNT: case FUNC_AGG_AVG:
                case FUNC_AGG_MIN: case FUNC_AGG_MAX:
                    break;
                }
            }
        }
        return "?";
    }

    /* 2. select_exprs path (window functions / mixed columns+windows) */
    if (s->select_exprs_count > 0 && (uint32_t)i < s->select_exprs_count) {
        struct select_expr *se = &q->arena.select_exprs.items[s->select_exprs_start + i];
        if (se->alias.len > 0) {
            int n = (int)(se->alias.len < (size_t)(tmp_sz - 1) ? se->alias.len : (size_t)(tmp_sz - 1));
            memcpy(tmp, se->alias.data, n);
            tmp[n] = '\0';
            return tmp;
        }
        if (se->kind == SEL_COLUMN) {
            if (t) {
                for (size_t j = 0; j < t->columns.count; j++) {
                    if (sv_eq_cstr(se->column, t->columns.items[j].name))
                        return t->columns.items[j].name;
                }
            }
            int n = (int)(se->column.len < (size_t)(tmp_sz - 1) ? se->column.len : (size_t)(tmp_sz - 1));
            memcpy(tmp, se->column.data, n);
            tmp[n] = '\0';
            return tmp;
        }
        if (se->kind == SEL_WINDOW) {
            switch (se->win.func) {
            case WIN_ROW_NUMBER:   return "row_number";
            case WIN_RANK:         return "rank";
            case WIN_DENSE_RANK:   return "dense_rank";
            case WIN_NTILE:        return "ntile";
            case WIN_PERCENT_RANK: return "percent_rank";
            case WIN_CUME_DIST:    return "cume_dist";
            case WIN_LAG:          return "lag";
            case WIN_LEAD:         return "lead";
            case WIN_FIRST_VALUE:  return "first_value";
            case WIN_LAST_VALUE:   return "last_value";
            case WIN_NTH_VALUE:    return "nth_value";
            case WIN_SUM:          return "sum";
            case WIN_COUNT:        return "count";
            case WIN_AVG:          return "avg";
            }
        }
        return "?";
    }

    /* 3. aggregates path (GROUP BY cols as AGG_NONE + aggregate functions) */
    if (s->aggregates_count > 0 && (uint32_t)i < s->aggregates_count) {
        struct agg_expr *ae = &q->arena.aggregates.items[s->aggregates_start + i];
        /* alias takes priority */
        if (ae->alias.len > 0) {
            int n = (int)(ae->alias.len < (size_t)(tmp_sz - 1) ? ae->alias.len : (size_t)(tmp_sz - 1));
            memcpy(tmp, ae->alias.data, n);
            tmp[n] = '\0';
            return tmp;
        }
        /* AGG_NONE = GROUP BY column — use the column name */
        if (ae->func == AGG_NONE && ae->column.len > 0) {
            if (t) {
                for (size_t j = 0; j < t->columns.count; j++) {
                    if (sv_eq_cstr(ae->column, t->columns.items[j].name))
                        return t->columns.items[j].name;
                }
            }
            int n = (int)(ae->column.len < (size_t)(tmp_sz - 1) ? ae->column.len : (size_t)(tmp_sz - 1));
            memcpy(tmp, ae->column.data, n);
            tmp[n] = '\0';
            return tmp;
        }
        switch (ae->func) {
        case AGG_SUM:   return "sum";
        case AGG_COUNT: return "count";
        case AGG_AVG:   return "avg";
        case AGG_MIN:   return "min";
        case AGG_MAX:   return "max";
        case AGG_NONE:  return "?";
        }
    }

    /* 4. fallback: table column name */
    if (t && (size_t)i < t->columns.count)
        return t->columns.items[i].name;

    return "?";
}

/* Format a cell value into the output buffer */
static int format_cell(char *buf, int buf_len, int pos, struct cell *cell) {
    if (cell->is_null)
        return buf_puts(buf, buf_len, pos, "NULL");

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
    case COLUMN_TYPE_DATE:
        date_to_str(cell->value.as_date, tmp, sizeof(tmp));
        text = tmp;
        break;
    case COLUMN_TYPE_TIME:
        time_to_str(cell->value.as_time, tmp, sizeof(tmp));
        text = tmp;
        break;
    case COLUMN_TYPE_TIMESTAMP:
        timestamp_to_str(cell->value.as_timestamp, tmp, sizeof(tmp));
        text = tmp;
        break;
    case COLUMN_TYPE_TIMESTAMPTZ:
        timestamptz_to_str(cell->value.as_timestamp, tmp, sizeof(tmp));
        text = tmp;
        break;
    case COLUMN_TYPE_INTERVAL:
        interval_to_str(cell->value.as_interval, tmp, sizeof(tmp));
        text = tmp;
        break;
    case COLUMN_TYPE_TEXT:
        text = cell->value.as_text ? cell->value.as_text : "NULL";
        break;
    case COLUMN_TYPE_ENUM:
        snprintf(tmp, sizeof(tmp), "%d", cell->value.as_enum);
        text = tmp;
        break;
    case COLUMN_TYPE_UUID:
        uuid_format(&cell->value.as_uuid, tmp);
        text = tmp;
        break;
    }
    if (text) pos = buf_puts(buf, buf_len, pos, text);
    return pos;
}

/*
 * Execute SQL and write results into out_buf as tab-separated text.
 * Format:
 *   - First line: column names (tab-separated)
 *   - Subsequent lines: row values (tab-separated), NULL shown as "NULL"
 *   - For non-SELECT: "OK"
 * Returns: number of bytes written (excluding NUL), or -1 on error.
 */
WASM_EXPORT
int mskql_exec(void *db_ptr, const char *sql, char *out_buf, int buf_len) {
    struct database *db = (struct database *)db_ptr;
    if (!db || !sql || !out_buf || buf_len <= 0) return -1;

    /* parse the query so we can extract column names */
    struct query q = {0};
    if (query_parse(sql, &q) != 0) {
        int pos = buf_puts(out_buf, buf_len, 0, "ERROR: ");
        if (q.arena.errmsg[0])
            pos = buf_puts(out_buf, buf_len, pos, q.arena.errmsg);
        else
            pos = buf_puts(out_buf, buf_len, pos, "parse error");
        out_buf[pos] = '\0';
        query_free(&q);
        return -1;
    }

    struct rows result = {0};
    int rc = db_exec(db, &q, &result, NULL);

    int pos = 0;

    if (rc != 0) {
        pos = buf_puts(out_buf, buf_len, 0, "ERROR: ");
        if (q.arena.errmsg[0])
            pos = buf_puts(out_buf, buf_len, pos, q.arena.errmsg);
        else
            pos = buf_puts(out_buf, buf_len, pos, "query execution failed");
        out_buf[pos] = '\0';
        rows_free(&result);
        query_free(&q);
        return -1;
    }

    if (result.count == 0) {
        pos = buf_puts(out_buf, buf_len, 0, "OK");
        out_buf[pos] = '\0';
        rows_free(&result);
        query_free(&q);
        return pos;
    }

    /* resolve table for column name lookup */
    struct table *t = NULL;
    if (q.query_type == QUERY_TYPE_SELECT && q.select.table.len > 0)
        t = db_find_table_sv(db, q.select.table);

    int ncols = (int)result.data[0].cells.count;

    /* emit header line */
    for (int i = 0; i < ncols; i++) {
        if (i > 0 && pos < buf_len - 1) out_buf[pos++] = '\t';
        char name_buf[256];
        const char *name = resolve_col_name(&q, db, t, i, name_buf, sizeof(name_buf));
        pos = buf_puts(out_buf, buf_len, pos, name);
    }
    if (pos < buf_len - 1) out_buf[pos++] = '\n';

    /* emit data rows */
    for (size_t r = 0; r < result.count; r++) {
        struct row *row = &result.data[r];
        for (size_t c = 0; c < row->cells.count; c++) {
            if (c > 0 && pos < buf_len - 1) out_buf[pos++] = '\t';
            pos = format_cell(out_buf, buf_len, pos, &row->cells.items[c]);
        }
        if (pos < buf_len - 1) out_buf[pos++] = '\n';
    }

    out_buf[pos] = '\0';
    rows_free(&result);
    query_free(&q);
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
