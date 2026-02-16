#include "pgwire.h"
#include "parser.h"
#include "plan.h"
#include "catalog.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <errno.h>
#include <strings.h>
#include <poll.h>
#include <fcntl.h>
#include <netinet/tcp.h>

/* ---- helpers: write big-endian integers ---- */

static void put_u32(uint8_t *buf, uint32_t v)
{
    buf[0] = (v >> 24) & 0xff;
    buf[1] = (v >> 16) & 0xff;
    buf[2] = (v >>  8) & 0xff;
    buf[3] =  v        & 0xff;
}

static void put_u16(uint8_t *buf, uint16_t v)
{
    buf[0] = (v >> 8) & 0xff;
    buf[1] =  v       & 0xff;
}

static uint32_t read_u32(const uint8_t *buf)
{
    return ((uint32_t)buf[0] << 24) |
           ((uint32_t)buf[1] << 16) |
           ((uint32_t)buf[2] <<  8) |
           ((uint32_t)buf[3]);
}

/* ---- low-level send helpers ---- */

static int send_all(int fd, const void *buf, size_t len)
{
    const uint8_t *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct pollfd pfd = { .fd = fd, .events = POLLOUT };
                int pr = poll(&pfd, 1, 5000);
                if (pr <= 0) return -1;
                continue;
            }
            return -1;
        }
        if (n == 0) return -1;
        p   += n;
        len -= (size_t)n;
    }
    return 0;
}

/* ---- dynamic buffer for building messages ---- */

struct msgbuf {
    uint8_t *data;
    size_t   len;
    size_t   cap;
};

static void msgbuf_init(struct msgbuf *m)
{
    m->data = NULL;
    m->len  = 0;
    m->cap  = 0;
}

static void msgbuf_free(struct msgbuf *m)
{
    free(m->data);
    m->data = NULL;
    m->len  = 0;
    m->cap  = 0;
}

static void msgbuf_ensure(struct msgbuf *m, size_t extra)
{
    if (m->len + extra > m->cap) {
        size_t newcap = m->cap ? m->cap * 2 : 256;
        while (newcap < m->len + extra) newcap *= 2;
        void *tmp = realloc(m->data, newcap);
        if (!tmp) { fprintf(stderr, "msgbuf_ensure: out of memory\n"); abort(); }
        m->data = tmp;
        m->cap  = newcap;
    }
}

static void msgbuf_push(struct msgbuf *m, const void *data, size_t len)
{
    msgbuf_ensure(m, len);
    memcpy(m->data + m->len, data, len);
    m->len += len;
}

static void msgbuf_push_byte(struct msgbuf *m, uint8_t b)
{
    msgbuf_push(m, &b, 1);
}

static void msgbuf_push_u16(struct msgbuf *m, uint16_t v)
{
    uint8_t buf[2];
    put_u16(buf, v);
    msgbuf_push(m, buf, 2);
}

static void msgbuf_push_u32(struct msgbuf *m, uint32_t v)
{
    uint8_t buf[4];
    put_u32(buf, v);
    msgbuf_push(m, buf, 4);
}

static void msgbuf_push_cstr(struct msgbuf *m, const char *s)
{
    msgbuf_push(m, s, strlen(s) + 1);
}

/* send a complete PG message: type byte + int32 length + body */
static int msg_send(int fd, char type, struct msgbuf *body)
{
    uint8_t hdr[5];
    hdr[0] = (uint8_t)type;
    put_u32(hdr + 1, (uint32_t)(body->len + 4));
    if (send_all(fd, hdr, 5) != 0) return -1;
    if (body->len > 0 && send_all(fd, body->data, body->len) != 0) return -1;
    return 0;
}

/* ---- protocol messages (all share a caller-owned msgbuf, reset before use) ---- */

static int send_auth_ok(int fd, struct msgbuf *m)
{
    m->len = 0;
    msgbuf_push_u32(m, 0); /* AuthenticationOk */
    return msg_send(fd, 'R', m);
}

static int send_parameter_status(int fd, struct msgbuf *m,
                                 const char *name, const char *value)
{
    m->len = 0;
    msgbuf_push_cstr(m, name);
    msgbuf_push_cstr(m, value);
    return msg_send(fd, 'S', m);
}

static int send_ready_for_query(int fd, struct msgbuf *m, char status)
{
    m->len = 0;
    msgbuf_push_byte(m, (uint8_t)status);
    return msg_send(fd, 'Z', m);
}

static int send_error(int fd, struct msgbuf *m, const char *severity,
                      const char *code, const char *message)
{
    m->len = 0;
    msgbuf_push_byte(m, 'S'); msgbuf_push_cstr(m, severity);
    msgbuf_push_byte(m, 'V'); msgbuf_push_cstr(m, severity);
    msgbuf_push_byte(m, 'C'); msgbuf_push_cstr(m, code);
    msgbuf_push_byte(m, 'M'); msgbuf_push_cstr(m, message);
    msgbuf_push_byte(m, 0);   /* terminator */
    return msg_send(fd, 'E', m);
}

static int send_command_complete(int fd, struct msgbuf *m, const char *tag)
{
    m->len = 0;
    msgbuf_push_cstr(m, tag);
    return msg_send(fd, 'C', m);
}

static int send_empty_query(int fd, struct msgbuf *m)
{
    m->len = 0;
    return msg_send(fd, 'I', m);
}

/* map our column types to PG OIDs — uses unified table from column.h */
static uint32_t column_type_to_oid(enum column_type t) { return pg_type_lookup(t)->oid; }

/* push a cell value directly into a msgbuf as a pgwire text field (len + data) */
static void msgbuf_push_cell(struct msgbuf *m, const struct cell *c)
{
    if (c->is_null) {
        msgbuf_push_u32(m, (uint32_t)-1);
        return;
    }
    char buf[64];
    const char *txt;
    size_t len;
    switch (c->type) {
        case COLUMN_TYPE_SMALLINT:
            len = (size_t)snprintf(buf, sizeof(buf), "%d", (int)c->value.as_smallint);
            txt = buf;
            break;
        case COLUMN_TYPE_INT:
            len = (size_t)snprintf(buf, sizeof(buf), "%d", c->value.as_int);
            txt = buf;
            break;
        case COLUMN_TYPE_FLOAT:
            len = (size_t)snprintf(buf, sizeof(buf), "%g", c->value.as_float);
            txt = buf;
            break;
        case COLUMN_TYPE_BOOLEAN:
            len = (size_t)snprintf(buf, sizeof(buf), "%s", c->value.as_bool ? "t" : "f");
            txt = buf;
            break;
        case COLUMN_TYPE_BIGINT:
            len = (size_t)snprintf(buf, sizeof(buf), "%lld", c->value.as_bigint);
            txt = buf;
            break;
        case COLUMN_TYPE_NUMERIC:
            len = (size_t)snprintf(buf, sizeof(buf), "%g", c->value.as_numeric);
            txt = buf;
            break;
        case COLUMN_TYPE_DATE:
            date_to_str(c->value.as_date, buf, sizeof(buf));
            txt = buf; len = strlen(buf);
            break;
        case COLUMN_TYPE_TIME:
            time_to_str(c->value.as_time, buf, sizeof(buf));
            txt = buf; len = strlen(buf);
            break;
        case COLUMN_TYPE_TIMESTAMP:
            timestamp_to_str(c->value.as_timestamp, buf, sizeof(buf));
            txt = buf; len = strlen(buf);
            break;
        case COLUMN_TYPE_TIMESTAMPTZ:
            timestamptz_to_str(c->value.as_timestamp, buf, sizeof(buf));
            txt = buf; len = strlen(buf);
            break;
        case COLUMN_TYPE_INTERVAL:
            interval_to_str(c->value.as_interval, buf, sizeof(buf));
            txt = buf; len = strlen(buf);
            break;
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_UUID:
            if (!c->value.as_text) {
                msgbuf_push_u32(m, (uint32_t)-1);
                return;
            }
            txt = c->value.as_text;
            len = strlen(txt);
            break;
    }
    msgbuf_push_u32(m, (uint32_t)len);
    msgbuf_push(m, txt, len);
}

/* ---- per-client state for poll()-based multiplexing ---- */

#define MAX_CLIENTS 64

/* ---- Extended Query Protocol: prepared statements & portals ---- */

#define MAX_PARAMS 64
#define MAX_PREPARED 32
#define MAX_PORTALS 16

struct prepared_stmt {
    char *name;                 /* empty string = unnamed */
    char *sql;                  /* original SQL with $1, $2, ... */
    uint32_t *param_oids;       /* client-specified parameter OIDs (0 = unspecified) */
    uint16_t nparams;           /* number of parameters from Parse */
    int in_use;
};

struct portal {
    char *name;                 /* empty string = unnamed */
    char *sql;                  /* SQL with parameters substituted */
    int16_t *result_formats;    /* per-column format codes (0=text, 1=binary) */
    uint16_t nresult_formats;
    int max_rows;               /* 0 = unlimited */
    int in_use;
    /* back-reference to the prepared statement for Describe */
    int stmt_idx;               /* index into prepared[] or -1 */
};

enum client_phase {
    PHASE_STARTUP,      /* waiting for startup message (possibly after SSL) */
    PHASE_SSL_RETRY,    /* sent 'N' for SSLRequest, waiting for real startup */
    PHASE_QUERY_LOOP,   /* authenticated, processing queries */
};

struct client_state {
    int fd;
    enum client_phase phase;
    struct msgbuf send_buf;     /* reusable send buffer */
    uint8_t *recv_buf;          /* partial read accumulator */
    size_t recv_len;
    size_t recv_cap;
    struct query_arena arena;   /* connection-scoped arena, reset per request */
    struct txn_state txn;       /* per-connection transaction state */
    /* Extended Query Protocol state */
    struct prepared_stmt prepared[MAX_PREPARED];
    struct portal portals[MAX_PORTALS];
    int extended_error;         /* 1 = error occurred, skip until Sync */
    /* COPY FROM STDIN state */
    int copy_in_active;         /* 1 = receiving CopyData */
    struct table *copy_in_table;
    char copy_in_delim;
    int copy_in_is_csv;
    size_t copy_in_row_count;
    char copy_in_linebuf[8192];
    size_t copy_in_linelen;
    int copy_in_line_overflow;  /* 1 = current line exceeded buffer */
};

static void prepared_stmt_free(struct prepared_stmt *ps)
{
    free(ps->name);
    free(ps->sql);
    free(ps->param_oids);
    memset(ps, 0, sizeof(*ps));
}

static void portal_free(struct portal *p)
{
    free(p->name);
    free(p->sql);
    free(p->result_formats);
    memset(p, 0, sizeof(*p));
}

static int find_prepared(struct client_state *c, const char *name)
{
    for (int i = 0; i < MAX_PREPARED; i++) {
        if (c->prepared[i].in_use && strcmp(c->prepared[i].name, name) == 0)
            return i;
    }
    return -1;
}

static int find_portal(struct client_state *c, const char *name)
{
    for (int i = 0; i < MAX_PORTALS; i++) {
        if (c->portals[i].in_use && strcmp(c->portals[i].name, name) == 0)
            return i;
    }
    return -1;
}

static int alloc_prepared(struct client_state *c)
{
    for (int i = 0; i < MAX_PREPARED; i++) {
        if (!c->prepared[i].in_use) return i;
    }
    return -1;
}

static int alloc_portal(struct client_state *c)
{
    for (int i = 0; i < MAX_PORTALS; i++) {
        if (!c->portals[i].in_use) return i;
    }
    return -1;
}

/* Substitute $1, $2, ... in SQL with literal parameter values.
 * Text parameters are single-quoted with internal quotes doubled.
 * NULL parameters become the literal string NULL.
 * Returns a malloc'd string the caller must free. */
static char *substitute_params(const char *sql, const char **param_values,
                               const int *param_lengths, const int *param_formats,
                               uint16_t nparams)
{
    /* estimate output size */
    size_t sql_len = strlen(sql);
    size_t est = sql_len + 256;
    char *out = malloc(est);
    if (!out) return NULL;
    size_t pos = 0;

    for (const char *p = sql; *p; ) {
        if (*p == '$' && p[1] >= '1' && p[1] <= '9') {
            /* parse parameter number */
            const char *start = p;
            p++; /* skip $ */
            int num = 0;
            while (*p >= '0' && *p <= '9') {
                num = num * 10 + (*p - '0');
                p++;
            }
            if (num < 1 || num > nparams) {
                /* not a valid param ref — copy literally */
                size_t span = (size_t)(p - start);
                if (pos + span + 1 > est) {
                    est = (pos + span + 1) * 2;
                    char *tmp = realloc(out, est);
                    if (!tmp) { free(out); return NULL; }
                    out = tmp;
                }
                memcpy(out + pos, start, span);
                pos += span;
                continue;
            }
            int idx = num - 1;
            if (!param_values[idx]) {
                /* NULL parameter */
                if (pos + sizeof("NULL") > est) { est = (pos + sizeof("NULL")) * 2; char *tmp = realloc(out, est); if (!tmp) { free(out); return NULL; } out = tmp; }
                memcpy(out + pos, "NULL", sizeof("NULL") - 1);
                pos += sizeof("NULL") - 1;
            } else if (param_formats && param_formats[idx] == 1) {
                /* binary format — quote as text with escaping */
                const char *val = param_values[idx];
                size_t vlen = param_lengths ? (size_t)param_lengths[idx] : strlen(val);
                size_t need = vlen * 2 + 3;
                if (pos + need > est) { est = (pos + need) * 2; char *tmp = realloc(out, est); if (!tmp) { free(out); return NULL; } out = tmp; }
                out[pos++] = '\'';
                for (size_t k = 0; k < vlen; k++) {
                    if (val[k] == '\'') out[pos++] = '\'';
                    out[pos++] = val[k];
                }
                out[pos++] = '\'';
            } else {
                /* text format — quote the value */
                const char *val = param_values[idx];
                size_t vlen = param_lengths ? (size_t)param_lengths[idx] : strlen(val);

                /* check if value looks numeric (no quoting needed) */
                int is_numeric = (vlen > 0);
                int has_dot = 0;
                int has_digit = 0;
                for (size_t k = 0; k < vlen && is_numeric; k++) {
                    char ch = val[k];
                    if (ch == '-' && k == 0) continue;
                    if (ch == '.' && !has_dot) { has_dot = 1; continue; }
                    if (ch >= '0' && ch <= '9') { has_digit = 1; continue; }
                    is_numeric = 0;
                }
                if (!has_digit) is_numeric = 0;
                /* also treat 't'/'f' as boolean literals */
                int is_bool = (vlen == 1 && (val[0] == 't' || val[0] == 'f'));

                if (is_numeric && vlen > 0 && !(vlen == 1 && val[0] == '-')) {
                    /* numeric — insert without quotes */
                    if (pos + vlen + 1 > est) { est = (pos + vlen + 1) * 2; char *tmp = realloc(out, est); if (!tmp) { free(out); return NULL; } out = tmp; }
                    memcpy(out + pos, val, vlen);
                    pos += vlen;
                } else if (is_bool) {
                    /* boolean — insert as TRUE/FALSE */
                    const char *bval = (val[0] == 't') ? "TRUE" : "FALSE";
                    size_t blen = (val[0] == 't') ? 4 : 5;
                    if (pos + blen + 1 > est) { est = (pos + blen + 1) * 2; char *tmp = realloc(out, est); if (!tmp) { free(out); return NULL; } out = tmp; }
                    memcpy(out + pos, bval, blen);
                    pos += blen;
                } else {
                    /* text — single-quote with escaping */
                    size_t need = vlen * 2 + 3;
                    if (pos + need > est) { est = (pos + need) * 2; char *tmp = realloc(out, est); if (!tmp) { free(out); return NULL; } out = tmp; }
                    out[pos++] = '\'';
                    for (size_t k = 0; k < vlen; k++) {
                        if (val[k] == '\'') out[pos++] = '\'';
                        out[pos++] = val[k];
                    }
                    out[pos++] = '\'';
                }
            }
        } else {
            if (pos + 2 > est) { est = (pos + 2) * 2; char *tmp = realloc(out, est); if (!tmp) { free(out); return NULL; } out = tmp; }
            out[pos++] = *p++;
        }
    }
    out[pos] = '\0';
    return out;
}

static uint16_t read_u16(const uint8_t *buf)
{
    return ((uint16_t)buf[0] << 8) | (uint16_t)buf[1];
}

static int16_t read_i16(const uint8_t *buf)
{
    return (int16_t)(((uint16_t)buf[0] << 8) | (uint16_t)buf[1]);
}

static int32_t read_i32(const uint8_t *buf)
{
    return (int32_t)read_u32(buf);
}

static void client_init(struct client_state *c, int fd)
{
    c->fd = fd;
    c->phase = PHASE_STARTUP;
    msgbuf_init(&c->send_buf);
    c->recv_buf = NULL;
    c->recv_len = 0;
    c->recv_cap = 0;
    query_arena_init(&c->arena);
    c->txn.in_transaction = 0;
    c->txn.snapshot = NULL;
    memset(c->prepared, 0, sizeof(c->prepared));
    memset(c->portals, 0, sizeof(c->portals));
    c->extended_error = 0;
}

static void client_free(struct client_state *c)
{
    if (c->fd >= 0) close(c->fd);
    c->fd = -1;
    msgbuf_free(&c->send_buf);
    free(c->recv_buf);
    c->recv_buf = NULL;
    c->recv_len = 0;
    c->recv_cap = 0;
    for (int i = 0; i < MAX_PREPARED; i++)
        if (c->prepared[i].in_use) prepared_stmt_free(&c->prepared[i]);
    for (int i = 0; i < MAX_PORTALS; i++)
        if (c->portals[i].in_use) portal_free(&c->portals[i]);
    query_arena_destroy(&c->arena);
}

/* rollback any open transaction when a client disconnects, then free */
static void client_disconnect(struct client_state *c, struct database *db)
{
    if (c->txn.in_transaction) {
        db->active_txn = &c->txn;
        struct query q = {0};
        q.query_type = QUERY_TYPE_ROLLBACK;
        struct rows r = {0};
        db_exec(db, &q, &r, NULL);
        rows_free(&r);
        db->active_txn = NULL;
    }
    client_free(c);
}

static void client_recv_ensure(struct client_state *c, size_t extra)
{
    if (c->recv_len + extra > c->recv_cap) {
        size_t newcap = c->recv_cap ? c->recv_cap * 2 : 4096;
        while (newcap < c->recv_len + extra) newcap *= 2;
        void *tmp = realloc(c->recv_buf, newcap);
        if (!tmp) { fprintf(stderr, "client_recv_ensure: OOM\n"); abort(); }
        c->recv_buf = tmp;
        c->recv_cap = newcap;
    }
}

/* consume n bytes from the front of the recv buffer */
static void client_recv_consume(struct client_state *c, size_t n)
{
    if (n >= c->recv_len) {
        c->recv_len = 0;
    } else {
        memmove(c->recv_buf, c->recv_buf + n, c->recv_len - n);
        c->recv_len -= n;
    }
}

static int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* ---- query result sending ---- */

static int send_row_description(int fd, struct database *db, struct query *q,
                                struct rows *result)
{
    struct msgbuf m;
    msgbuf_init(&m);

    /* figure out column info */
    int ncols = 0;
    struct table *t = NULL;

    if (result->count > 0) {
        ncols = (int)result->data[0].cells.count;
    }

    /* try to get column metadata from the table */
    switch (q->query_type) {
    case QUERY_TYPE_SELECT:   if (q->select.table.len > 0) t = db_find_table_sv(db, q->select.table); break;
    case QUERY_TYPE_DELETE:   if (q->del.table.len > 0)    t = db_find_table_sv(db, q->del.table);    break;
    case QUERY_TYPE_UPDATE:   if (q->update.table.len > 0) t = db_find_table_sv(db, q->update.table); break;
    case QUERY_TYPE_INSERT:   if (q->insert.table.len > 0) t = db_find_table_sv(db, q->insert.table); break;
    case QUERY_TYPE_CREATE:
    case QUERY_TYPE_DROP:
    case QUERY_TYPE_CREATE_INDEX:
    case QUERY_TYPE_DROP_INDEX:
    case QUERY_TYPE_CREATE_TYPE:
    case QUERY_TYPE_DROP_TYPE:
    case QUERY_TYPE_ALTER:
    case QUERY_TYPE_BEGIN:
    case QUERY_TYPE_COMMIT:
    case QUERY_TYPE_ROLLBACK:
    case QUERY_TYPE_CREATE_SEQUENCE:
    case QUERY_TYPE_DROP_SEQUENCE:
    case QUERY_TYPE_CREATE_VIEW:
    case QUERY_TYPE_DROP_VIEW:
    case QUERY_TYPE_TRUNCATE:
    case QUERY_TYPE_EXPLAIN:
    case QUERY_TYPE_COPY:
    case QUERY_TYPE_SET:
    case QUERY_TYPE_SHOW:
        break;
    }

    msgbuf_push_u16(&m, (uint16_t)ncols);

    /* For DELETE/UPDATE/INSERT RETURNING, use table column metadata directly */
    if (q->query_type != QUERY_TYPE_SELECT) {
        for (int i = 0; i < ncols; i++) {
            const char *colname = "?";
            uint32_t type_oid = 25; /* text */
            if (t && (size_t)i < t->columns.count) {
                colname  = t->columns.items[i].name;
                type_oid = column_type_to_oid(t->columns.items[i].type);
            } else if (result->count > 0 && (size_t)i < result->data[0].cells.count) {
                type_oid = column_type_to_oid(result->data[0].cells.items[i].type);
            }
            msgbuf_push_cstr(&m, colname);
            msgbuf_push_u32(&m, 0);
            msgbuf_push_u16(&m, 0);
            msgbuf_push_u32(&m, type_oid);
            msgbuf_push_u16(&m, (uint16_t)-1);
            msgbuf_push_u32(&m, (uint32_t)-1);
            msgbuf_push_u16(&m, 0);
        }
        int rc = msg_send(fd, 'T', &m);
        msgbuf_free(&m);
        return rc;
    }

    for (int i = 0; i < ncols; i++) {
        const char *colname = "?";
        uint32_t type_oid = 25; /* text */

        if (q->select.parsed_columns_count > 0 && (uint32_t)i < q->select.parsed_columns_count) {
            struct select_column *sc = &q->arena.select_cols.items[q->select.parsed_columns_start + i];
            if (sc->alias.len > 0) {
                static __thread char abuf[256];
                size_t al = sc->alias.len < 255 ? sc->alias.len : 255;
                memcpy(abuf, sc->alias.data, al);
                abuf[al] = '\0';
                colname = abuf;
            }
            if (sc->expr_idx != IDX_NONE) {
                struct expr *e = &EXPR(&q->arena, sc->expr_idx);
                if (e->type == EXPR_COLUMN_REF && t) {
                    int ci = table_find_column_sv(t, e->column_ref.column);
                    if (ci >= 0) {
                        if (sc->alias.len == 0)
                            colname = t->columns.items[ci].name;
                        type_oid = column_type_to_oid(t->columns.items[ci].type);
                    }
                } else if (result->count > 0 && (size_t)i < result->data[0].cells.count) {
                    type_oid = column_type_to_oid(result->data[0].cells.items[i].type);
                }
            } else if (result->count > 0 && (size_t)i < result->data[0].cells.count) {
                type_oid = column_type_to_oid(result->data[0].cells.items[i].type);
            }
        } else if (q->select.select_exprs_count > 0 && (uint32_t)i < q->select.select_exprs_count) {
            struct select_expr *se = &q->arena.select_exprs.items[q->select.select_exprs_start + i];
            if (se->kind == SEL_COLUMN) {
                /* try to get name from table */
                int ci = -1;
                if (t) {
                    for (size_t j = 0; j < t->columns.count; j++) {
                        if (sv_eq_cstr(se->column, t->columns.items[j].name)) {
                            ci = (int)j; break;
                        }
                    }
                }
                if (ci >= 0) {
                    colname = t->columns.items[ci].name;
                    type_oid = column_type_to_oid(t->columns.items[ci].type);
                }
            } else {
                switch (se->win.func) {
                    case WIN_ROW_NUMBER:   colname = "row_number";   break;
                    case WIN_RANK:         colname = "rank";         break;
                    case WIN_DENSE_RANK:   colname = "dense_rank";   break;
                    case WIN_NTILE:        colname = "ntile";        break;
                    case WIN_PERCENT_RANK: colname = "percent_rank"; break;
                    case WIN_CUME_DIST:    colname = "cume_dist";    break;
                    case WIN_LAG:          colname = "lag";          break;
                    case WIN_LEAD:         colname = "lead";         break;
                    case WIN_FIRST_VALUE:  colname = "first_value";  break;
                    case WIN_LAST_VALUE:   colname = "last_value";   break;
                    case WIN_NTH_VALUE:    colname = "nth_value";    break;
                    case WIN_SUM:          colname = "sum";          break;
                    case WIN_COUNT:        colname = "count";        break;
                    case WIN_AVG:          colname = "avg";          break;
                }
                if (result->count > 0)
                    type_oid = column_type_to_oid(result->data[0].cells.items[i].type);
            }
        } else if (q->select.aggregates_count > 0 && (uint32_t)i < q->select.aggregates_count) {
            switch (q->arena.aggregates.items[q->select.aggregates_start + i].func) {
                case AGG_SUM:        colname = "sum";        break;
                case AGG_COUNT:      colname = "count";      break;
                case AGG_AVG:        colname = "avg";        break;
                case AGG_MIN:        colname = "min";        break;
                case AGG_MAX:        colname = "max";        break;
                case AGG_STRING_AGG: colname = "string_agg"; break;
                case AGG_ARRAY_AGG:  colname = "array_agg";  break;
                case AGG_NONE:       colname = "?";          break;
            }
            if (result->count > 0)
                type_oid = column_type_to_oid(result->data[0].cells.items[i].type);
        } else if (t && (size_t)i < t->columns.count) {
            colname  = t->columns.items[i].name;
            type_oid = column_type_to_oid(t->columns.items[i].type);
        } else if (result->count > 0) {
            type_oid = column_type_to_oid(result->data[0].cells.items[i].type);
        }

        msgbuf_push_cstr(&m, colname);     /* field name */
        msgbuf_push_u32(&m, 0);            /* table OID */
        msgbuf_push_u16(&m, 0);            /* column attr number */
        msgbuf_push_u32(&m, type_oid);     /* type OID */
        msgbuf_push_u16(&m, (uint16_t)-1); /* type size (-1 = variable) */
        msgbuf_push_u32(&m, (uint32_t)-1); /* type modifier */
        msgbuf_push_u16(&m, 0);            /* format code (0 = text) */
    }

    int rc = msg_send(fd, 'T', &m);
    msgbuf_free(&m);
    return rc;
}

static int send_data_rows(int fd, struct rows *result)
{
    struct msgbuf m;
    msgbuf_init(&m);

    for (size_t i = 0; i < result->count; i++) {
        struct row *r = &result->data[i];
        m.len = 0; /* reset buffer, reuse backing memory */

        msgbuf_push_u16(&m, (uint16_t)r->cells.count);

        for (size_t j = 0; j < r->cells.count; j++)
            msgbuf_push_cell(&m, &r->cells.items[j]);

        if (msg_send(fd, 'D', &m) != 0) {
            msgbuf_free(&m);
            return -1;
        }
    }
    msgbuf_free(&m);
    return 0;
}

/* ---- direct columnar→wire send (bypasses block_to_rows) ---- */

/* Fast int32 → decimal string. Returns length written. buf must be >= 12 bytes. */
static inline size_t fast_i32_to_str(int32_t v, char *buf)
{
    char tmp[12];
    int neg = 0;
    uint32_t uv;
    if (v < 0) { neg = 1; uv = (uint32_t)(-(int64_t)v); } else { uv = (uint32_t)v; }
    int pos = 0;
    do { tmp[pos++] = '0' + (char)(uv % 10); uv /= 10; } while (uv);
    size_t len = (size_t)pos + (size_t)neg;
    int out = 0;
    if (neg) buf[out++] = '-';
    while (pos > 0) buf[out++] = tmp[--pos];
    buf[out] = '\0';
    return len;
}

/* Fast int64 → decimal string. Returns length written. buf must be >= 21 bytes. */
static inline size_t fast_i64_to_str(int64_t v, char *buf)
{
    char tmp[21];
    int neg = 0;
    uint64_t uv;
    if (v < 0) { neg = 1; uv = (uint64_t)(-v); } else { uv = (uint64_t)v; }
    int pos = 0;
    do { tmp[pos++] = '0' + (char)(uv % 10); uv /= 10; } while (uv);
    size_t len = (size_t)pos + (size_t)neg;
    int out = 0;
    if (neg) buf[out++] = '-';
    while (pos > 0) buf[out++] = tmp[--pos];
    buf[out] = '\0';
    return len;
}

/* Push a col_block cell value directly into a msgbuf as a pgwire text field. */
static void msgbuf_push_col_cell(struct msgbuf *m, const struct col_block *cb, uint16_t ri)
{
    if (cb->nulls[ri]) {
        msgbuf_push_u32(m, (uint32_t)-1);
        return;
    }
    char buf[64];
    const char *txt;
    size_t len;
    switch (cb->type) {
    case COLUMN_TYPE_SMALLINT:
        len = fast_i32_to_str((int32_t)cb->data.i16[ri], buf);
        txt = buf;
        break;
    case COLUMN_TYPE_INT:
        len = fast_i32_to_str(cb->data.i32[ri], buf);
        txt = buf;
        break;
    case COLUMN_TYPE_BOOLEAN:
        buf[0] = cb->data.i32[ri] ? 't' : 'f';
        buf[1] = '\0';
        len = 1;
        txt = buf;
        break;
    case COLUMN_TYPE_BIGINT:
        len = fast_i64_to_str(cb->data.i64[ri], buf);
        txt = buf;
        break;
    case COLUMN_TYPE_FLOAT:
    case COLUMN_TYPE_NUMERIC:
        len = (size_t)snprintf(buf, sizeof(buf), "%g", cb->data.f64[ri]);
        txt = buf;
        break;
    case COLUMN_TYPE_DATE:
        date_to_str(cb->data.i32[ri], buf, sizeof(buf));
        txt = buf; len = strlen(buf);
        break;
    case COLUMN_TYPE_TIME:
        time_to_str(cb->data.i64[ri], buf, sizeof(buf));
        txt = buf; len = strlen(buf);
        break;
    case COLUMN_TYPE_TIMESTAMP:
        timestamp_to_str(cb->data.i64[ri], buf, sizeof(buf));
        txt = buf; len = strlen(buf);
        break;
    case COLUMN_TYPE_TIMESTAMPTZ:
        timestamptz_to_str(cb->data.i64[ri], buf, sizeof(buf));
        txt = buf; len = strlen(buf);
        break;
    case COLUMN_TYPE_INTERVAL:
        interval_to_str(cb->data.iv[ri], buf, sizeof(buf));
        txt = buf; len = strlen(buf);
        break;
    case COLUMN_TYPE_TEXT:
    case COLUMN_TYPE_ENUM:
    case COLUMN_TYPE_UUID:
        if (!cb->data.str[ri]) {
            msgbuf_push_u32(m, (uint32_t)-1);
            return;
        }
        txt = cb->data.str[ri];
        len = strlen(txt);
        break;
    }
    msgbuf_push_u32(m, (uint32_t)len);
    msgbuf_push(m, txt, len);
}

/* Send RowDescription using table metadata + plan column info.
 * ncols/col_types come from the plan's output columns.
 * join_tables/n_join_tables: optional array of joined tables for column name resolution. */
static int send_row_desc_plan(int fd, struct table *t, struct table **join_tables,
                              int n_join_tables, struct query *q,
                              uint16_t ncols, const enum column_type *col_types)
{
    struct msgbuf m;
    msgbuf_init(&m);
    msgbuf_push_u16(&m, ncols);

    int select_all = sv_eq_cstr(q->select.columns, "*");
    /* Detect table.* pattern (e.g. SELECT t.*) and treat as SELECT * */
    if (!select_all && q->select.parsed_columns_count == 1) {
        struct select_column *sc0 = &q->arena.select_cols.items[q->select.parsed_columns_start];
        if (sc0->expr_idx != IDX_NONE) {
            struct expr *e0 = &EXPR(&q->arena, sc0->expr_idx);
            if (e0->type == EXPR_COLUMN_REF && e0->column_ref.column.len == 1 &&
                e0->column_ref.column.data[0] == '*')
                select_all = 1;
        }
    }
    /* Build cumulative column offset table for SELECT * resolution */
    uint16_t tbl_offsets[9] = {0};
    int tbl_count = 0;
    struct table *all_tables[9] = {0};
    if (t) { all_tables[0] = t; tbl_offsets[0] = 0; tbl_count = 1; }
    for (int j = 0; j < n_join_tables && tbl_count < 9; j++) {
        if (!join_tables || !join_tables[j]) continue;
        tbl_offsets[tbl_count] = tbl_count > 0
            ? tbl_offsets[tbl_count - 1] + (uint16_t)all_tables[tbl_count - 1]->columns.count
            : 0;
        all_tables[tbl_count] = join_tables[j];
        tbl_count++;
    }

    for (uint16_t i = 0; i < ncols; i++) {
        const char *colname = "?";
        uint32_t type_oid = column_type_to_oid(col_types[i]);

        if (q->select.select_exprs_count > 0 && (size_t)i < q->select.select_exprs_count) {
            /* Window query: column names from select_exprs */
            struct select_expr *se = &q->arena.select_exprs.items[q->select.select_exprs_start + i];
            if (se->alias.len > 0) {
                static __thread char se_alias_buf[256];
                size_t alen = se->alias.len < 255 ? se->alias.len : 255;
                memcpy(se_alias_buf, se->alias.data, alen);
                se_alias_buf[alen] = '\0';
                colname = se_alias_buf;
            } else if (se->kind == SEL_COLUMN && se->column.len > 0) {
                /* Strip table prefix from column name */
                sv col = se->column;
                for (size_t kk = 0; kk < col.len; kk++) {
                    if (col.data[kk] == '.') { col = sv_from(col.data+kk+1, col.len-kk-1); break; }
                }
                static __thread char se_col_buf[256];
                size_t clen = col.len < 255 ? col.len : 255;
                memcpy(se_col_buf, col.data, clen);
                se_col_buf[clen] = '\0';
                colname = se_col_buf;
            }
        } else if (select_all && !t && q->select.has_generate_series && i == 0) {
            if (q->select.gs_col_alias.len > 0) {
                static __thread char gs_col_buf[256];
                size_t clen = q->select.gs_col_alias.len < 255 ? q->select.gs_col_alias.len : 255;
                memcpy(gs_col_buf, q->select.gs_col_alias.data, clen);
                gs_col_buf[clen] = '\0';
                colname = gs_col_buf;
            } else {
                colname = "generate_series";
            }
        } else if (select_all && tbl_count > 0) {
            for (int ti = tbl_count - 1; ti >= 0; ti--) {
                if (i >= tbl_offsets[ti] && (size_t)(i - tbl_offsets[ti]) < all_tables[ti]->columns.count) {
                    colname = all_tables[ti]->columns.items[i - tbl_offsets[ti]].name;
                    break;
                }
            }
        } else if (q->select.parsed_columns_count > 0 && (uint32_t)i < q->select.parsed_columns_count) {
            struct select_column *sc = &q->arena.select_cols.items[q->select.parsed_columns_start + i];
            if (sc->alias.len > 0) {
                /* use alias as column name — need NUL-terminated copy */
                static __thread char alias_buf[256];
                size_t alen = sc->alias.len < 255 ? sc->alias.len : 255;
                memcpy(alias_buf, sc->alias.data, alen);
                alias_buf[alen] = '\0';
                colname = alias_buf;
            } else if (sc->expr_idx != IDX_NONE) {
                struct expr *e = &EXPR(&q->arena, sc->expr_idx);
                switch (e->type) {
                case EXPR_COLUMN_REF: {
                    for (int ti = 0; ti < tbl_count; ti++) {
                        int ci = table_find_column_sv(all_tables[ti], e->column_ref.column);
                        if (ci >= 0) { colname = all_tables[ti]->columns.items[ci].name; break; }
                    }
                    break;
                }
                case EXPR_FUNC_CALL: {
                    static const char *func_names[] = {
                        [FUNC_COALESCE]="coalesce", [FUNC_NULLIF]="nullif",
                        [FUNC_GREATEST]="greatest", [FUNC_LEAST]="least",
                        [FUNC_UPPER]="upper", [FUNC_LOWER]="lower",
                        [FUNC_LENGTH]="length", [FUNC_TRIM]="trim",
                        [FUNC_SUBSTRING]="substring", [FUNC_ABS]="abs",
                        [FUNC_CEIL]="ceil", [FUNC_FLOOR]="floor",
                        [FUNC_ROUND]="round", [FUNC_POWER]="power",
                        [FUNC_SQRT]="sqrt", [FUNC_MOD]="mod",
                        [FUNC_SIGN]="sign", [FUNC_RANDOM]="random",
                        [FUNC_REPLACE]="replace", [FUNC_LPAD]="lpad",
                        [FUNC_RPAD]="rpad", [FUNC_CONCAT]="concat",
                        [FUNC_CONCAT_WS]="concat_ws", [FUNC_POSITION]="position",
                        [FUNC_SPLIT_PART]="split_part", [FUNC_LEFT]="left",
                        [FUNC_RIGHT]="right", [FUNC_REPEAT]="repeat",
                        [FUNC_REVERSE]="reverse", [FUNC_INITCAP]="initcap",
                        [FUNC_NOW]="now", [FUNC_CURRENT_TIMESTAMP]="current_timestamp",
                        [FUNC_CURRENT_DATE]="current_date", [FUNC_EXTRACT]="extract",
                        [FUNC_DATE_TRUNC]="date_trunc", [FUNC_DATE_PART]="date_part",
                        [FUNC_AGE]="age", [FUNC_TO_CHAR]="to_char",
                        [FUNC_NEXTVAL]="nextval", [FUNC_CURRVAL]="currval",
                        [FUNC_GEN_RANDOM_UUID]="gen_random_uuid",
                    };
                    int fn = (int)e->func_call.func;
                    if (fn >= 0 && fn < (int)(sizeof(func_names)/sizeof(func_names[0])) && func_names[fn])
                        colname = func_names[fn];
                    break;
                }
                case EXPR_CAST: {
                    /* For CAST/:: expressions, use the inner expression's column name */
                    uint32_t inner_idx = e->cast.operand;
                    if (inner_idx != IDX_NONE) {
                        struct expr *inner = &EXPR(&q->arena, inner_idx);
                        if (inner->type == EXPR_COLUMN_REF && t) {
                            int ci = table_find_column_sv(t, inner->column_ref.column);
                            if (ci >= 0) colname = t->columns.items[ci].name;
                        }
                    }
                    break;
                }
                case EXPR_LITERAL:
                case EXPR_BINARY_OP:
                case EXPR_UNARY_OP:
                case EXPR_CASE_WHEN:
                case EXPR_SUBQUERY:
                case EXPR_IS_NULL:
                case EXPR_EXISTS:
                    break;
                }
            }
        } else if (q->select.aggregates_count > 0 && (uint32_t)i < q->select.aggregates_count &&
                   q->select.parsed_columns_count == 0 && q->select.select_exprs_count == 0) {
            /* Simple aggregate (no GROUP BY): derive column name from aggregate */
            struct agg_expr *ae = &q->arena.aggregates.items[q->select.aggregates_start + i];
            if (ae->alias.len > 0) {
                static __thread char agg_alias_buf[256];
                size_t alen = ae->alias.len < 255 ? ae->alias.len : 255;
                memcpy(agg_alias_buf, ae->alias.data, alen);
                agg_alias_buf[alen] = '\0';
                colname = agg_alias_buf;
            } else {
                switch (ae->func) {
                    case AGG_SUM:        colname = "sum";        break;
                    case AGG_COUNT:      colname = "count";      break;
                    case AGG_AVG:        colname = "avg";        break;
                    case AGG_MIN:        colname = "min";        break;
                    case AGG_MAX:        colname = "max";        break;
                    case AGG_STRING_AGG: colname = "string_agg"; break;
                    case AGG_ARRAY_AGG:  colname = "array_agg";  break;
                    case AGG_NONE:       break;
                }
            }
        } else if (t && (size_t)i < t->columns.count) {
            colname = t->columns.items[i].name;
        }

        msgbuf_push_cstr(&m, colname);
        msgbuf_push_u32(&m, 0);            /* table OID */
        msgbuf_push_u16(&m, 0);            /* column attr number */
        msgbuf_push_u32(&m, type_oid);     /* type OID */
        msgbuf_push_u16(&m, (uint16_t)-1); /* type size */
        msgbuf_push_u32(&m, (uint32_t)-1); /* type modifier */
        msgbuf_push_u16(&m, 0);            /* format code (text) */
    }

    int rc = msg_send(fd, 'T', &m);
    msgbuf_free(&m);
    return rc;
}

/* ---- Result cache: replay cached wire bytes for identical SELECT queries ---- */

#define RCACHE_SLOTS 8192
#define RCACHE_MAX_BYTES (4 * 1024 * 1024) /* max cached wire data per entry */

struct rcache_entry {
    uint32_t sql_hash;
    uint64_t generation;       /* sum of all table generations at cache time */
    uint8_t *wire_data;        /* RowDescription + DataRows (heap-allocated) */
    size_t   wire_len;
    uint8_t *full_reply;       /* wire_data + CommandComplete + ReadyForQuery */
    size_t   full_reply_len;
    int      row_count;
    int      valid;
};

static struct rcache_entry g_rcache[RCACHE_SLOTS];

static uint32_t rcache_hash_sql(const char *sql, size_t len)
{
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < len; i++) {
        h ^= (uint8_t)sql[i];
        h *= 16777619u;
    }
    return h;
}


static void rcache_invalidate_all(void)
{
    for (int i = 0; i < RCACHE_SLOTS; i++) {
        if (g_rcache[i].valid) {
            free(g_rcache[i].wire_data);
            g_rcache[i].wire_data = NULL;
            free(g_rcache[i].full_reply);
            g_rcache[i].full_reply = NULL;
            g_rcache[i].valid = 0;
        }
    }
}

/* Try to execute a SELECT via the plan executor and send results directly
 * from columnar blocks to the wire, bypassing block_to_rows entirely.
 * Returns row count on success, -1 if the query can't use this path.
 * sql/sql_len are the original SQL string for result caching. */
/* ---- try_plan_send helpers ---- */

/* Build a RowDescription message body into rd_buf for the plan executor path.
 * Resolves column names using 6 strategies: select_exprs aliases, generate_series,
 * table columns (t1/t2), parsed_columns aliases, and column ref expressions. */
static void build_row_desc_for_plan(struct msgbuf *rd_buf, struct query *q,
                                    struct table *t, struct table **join_tables,
                                    int n_join_tables,
                                    uint16_t ncols, const enum column_type *col_types)
{
    msgbuf_push_u16(rd_buf, ncols);
    int select_all_rd = sv_eq_cstr(q->select.columns, "*");
    /* Detect table.* pattern (e.g. SELECT t.*) and treat as SELECT * */
    if (!select_all_rd && q->select.parsed_columns_count == 1) {
        struct select_column *sc0 = &q->arena.select_cols.items[q->select.parsed_columns_start];
        if (sc0->expr_idx != IDX_NONE) {
            struct expr *e0 = &EXPR(&q->arena, sc0->expr_idx);
            if (e0->type == EXPR_COLUMN_REF && e0->column_ref.column.len == 1 &&
                e0->column_ref.column.data[0] == '*')
                select_all_rd = 1;
        }
    }
    /* Build cumulative column offset table for SELECT * resolution across N tables */
    uint16_t tbl_offsets[9] = {0}; /* [t] + up to 8 join tables */
    int tbl_count = 0;
    struct table *all_tables[9] = {0};
    if (t) { all_tables[0] = t; tbl_offsets[0] = 0; tbl_count = 1; }
    for (int j = 0; j < n_join_tables && tbl_count < 9; j++) {
        if (!join_tables[j]) continue;
        tbl_offsets[tbl_count] = tbl_count > 0
            ? tbl_offsets[tbl_count - 1] + (uint16_t)all_tables[tbl_count - 1]->columns.count
            : 0;
        all_tables[tbl_count] = join_tables[j];
        tbl_count++;
    }
    for (uint16_t i = 0; i < ncols; i++) {
        const char *colname = "?";
        uint32_t type_oid = column_type_to_oid(col_types[i]);
        if (q->select.select_exprs_count > 0 && (size_t)i < q->select.select_exprs_count) {
            struct select_expr *se = &q->arena.select_exprs.items[q->select.select_exprs_start + i];
            if (se->alias.len > 0) {
                static __thread char se_alias_buf2[256];
                size_t alen = se->alias.len < 255 ? se->alias.len : 255;
                memcpy(se_alias_buf2, se->alias.data, alen);
                se_alias_buf2[alen] = '\0';
                colname = se_alias_buf2;
            } else if (se->kind == SEL_COLUMN && se->column.len > 0) {
                sv col = se->column;
                for (size_t kk = 0; kk < col.len; kk++)
                    if (col.data[kk] == '.') { col = sv_from(col.data+kk+1, col.len-kk-1); break; }
                static __thread char se_col_buf2[256];
                size_t clen = col.len < 255 ? col.len : 255;
                memcpy(se_col_buf2, col.data, clen);
                se_col_buf2[clen] = '\0';
                colname = se_col_buf2;
            }
        } else if (select_all_rd && !t && q->select.has_generate_series && i == 0) {
            colname = q->select.gs_col_alias.len > 0 ? "generate_series" : "generate_series";
        } else if (select_all_rd && tbl_count > 0) {
            /* Walk the table array to find which table owns column i */
            for (int ti = tbl_count - 1; ti >= 0; ti--) {
                if (i >= tbl_offsets[ti] && (size_t)(i - tbl_offsets[ti]) < all_tables[ti]->columns.count) {
                    colname = all_tables[ti]->columns.items[i - tbl_offsets[ti]].name;
                    break;
                }
            }
        } else if (q->select.parsed_columns_count > 0 && (uint32_t)i < q->select.parsed_columns_count) {
            struct select_column *sc = &q->arena.select_cols.items[q->select.parsed_columns_start + i];
            if (sc->alias.len > 0) {
                static __thread char alias_buf2[256];
                size_t alen = sc->alias.len < 255 ? sc->alias.len : 255;
                memcpy(alias_buf2, sc->alias.data, alen);
                alias_buf2[alen] = '\0';
                colname = alias_buf2;
            } else if (sc->expr_idx != IDX_NONE) {
                struct expr *e = &EXPR(&q->arena, sc->expr_idx);
                if (e->type == EXPR_COLUMN_REF) {
                    /* Search all tables for the column */
                    for (int ti = 0; ti < tbl_count; ti++) {
                        int ci = table_find_column_sv(all_tables[ti], e->column_ref.column);
                        if (ci >= 0) { colname = all_tables[ti]->columns.items[ci].name; break; }
                    }
                }
            }
        } else if (t && (size_t)i < t->columns.count) {
            colname = t->columns.items[i].name;
        }
        msgbuf_push_cstr(rd_buf, colname);
        msgbuf_push_u32(rd_buf, 0);
        msgbuf_push_u16(rd_buf, 0);
        msgbuf_push_u32(rd_buf, type_oid);
        msgbuf_push_u16(rd_buf, (uint16_t)-1);
        msgbuf_push_u32(rd_buf, (uint32_t)-1);
        msgbuf_push_u16(rd_buf, 0);
    }
}

/* Store plan executor result in the result cache.
 * cache_buf contains RowDescription + all DataRow messages. */
static void rcache_store_plan_result(struct rcache_entry *rce, uint32_t rc_hash,
                                     uint64_t rc_gen, int total_rows,
                                     const struct msgbuf *cache_buf)
{
    if (cache_buf->len == 0 || cache_buf->len > RCACHE_MAX_BYTES)
        return;
    if (rce->valid) { free(rce->wire_data); free(rce->full_reply); }
    rce->wire_data = (uint8_t *)malloc(cache_buf->len);
    if (!rce->wire_data) return;
    memcpy(rce->wire_data, cache_buf->data, cache_buf->len);
    rce->wire_len = cache_buf->len;
    rce->sql_hash = rc_hash;
    rce->generation = rc_gen;
    rce->row_count = total_rows;
    rce->valid = 1;
    /* Pre-build full reply: wire_data + CommandComplete + ReadyForQuery */
    char tag[128];
    int tag_len = snprintf(tag, sizeof(tag), "SELECT %d", total_rows);
    size_t cc_body = (size_t)tag_len + 1;
    size_t full_len = cache_buf->len + 5 + cc_body + 6;
    rce->full_reply = (uint8_t *)malloc(full_len);
    if (rce->full_reply) {
        size_t off = 0;
        memcpy(rce->full_reply + off, cache_buf->data, cache_buf->len);
        off += cache_buf->len;
        rce->full_reply[off++] = 'C';
        put_u32(rce->full_reply + off, (uint32_t)(cc_body + 4));
        off += 4;
        memcpy(rce->full_reply + off, tag, (size_t)tag_len + 1);
        off += (size_t)tag_len + 1;
        rce->full_reply[off++] = 'Z';
        put_u32(rce->full_reply + off, 5);
        off += 4;
        rce->full_reply[off++] = 'I';
        rce->full_reply_len = off;
    } else {
        rce->full_reply_len = 0;
    }
}

/* ---- try_plan_send orchestrator ---- */

static int try_plan_send(int fd, struct database *db, struct query *q,
                         struct query_arena *conn_arena,
                         const char *sql, size_t sql_len)
{
    (void)conn_arena;
    if (q->query_type != QUERY_TYPE_SELECT) return -1;

    /* ---- Result cache lookup ---- */
    uint32_t rc_hash = rcache_hash_sql(sql, sql_len);
    uint32_t rc_slot = rc_hash & (RCACHE_SLOTS - 1);
    uint64_t rc_gen = db->total_generation;
    struct rcache_entry *rce = &g_rcache[rc_slot];
    if (rce->valid && rce->sql_hash == rc_hash && rce->generation == rc_gen) {
        if (send_all(fd, rce->wire_data, rce->wire_len) == 0)
            return rce->row_count;
    }

    struct query_select *s = &q->select;

    /* ---- CTE materialization ---- */
    struct table *cte_temps[32] = {0};
    size_t n_cte_temps = 0;
    struct query_arena *qa = &q->arena;
    struct table *from_sub_temp = NULL;
    uint32_t saved_from_sub_sql = s->from_subquery_sql;

    uint32_t saved_ctes_count = s->ctes_count;
    uint32_t saved_ctes_start = s->ctes_start;
    uint32_t saved_cte_name = s->cte_name;
    uint32_t saved_cte_sql = s->cte_sql;

    if (s->ctes_count > 0 && !s->has_recursive_cte) {
        for (uint32_t ci = 0; ci < s->ctes_count && n_cte_temps < 32; ci++) {
            struct cte_def *cd = &qa->ctes.items[s->ctes_start + ci];
            if (cd->is_recursive) goto cte_bail;
            const char *cd_sql = ASTRING(qa, cd->sql_idx);
            const char *cd_name = ASTRING(qa, cd->name_idx);
            cte_temps[n_cte_temps] = materialize_subquery(db, cd_sql, cd_name);
            if (!cte_temps[n_cte_temps]) goto cte_bail;
            n_cte_temps++;
        }
        s->ctes_count = 0;
        s->cte_name = IDX_NONE;
        s->cte_sql = IDX_NONE;
    } else if (s->cte_name != IDX_NONE && s->cte_sql != IDX_NONE &&
               !s->has_recursive_cte) {
        const char *cd_sql = ASTRING(qa, s->cte_sql);
        const char *cd_name = ASTRING(qa, s->cte_name);
        cte_temps[0] = materialize_subquery(db, cd_sql, cd_name);
        if (!cte_temps[0]) return -1;
        n_cte_temps = 1;
        s->cte_name = IDX_NONE;
        s->cte_sql = IDX_NONE;
    }

    if (0) {
    cte_bail:;
    cte_restore_bail:
        s->ctes_count = saved_ctes_count;
        s->ctes_start = saved_ctes_start;
        s->cte_name = saved_cte_name;
        s->cte_sql = saved_cte_sql;
        s->from_subquery_sql = saved_from_sub_sql;
        if (from_sub_temp) remove_temp_table(db, from_sub_temp);
        for (size_t ci = n_cte_temps; ci > 0; ci--)
            remove_temp_table(db, cte_temps[ci - 1]);
        return -1;
    }

    /* ---- FROM subquery materialization ---- */
    if (s->from_subquery_sql != IDX_NONE) {
        char alias_buf[256];
        if (s->from_subquery_alias.len > 0)
            snprintf(alias_buf, sizeof(alias_buf), "%.*s",
                     (int)s->from_subquery_alias.len, s->from_subquery_alias.data);
        else
            snprintf(alias_buf, sizeof(alias_buf), "_from_sub");
        from_sub_temp = materialize_subquery(db, ASTRING(qa, s->from_subquery_sql), alias_buf);
        if (!from_sub_temp) goto cte_restore_bail;
        /* If subquery returned 0 rows, column inference fails (0 columns) — bail to legacy */
        if (from_sub_temp->columns.count == 0) goto cte_restore_bail;
        s->table = sv_from(from_sub_temp->name, strlen(from_sub_temp->name));
        s->from_subquery_sql = IDX_NONE;
    }

    struct table *t = NULL;
    if (s->table.len > 0)
        t = db_find_table_sv(db, s->table);
    if (!t && !s->has_generate_series) {
        if (from_sub_temp) { s->from_subquery_sql = saved_from_sub_sql; remove_temp_table(db, from_sub_temp); }
        goto cte_restore_bail;
    }

    struct table *join_tables[8] = {0};
    int n_join_tables = 0;
    if (s->has_join && s->joins_count >= 1) {
        for (uint32_t j = 0; j < s->joins_count && n_join_tables < 8; j++) {
            struct join_info *ji = &q->arena.joins.items[s->joins_start + j];
            join_tables[n_join_tables] = db_find_table_sv(db, ji->join_table);
            if (join_tables[n_join_tables]) n_join_tables++;
        }
    }
    struct plan_result pr = plan_build_select(t, s, &q->arena, db);
    if (pr.status != PLAN_OK)
        goto cte_restore_bail;

    struct plan_exec_ctx ctx;
    plan_exec_init(&ctx, &q->arena, db, pr.node);

    uint16_t ncols = plan_node_ncols(&q->arena, pr.node);
    if (ncols == 0)
        goto cte_cleanup;

    /* Pull first block to determine column types for RowDescription */
    struct row_block block;
    row_block_alloc(&block, ncols, &q->arena.scratch);

    int rc = plan_next_block(&ctx, pr.node, &block);
    if (rc != 0) {
        /* No rows — send empty RowDescription + zero data rows */
        enum column_type types[64];
        for (uint16_t i = 0; i < ncols && i < 64; i++)
            types[i] = (t && i < t->columns.count) ? t->columns.items[i].type : COLUMN_TYPE_INT;
        send_row_desc_plan(fd, t, join_tables, n_join_tables, q, ncols, types);
        goto cte_cleanup_ok;
    }

    /* Determine column types from first block */
    enum column_type col_types[64];
    for (uint16_t i = 0; i < ncols && i < 64; i++)
        col_types[i] = block.cols[i].type;

    /* Build RowDescription */
    struct msgbuf rd_buf;
    msgbuf_init(&rd_buf);
    build_row_desc_for_plan(&rd_buf, q, t, join_tables, n_join_tables, ncols, col_types);

    if (msg_send(fd, 'T', &rd_buf) != 0) {
        msgbuf_free(&rd_buf);
        goto cte_cleanup;
    }

    /* Send data rows directly from blocks — batched into a large buffer */
    struct msgbuf wire;
    msgbuf_init(&wire);
    msgbuf_ensure(&wire, 65536);

    /* Accumulate all wire bytes for result cache */
    struct msgbuf cache_buf;
    msgbuf_init(&cache_buf);
    {
        uint8_t hdr[5];
        hdr[0] = 'T';
        uint32_t rd_body_len = (uint32_t)(rd_buf.len + 4);
        put_u32(hdr + 1, rd_body_len);
        msgbuf_push(&cache_buf, hdr, 5);
        msgbuf_push(&cache_buf, rd_buf.data, rd_buf.len);
    }
    msgbuf_free(&rd_buf);

    size_t total_rows = 0;

    for (;;) {
        uint16_t active = row_block_active_count(&block);
        for (uint16_t r = 0; r < active; r++) {
            uint16_t ri = row_block_row_idx(&block, r);
            size_t msg_start = wire.len;
            msgbuf_ensure(&wire, 5);
            wire.len += 5;
            msgbuf_push_u16(&wire, ncols);
            for (uint16_t c = 0; c < ncols; c++)
                msgbuf_push_col_cell(&wire, &block.cols[c], ri);
            wire.data[msg_start] = 'D';
            uint32_t body_len = (uint32_t)(wire.len - msg_start - 1);
            put_u32(wire.data + msg_start + 1, body_len);
            if (cache_buf.len < RCACHE_MAX_BYTES)
                msgbuf_push(&cache_buf, wire.data + msg_start, wire.len - msg_start);
            total_rows++;
        }

        if (wire.len >= 65536) {
            if (send_all(fd, wire.data, wire.len) != 0) {
                msgbuf_free(&wire);
                msgbuf_free(&cache_buf);
                goto cte_cleanup;
            }
            wire.len = 0;
        }

        row_block_reset(&block);
        if (plan_next_block(&ctx, pr.node, &block) != 0)
            break;
    }

    if (wire.len > 0) {
        if (send_all(fd, wire.data, wire.len) != 0) {
            msgbuf_free(&wire);
            msgbuf_free(&cache_buf);
            goto cte_cleanup;
        }
    }
    msgbuf_free(&wire);

    /* Store in result cache */
    rcache_store_plan_result(rce, rc_hash, rc_gen, (int)total_rows, &cache_buf);
    msgbuf_free(&cache_buf);

    if (from_sub_temp) remove_temp_table(db, from_sub_temp);
    for (size_t ci = n_cte_temps; ci > 0; ci--)
        remove_temp_table(db, cte_temps[ci - 1]);
    return (int)total_rows;

cte_cleanup_ok:
    if (from_sub_temp) remove_temp_table(db, from_sub_temp);
    for (size_t ci = n_cte_temps; ci > 0; ci--)
        remove_temp_table(db, cte_temps[ci - 1]);
    return 0;

cte_cleanup:
    if (from_sub_temp) remove_temp_table(db, from_sub_temp);
    for (size_t ci = n_cte_temps; ci > 0; ci--)
        remove_temp_table(db, cte_temps[ci - 1]);
    return -1;
}

/* ---- catalog helpers (kept for pgwire column name resolution) ---- */

/* ---- handle a single query string (may contain one statement) ---- */

/* Inner handler: parse, execute, send results + CommandComplete.
 * If skip_row_desc is set, RowDescription is NOT sent (Extended Query Protocol:
 * the client already received it from Describe, or will infer types).
 * Does NOT send ReadyForQuery — caller decides when to send it.
 * Returns 0 on success, -1 on error (error already sent to client). */
/* ---- handle_query_inner helpers ---- */

/* COPY TO STDOUT: send table data using CopyOut wire protocol.
 * Returns 0 on success, -1 on error. */
static int handle_copy_to_stdout(int fd, struct database *db, struct query *q,
                                 struct msgbuf *m)
{
    struct table *ct = db_find_table_sv(db, q->copy.table);
    if (!ct) {
        send_error(fd, m, "ERROR", "42P01", "table not found");
        return -1;
    }
    char delim = q->copy.is_csv ? ',' : '\t';
    /* CopyOutResponse: 'H', int32 len, int8 format(0=text), int16 ncols, int16[ncols] col_formats(0=text) */
    uint16_t ncols = (uint16_t)ct->columns.count;
    m->len = 0;
    msgbuf_push_byte(m, 0); /* overall format: text */
    msgbuf_push_u16(m, ncols);
    for (uint16_t i = 0; i < ncols; i++)
        msgbuf_push_u16(m, 0); /* per-column format: text */
    msg_send(fd, 'H', m);
    /* CSV HEADER row */
    if (q->copy.has_header) {
        m->len = 0;
        for (uint16_t i = 0; i < ncols; i++) {
            if (i > 0) msgbuf_push_byte(m, (uint8_t)delim);
            const char *cn = ct->columns.items[i].name;
            msgbuf_push(m, (const uint8_t *)cn, strlen(cn));
        }
        msgbuf_push_byte(m, '\n');
        msg_send(fd, 'd', m);
    }
    /* Data rows */
    for (size_t r = 0; r < ct->rows.count; r++) {
        struct row *row = &ct->rows.items[r];
        m->len = 0;
        for (uint16_t c = 0; c < ncols && c < (uint16_t)row->cells.count; c++) {
            if (c > 0) msgbuf_push_byte(m, (uint8_t)delim);
            struct cell *cell = &row->cells.items[c];
            if (cell->is_null || (column_type_is_text(cell->type) && !cell->value.as_text)) {
                if (q->copy.is_csv) {
                    /* CSV: empty field for NULL */
                } else {
                    msgbuf_push(m, (const uint8_t *)"\\N", 2);
                }
            } else {
                char buf[128];
                const char *val = NULL;
                size_t vlen = 0;
                if (column_type_is_text(cell->type)) {
                    val = cell->value.as_text;
                    vlen = strlen(val);
                } else if (cell->type == COLUMN_TYPE_DATE) {
                    date_to_str(cell->value.as_date, buf, sizeof(buf));
                    val = buf; vlen = strlen(buf);
                } else if (cell->type == COLUMN_TYPE_TIME) {
                    time_to_str(cell->value.as_time, buf, sizeof(buf));
                    val = buf; vlen = strlen(buf);
                } else if (cell->type == COLUMN_TYPE_TIMESTAMP) {
                    timestamp_to_str(cell->value.as_timestamp, buf, sizeof(buf));
                    val = buf; vlen = strlen(buf);
                } else if (cell->type == COLUMN_TYPE_TIMESTAMPTZ) {
                    timestamptz_to_str(cell->value.as_timestamp, buf, sizeof(buf));
                    val = buf; vlen = strlen(buf);
                } else if (cell->type == COLUMN_TYPE_INTERVAL) {
                    interval_to_str(cell->value.as_interval, buf, sizeof(buf));
                    val = buf; vlen = strlen(buf);
                } else if (cell->type == COLUMN_TYPE_INT) {
                    vlen = (size_t)snprintf(buf, sizeof(buf), "%d", cell->value.as_int);
                    val = buf;
                } else if (cell->type == COLUMN_TYPE_BIGINT) {
                    vlen = (size_t)snprintf(buf, sizeof(buf), "%lld", cell->value.as_bigint);
                    val = buf;
                } else if (cell->type == COLUMN_TYPE_FLOAT || cell->type == COLUMN_TYPE_NUMERIC) {
                    vlen = (size_t)snprintf(buf, sizeof(buf), "%g", cell->value.as_float);
                    val = buf;
                } else if (cell->type == COLUMN_TYPE_BOOLEAN) {
                    val = cell->value.as_bool ? "t" : "f";
                    vlen = 1;
                } else if (cell->type == COLUMN_TYPE_SMALLINT) {
                    vlen = (size_t)snprintf(buf, sizeof(buf), "%d", (int)cell->value.as_smallint);
                    val = buf;
                } else {
                    val = "?"; vlen = 1;
                }
                if (val) msgbuf_push(m, (const uint8_t *)val, vlen);
            }
        }
        msgbuf_push_byte(m, '\n');
        msg_send(fd, 'd', m);
    }
    /* CopyDone */
    m->len = 0;
    msg_send(fd, 'c', m);
    char tag[128];
    snprintf(tag, sizeof(tag), "COPY %zu", ct->rows.count);
    send_command_complete(fd, m, tag);
    return 0;
}

/* Build command tag and send result data for the given query type.
 * Writes the tag into tag_buf (must be >= 128 bytes). */
static void build_command_tag(int fd, struct database *db, struct query *q,
                              struct rows *result, struct msgbuf *m,
                              int skip_row_desc, int rc,
                              char *tag_buf, size_t tag_sz)
{
    switch (q->query_type) {
        case QUERY_TYPE_CREATE:
            if (q->create_table.as_select_sql != IDX_NONE && result->count > 0) {
                size_t sel_count = (size_t)result->data[0].cells.items[0].value.as_int;
                snprintf(tag_buf, tag_sz, "SELECT %zu", sel_count);
            } else {
                snprintf(tag_buf, tag_sz, "CREATE TABLE");
            }
            break;
        case QUERY_TYPE_DROP:
            snprintf(tag_buf, tag_sz, "DROP TABLE");
            break;
        case QUERY_TYPE_SELECT:
            if (!skip_row_desc)
                send_row_description(fd, db, q, result);
            send_data_rows(fd, result);
            snprintf(tag_buf, tag_sz, "SELECT %zu", result->count);
            break;
        case QUERY_TYPE_INSERT:
            if (result->count > 0) {
                if (!skip_row_desc)
                    send_row_description(fd, db, q, result);
                send_data_rows(fd, result);
            }
            {
                size_t ins_count = q->insert.insert_rows_count;
                if (rc > 0) ins_count = (size_t)rc; /* INSERT...SELECT */
                snprintf(tag_buf, tag_sz, "INSERT 0 %zu", ins_count);
            }
            break;
        case QUERY_TYPE_DELETE: {
            if (q->del.has_returning && result->count > 0) {
                if (!skip_row_desc)
                    send_row_description(fd, db, q, result);
                send_data_rows(fd, result);
                snprintf(tag_buf, tag_sz, "DELETE %zu", result->count);
            } else {
                size_t del_count = 0;
                if (result->count > 0 && result->data[0].cells.count > 0)
                    del_count = (size_t)result->data[0].cells.items[0].value.as_int;
                snprintf(tag_buf, tag_sz, "DELETE %zu", del_count);
            }
            break;
        }
        case QUERY_TYPE_UPDATE: {
            if (q->update.has_returning && result->count > 0) {
                if (!skip_row_desc)
                    send_row_description(fd, db, q, result);
                send_data_rows(fd, result);
                snprintf(tag_buf, tag_sz, "UPDATE %zu", result->count);
            } else {
                size_t upd_count = 0;
                if (result->count > 0 && result->data[0].cells.count > 0)
                    upd_count = (size_t)result->data[0].cells.items[0].value.as_int;
                snprintf(tag_buf, tag_sz, "UPDATE %zu", upd_count);
            }
            break;
        }
        case QUERY_TYPE_CREATE_INDEX:
            snprintf(tag_buf, tag_sz, "CREATE INDEX");
            break;
        case QUERY_TYPE_DROP_INDEX:
            snprintf(tag_buf, tag_sz, "DROP INDEX");
            break;
        case QUERY_TYPE_CREATE_TYPE:
            snprintf(tag_buf, tag_sz, "CREATE TYPE");
            break;
        case QUERY_TYPE_DROP_TYPE:
            snprintf(tag_buf, tag_sz, "DROP TYPE");
            break;
        case QUERY_TYPE_ALTER:
            snprintf(tag_buf, tag_sz, "ALTER TABLE");
            break;
        case QUERY_TYPE_CREATE_SEQUENCE:
            snprintf(tag_buf, tag_sz, "CREATE SEQUENCE");
            break;
        case QUERY_TYPE_DROP_SEQUENCE:
            snprintf(tag_buf, tag_sz, "DROP SEQUENCE");
            break;
        case QUERY_TYPE_CREATE_VIEW:
            snprintf(tag_buf, tag_sz, "CREATE VIEW");
            break;
        case QUERY_TYPE_DROP_VIEW:
            snprintf(tag_buf, tag_sz, "DROP VIEW");
            break;
        case QUERY_TYPE_TRUNCATE:
            snprintf(tag_buf, tag_sz, "TRUNCATE TABLE");
            break;
        case QUERY_TYPE_EXPLAIN:
            if (result->count > 0) {
                if (!skip_row_desc)
                    send_row_description(fd, db, q, result);
                send_data_rows(fd, result);
            }
            snprintf(tag_buf, tag_sz, "EXPLAIN");
            break;
        case QUERY_TYPE_BEGIN:
            snprintf(tag_buf, tag_sz, "BEGIN");
            break;
        case QUERY_TYPE_COMMIT:
            snprintf(tag_buf, tag_sz, "COMMIT");
            break;
        case QUERY_TYPE_ROLLBACK:
            snprintf(tag_buf, tag_sz, "ROLLBACK");
            break;
        case QUERY_TYPE_COPY:
            snprintf(tag_buf, tag_sz, "COPY");
            break;
        case QUERY_TYPE_SET:
            snprintf(tag_buf, tag_sz, "SET");
            break;
        case QUERY_TYPE_SHOW:
            if (!skip_row_desc) {
                /* send a single-column RowDescription with the parameter name */
                char col_name[128];
                snprintf(col_name, sizeof(col_name), "%.*s",
                         (int)q->show.parameter.len, q->show.parameter.data);
                m->len = 0;
                msgbuf_push_u16(m, 1); /* 1 column */
                msgbuf_push(m, (const uint8_t *)col_name, strlen(col_name));
                msgbuf_push_byte(m, 0); /* null terminator */
                msgbuf_push_u32(m, 0);  /* table OID */
                msgbuf_push_u16(m, 0);  /* column attr number */
                msgbuf_push_u32(m, 25); /* type OID: text */
                msgbuf_push_u16(m, (uint16_t)-1); /* type size */
                msgbuf_push_u32(m, 0);  /* type modifier */
                msgbuf_push_u16(m, 0);  /* format: text */
                msg_send(fd, 'T', m);
            }
            send_data_rows(fd, result);
            snprintf(tag_buf, tag_sz, "SHOW");
            break;
    }
}

/* ---- handle_query_inner orchestrator ---- */

static int handle_query_inner(int fd, struct database *db, const char *sql,
                              struct msgbuf *m, struct query_arena *conn_arena,
                              int skip_row_desc)
{
    /* reject oversized queries */
    size_t sql_len = strlen(sql);
    if (sql_len > 1024 * 1024) {
        send_error(fd, m, "ERROR", "54000", "query exceeds maximum length (1 MB)");
        return -1;
    }

    /* skip empty / whitespace-only queries */
    const char *p = sql;
    while (*p && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    if (*p == '\0') {
        send_empty_query(fd, m);
        return 0;
    }

    /* Internal test-only command: reset database to clean state */
    if (strncmp(sql, "SELECT __reset_db()", sizeof("SELECT __reset_db()") - 1) == 0) {
        db_reset(db);
        rcache_invalidate_all();
        if (!skip_row_desc) {
            m->len = 0;
            msgbuf_push_u16(m, 1); /* 1 column */
            msgbuf_push_cstr(m, "__reset_db");
            msgbuf_push_u32(m, 0);            /* table OID */
            msgbuf_push_u16(m, 0);            /* column attr */
            msgbuf_push_u32(m, 25);           /* type OID: text */
            msgbuf_push_u16(m, (uint16_t)-1); /* type size */
            msgbuf_push_u32(m, (uint32_t)-1); /* type modifier */
            msgbuf_push_u16(m, 0);            /* format: text */
            msg_send(fd, 'T', m);
        }
        m->len = 0;
        msgbuf_push_u16(m, 1);  /* 1 column */
        msgbuf_push_u32(m, 2);  /* 2 bytes */
        msgbuf_push(m, (const uint8_t *)"ok", 2);
        msg_send(fd, 'D', m);
        send_command_complete(fd, m, "SELECT 1");
        return 0;
    }

    /* Fast result cache check — skip parse entirely on cache hit */
    if (!skip_row_desc) {
        uint32_t rc_hash = rcache_hash_sql(sql, sql_len);
        uint32_t rc_slot = rc_hash & (RCACHE_SLOTS - 1);
        uint64_t rc_gen = db->total_generation;
        struct rcache_entry *rce = &g_rcache[rc_slot];
        if (rce->valid && rce->sql_hash == rc_hash && rce->generation == rc_gen) {
            if (send_all(fd, rce->wire_data, rce->wire_len) == 0) {
                char tag[128];
                snprintf(tag, sizeof(tag), "SELECT %d", rce->row_count);
                send_command_complete(fd, m, tag);
                return 0;
            }
        }
    }

    /* Refresh catalog tables if the query references system schemas */
    if (strstr(sql, "pg_") || strstr(sql, "information_schema"))
        catalog_refresh(db);

    /* Queries referencing catalog tables with syntax we can't parse */
    if (strstr(sql, "pg_policy") || strstr(sql, "pg_depend") ||
        strstr(sql, "pg_trigger") || strstr(sql, "pg_rewrite") ||
        strstr(sql, "pg_inherits") || strstr(sql, "pg_foreign_table") ||
        strstr(sql, "pg_stat_user_tables") || strstr(sql, "pg_publication") ||
        strstr(sql, "pg_stat_all_tables") || strstr(sql, "pg_statistic_ext")) {
        if (!skip_row_desc) {
            m->len = 0;
            msgbuf_push_u16(m, 0); /* 0 columns */
            msg_send(fd, 'T', m);
        }
        send_command_complete(fd, m, "SELECT 0");
        return 0;
    }

    struct query q = {0};
    if (query_parse_into(sql, &q, conn_arena) != 0) {
        const char *code = conn_arena->sqlstate[0] ? conn_arena->sqlstate : "42601";
        const char *msg  = conn_arena->errmsg[0]   ? conn_arena->errmsg   : "syntax error or unsupported statement";
        send_error(fd, m, "ERROR", code, msg);
        return -1;
    }

    /* Fast path: try direct columnar→wire send for SELECT queries */
    if (q.query_type == QUERY_TYPE_SELECT && !skip_row_desc) {
        int plan_rows = try_plan_send(fd, db, &q, conn_arena, sql, sql_len);
        if (plan_rows >= 0) {
            *conn_arena = q.arena;
            char tag[128];
            snprintf(tag, sizeof(tag), "SELECT %d", plan_rows);
            send_command_complete(fd, m, tag);
            return 0;
        }
    }

    /* COPY TO STDOUT */
    if (q.query_type == QUERY_TYPE_COPY && !q.copy.is_from)
        return handle_copy_to_stdout(fd, db, &q, m);

    /* COPY FROM STDIN — send CopyInResponse and return 1 to signal copy-in mode */
    if (q.query_type == QUERY_TYPE_COPY && q.copy.is_from) {
        struct table *ct = db_find_table_sv(db, q.copy.table);
        if (!ct) {
            send_error(fd, m, "ERROR", "42P01", "table not found");
            return -1;
        }
        uint16_t ncols = (uint16_t)ct->columns.count;
        m->len = 0;
        msgbuf_push_byte(m, 0); /* text format */
        msgbuf_push_u16(m, ncols);
        for (uint16_t i = 0; i < ncols; i++)
            msgbuf_push_u16(m, 0);
        msg_send(fd, 'G', m);
        return 1;
    }

    /* Execute query */
    struct rows *result = &conn_arena->result;
    result->arena_owns_text = 1;
    int rc = db_exec(db, &q, result, &conn_arena->result_text);

    conn_arena->scratch = q.arena.scratch;
    conn_arena->bump = q.arena.bump;
    conn_arena->plan_nodes = q.arena.plan_nodes;
    conn_arena->cells = q.arena.cells;
    conn_arena->svs = q.arena.svs;
    memcpy(conn_arena->errmsg, q.arena.errmsg, sizeof(conn_arena->errmsg));
    memcpy(conn_arena->sqlstate, q.arena.sqlstate, sizeof(conn_arena->sqlstate));

    if (rc < 0) {
        const char *code = conn_arena->sqlstate[0] ? conn_arena->sqlstate : "42000";
        const char *msg  = conn_arena->errmsg[0]   ? conn_arena->errmsg   : "query execution failed";
        send_error(fd, m, "ERROR", code, msg);
        arena_free_result_rows(conn_arena);
        return -1;
    }

    /* Build command tag, send result data, and complete */
    char tag[128];
    build_command_tag(fd, db, &q, result, m, skip_row_desc, rc, tag, sizeof(tag));
    send_command_complete(fd, m, tag);
    arena_free_result_rows(conn_arena);
    return 0;
}

/* Process a single line of COPY FROM data, inserting a row into the table. */
static void copy_in_process_line(struct client_state *c, struct database *db, const char *line, size_t len)
{
    struct table *ct = c->copy_in_table;
    if (!ct) return;
    uint16_t ncols = (uint16_t)ct->columns.count;
    char delim = c->copy_in_delim;
    /* skip terminator line "\." */
    if (len == 2 && line[0] == '\\' && line[1] == '.') return;
    if (len == 0) return;
    struct row new_row = {0};
    da_init(&new_row.cells);
    const char *p = line;
    const char *end = line + len;
    for (uint16_t ci = 0; ci < ncols; ci++) {
        const char *field_start = p;
        while (p < end && *p != delim) p++;
        size_t flen = (size_t)(p - field_start);
        if (p < end && *p == delim) p++;
        struct cell cell = {0};
        if (!c->copy_in_is_csv && flen == 2 &&
            field_start[0] == '\\' && field_start[1] == 'N') {
            cell.is_null = 1;
            cell.type = ct->columns.items[ci].type;
        } else {
            char fstr_buf[8192];
            char *fstr = fstr_buf;
            char *fstr_dyn = NULL;
            if (flen >= sizeof(fstr_buf)) {
                fstr_dyn = (char *)malloc(flen + 1);
                if (!fstr_dyn) return; /* OOM — skip row */
                fstr = fstr_dyn;
            }
            memcpy(fstr, field_start, flen);
            fstr[flen] = '\0';
            enum column_type ctype = ct->columns.items[ci].type;
            cell.type = ctype;
            if (ctype == COLUMN_TYPE_INT) {
                cell.value.as_int = atoi(fstr);
            } else if (ctype == COLUMN_TYPE_BIGINT) {
                cell.value.as_bigint = atoll(fstr);
            } else if (ctype == COLUMN_TYPE_FLOAT || ctype == COLUMN_TYPE_NUMERIC) {
                cell.value.as_float = atof(fstr);
            } else if (ctype == COLUMN_TYPE_BOOLEAN) {
                cell.value.as_bool = (fstr[0] == 't' || fstr[0] == 'T' || fstr[0] == '1');
            } else if (ctype == COLUMN_TYPE_SMALLINT) {
                cell.value.as_smallint = (int16_t)atoi(fstr);
            } else if (ctype == COLUMN_TYPE_DATE) {
                cell.value.as_date = date_from_str(fstr);
            } else if (ctype == COLUMN_TYPE_TIME) {
                cell.value.as_time = time_from_str(fstr);
            } else if (ctype == COLUMN_TYPE_TIMESTAMP || ctype == COLUMN_TYPE_TIMESTAMPTZ) {
                cell.value.as_timestamp = timestamp_from_str(fstr);
            } else if (ctype == COLUMN_TYPE_INTERVAL) {
                cell.value.as_interval = interval_from_str(fstr);
            } else {
                cell.type = COLUMN_TYPE_TEXT;
                cell.value.as_text = strdup(fstr);
            }
            free(fstr_dyn);
        }
        da_push(&new_row.cells, cell);
    }
    da_push(&ct->rows, new_row);
    ct->generation++;
    db->total_generation++;
    c->copy_in_row_count++;
}

/* Simple Query handler: execute + ReadyForQuery.
 * Returns 1 if COPY FROM STDIN was initiated (caller must not send RFQ yet). */
static int handle_query_sq(struct client_state *c, struct database *db,
                           const char *sql)
{
    /* Ultra-fast cache hit path: send pre-built full reply in one write().
     * The full_reply contains wire_data + CommandComplete + ReadyForQuery,
     * pre-built at cache store time. Zero malloc, zero memcpy, one syscall. */
    if (!c->txn.in_transaction) {
        size_t sql_len = strlen(sql);
        uint32_t rc_hash = rcache_hash_sql(sql, sql_len);
        uint32_t rc_slot = rc_hash & (RCACHE_SLOTS - 1);
        uint64_t rc_gen = db->total_generation;
        struct rcache_entry *rce = &g_rcache[rc_slot];
        if (rce->valid && rce->sql_hash == rc_hash && rce->generation == rc_gen &&
            rce->full_reply && rce->full_reply_len > 0) {
            if (send_all(c->fd, rce->full_reply, rce->full_reply_len) == 0)
                return 0;
        }
    }

    db->active_txn = &c->txn;
    int rc = handle_query_inner(c->fd, db, sql, &c->send_buf, &c->arena, 0);
    db->active_txn = NULL;
    if (rc == 1) {
        /* COPY FROM STDIN initiated — parse the query again to get table info */
        struct query q = {0};
        if (query_parse_into(sql, &q, &c->arena) == 0 && q.query_type == QUERY_TYPE_COPY) {
            c->copy_in_active = 1;
            c->copy_in_table = db_find_table_sv(db, q.copy.table);
            c->copy_in_delim = q.copy.is_csv ? ',' : '\t';
            c->copy_in_is_csv = q.copy.is_csv;
            c->copy_in_row_count = 0;
            c->copy_in_linelen = 0;
        }
        return 1;
    }
    send_ready_for_query(c->fd, &c->send_buf, c->txn.in_transaction ? 'T' : 'I');
    return 0;
}

/* ---- Extended Query Protocol message handlers ---- */

/* Send ParseComplete ('1') */
static int send_parse_complete(int fd, struct msgbuf *m)
{
    m->len = 0;
    return msg_send(fd, '1', m);
}

/* Send BindComplete ('2') */
static int send_bind_complete(int fd, struct msgbuf *m)
{
    m->len = 0;
    return msg_send(fd, '2', m);
}

/* Send CloseComplete ('3') */
static int send_close_complete(int fd, struct msgbuf *m)
{
    m->len = 0;
    return msg_send(fd, '3', m);
}

/* Send NoData ('n') */
static int send_no_data(int fd, struct msgbuf *m)
{
    m->len = 0;
    return msg_send(fd, 'n', m);
}

/* Send ParameterDescription ('t') */
static int send_parameter_description(int fd, struct msgbuf *m,
                                      const uint32_t *oids, uint16_t nparams)
{
    m->len = 0;
    msgbuf_push_u16(m, nparams);
    for (uint16_t i = 0; i < nparams; i++)
        msgbuf_push_u32(m, oids ? oids[i] : 0);
    return msg_send(fd, 't', m);
}

/* Handle Parse message ('P'):
 *   string stmt_name, string query, int16 nparams, int32[nparams] param_oids
 */
static int handle_parse(struct client_state *c, struct database *db,
                        const uint8_t *body, uint32_t body_len)
{
    (void)db;
    const uint8_t *p = body;
    const uint8_t *end = body + body_len;

    /* statement name (NUL-terminated) */
    const char *stmt_name = (const char *)p;
    while (p < end && *p) p++;
    if (p >= end) {
        send_error(c->fd, &c->send_buf, "ERROR", "08P01", "invalid Parse message");
        return -1;
    }
    p++; /* skip NUL */

    /* query string (NUL-terminated) */
    const char *query = (const char *)p;
    while (p < end && *p) p++;
    if (p >= end) {
        send_error(c->fd, &c->send_buf, "ERROR", "08P01", "invalid Parse message");
        return -1;
    }
    p++; /* skip NUL */

    /* number of parameter types */
    uint16_t nparams = 0;
    if (p + 2 <= end) {
        nparams = read_u16(p);
        p += 2;
    }

    /* parameter type OIDs */
    uint32_t *param_oids = NULL;
    if (nparams > 0) {
        if (p + (size_t)nparams * 4 > end) {
            send_error(c->fd, &c->send_buf, "ERROR", "08P01", "invalid Parse message");
            return -1;
        }
        param_oids = malloc(nparams * sizeof(uint32_t));
        for (uint16_t i = 0; i < nparams; i++) {
            param_oids[i] = read_u32(p);
            p += 4;
        }
    }

    /* if unnamed statement already exists, replace it */
    int idx = find_prepared(c, stmt_name);
    if (idx >= 0) {
        prepared_stmt_free(&c->prepared[idx]);
    } else {
        idx = alloc_prepared(c);
        if (idx < 0) {
            free(param_oids);
            send_error(c->fd, &c->send_buf, "ERROR", "53000",
                       "too many prepared statements");
            return -1;
        }
    }

    c->prepared[idx].name = strdup(stmt_name);
    c->prepared[idx].sql = strdup(query);
    c->prepared[idx].param_oids = param_oids;
    c->prepared[idx].nparams = nparams;
    c->prepared[idx].in_use = 1;

    send_parse_complete(c->fd, &c->send_buf);
    return 0;
}

/* Handle Bind message ('B'):
 *   string portal_name, string stmt_name,
 *   int16 nformat_codes, int16[nformat_codes] format_codes,
 *   int16 nparams, (int32 len, byte[len] value)[nparams],
 *   int16 nresult_format_codes, int16[nresult_format_codes] result_format_codes
 */
static int handle_bind(struct client_state *c, struct database *db,
                       const uint8_t *body, uint32_t body_len)
{
    (void)db;
    const uint8_t *p = body;
    const uint8_t *end = body + body_len;

    /* portal name */
    const char *portal_name = (const char *)p;
    while (p < end && *p) p++;
    if (p >= end) goto bad_msg;
    p++;

    /* statement name */
    const char *stmt_name = (const char *)p;
    while (p < end && *p) p++;
    if (p >= end) goto bad_msg;
    p++;

    /* find the prepared statement */
    int stmt_idx = find_prepared(c, stmt_name);
    if (stmt_idx < 0) {
        send_error(c->fd, &c->send_buf, "ERROR", "26000",
                   "prepared statement does not exist");
        return -1;
    }
    struct prepared_stmt *ps = &c->prepared[stmt_idx];

    /* parameter format codes */
    if (p + 2 > end) goto bad_msg;
    uint16_t nformat_codes = read_u16(p); p += 2;
    int *param_formats = NULL;
    if (nformat_codes > 0) {
        if (p + (size_t)nformat_codes * 2 > end) goto bad_msg;
        param_formats = malloc(nformat_codes * sizeof(int));
        for (uint16_t i = 0; i < nformat_codes; i++) {
            param_formats[i] = read_i16(p);
            p += 2;
        }
    }

    /* parameter values */
    if (p + 2 > end) { free(param_formats); goto bad_msg; }
    uint16_t nparams = read_u16(p); p += 2;

    const char **param_values = NULL;
    int *param_lengths = NULL;
    if (nparams > 0) {
        param_values = calloc(nparams, sizeof(char *));
        param_lengths = calloc(nparams, sizeof(int));
    }

    for (uint16_t i = 0; i < nparams; i++) {
        if (p + 4 > end) {
            free(param_formats); free(param_values); free(param_lengths);
            goto bad_msg;
        }
        int32_t len = read_i32(p); p += 4;
        if (len == -1) {
            /* NULL parameter */
            param_values[i] = NULL;
            param_lengths[i] = 0;
        } else {
            if (len < 0 || p + (size_t)len > end) {
                free(param_formats); free(param_values); free(param_lengths);
                goto bad_msg;
            }
            /* make a NUL-terminated copy */
            char *val = malloc((size_t)len + 1);
            memcpy(val, p, (size_t)len);
            val[len] = '\0';
            param_values[i] = val;
            param_lengths[i] = len;
            p += (size_t)len;
        }
    }

    /* result format codes */
    uint16_t nresult_formats = 0;
    int16_t *result_formats = NULL;
    if (p + 2 <= end) {
        nresult_formats = read_u16(p); p += 2;
        if (nresult_formats > 0 && p + (size_t)nresult_formats * 2 <= end) {
            result_formats = malloc(nresult_formats * sizeof(int16_t));
            for (uint16_t i = 0; i < nresult_formats; i++) {
                result_formats[i] = read_i16(p);
                p += 2;
            }
        }
    }

    /* substitute parameters into SQL */
    char *bound_sql = substitute_params(ps->sql, param_values, param_lengths,
                                        param_formats, nparams);

    /* free parameter value copies */
    for (uint16_t i = 0; i < nparams; i++)
        free((void *)param_values[i]);
    free(param_values);
    free(param_lengths);
    free(param_formats);

    if (!bound_sql) {
        free(result_formats);
        send_error(c->fd, &c->send_buf, "ERROR", "XX000",
                   "out of memory during parameter substitution");
        return -1;
    }

    /* create or replace portal */
    int pidx = find_portal(c, portal_name);
    if (pidx >= 0) {
        portal_free(&c->portals[pidx]);
    } else {
        pidx = alloc_portal(c);
        if (pidx < 0) {
            free(bound_sql);
            free(result_formats);
            send_error(c->fd, &c->send_buf, "ERROR", "53000",
                       "too many portals");
            return -1;
        }
    }

    c->portals[pidx].name = strdup(portal_name);
    c->portals[pidx].sql = bound_sql;
    c->portals[pidx].result_formats = result_formats;
    c->portals[pidx].nresult_formats = nresult_formats;
    c->portals[pidx].max_rows = 0;
    c->portals[pidx].in_use = 1;
    c->portals[pidx].stmt_idx = stmt_idx;

    send_bind_complete(c->fd, &c->send_buf);
    return 0;

bad_msg:
    send_error(c->fd, &c->send_buf, "ERROR", "08P01", "invalid Bind message");
    return -1;
}

/* Handle Describe message ('D'):
 *   byte type ('S' = statement, 'P' = portal), string name
 */
static int handle_describe(struct client_state *c, struct database *db,
                           const uint8_t *body, uint32_t body_len)
{
    if (body_len < 2) {
        send_error(c->fd, &c->send_buf, "ERROR", "08P01", "invalid Describe message");
        return -1;
    }

    char desc_type = (char)body[0];
    const char *name = (const char *)(body + 1);

    if (desc_type == 'S') {
        /* Describe Statement → ParameterDescription + RowDescription (or NoData) */
        int idx = find_prepared(c, name);
        if (idx < 0) {
            send_error(c->fd, &c->send_buf, "ERROR", "26000",
                       "prepared statement does not exist");
            return -1;
        }
        struct prepared_stmt *ps = &c->prepared[idx];

        /* Send ParameterDescription */
        send_parameter_description(c->fd, &c->send_buf,
                                   ps->param_oids, ps->nparams);

        /* Try to determine if this is a SELECT by doing a trial parse.
         * If it's a SELECT, send RowDescription; otherwise send NoData. */
        /* For simplicity, substitute dummy values and try to parse */
        const char **dummy_vals = NULL;
        if (ps->nparams > 0) {
            dummy_vals = calloc(ps->nparams, sizeof(char *));
            for (uint16_t i = 0; i < ps->nparams; i++)
                dummy_vals[i] = "0"; /* dummy value for type inference */
        }
        char *test_sql = substitute_params(ps->sql, dummy_vals,
                                           NULL, NULL, ps->nparams);
        free(dummy_vals);

        if (test_sql) {
            struct query q = {0};
            if (query_parse_into(test_sql, &q, &c->arena) == 0) {
                if (q.query_type == QUERY_TYPE_SELECT) {
                    /* try to get table for column metadata */
                    struct table *t = NULL;
                    if (q.select.table.len > 0)
                        t = db_find_table_sv(db, q.select.table);

                    if (t) {
                        /* build RowDescription from table metadata */
                        int select_all = sv_eq_cstr(q.select.columns, "*");
                        struct msgbuf rm;
                        msgbuf_init(&rm);
                        uint16_t ncols;
                        if (select_all) {
                            ncols = (uint16_t)t->columns.count;
                        } else if (q.select.parsed_columns_count > 0) {
                            ncols = (uint16_t)q.select.parsed_columns_count;
                        } else {
                            ncols = (uint16_t)t->columns.count;
                        }
                        msgbuf_push_u16(&rm, ncols);
                        for (uint16_t i = 0; i < ncols; i++) {
                            const char *colname = "?";
                            uint32_t type_oid = 25;
                            if (select_all && (size_t)i < t->columns.count) {
                                colname = t->columns.items[i].name;
                                type_oid = column_type_to_oid(t->columns.items[i].type);
                            } else if (q.select.parsed_columns_count > 0 &&
                                       (uint32_t)i < q.select.parsed_columns_count) {
                                struct select_column *sc =
                                    &q.arena.select_cols.items[q.select.parsed_columns_start + i];
                                if (sc->alias.len > 0) {
                                    static __thread char abuf[256];
                                    size_t al = sc->alias.len < 255 ? sc->alias.len : 255;
                                    memcpy(abuf, sc->alias.data, al);
                                    abuf[al] = '\0';
                                    colname = abuf;
                                } else if (sc->expr_idx != IDX_NONE) {
                                    struct expr *e = &EXPR(&q.arena, sc->expr_idx);
                                    if (e->type == EXPR_COLUMN_REF) {
                                        int ci = table_find_column_sv(t, e->column_ref.column);
                                        if (ci >= 0) {
                                            colname = t->columns.items[ci].name;
                                            type_oid = column_type_to_oid(t->columns.items[ci].type);
                                        }
                                    }
                                }
                            } else if ((size_t)i < t->columns.count) {
                                colname = t->columns.items[i].name;
                                type_oid = column_type_to_oid(t->columns.items[i].type);
                            }
                            msgbuf_push_cstr(&rm, colname);
                            msgbuf_push_u32(&rm, 0);
                            msgbuf_push_u16(&rm, 0);
                            msgbuf_push_u32(&rm, type_oid);
                            msgbuf_push_u16(&rm, (uint16_t)-1);
                            msgbuf_push_u32(&rm, (uint32_t)-1);
                            msgbuf_push_u16(&rm, 0);
                        }
                        msg_send(c->fd, 'T', &rm);
                        msgbuf_free(&rm);
                    } else {
                        send_no_data(c->fd, &c->send_buf);
                    }
                } else {
                    send_no_data(c->fd, &c->send_buf);
                }
            } else {
                send_no_data(c->fd, &c->send_buf);
            }
            free(test_sql);
        } else {
            send_no_data(c->fd, &c->send_buf);
        }
    } else if (desc_type == 'P') {
        /* Describe Portal → RowDescription (or NoData) */
        int idx = find_portal(c, name);
        if (idx < 0) {
            send_error(c->fd, &c->send_buf, "ERROR", "34000",
                       "portal does not exist");
            return -1;
        }
        struct portal *portal = &c->portals[idx];

        /* Parse the bound SQL to determine if it produces rows */
        struct query q = {0};
        if (query_parse_into(portal->sql, &q, &c->arena) == 0 &&
            q.query_type == QUERY_TYPE_SELECT) {
            struct table *t = NULL;
            if (q.select.table.len > 0)
                t = db_find_table_sv(db, q.select.table);
            if (t) {
                int select_all = sv_eq_cstr(q.select.columns, "*");
                struct msgbuf rm;
                msgbuf_init(&rm);
                uint16_t ncols;
                if (select_all) {
                    ncols = (uint16_t)t->columns.count;
                } else if (q.select.parsed_columns_count > 0) {
                    ncols = (uint16_t)q.select.parsed_columns_count;
                } else {
                    ncols = (uint16_t)t->columns.count;
                }
                msgbuf_push_u16(&rm, ncols);
                for (uint16_t i = 0; i < ncols; i++) {
                    const char *colname = "?";
                    uint32_t type_oid = 25;
                    if (select_all && (size_t)i < t->columns.count) {
                        colname = t->columns.items[i].name;
                        type_oid = column_type_to_oid(t->columns.items[i].type);
                    } else if ((size_t)i < t->columns.count) {
                        colname = t->columns.items[i].name;
                        type_oid = column_type_to_oid(t->columns.items[i].type);
                    }
                    msgbuf_push_cstr(&rm, colname);
                    msgbuf_push_u32(&rm, 0);
                    msgbuf_push_u16(&rm, 0);
                    msgbuf_push_u32(&rm, type_oid);
                    msgbuf_push_u16(&rm, (uint16_t)-1);
                    msgbuf_push_u32(&rm, (uint32_t)-1);
                    msgbuf_push_u16(&rm, 0);
                }
                msg_send(c->fd, 'T', &rm);
                msgbuf_free(&rm);
            } else {
                send_no_data(c->fd, &c->send_buf);
            }
        } else {
            send_no_data(c->fd, &c->send_buf);
        }
    } else {
        send_error(c->fd, &c->send_buf, "ERROR", "08P01",
                   "invalid Describe target type");
        return -1;
    }

    return 0;
}

/* Handle Execute message ('E'):
 *   string portal_name, int32 max_rows (0 = unlimited)
 */
static int handle_execute(struct client_state *c, struct database *db,
                          const uint8_t *body, uint32_t body_len)
{
    const uint8_t *p = body;
    const uint8_t *end = body + body_len;

    /* portal name */
    const char *portal_name = (const char *)p;
    while (p < end && *p) p++;
    if (p >= end) {
        send_error(c->fd, &c->send_buf, "ERROR", "08P01", "invalid Execute message");
        return -1;
    }
    p++;

    /* max_rows (0 = no limit) */
    int32_t max_rows = 0;
    if (p + 4 <= end) {
        max_rows = read_i32(p);
        p += 4;
    }
    (void)max_rows; /* we always return all rows for now */

    int idx = find_portal(c, portal_name);
    if (idx < 0) {
        send_error(c->fd, &c->send_buf, "ERROR", "34000",
                   "portal does not exist");
        return -1;
    }

    /* Execute the bound SQL (no ReadyForQuery — that comes from Sync).
     * Always send RowDescription from Execute (skip_row_desc=0) so that
     * JDBC drivers always receive field structure before DataRows.
     * Describe may have sent RowDescription too — drivers handle this. */
    db->active_txn = &c->txn;
    handle_query_inner(c->fd, db, c->portals[idx].sql,
                       &c->send_buf, &c->arena, 0);
    db->active_txn = NULL;
    return 0;
}

/* Handle Close message ('C'):
 *   byte type ('S' = statement, 'P' = portal), string name
 */
static int handle_close(struct client_state *c,
                        const uint8_t *body, uint32_t body_len)
{
    if (body_len < 2) {
        send_error(c->fd, &c->send_buf, "ERROR", "08P01", "invalid Close message");
        return -1;
    }

    char close_type = (char)body[0];
    const char *name = (const char *)(body + 1);

    if (close_type == 'S') {
        int idx = find_prepared(c, name);
        if (idx >= 0) prepared_stmt_free(&c->prepared[idx]);
    } else if (close_type == 'P') {
        int idx = find_portal(c, name);
        if (idx >= 0) portal_free(&c->portals[idx]);
    }

    send_close_complete(c->fd, &c->send_buf);
    return 0;
}

/* ---- startup handshake (non-blocking, returns 0=ok, -1=disconnect, 1=need more data) ---- */

static int complete_auth(struct client_state *c)
{
    send_auth_ok(c->fd, &c->send_buf);
    send_parameter_status(c->fd, &c->send_buf, "server_version", "15.0");
    send_parameter_status(c->fd, &c->send_buf, "server_encoding", "UTF8");
    send_parameter_status(c->fd, &c->send_buf, "client_encoding", "UTF8");
    send_parameter_status(c->fd, &c->send_buf, "DateStyle", "ISO, MDY");
    send_parameter_status(c->fd, &c->send_buf, "integer_datetimes", "on");

    /* BackendKeyData (fake) */
    c->send_buf.len = 0;
    msgbuf_push_u32(&c->send_buf, (uint32_t)getpid());
    msgbuf_push_u32(&c->send_buf, 0);
    msg_send(c->fd, 'K', &c->send_buf);

    send_ready_for_query(c->fd, &c->send_buf, 'I');
    c->phase = PHASE_QUERY_LOOP;
    return 0;
}

static int process_startup(struct client_state *c)
{
    /* startup messages have no type byte — just int32 length + int32 version + params */
    /* need at least 4 bytes for the length */
    if (c->recv_len < 4) return 1;
    uint32_t startup_len = read_u32(c->recv_buf);
    if (startup_len < 8 || startup_len > 65536) return -1;
    if (c->recv_len < startup_len) return 1; /* need more data */

    uint32_t version = read_u32(c->recv_buf + 4);

    /* SSLRequest (code 80877103) — decline with 'N', wait for real startup */
    if (version == 80877103) {
        uint8_t n = 'N';
        if (send_all(c->fd, &n, 1) != 0) return -1;
        client_recv_consume(c, startup_len);
        c->phase = PHASE_SSL_RETRY;
        return 0;
    }

    /* expect protocol 3.0 */
    uint16_t major = (version >> 16) & 0xffff;
    if (major != 3) {
        fprintf(stderr, "[pgwire] unsupported protocol version %u.%u\n",
                major, version & 0xffff);
        return -1;
    }

    client_recv_consume(c, startup_len);
    return complete_auth(c);
}

/* ---- process buffered query-loop messages (returns 0=ok, -1=disconnect) ---- */

static int process_messages(struct client_state *c, struct database *db)
{
    for (;;) {
        /* need at least 5 bytes: 1 type + 4 length */
        if (c->recv_len < 5) return 0;
        uint8_t msg_type = c->recv_buf[0];
        uint32_t msg_len = read_u32(c->recv_buf + 1);
        if (msg_len < 4) return -1;
        if (msg_len > 16 * 1024 * 1024) return -1; /* reject messages > 16 MB */
        size_t total = 1 + msg_len; /* type byte + length-inclusive body */
        if (c->recv_len < total) return 0; /* need more data */

        uint32_t body_len = msg_len - 4;

        /* In extended query mode, after an error the backend must discard
         * all messages until it sees a Sync (or Terminate). */
        if (c->extended_error && msg_type != 'S' && msg_type != 'X') {
            client_recv_consume(c, total);
            continue;
        }

        switch (msg_type) {
            case 'Q': { /* Simple Query */
                c->extended_error = 0; /* Simple Query resets error state */
                if (body_len > 0) {
                    char *sql = (char *)(c->recv_buf + 5);
                    char saved = sql[body_len];
                    sql[body_len] = '\0';
                    handle_query_sq(c, db, sql);
                    sql[body_len] = saved;
                }
                break;
            }

            /* ---- COPY FROM STDIN data messages ---- */
            case 'd': { /* CopyData */
                if (c->copy_in_active) {
                    const uint8_t *data = c->recv_buf + 5;
                    for (uint32_t di = 0; di < body_len; di++) {
                        if (data[di] == '\n') {
                            if (c->copy_in_line_overflow) {
                                /* skip this overflowed line */
                                c->copy_in_line_overflow = 0;
                            } else {
                                c->copy_in_linebuf[c->copy_in_linelen] = '\0';
                                copy_in_process_line(c, db, c->copy_in_linebuf, c->copy_in_linelen);
                            }
                            c->copy_in_linelen = 0;
                        } else if (!c->copy_in_line_overflow) {
                            if (c->copy_in_linelen < sizeof(c->copy_in_linebuf) - 1) {
                                c->copy_in_linebuf[c->copy_in_linelen++] = (char)data[di];
                            } else {
                                c->copy_in_line_overflow = 1;
                            }
                        }
                    }
                }
                break;
            }
            case 'c': { /* CopyDone */
                if (c->copy_in_active) {
                    /* flush any remaining partial line */
                    if (c->copy_in_linelen > 0) {
                        c->copy_in_linebuf[c->copy_in_linelen] = '\0';
                        copy_in_process_line(c, db, c->copy_in_linebuf, c->copy_in_linelen);
                        c->copy_in_linelen = 0;
                    }
                    /* Invalidate scan cache */
                    if (c->copy_in_table)
                        c->copy_in_table->scan_cache.generation = 0;
                    char tag[128];
                    snprintf(tag, sizeof(tag), "COPY %zu", c->copy_in_row_count);
                    send_command_complete(c->fd, &c->send_buf, tag);
                    send_ready_for_query(c->fd, &c->send_buf,
                                         c->txn.in_transaction ? 'T' : 'I');
                    c->copy_in_active = 0;
                    c->copy_in_table = NULL;
                }
                break;
            }
            case 'f': { /* CopyFail */
                if (c->copy_in_active) {
                    c->copy_in_active = 0;
                    c->copy_in_table = NULL;
                    send_error(c->fd, &c->send_buf, "ERROR", "57014", "COPY FROM cancelled");
                    send_ready_for_query(c->fd, &c->send_buf,
                                         c->txn.in_transaction ? 'T' : 'I');
                }
                break;
            }

            /* ---- Extended Query Protocol ---- */

            case 'P': { /* Parse */
                const uint8_t *body = c->recv_buf + 5;
                if (handle_parse(c, db, body, body_len) < 0)
                    c->extended_error = 1;
                break;
            }
            case 'B': { /* Bind */
                const uint8_t *body = c->recv_buf + 5;
                if (handle_bind(c, db, body, body_len) < 0)
                    c->extended_error = 1;
                break;
            }
            case 'D': { /* Describe */
                const uint8_t *body = c->recv_buf + 5;
                if (handle_describe(c, db, body, body_len) < 0)
                    c->extended_error = 1;
                break;
            }
            case 'E': { /* Execute */
                const uint8_t *body = c->recv_buf + 5;
                if (handle_execute(c, db, body, body_len) < 0)
                    c->extended_error = 1;
                break;
            }
            case 'H': { /* Flush — send any pending output */
                /* We write synchronously, so nothing to flush */
                break;
            }
            case 'S': { /* Sync — end of extended query pipeline */
                c->extended_error = 0; /* clear error state */
                send_ready_for_query(c->fd, &c->send_buf,
                                     c->txn.in_transaction ? 'T' : 'I');
                break;
            }
            case 'C': { /* Close (statement or portal) */
                const uint8_t *body = c->recv_buf + 5;
                handle_close(c, body, body_len);
                break;
            }

            case 'X': /* Terminate */
                return -1;
            default:
                /* ignore unknown messages (including 'd'/'c'/'f' when not in copy mode) */
                break;
        }

        client_recv_consume(c, total);
    }
}

/* ---- public API ---- */

int pgwire_init(struct pgwire_server *srv, struct database *db, int port)
{
    srv->db   = db;
    srv->port = port;

    srv->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (srv->listen_fd < 0) {
        perror("socket");
        return -1;
    }

    int opt = 1;
    setsockopt(srv->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons((uint16_t)port);

    if (bind(srv->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(srv->listen_fd);
        return -1;
    }

    if (listen(srv->listen_fd, 128) < 0) {
        perror("listen");
        close(srv->listen_fd);
        return -1;
    }

    return 0;
}

extern volatile sig_atomic_t g_running;

/* self-pipe for waking poll() from signal handler */
static int g_wakeup_pipe[2] = {-1, -1};

void pgwire_signal_wakeup(void)
{
    if (g_wakeup_pipe[1] >= 0) {
        char c = 1;
        (void)write(g_wakeup_pipe[1], &c, 1);
    }
}

int pgwire_run(struct pgwire_server *srv)
{
    printf("[pgwire] listening on port %d\n", srv->port);
    printf("[pgwire] connect with: psql -h 127.0.0.1 -p %d\n", srv->port);

    /* set up self-pipe for signal wakeup */
    if (pipe(g_wakeup_pipe) != 0) { perror("pipe"); return -1; }
    set_nonblocking(g_wakeup_pipe[0]);
    set_nonblocking(g_wakeup_pipe[1]);

    /* fds layout: [0]=wakeup pipe read, [1]=listen, [2..]=clients */
    struct pollfd fds[2 + MAX_CLIENTS];
    struct client_state clients[MAX_CLIENTS];
    int nclients = 0;

    set_nonblocking(srv->listen_fd);
    fds[0].fd = g_wakeup_pipe[0];
    fds[0].events = POLLIN;
    fds[1].fd = srv->listen_fd;
    fds[1].events = POLLIN;

    while (g_running) {
        /* build pollfd array: [0]=wakeup, [1]=listen, [2..nclients+1]=clients */
        for (int i = 0; i < nclients; i++) {
            fds[2 + i].fd = clients[i].fd;
            fds[2 + i].events = POLLIN;
            fds[2 + i].revents = 0;
        }
        fds[0].revents = 0;
        fds[1].revents = 0;

        int nready = poll(fds, (nfds_t)(2 + nclients), -1 /* block until event */);
        if (nready < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }
        if (nready == 0) continue; /* timeout, check g_running */

        /* drain wakeup pipe if signalled */
        if (fds[0].revents & POLLIN) {
            char buf[64];
            while (read(g_wakeup_pipe[0], buf, sizeof(buf)) > 0) {}
            if (!g_running) break;
        }

        /* check listen socket for new connections */
        if (fds[1].revents & POLLIN) {
            for (;;) {
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(srv->listen_fd,
                                       (struct sockaddr *)&client_addr, &client_len);
                if (client_fd < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                    if (errno == EINTR) break;
                    perror("accept");
                    break;
                }
                if (nclients >= MAX_CLIENTS) {
                    fprintf(stderr, "[pgwire] max clients reached, rejecting\n");
                    close(client_fd);
                    continue;
                }
                set_nonblocking(client_fd);
                { int one = 1; setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)); }
#if defined(TCP_NOPUSH)
                { int one = 1; setsockopt(client_fd, IPPROTO_TCP, TCP_NOPUSH, &one, sizeof(one)); }
#elif defined(TCP_CORK)
                { int one = 1; setsockopt(client_fd, IPPROTO_TCP, TCP_CORK, &one, sizeof(one)); }
#endif
                client_init(&clients[nclients], client_fd);
                nclients++;
            }
        }

        /* Phase 1: read all ready clients (batch I/O before execution) */
        for (int i = 0; i < nclients; i++) {
            if (!(fds[2 + i].revents & (POLLIN | POLLHUP | POLLERR)))
                continue;

            client_recv_ensure(&clients[i], 4096);
            ssize_t n = read(clients[i].fd,
                             clients[i].recv_buf + clients[i].recv_len,
                             clients[i].recv_cap - clients[i].recv_len);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                    continue;
                client_disconnect(&clients[i], srv->db);
                clients[i] = clients[nclients - 1];
                fds[2 + i].revents = fds[2 + nclients - 1].revents;
                nclients--;
                i--;
                continue;
            }
            if (n == 0) {
                client_disconnect(&clients[i], srv->db);
                clients[i] = clients[nclients - 1];
                fds[2 + i].revents = fds[2 + nclients - 1].revents;
                nclients--;
                i--;
                continue;
            }
            clients[i].recv_len += (size_t)n;
        }

        /* Phase 2: process all clients' buffered messages */
        for (int i = 0; i < nclients; i++) {
            if (clients[i].recv_len == 0) continue;

            int rc;
            switch (clients[i].phase) {
                case PHASE_STARTUP:
                case PHASE_SSL_RETRY:
                    rc = process_startup(&clients[i]);
                    break;
                case PHASE_QUERY_LOOP:
                    rc = process_messages(&clients[i], srv->db);
                    break;
            }

            if (rc < 0) {
                client_disconnect(&clients[i], srv->db);
                clients[i] = clients[nclients - 1];
                nclients--;
                i--;
            }
        }
    }

    /* clean up all remaining clients */
    for (int i = 0; i < nclients; i++)
        client_disconnect(&clients[i], srv->db);

    /* close wakeup pipe */
    close(g_wakeup_pipe[0]); g_wakeup_pipe[0] = -1;
    close(g_wakeup_pipe[1]); g_wakeup_pipe[1] = -1;

    return 0;
}

void pgwire_stop(struct pgwire_server *srv)
{
    if (srv->listen_fd >= 0) {
        close(srv->listen_fd);
        srv->listen_fd = -1;
    }
}
