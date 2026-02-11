#include "pgwire.h"
#include "parser.h"
#include "plan.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <errno.h>
#include <poll.h>
#include <fcntl.h>

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
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                continue;
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

/* map our column types to PG OIDs */
static uint32_t column_type_to_oid(enum column_type t)
{
    switch (t) {
        case COLUMN_TYPE_INT:       return 23;    /* int4 */
        case COLUMN_TYPE_FLOAT:     return 701;   /* float8 */
        case COLUMN_TYPE_TEXT:      return 25;    /* text */
        case COLUMN_TYPE_ENUM:      return 25;    /* text (enums sent as text) */
        case COLUMN_TYPE_BOOLEAN:   return 16;    /* bool */
        case COLUMN_TYPE_BIGINT:    return 20;    /* int8 */
        case COLUMN_TYPE_NUMERIC:   return 1700;  /* numeric */
        case COLUMN_TYPE_DATE:      return 1082;  /* date */
        case COLUMN_TYPE_TIME:      return 1083;  /* time */
        case COLUMN_TYPE_TIMESTAMP: return 1114;  /* timestamp */
        case COLUMN_TYPE_TIMESTAMPTZ: return 1184; /* timestamptz */
        case COLUMN_TYPE_INTERVAL:  return 1186;  /* interval */
        case COLUMN_TYPE_UUID:      return 2950;  /* uuid */
    }
    return 25;
}

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
        case COLUMN_TYPE_TEXT:
        case COLUMN_TYPE_ENUM:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_TIME:
        case COLUMN_TYPE_TIMESTAMP:
        case COLUMN_TYPE_TIMESTAMPTZ:
        case COLUMN_TYPE_INTERVAL:
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
};

static void client_init(struct client_state *c, int fd)
{
    c->fd = fd;
    c->phase = PHASE_STARTUP;
    msgbuf_init(&c->send_buf);
    c->recv_buf = NULL;
    c->recv_len = 0;
    c->recv_cap = 0;
    query_arena_init(&c->arena);
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
    query_arena_destroy(&c->arena);
}

/* rollback any open transaction when a client disconnects, then free */
static void client_disconnect(struct client_state *c, struct database *db)
{
    if (db->in_transaction) {
        struct query q = {0};
        q.query_type = QUERY_TYPE_ROLLBACK;
        struct rows r = {0};
        db_exec(db, &q, &r, NULL);
        rows_free(&r);
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
    if (q->query_type == QUERY_TYPE_SELECT && q->select.table.len > 0) {
        t = db_find_table_sv(db, q->select.table);
    }

    msgbuf_push_u16(&m, (uint16_t)ncols);

    for (int i = 0; i < ncols; i++) {
        const char *colname = "?";
        uint32_t type_oid = 25; /* text */

        if (q->select.select_exprs_count > 0 && (uint32_t)i < q->select.select_exprs_count) {
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
                case AGG_SUM:   colname = "sum";   break;
                case AGG_COUNT: colname = "count"; break;
                case AGG_AVG:   colname = "avg";   break;
                case AGG_MIN:   colname = "min";   break;
                case AGG_MAX:   colname = "max";   break;
                case AGG_NONE:  colname = "?";     break;
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
    if (cb->type == COLUMN_TYPE_INT || cb->type == COLUMN_TYPE_BOOLEAN) {
        if (cb->type == COLUMN_TYPE_BOOLEAN) {
            buf[0] = cb->data.i32[ri] ? 't' : 'f';
            buf[1] = '\0';
            len = 1;
        } else {
            len = fast_i32_to_str(cb->data.i32[ri], buf);
        }
        txt = buf;
    } else if (cb->type == COLUMN_TYPE_BIGINT) {
        len = fast_i64_to_str(cb->data.i64[ri], buf);
        txt = buf;
    } else if (cb->type == COLUMN_TYPE_FLOAT || cb->type == COLUMN_TYPE_NUMERIC) {
        len = (size_t)snprintf(buf, sizeof(buf), "%g", cb->data.f64[ri]);
        txt = buf;
    } else {
        /* TEXT, ENUM, DATE, TIME, TIMESTAMP, etc. */
        if (!cb->data.str[ri]) {
            msgbuf_push_u32(m, (uint32_t)-1);
            return;
        }
        txt = cb->data.str[ri];
        len = strlen(txt);
    }
    msgbuf_push_u32(m, (uint32_t)len);
    msgbuf_push(m, txt, len);
}

/* Send RowDescription using table metadata + plan column info.
 * ncols/col_types come from the plan's output columns.
 * t2 is optional — non-NULL for JOIN queries (columns from two tables). */
static int send_row_desc_plan(int fd, struct table *t, struct table *t2,
                              struct query *q,
                              uint16_t ncols, const enum column_type *col_types)
{
    struct msgbuf m;
    msgbuf_init(&m);
    msgbuf_push_u16(&m, ncols);

    int select_all = sv_eq_cstr(q->select.columns, "*");
    uint16_t t1_ncols = t ? (uint16_t)t->columns.count : 0;

    for (uint16_t i = 0; i < ncols; i++) {
        const char *colname = "?";
        uint32_t type_oid = column_type_to_oid(col_types[i]);

        if (select_all && t && (size_t)i < t->columns.count) {
            colname = t->columns.items[i].name;
        } else if (select_all && t2 && i >= t1_ncols &&
                   (size_t)(i - t1_ncols) < t2->columns.count) {
            colname = t2->columns.items[i - t1_ncols].name;
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
                if (e->type == EXPR_COLUMN_REF) {
                    int ci = -1;
                    if (t) ci = table_find_column_sv(t, e->column_ref.column);
                    if (ci >= 0) {
                        colname = t->columns.items[ci].name;
                    } else if (t2) {
                        ci = table_find_column_sv(t2, e->column_ref.column);
                        if (ci >= 0) colname = t2->columns.items[ci].name;
                    }
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

/* Try to execute a SELECT via the plan executor and send results directly
 * from columnar blocks to the wire, bypassing block_to_rows entirely.
 * Returns row count on success, -1 if the query can't use this path. */
static int try_plan_send(int fd, struct database *db, struct query *q,
                         struct query_arena *conn_arena)
{
    (void)conn_arena;
    if (q->query_type != QUERY_TYPE_SELECT) return -1;

    struct query_select *s = &q->select;
    struct table *t = NULL;
    if (s->table.len > 0)
        t = db_find_table_sv(db, s->table);
    if (!t) return -1;

    /* For JOINs, find the second table */
    struct table *t2 = NULL;
    if (s->has_join && s->joins_count == 1) {
        struct join_info *ji = &q->arena.joins.items[s->joins_start];
        t2 = db_find_table_sv(db, ji->join_table);
    }

    uint32_t plan_root = plan_build_select(t, s, &q->arena, db);
    if (plan_root == IDX_NONE) return -1;

    struct plan_exec_ctx ctx;
    plan_exec_init(&ctx, &q->arena, db, plan_root);

    uint16_t ncols = plan_node_ncols(&q->arena, plan_root);
    if (ncols == 0) return -1;

    /* Pull first block to determine column types for RowDescription */
    struct row_block block;
    row_block_alloc(&block, ncols, &q->arena.scratch);

    int rc = plan_next_block(&ctx, plan_root, &block);
    if (rc != 0) {
        /* No rows — send empty RowDescription + zero data rows */
        enum column_type types[64];
        for (uint16_t i = 0; i < ncols && i < 64; i++)
            types[i] = (i < t->columns.count) ? t->columns.items[i].type : COLUMN_TYPE_TEXT;
        send_row_desc_plan(fd, t, t2, q, ncols, types);
        return 0;
    }

    /* Determine column types from first block */
    enum column_type col_types[64];
    for (uint16_t i = 0; i < ncols && i < 64; i++)
        col_types[i] = block.cols[i].type;

    if (send_row_desc_plan(fd, t, t2, q, ncols, col_types) != 0)
        return -1;

    /* Send data rows directly from blocks — batched into a large buffer */
    struct msgbuf wire;
    msgbuf_init(&wire);
    /* Pre-allocate ~64KB to reduce reallocs */
    msgbuf_ensure(&wire, 65536);

    size_t total_rows = 0;

    /* Process first block + subsequent blocks */
    for (;;) {
        uint16_t active = row_block_active_count(&block);
        for (uint16_t r = 0; r < active; r++) {
            uint16_t ri = row_block_row_idx(&block, r);

            /* Build one DataRow message: type('D') + len(4) + body */
            /* We accumulate the body in a temp area, then prepend header */
            size_t msg_start = wire.len;

            /* Reserve space for header: 1 byte type + 4 bytes length */
            msgbuf_ensure(&wire, 5);
            wire.len += 5;

            /* Column count */
            msgbuf_push_u16(&wire, ncols);

            /* Column values */
            for (uint16_t c = 0; c < ncols; c++)
                msgbuf_push_col_cell(&wire, &block.cols[c], ri);

            /* Fill in header */
            wire.data[msg_start] = 'D';
            uint32_t body_len = (uint32_t)(wire.len - msg_start - 1); /* exclude type byte */
            put_u32(wire.data + msg_start + 1, body_len);

            total_rows++;
        }

        /* Flush if buffer is large enough (~64KB) */
        if (wire.len >= 65536) {
            if (send_all(fd, wire.data, wire.len) != 0) {
                msgbuf_free(&wire);
                return -1;
            }
            wire.len = 0;
        }

        /* Get next block */
        row_block_reset(&block);
        if (plan_next_block(&ctx, plan_root, &block) != 0)
            break;
    }

    /* Flush remaining data */
    if (wire.len > 0) {
        if (send_all(fd, wire.data, wire.len) != 0) {
            msgbuf_free(&wire);
            return -1;
        }
    }
    msgbuf_free(&wire);

    return (int)total_rows;
}

/* ---- handle a single query string (may contain one statement) ---- */

static int handle_query(int fd, struct database *db, const char *sql,
                        struct msgbuf *m, struct query_arena *conn_arena)
{
    /* skip empty / whitespace-only queries */
    const char *p = sql;
    while (*p && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    if (*p == '\0') {
        send_empty_query(fd, m);
        send_ready_for_query(fd, m, db->in_transaction ? 'T' : 'I');
        return 0;
    }

    struct query q = {0};
    if (query_parse_into(sql, &q, conn_arena) != 0) {
        /* arena is reset on next request — no query_free needed */
        send_error(fd, m, "ERROR", "42601", "syntax error or unsupported statement");
        send_ready_for_query(fd, m, db->in_transaction ? 'T' : 'I');
        return 0;
    }

    /* Fast path: try direct columnar→wire send for SELECT queries */
    if (q.query_type == QUERY_TYPE_SELECT) {
        int plan_rows = try_plan_send(fd, db, &q, conn_arena);
        if (plan_rows >= 0) {
            char tag[128];
            snprintf(tag, sizeof(tag), "SELECT %d", plan_rows);
            send_command_complete(fd, m, tag);
            send_ready_for_query(fd, m, db->in_transaction ? 'T' : 'I');
            return 0;
        }
    }

    struct rows *result = &conn_arena->result;
    result->arena_owns_text = 1;
    int rc = db_exec(db, &q, result, &conn_arena->result_text);

    if (rc < 0) {
        send_error(fd, m, "ERROR", "42000", "query execution failed");
        arena_free_result_rows(conn_arena);
        send_ready_for_query(fd, m, db->in_transaction ? 'T' : 'I');
        return 0;
    }

    /* build command tag */
    char tag[128];
    switch (q.query_type) {
        case QUERY_TYPE_CREATE:
            snprintf(tag, sizeof(tag), "CREATE TABLE");
            break;
        case QUERY_TYPE_DROP:
            snprintf(tag, sizeof(tag), "DROP TABLE");
            break;
        case QUERY_TYPE_SELECT:
            /* send RowDescription + DataRows */
            send_row_description(fd, db, &q, result);
            send_data_rows(fd, result);
            snprintf(tag, sizeof(tag), "SELECT %zu", result->count);
            break;
        case QUERY_TYPE_INSERT:
            if (result->count > 0) {
                /* RETURNING — send rows */
                send_row_description(fd, db, &q, result);
                send_data_rows(fd, result);
            }
            {
                size_t ins_count = q.insert.insert_rows_count;
                if (rc > 0) ins_count = (size_t)rc; /* INSERT...SELECT */
                snprintf(tag, sizeof(tag), "INSERT 0 %zu", ins_count);
            }
            break;
        case QUERY_TYPE_DELETE: {
            if (q.del.has_returning && result->count > 0) {
                send_row_description(fd, db, &q, result);
                send_data_rows(fd, result);
                snprintf(tag, sizeof(tag), "DELETE %zu", result->count);
            } else {
                size_t del_count = 0;
                if (result->count > 0 && result->data[0].cells.count > 0)
                    del_count = (size_t)result->data[0].cells.items[0].value.as_int;
                snprintf(tag, sizeof(tag), "DELETE %zu", del_count);
            }
            break;
        }
        case QUERY_TYPE_UPDATE: {
            if (q.update.has_returning && result->count > 0) {
                send_row_description(fd, db, &q, result);
                send_data_rows(fd, result);
                snprintf(tag, sizeof(tag), "UPDATE %zu", result->count);
            } else {
                size_t upd_count = 0;
                if (result->count > 0 && result->data[0].cells.count > 0)
                    upd_count = (size_t)result->data[0].cells.items[0].value.as_int;
                snprintf(tag, sizeof(tag), "UPDATE %zu", upd_count);
            }
            break;
        }
        case QUERY_TYPE_CREATE_INDEX:
            snprintf(tag, sizeof(tag), "CREATE INDEX");
            break;
        case QUERY_TYPE_DROP_INDEX:
            snprintf(tag, sizeof(tag), "DROP INDEX");
            break;
        case QUERY_TYPE_CREATE_TYPE:
            snprintf(tag, sizeof(tag), "CREATE TYPE");
            break;
        case QUERY_TYPE_DROP_TYPE:
            snprintf(tag, sizeof(tag), "DROP TYPE");
            break;
        case QUERY_TYPE_ALTER:
            snprintf(tag, sizeof(tag), "ALTER TABLE");
            break;
        case QUERY_TYPE_CREATE_SEQUENCE:
            snprintf(tag, sizeof(tag), "CREATE SEQUENCE");
            break;
        case QUERY_TYPE_DROP_SEQUENCE:
            snprintf(tag, sizeof(tag), "DROP SEQUENCE");
            break;
        case QUERY_TYPE_CREATE_VIEW:
            snprintf(tag, sizeof(tag), "CREATE VIEW");
            break;
        case QUERY_TYPE_DROP_VIEW:
            snprintf(tag, sizeof(tag), "DROP VIEW");
            break;
        case QUERY_TYPE_BEGIN:
            snprintf(tag, sizeof(tag), "BEGIN");
            break;
        case QUERY_TYPE_COMMIT:
            snprintf(tag, sizeof(tag), "COMMIT");
            break;
        case QUERY_TYPE_ROLLBACK:
            snprintf(tag, sizeof(tag), "ROLLBACK");
            break;
    }

    send_command_complete(fd, m, tag);
    arena_free_result_rows(conn_arena);
    /* no query_free — arena is connection-scoped, reset on next request */
    send_ready_for_query(fd, m, db->in_transaction ? 'T' : 'I');
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

        switch (msg_type) {
            case 'Q': { /* Simple Query */
                if (body_len > 0) {
                    /* NUL-terminate the SQL in-place (safe — we own the buffer) */
                    c->recv_buf[1 + msg_len] = '\0'; /* just past the message */
                    /* but actually the body starts at offset 5 */
                    char *sql = (char *)(c->recv_buf + 5);
                    /* ensure NUL termination within the body */
                    char saved = sql[body_len];
                    sql[body_len] = '\0';
                    handle_query(c->fd, db, sql, &c->send_buf, &c->arena);
                    sql[body_len] = saved;
                }
                break;
            }
            case 'X': /* Terminate */
                return -1;
            default:
                /* ignore unknown messages */
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
                client_init(&clients[nclients], client_fd);
                nclients++;
            }
        }

        /* process each client */
        for (int i = 0; i < nclients; i++) {
            if (!(fds[2 + i].revents & (POLLIN | POLLHUP | POLLERR)))
                continue;

            /* read available data */
            client_recv_ensure(&clients[i], 4096);
            ssize_t n = read(clients[i].fd,
                             clients[i].recv_buf + clients[i].recv_len,
                             clients[i].recv_cap - clients[i].recv_len);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                    continue; /* no data yet, not an error */
                /* real error — disconnect */
                client_disconnect(&clients[i], srv->db);
                clients[i] = clients[nclients - 1];
                nclients--;
                i--;
                continue;
            }
            if (n == 0) {
                /* EOF — client disconnected */
                client_disconnect(&clients[i], srv->db);
                clients[i] = clients[nclients - 1];
                nclients--;
                i--;
                continue;
            }
            clients[i].recv_len += (size_t)n;

            /* process buffered data based on phase */
            int rc;
            switch (clients[i].phase) {
                case PHASE_STARTUP:
                case PHASE_SSL_RETRY:
                    rc = process_startup(&clients[i]);
                    break;
                case PHASE_QUERY_LOOP:
                    rc = process_messages(&clients[i], srv->db);
                    break;
                default:
                    rc = -1;
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
