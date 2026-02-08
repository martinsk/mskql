#include "pgwire.h"
#include "parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <errno.h>

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
        if (n <= 0) return -1;
        p   += n;
        len -= (size_t)n;
    }
    return 0;
}

static int read_all(int fd, void *buf, size_t len)
{
    uint8_t *p = buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n <= 0) return -1;
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

/* ---- protocol messages ---- */

static int send_auth_ok(int fd)
{
    struct msgbuf m;
    msgbuf_init(&m);
    msgbuf_push_u32(&m, 0); /* AuthenticationOk */
    int rc = msg_send(fd, 'R', &m);
    msgbuf_free(&m);
    return rc;
}

static int send_parameter_status(int fd, const char *name, const char *value)
{
    struct msgbuf m;
    msgbuf_init(&m);
    msgbuf_push_cstr(&m, name);
    msgbuf_push_cstr(&m, value);
    int rc = msg_send(fd, 'S', &m);
    msgbuf_free(&m);
    return rc;
}

static int send_ready_for_query(int fd, char status)
{
    struct msgbuf m;
    msgbuf_init(&m);
    msgbuf_push_byte(&m, (uint8_t)status);
    int rc = msg_send(fd, 'Z', &m);
    msgbuf_free(&m);
    return rc;
}

static int send_error(int fd, const char *severity, const char *code,
                      const char *message)
{
    struct msgbuf m;
    msgbuf_init(&m);
    msgbuf_push_byte(&m, 'S'); msgbuf_push_cstr(&m, severity);
    msgbuf_push_byte(&m, 'V'); msgbuf_push_cstr(&m, severity);
    msgbuf_push_byte(&m, 'C'); msgbuf_push_cstr(&m, code);
    msgbuf_push_byte(&m, 'M'); msgbuf_push_cstr(&m, message);
    msgbuf_push_byte(&m, 0);   /* terminator */
    int rc = msg_send(fd, 'E', &m);
    msgbuf_free(&m);
    return rc;
}

static int send_command_complete(int fd, const char *tag)
{
    struct msgbuf m;
    msgbuf_init(&m);
    msgbuf_push_cstr(&m, tag);
    int rc = msg_send(fd, 'C', &m);
    msgbuf_free(&m);
    return rc;
}

static int send_empty_query(int fd)
{
    struct msgbuf m;
    msgbuf_init(&m);
    int rc = msg_send(fd, 'I', &m);
    msgbuf_free(&m);
    return rc;
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
        case COLUMN_TYPE_TIMESTAMP: return 1114;  /* timestamp */
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
        case COLUMN_TYPE_TIMESTAMP:
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
    if (q->table.len > 0) {
        t = db_find_table_sv(db, q->table);
    }

    msgbuf_push_u16(&m, (uint16_t)ncols);

    for (int i = 0; i < ncols; i++) {
        const char *colname = "?";
        uint32_t type_oid = 25; /* text */

        if (q->select_exprs.count > 0 && (size_t)i < q->select_exprs.count) {
            struct select_expr *se = &q->select_exprs.items[i];
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
                    case WIN_ROW_NUMBER: colname = "row_number"; break;
                    case WIN_RANK:       colname = "rank";       break;
                    case WIN_SUM:        colname = "sum";        break;
                    case WIN_COUNT:      colname = "count";      break;
                    case WIN_AVG:        colname = "avg";        break;
                }
                if (result->count > 0)
                    type_oid = column_type_to_oid(result->data[0].cells.items[i].type);
            }
        } else if (q->aggregates.count > 0 && (size_t)i < q->aggregates.count) {
            switch (q->aggregates.items[i].func) {
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

/* ---- handle a single query string (may contain one statement) ---- */

static int handle_query(int fd, struct database *db, const char *sql)
{
    /* skip empty / whitespace-only queries */
    const char *p = sql;
    while (*p && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    if (*p == '\0') {
        send_empty_query(fd);
        send_ready_for_query(fd, 'I');
        return 0;
    }

    struct query q = {0};
    if (query_parse(sql, &q) != 0) {
        query_free(&q);
        send_error(fd, "ERROR", "42601", "syntax error or unsupported statement");
        send_ready_for_query(fd, 'I');
        return 0;
    }

    struct rows result = {0};
    int rc = db_exec(db, &q, &result);

    if (rc != 0) {
        send_error(fd, "ERROR", "42000", "query execution failed");
        rows_free(&result);
        query_free(&q);
        send_ready_for_query(fd, 'I');
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
            send_row_description(fd, db, &q, &result);
            send_data_rows(fd, &result);
            snprintf(tag, sizeof(tag), "SELECT %zu", result.count);
            break;
        case QUERY_TYPE_INSERT:
            if (result.count > 0) {
                /* RETURNING — send rows */
                send_row_description(fd, db, &q, &result);
                send_data_rows(fd, &result);
            }
            snprintf(tag, sizeof(tag), "INSERT 0 %zu",
                     q.insert_rows.count);
            break;
        case QUERY_TYPE_DELETE: {
            size_t del_count = 0;
            if (result.count > 0 && result.data[0].cells.count > 0)
                del_count = (size_t)result.data[0].cells.items[0].value.as_int;
            snprintf(tag, sizeof(tag), "DELETE %zu", del_count);
            break;
        }
        case QUERY_TYPE_UPDATE: {
            size_t upd_count = 0;
            if (result.count > 0 && result.data[0].cells.count > 0)
                upd_count = (size_t)result.data[0].cells.items[0].value.as_int;
            snprintf(tag, sizeof(tag), "UPDATE %zu", upd_count);
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

    send_command_complete(fd, tag);
    rows_free(&result);
    query_free(&q);
    send_ready_for_query(fd, 'I');
    return 0;
}

/* ---- handle a single client connection ---- */

static void handle_client(int client_fd, struct database *db)
{

    /* read startup message: int32 length, int32 protocol version, then params */
    uint8_t lenbuf[4];
    if (read_all(client_fd, lenbuf, 4) != 0) goto done;
    uint32_t startup_len = read_u32(lenbuf);
    if (startup_len < 8 || startup_len > 65536) goto done;

    uint8_t *startup = malloc(startup_len);
    put_u32(startup, startup_len);
    if (read_all(client_fd, startup + 4, startup_len - 4) != 0) {
        free(startup);
        goto done;
    }

    uint32_t version = read_u32(startup + 4);

    /* handle SSLRequest (code 80877103) — decline with 'N' */
    if (version == 80877103) {
        uint8_t n = 'N';
        send_all(client_fd, &n, 1);
        free(startup);

        /* client will retry with a normal startup */
        if (read_all(client_fd, lenbuf, 4) != 0) goto done;
        startup_len = read_u32(lenbuf);
        if (startup_len < 8 || startup_len > 65536) goto done;
        startup = malloc(startup_len);
        put_u32(startup, startup_len);
        if (read_all(client_fd, startup + 4, startup_len - 4) != 0) {
            free(startup);
            goto done;
        }
        version = read_u32(startup + 4);
    }

    free(startup);

    /* expect protocol 3.0 */
    uint16_t major = (version >> 16) & 0xffff;
    uint16_t minor = version & 0xffff;
    if (major != 3) {
        fprintf(stderr, "[pgwire] unsupported protocol version %d.%d\n",
                major, minor);
        goto done;
    }

    /* send AuthenticationOk */
    send_auth_ok(client_fd);

    /* send some parameter status messages that clients expect */
    send_parameter_status(client_fd, "server_version", "15.0");
    send_parameter_status(client_fd, "server_encoding", "UTF8");
    send_parameter_status(client_fd, "client_encoding", "UTF8");
    send_parameter_status(client_fd, "DateStyle", "ISO, MDY");
    send_parameter_status(client_fd, "integer_datetimes", "on");

    /* BackendKeyData (fake) */
    {
        struct msgbuf m;
        msgbuf_init(&m);
        msgbuf_push_u32(&m, (uint32_t)getpid());
        msgbuf_push_u32(&m, 0); /* secret key */
        msg_send(client_fd, 'K', &m);
        msgbuf_free(&m);
    }

    send_ready_for_query(client_fd, 'I');

    /* main message loop */
    for (;;) {
        uint8_t msg_type;
        if (read_all(client_fd, &msg_type, 1) != 0) break;

        uint8_t len_buf[4];
        if (read_all(client_fd, len_buf, 4) != 0) break;
        uint32_t msg_len = read_u32(len_buf);
        if (msg_len < 4) break;

        uint32_t body_len = msg_len - 4;
        char *body = NULL;
        if (body_len > 0) {
            body = malloc(body_len + 1);
            if (read_all(client_fd, body, body_len) != 0) {
                free(body);
                break;
            }
            body[body_len] = '\0';
        }

        switch (msg_type) {
            case 'Q': /* Simple Query */
                if (body) {
                    handle_query(client_fd, db, body);
                }
                break;
            case 'X': /* Terminate */
                free(body);
                goto done;
            default:
                /* ignore unknown messages */
                break;
        }

        free(body);
    }

done:
    close(client_fd);
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

    if (listen(srv->listen_fd, 8) < 0) {
        perror("listen");
        close(srv->listen_fd);
        return -1;
    }

    return 0;
}

extern volatile sig_atomic_t g_running;

int pgwire_run(struct pgwire_server *srv)
{
    printf("[pgwire] listening on port %d\n", srv->port);
    printf("[pgwire] connect with: psql -h 127.0.0.1 -p %d\n", srv->port);

    while (g_running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(srv->listen_fd,
                               (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) {
                if (!g_running) break;
                continue;
            }
            perror("accept");
            return -1;
        }
        handle_client(client_fd, srv->db);
    }

    return 0;
}

void pgwire_stop(struct pgwire_server *srv)
{
    if (srv->listen_fd >= 0) {
        close(srv->listen_fd);
        srv->listen_fd = -1;
    }
}
