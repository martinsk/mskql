/*
 * test_extended.c — Extended Query Protocol tests for mskql pgwire server
 *
 * These tests exercise the PostgreSQL Extended Query Protocol (Parse, Bind,
 * Describe, Execute, Sync, Close) using raw wire-level protocol messages.
 * No external dependencies — pure C with POSIX sockets.
 *
 * The server must be running on 127.0.0.1:5433 before running these tests,
 * OR this binary will start/stop the server automatically.
 *
 * Build:  make -C tests/cases/extended
 * Run:    ./build/test_extended
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <signal.h>
#include <errno.h>
#include <math.h>

#define SERVER_HOST "127.0.0.1"
static int SERVER_PORT = 15401;

static int g_pass = 0;
static int g_fail = 0;

/* ------------------------------------------------------------------ */
/*  Low-level pgwire helpers                                           */
/* ------------------------------------------------------------------ */

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

static uint32_t get_u32(const uint8_t *buf)
{
    return ((uint32_t)buf[0] << 24) |
           ((uint32_t)buf[1] << 16) |
           ((uint32_t)buf[2] <<  8) |
           ((uint32_t)buf[3]);
}

static int16_t get_i16(const uint8_t *buf)
{
    return (int16_t)(((uint16_t)buf[0] << 8) | buf[1]);
}

static int32_t get_i32(const uint8_t *buf)
{
    return (int32_t)get_u32(buf);
}

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

/* ------------------------------------------------------------------ */
/*  Dynamic buffer for building wire messages                          */
/* ------------------------------------------------------------------ */

struct wbuf {
    uint8_t *data;
    size_t   len;
    size_t   cap;
};

static void wbuf_init(struct wbuf *w)
{
    w->data = NULL;
    w->len  = 0;
    w->cap  = 0;
}

static void wbuf_free(struct wbuf *w)
{
    free(w->data);
    w->data = NULL;
    w->len = w->cap = 0;
}

static void wbuf_ensure(struct wbuf *w, size_t need)
{
    if (w->len + need <= w->cap) return;
    size_t nc = w->cap ? w->cap * 2 : 256;
    while (nc < w->len + need) nc *= 2;
    w->data = realloc(w->data, nc);
    w->cap = nc;
}

static void wbuf_push(struct wbuf *w, const void *data, size_t len)
{
    wbuf_ensure(w, len);
    memcpy(w->data + w->len, data, len);
    w->len += len;
}

static void wbuf_push_u8(struct wbuf *w, uint8_t v)
{
    wbuf_push(w, &v, 1);
}

static void wbuf_push_u16(struct wbuf *w, uint16_t v)
{
    uint8_t buf[2];
    put_u16(buf, v);
    wbuf_push(w, buf, 2);
}

static void wbuf_push_u32(struct wbuf *w, uint32_t v)
{
    uint8_t buf[4];
    put_u32(buf, v);
    wbuf_push(w, buf, 4);
}

static void wbuf_push_cstr(struct wbuf *w, const char *s)
{
    size_t len = strlen(s) + 1;
    wbuf_push(w, s, len);
}

/* ------------------------------------------------------------------ */
/*  Connection helpers                                                 */
/* ------------------------------------------------------------------ */

static int tcp_connect(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_HOST, &addr.sin_addr);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int pg_startup(int fd)
{
    const char params[] = "user\0test\0\0";
    uint32_t len = 4 + 4 + sizeof(params);
    uint8_t buf[64];
    put_u32(buf, len);
    put_u32(buf + 4, 196608); /* 3.0 */
    memcpy(buf + 8, params, sizeof(params));
    if (send_all(fd, buf, len) != 0) return -1;

    for (;;) {
        uint8_t type;
        if (read_all(fd, &type, 1) != 0) return -1;
        uint8_t lbuf[4];
        if (read_all(fd, lbuf, 4) != 0) return -1;
        uint32_t mlen = get_u32(lbuf);
        if (mlen < 4) return -1;
        uint32_t body_len = mlen - 4;
        if (body_len > 0) {
            uint8_t *body = malloc(body_len);
            if (read_all(fd, body, body_len) != 0) { free(body); return -1; }
            free(body);
        }
        if (type == 'Z') return 0;
    }
}

static void pg_close(int fd)
{
    uint8_t msg[5] = { 'X', 0, 0, 0, 4 };
    send_all(fd, msg, 5);
    close(fd);
}

/* ------------------------------------------------------------------ */
/*  Simple Query helper (reused for DDL/DML setup)                     */
/* ------------------------------------------------------------------ */

/* Send Simple Query, read until ReadyForQuery.
 * If out_data is non-NULL, appends DataRow text fields (pipe-separated, newline per row). */
static int pg_query(int fd, const char *sql, char *out_data, size_t out_cap)
{
    size_t sql_len = strlen(sql) + 1;
    uint32_t msg_len = 4 + (uint32_t)sql_len;
    uint8_t hdr[5];
    hdr[0] = 'Q';
    put_u32(hdr + 1, msg_len);
    if (send_all(fd, hdr, 5) != 0) return -1;
    if (send_all(fd, sql, sql_len) != 0) return -1;

    if (out_data) out_data[0] = '\0';
    size_t out_pos = 0;

    for (;;) {
        uint8_t type;
        if (read_all(fd, &type, 1) != 0) return -1;
        uint8_t lbuf[4];
        if (read_all(fd, lbuf, 4) != 0) return -1;
        uint32_t mlen = get_u32(lbuf);
        if (mlen < 4) return -1;
        uint32_t body_len = mlen - 4;
        uint8_t *body = NULL;
        if (body_len > 0) {
            body = malloc(body_len);
            if (read_all(fd, body, body_len) != 0) { free(body); return -1; }
        }

        if (type == 'D' && out_data && body) {
            uint16_t ncols = ((uint16_t)body[0] << 8) | body[1];
            size_t off = 2;
            for (uint16_t c = 0; c < ncols; c++) {
                if (off + 4 > body_len) break;
                int32_t flen = get_i32(body + off);
                off += 4;
                if (c > 0 && out_pos < out_cap - 1)
                    out_data[out_pos++] = '|';
                if (flen >= 0 && off + (uint32_t)flen <= body_len) {
                    size_t to_copy = (size_t)flen;
                    if (out_pos + to_copy >= out_cap - 1)
                        to_copy = out_cap - out_pos - 2;
                    memcpy(out_data + out_pos, body + off, to_copy);
                    out_pos += to_copy;
                    off += (uint32_t)flen;
                } else if (flen == -1) {
                    /* NULL — write marker */
                    if (out_pos + 4 < out_cap - 1) {
                        memcpy(out_data + out_pos, "NULL", 4);
                        out_pos += 4;
                    }
                }
            }
            if (out_pos < out_cap - 1) out_data[out_pos++] = '\n';
            out_data[out_pos] = '\0';
        }

        if (type == 'Z') {
            int status = body ? body[0] : '?';
            free(body);
            return status;
        }
        free(body);
    }
}

/* ------------------------------------------------------------------ */
/*  Extended Query Protocol helpers                                    */
/* ------------------------------------------------------------------ */

/* Send Parse message ('P'):
 *   string stmt_name, string query, int16 num_param_types, [int32 oid]... */
static int pg_parse(int fd, const char *stmt_name, const char *query,
                    int nparams, const uint32_t *param_oids)
{
    struct wbuf w;
    wbuf_init(&w);
    wbuf_push_cstr(&w, stmt_name);
    wbuf_push_cstr(&w, query);
    wbuf_push_u16(&w, (uint16_t)nparams);
    for (int i = 0; i < nparams; i++)
        wbuf_push_u32(&w, param_oids ? param_oids[i] : 0);

    uint8_t hdr[5];
    hdr[0] = 'P';
    put_u32(hdr + 1, 4 + (uint32_t)w.len);
    int rc = send_all(fd, hdr, 5);
    if (rc == 0) rc = send_all(fd, w.data, w.len);
    wbuf_free(&w);
    return rc;
}

/* Send Bind message ('B'):
 *   string portal_name, string stmt_name,
 *   int16 num_format_codes, [int16 format_code]...,
 *   int16 num_params, [int32 len, bytes data]...,
 *   int16 num_result_format_codes, [int16 format_code]... */
static int pg_bind(int fd, const char *portal, const char *stmt,
                   int nparams, const char **param_values, const int *param_lengths)
{
    struct wbuf w;
    wbuf_init(&w);
    wbuf_push_cstr(&w, portal);
    wbuf_push_cstr(&w, stmt);
    /* parameter format codes: 0 = all text */
    wbuf_push_u16(&w, 0);
    /* parameters */
    wbuf_push_u16(&w, (uint16_t)nparams);
    for (int i = 0; i < nparams; i++) {
        if (param_values[i] == NULL) {
            /* NULL parameter: length = -1 */
            wbuf_push_u32(&w, (uint32_t)-1);
        } else {
            int len = param_lengths ? param_lengths[i] : (int)strlen(param_values[i]);
            wbuf_push_u32(&w, (uint32_t)len);
            wbuf_push(&w, param_values[i], (size_t)len);
        }
    }
    /* result format codes: 0 = all text */
    wbuf_push_u16(&w, 0);

    uint8_t hdr[5];
    hdr[0] = 'B';
    put_u32(hdr + 1, 4 + (uint32_t)w.len);
    int rc = send_all(fd, hdr, 5);
    if (rc == 0) rc = send_all(fd, w.data, w.len);
    wbuf_free(&w);
    return rc;
}

/* Send Describe message ('D'):
 *   byte type ('S' = statement, 'P' = portal), string name */
static int pg_describe(int fd, char type, const char *name)
{
    struct wbuf w;
    wbuf_init(&w);
    wbuf_push_u8(&w, (uint8_t)type);
    wbuf_push_cstr(&w, name);

    uint8_t hdr[5];
    hdr[0] = 'D';
    put_u32(hdr + 1, 4 + (uint32_t)w.len);
    int rc = send_all(fd, hdr, 5);
    if (rc == 0) rc = send_all(fd, w.data, w.len);
    wbuf_free(&w);
    return rc;
}

/* Send Execute message ('E'):
 *   string portal_name, int32 max_rows (0 = unlimited) */
static int pg_execute(int fd, const char *portal, int max_rows)
{
    struct wbuf w;
    wbuf_init(&w);
    wbuf_push_cstr(&w, portal);
    wbuf_push_u32(&w, (uint32_t)max_rows);

    uint8_t hdr[5];
    hdr[0] = 'E';
    put_u32(hdr + 1, 4 + (uint32_t)w.len);
    int rc = send_all(fd, hdr, 5);
    if (rc == 0) rc = send_all(fd, w.data, w.len);
    wbuf_free(&w);
    return rc;
}

/* Send Sync message ('S') */
static int pg_sync(int fd)
{
    uint8_t msg[5] = { 'S', 0, 0, 0, 4 };
    return send_all(fd, msg, 5);
}

/* Send Close message ('C'):
 *   byte type ('S' = statement, 'P' = portal), string name */
static int pg_close_stmt(int fd, char type, const char *name)
{
    struct wbuf w;
    wbuf_init(&w);
    wbuf_push_u8(&w, (uint8_t)type);
    wbuf_push_cstr(&w, name);

    uint8_t hdr[5];
    hdr[0] = 'C';
    put_u32(hdr + 1, 4 + (uint32_t)w.len);
    int rc = send_all(fd, hdr, 5);
    if (rc == 0) rc = send_all(fd, w.data, w.len);
    wbuf_free(&w);
    return rc;
}

/* Send Flush message ('H') */
static int pg_flush(int fd)
{
    uint8_t msg[5] = { 'H', 0, 0, 0, 4 };
    return send_all(fd, msg, 5);
}

/* ------------------------------------------------------------------ */
/*  Response reading helpers                                           */
/* ------------------------------------------------------------------ */

/* Read one message from the server. Returns the type byte.
 * If out_body is non-NULL, *out_body is malloc'd with the body (caller frees).
 * *out_body_len is set to the body length. */
static int pg_read_msg(int fd, uint8_t *out_type, uint8_t **out_body, uint32_t *out_body_len)
{
    uint8_t type;
    if (read_all(fd, &type, 1) != 0) return -1;
    uint8_t lbuf[4];
    if (read_all(fd, lbuf, 4) != 0) return -1;
    uint32_t mlen = get_u32(lbuf);
    if (mlen < 4) return -1;
    uint32_t body_len = mlen - 4;
    uint8_t *body = NULL;
    if (body_len > 0) {
        body = malloc(body_len);
        if (!body) return -1;
        if (read_all(fd, body, body_len) != 0) { free(body); return -1; }
    }
    *out_type = type;
    if (out_body) *out_body = body; else free(body);
    if (out_body_len) *out_body_len = body_len;
    return 0;
}

/* Read messages until ReadyForQuery ('Z').
 * Collects DataRow fields into out_data (pipe-separated, newline per row).
 * Returns the ReadyForQuery status byte, or -1 on error. */
static int pg_read_until_ready(int fd, char *out_data, size_t out_cap)
{
    if (out_data) out_data[0] = '\0';
    size_t out_pos = 0;

    for (;;) {
        uint8_t type;
        uint8_t *body = NULL;
        uint32_t body_len = 0;
        if (pg_read_msg(fd, &type, &body, &body_len) != 0) return -1;

        if (type == 'D' && out_data && body && body_len >= 2) {
            uint16_t ncols = ((uint16_t)body[0] << 8) | body[1];
            size_t off = 2;
            for (uint16_t c = 0; c < ncols; c++) {
                if (off + 4 > body_len) break;
                int32_t flen = get_i32(body + off);
                off += 4;
                if (c > 0 && out_pos < out_cap - 1)
                    out_data[out_pos++] = '|';
                if (flen >= 0 && off + (uint32_t)flen <= body_len) {
                    size_t to_copy = (size_t)flen;
                    if (out_pos + to_copy >= out_cap - 1)
                        to_copy = out_cap - out_pos - 2;
                    memcpy(out_data + out_pos, body + off, to_copy);
                    out_pos += to_copy;
                    off += (uint32_t)flen;
                } else if (flen == -1) {
                    if (out_pos + 4 < out_cap - 1) {
                        memcpy(out_data + out_pos, "NULL", 4);
                        out_pos += 4;
                    }
                }
            }
            if (out_pos < out_cap - 1) out_data[out_pos++] = '\n';
            out_data[out_pos] = '\0';
        }

        if (type == 'Z') {
            int status = (body && body_len > 0) ? body[0] : '?';
            free(body);
            return status;
        }
        free(body);
    }
}

/* Execute a full extended query cycle: Parse → Bind → Describe(P) → Execute → Sync
 * and read all responses until ReadyForQuery.
 * Collects DataRow fields into out_data. Returns ReadyForQuery status or -1. */
static int pg_extended_query(int fd, const char *sql,
                             int nparams, const char **param_values,
                             char *out_data, size_t out_cap)
{
    /* Use unnamed statement and portal */
    if (pg_parse(fd, "", sql, nparams, NULL) != 0) return -1;
    if (pg_bind(fd, "", "", nparams, param_values, NULL) != 0) return -1;
    if (pg_describe(fd, 'P', "") != 0) return -1;
    if (pg_execute(fd, "", 0) != 0) return -1;
    if (pg_sync(fd) != 0) return -1;

    /* Read responses: ParseComplete, BindComplete, RowDescription/NoData,
     * DataRow..., CommandComplete, ReadyForQuery */
    return pg_read_until_ready(fd, out_data, out_cap);
}

/* Execute a full extended query cycle without parameters (no Describe). */
static int pg_extended_query_no_params(int fd, const char *sql,
                                       char *out_data, size_t out_cap)
{
    return pg_extended_query(fd, sql, 0, NULL, out_data, out_cap);
}

/* ------------------------------------------------------------------ */
/*  Test helpers                                                       */
/* ------------------------------------------------------------------ */

static void check(const char *test_name, int condition)
{
    if (condition) {
        g_pass++;
    } else {
        printf("  FAIL: %s\n", test_name);
        g_fail++;
    }
}

/* ------------------------------------------------------------------ */
/*  Server management                                                  */
/* ------------------------------------------------------------------ */

static pid_t g_server_pid = 0;

static void start_server(void)
{
    g_server_pid = fork();
    if (g_server_pid == 0) {
        char port_str[16];
        snprintf(port_str, sizeof(port_str), "%d", SERVER_PORT);
        setenv("MSKQL_PORT", port_str, 1);
        execl("./build/mskql_debug", "mskql_debug", NULL);
        perror("execl");
        _exit(1);
    }
    for (int i = 0; i < 40; i++) {
        usleep(250000);
        int fd = tcp_connect();
        if (fd >= 0) {
            close(fd);
            return;
        }
    }
    fprintf(stderr, "FATAL: server did not start\n");
    exit(1);
}

static void stop_server(void)
{
    if (g_server_pid > 0) {
        kill(g_server_pid, SIGTERM);
        waitpid(g_server_pid, NULL, 0);
        g_server_pid = 0;
    }
}

/* ------------------------------------------------------------------ */
/*  Test 1: Basic parameterized SELECT via extended protocol            */
/* ------------------------------------------------------------------ */

static void test_param_select(void)
{
    printf("  test: param_select\n");

    int fd = tcp_connect();
    if (fd < 0) { check("param_select: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("param_select: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_param (id INT, name TEXT)", NULL, 0);
    pg_query(fd, "INSERT INTO t_param (id, name) VALUES (1, 'alice')", NULL, 0);
    pg_query(fd, "INSERT INTO t_param (id, name) VALUES (2, 'bob')", NULL, 0);
    pg_query(fd, "INSERT INTO t_param (id, name) VALUES (3, 'charlie')", NULL, 0);

    /* Extended query: SELECT id, name FROM t_param WHERE id = $1 */
    const char *params[] = { "2" };
    char data[4096] = {0};
    int status = pg_extended_query(fd,
        "SELECT id, name FROM t_param WHERE id = $1",
        1, params, data, sizeof(data));

    check("param_select: got ReadyForQuery", status > 0);
    check("param_select: result contains '2|bob'", strstr(data, "2|bob") != NULL);
    check("param_select: result does not contain 'alice'", strstr(data, "alice") == NULL);
    check("param_select: result does not contain 'charlie'", strstr(data, "charlie") == NULL);

    pg_query(fd, "DROP TABLE t_param", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 2: Multiple parameters                                        */
/* ------------------------------------------------------------------ */

static void test_multi_param(void)
{
    printf("  test: multi_param\n");

    int fd = tcp_connect();
    if (fd < 0) { check("multi_param: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("multi_param: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_multi (id INT, name TEXT, score INT)", NULL, 0);
    pg_query(fd, "INSERT INTO t_multi VALUES (1, 'alice', 90)", NULL, 0);
    pg_query(fd, "INSERT INTO t_multi VALUES (2, 'bob', 85)", NULL, 0);
    pg_query(fd, "INSERT INTO t_multi VALUES (3, 'charlie', 95)", NULL, 0);

    const char *params[] = { "1", "88" };
    char data[4096] = {0};
    int status = pg_extended_query(fd,
        "SELECT name FROM t_multi WHERE id > $1 AND score > $2",
        2, params, data, sizeof(data));

    check("multi_param: got ReadyForQuery", status > 0);
    check("multi_param: result contains 'charlie'", strstr(data, "charlie") != NULL);
    check("multi_param: result does not contain 'alice'", strstr(data, "alice") == NULL);
    check("multi_param: result does not contain 'bob'", strstr(data, "bob") == NULL);

    pg_query(fd, "DROP TABLE t_multi", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 3: Parameterized INSERT                                       */
/* ------------------------------------------------------------------ */

static void test_param_insert(void)
{
    printf("  test: param_insert\n");

    int fd = tcp_connect();
    if (fd < 0) { check("param_insert: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("param_insert: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_ins (id INT, name TEXT)", NULL, 0);

    /* INSERT via extended protocol */
    const char *params[] = { "42", "dave" };
    pg_extended_query(fd, "INSERT INTO t_ins (id, name) VALUES ($1, $2)",
                      2, params, NULL, 0);

    /* Verify via simple query */
    char data[4096] = {0};
    pg_query(fd, "SELECT id, name FROM t_ins", data, sizeof(data));

    check("param_insert: result contains '42|dave'", strstr(data, "42|dave") != NULL);

    pg_query(fd, "DROP TABLE t_ins", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 4: NULL parameter                                             */
/* ------------------------------------------------------------------ */

static void test_null_param(void)
{
    printf("  test: null_param\n");

    int fd = tcp_connect();
    if (fd < 0) { check("null_param: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("null_param: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_null (id INT, name TEXT)", NULL, 0);

    /* INSERT with NULL second parameter */
    const char *params[] = { "1", NULL };
    pg_extended_query(fd, "INSERT INTO t_null (id, name) VALUES ($1, $2)",
                      2, params, NULL, 0);

    char data[4096] = {0};
    pg_query(fd, "SELECT id, name FROM t_null", data, sizeof(data));

    check("null_param: result contains '1|NULL'", strstr(data, "1|NULL") != NULL);

    pg_query(fd, "DROP TABLE t_null", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 5: Parameterized UPDATE                                       */
/* ------------------------------------------------------------------ */

static void test_param_update(void)
{
    printf("  test: param_update\n");

    int fd = tcp_connect();
    if (fd < 0) { check("param_update: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("param_update: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_upd (id INT, val INT)", NULL, 0);
    pg_query(fd, "INSERT INTO t_upd VALUES (1, 10)", NULL, 0);
    pg_query(fd, "INSERT INTO t_upd VALUES (2, 20)", NULL, 0);

    /* UPDATE via extended protocol */
    const char *params[] = { "99", "1" };
    pg_extended_query(fd, "UPDATE t_upd SET val = $1 WHERE id = $2",
                      2, params, NULL, 0);

    char data[4096] = {0};
    pg_query(fd, "SELECT id, val FROM t_upd ORDER BY id", data, sizeof(data));

    check("param_update: row 1 updated to 99", strstr(data, "1|99") != NULL);
    check("param_update: row 2 unchanged", strstr(data, "2|20") != NULL);

    pg_query(fd, "DROP TABLE t_upd", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 6: Parameterized DELETE                                       */
/* ------------------------------------------------------------------ */

static void test_param_delete(void)
{
    printf("  test: param_delete\n");

    int fd = tcp_connect();
    if (fd < 0) { check("param_delete: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("param_delete: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_del (id INT, name TEXT)", NULL, 0);
    pg_query(fd, "INSERT INTO t_del VALUES (1, 'a')", NULL, 0);
    pg_query(fd, "INSERT INTO t_del VALUES (2, 'b')", NULL, 0);
    pg_query(fd, "INSERT INTO t_del VALUES (3, 'c')", NULL, 0);

    const char *params[] = { "2" };
    pg_extended_query(fd, "DELETE FROM t_del WHERE id = $1",
                      1, params, NULL, 0);

    char data[4096] = {0};
    pg_query(fd, "SELECT id FROM t_del ORDER BY id", data, sizeof(data));

    check("param_delete: id=2 deleted", strstr(data, "2") == NULL);
    check("param_delete: id=1 remains", strstr(data, "1") != NULL);
    check("param_delete: id=3 remains", strstr(data, "3") != NULL);

    pg_query(fd, "DROP TABLE t_del", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 7: String parameter with single-quote escape                  */
/* ------------------------------------------------------------------ */

static void test_string_param_quotes(void)
{
    printf("  test: string_param_quotes\n");

    int fd = tcp_connect();
    if (fd < 0) { check("string_quotes: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("string_quotes: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_str (id INT, val TEXT)", NULL, 0);

    /* INSERT a string containing a single quote */
    const char *params[] = { "1", "it's a test" };
    pg_extended_query(fd, "INSERT INTO t_str (id, val) VALUES ($1, $2)",
                      2, params, NULL, 0);

    /* Read back via extended protocol */
    const char *sel_params[] = { "1" };
    char data[4096] = {0};
    pg_extended_query(fd, "SELECT val FROM t_str WHERE id = $1",
                      1, sel_params, data, sizeof(data));

    check("string_quotes: result contains \"it's a test\"",
          strstr(data, "it's a test") != NULL);

    pg_query(fd, "DROP TABLE t_str", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 8: Multiple statements reusing connection                     */
/* ------------------------------------------------------------------ */

static void test_reuse_connection(void)
{
    printf("  test: reuse_connection\n");

    int fd = tcp_connect();
    if (fd < 0) { check("reuse_conn: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("reuse_conn: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_reuse (id INT, name TEXT)", NULL, 0);

    /* Insert 10 rows via extended protocol, each as a separate cycle */
    for (int i = 0; i < 10; i++) {
        char id_str[16], name_str[32];
        snprintf(id_str, sizeof(id_str), "%d", i);
        snprintf(name_str, sizeof(name_str), "user_%d", i);
        const char *params[] = { id_str, name_str };
        int status = pg_extended_query(fd,
            "INSERT INTO t_reuse (id, name) VALUES ($1, $2)",
            2, params, NULL, 0);
        if (status < 0) {
            check("reuse_conn: insert failed", 0);
            pg_close(fd);
            return;
        }
    }

    char data[4096] = {0};
    pg_query(fd, "SELECT COUNT(*) FROM t_reuse", data, sizeof(data));
    check("reuse_conn: 10 rows inserted", strstr(data, "10") != NULL);

    pg_query(fd, "DROP TABLE t_reuse", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 9: Batch inserts via repeated extended queries                 */
/* ------------------------------------------------------------------ */

static void test_batch_inserts(void)
{
    printf("  test: batch_inserts\n");

    int fd = tcp_connect();
    if (fd < 0) { check("batch_ins: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("batch_ins: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_many (id INT, name TEXT)", NULL, 0);

    for (int i = 0; i < 5; i++) {
        char id_str[16], name_str[32];
        snprintf(id_str, sizeof(id_str), "%d", i);
        snprintf(name_str, sizeof(name_str), "name_%d", i);
        const char *params[] = { id_str, name_str };
        pg_extended_query(fd, "INSERT INTO t_many (id, name) VALUES ($1, $2)",
                          2, params, NULL, 0);
    }

    char data[4096] = {0};
    pg_extended_query_no_params(fd, "SELECT id, name FROM t_many ORDER BY id",
                                data, sizeof(data));

    check("batch_ins: first row is '0|name_0'", strstr(data, "0|name_0") != NULL);
    check("batch_ins: last row is '4|name_4'", strstr(data, "4|name_4") != NULL);

    /* Count rows by counting newlines */
    int nrows = 0;
    for (char *p = data; *p; p++) if (*p == '\n') nrows++;
    check("batch_ins: 5 rows total", nrows == 5);

    pg_query(fd, "DROP TABLE t_many", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 10: Non-parameterized query via extended protocol              */
/* ------------------------------------------------------------------ */

static void test_non_param_extended(void)
{
    printf("  test: non_param_extended\n");

    int fd = tcp_connect();
    if (fd < 0) { check("non_param: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("non_param: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_noparam (id INT)", NULL, 0);
    pg_query(fd, "INSERT INTO t_noparam VALUES (1)", NULL, 0);
    pg_query(fd, "INSERT INTO t_noparam VALUES (2)", NULL, 0);

    char data[4096] = {0};
    int status = pg_extended_query_no_params(fd,
        "SELECT * FROM t_noparam ORDER BY id", data, sizeof(data));

    check("non_param: got ReadyForQuery", status > 0);
    check("non_param: result contains '1'", strstr(data, "1\n") != NULL);
    check("non_param: result contains '2'", strstr(data, "2\n") != NULL);

    pg_query(fd, "DROP TABLE t_noparam", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 11: Float parameter                                           */
/* ------------------------------------------------------------------ */

static void test_float_param(void)
{
    printf("  test: float_param\n");

    int fd = tcp_connect();
    if (fd < 0) { check("float_param: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("float_param: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_float (id INT, val FLOAT)", NULL, 0);

    const char *params[] = { "1", "3.14" };
    pg_extended_query(fd, "INSERT INTO t_float VALUES ($1, $2)",
                      2, params, NULL, 0);

    const char *sel_params[] = { "1" };
    char data[4096] = {0};
    pg_extended_query(fd, "SELECT val FROM t_float WHERE id = $1",
                      1, sel_params, data, sizeof(data));

    check("float_param: result contains '3.14'", strstr(data, "3.14") != NULL);

    pg_query(fd, "DROP TABLE t_float", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 12: Boolean parameter                                         */
/* ------------------------------------------------------------------ */

static void test_bool_param(void)
{
    printf("  test: bool_param\n");

    int fd = tcp_connect();
    if (fd < 0) { check("bool_param: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("bool_param: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_bool (id INT, active BOOLEAN)", NULL, 0);

    const char *p1[] = { "1", "true" };
    pg_extended_query(fd, "INSERT INTO t_bool VALUES ($1, $2)", 2, p1, NULL, 0);
    const char *p2[] = { "2", "false" };
    pg_extended_query(fd, "INSERT INTO t_bool VALUES ($1, $2)", 2, p2, NULL, 0);

    const char *sel_params[] = { "true" };
    char data[4096] = {0};
    pg_extended_query(fd, "SELECT id FROM t_bool WHERE active = $1",
                      1, sel_params, data, sizeof(data));

    check("bool_param: result contains '1'", strstr(data, "1") != NULL);
    check("bool_param: result does not contain '2'", strstr(data, "2") == NULL);

    pg_query(fd, "DROP TABLE t_bool", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 13: Transaction via extended protocol                         */
/* ------------------------------------------------------------------ */

static void test_transaction_extended(void)
{
    printf("  test: transaction_extended\n");

    int fd = tcp_connect();
    if (fd < 0) { check("txn_ext: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("txn_ext: startup", 0); close(fd); return; }

    pg_extended_query_no_params(fd, "CREATE TABLE t_txn (id INT)", NULL, 0);
    int status = pg_extended_query_no_params(fd, "BEGIN", NULL, 0);
    check("txn_ext: BEGIN returns 'T'", status == 'T');

    pg_extended_query_no_params(fd, "INSERT INTO t_txn VALUES (1)", NULL, 0);
    status = pg_extended_query_no_params(fd, "COMMIT", NULL, 0);
    check("txn_ext: COMMIT returns 'I'", status == 'I');

    char data[4096] = {0};
    pg_extended_query_no_params(fd, "SELECT * FROM t_txn", data, sizeof(data));
    check("txn_ext: committed row visible", strstr(data, "1") != NULL);

    pg_query(fd, "DROP TABLE t_txn", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 14: Named prepared statement + Close                          */
/* ------------------------------------------------------------------ */

static void test_named_statement(void)
{
    printf("  test: named_statement\n");

    int fd = tcp_connect();
    if (fd < 0) { check("named_stmt: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("named_stmt: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_named (id INT, val TEXT)", NULL, 0);
    pg_query(fd, "INSERT INTO t_named VALUES (1, 'one')", NULL, 0);
    pg_query(fd, "INSERT INTO t_named VALUES (2, 'two')", NULL, 0);
    pg_query(fd, "INSERT INTO t_named VALUES (3, 'three')", NULL, 0);

    /* Parse a named statement */
    pg_parse(fd, "my_stmt", "SELECT val FROM t_named WHERE id = $1", 1, NULL);
    pg_sync(fd);
    int status = pg_read_until_ready(fd, NULL, 0);
    check("named_stmt: parse ok", status > 0);

    /* Execute it twice with different params */
    const char *p1[] = { "1" };
    pg_bind(fd, "", "my_stmt", 1, p1, NULL);
    pg_execute(fd, "", 0);
    pg_sync(fd);
    char data1[4096] = {0};
    status = pg_read_until_ready(fd, data1, sizeof(data1));
    check("named_stmt: exec 1 ok", status > 0);
    check("named_stmt: exec 1 returns 'one'", strstr(data1, "one") != NULL);

    const char *p2[] = { "3" };
    pg_bind(fd, "", "my_stmt", 1, p2, NULL);
    pg_execute(fd, "", 0);
    pg_sync(fd);
    char data2[4096] = {0};
    status = pg_read_until_ready(fd, data2, sizeof(data2));
    check("named_stmt: exec 2 ok", status > 0);
    check("named_stmt: exec 2 returns 'three'", strstr(data2, "three") != NULL);

    /* Close the named statement */
    pg_close_stmt(fd, 'S', "my_stmt");
    pg_sync(fd);
    pg_read_until_ready(fd, NULL, 0);

    pg_query(fd, "DROP TABLE t_named", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 15: Describe Statement returns ParameterDescription           */
/* ------------------------------------------------------------------ */

static void test_describe_statement(void)
{
    printf("  test: describe_statement\n");

    int fd = tcp_connect();
    if (fd < 0) { check("desc_stmt: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("desc_stmt: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE t_desc (id INT, name TEXT)", NULL, 0);
    pg_query(fd, "INSERT INTO t_desc VALUES (1, 'hello')", NULL, 0);

    /* Parse with 1 parameter */
    pg_parse(fd, "desc_test", "SELECT name FROM t_desc WHERE id = $1", 1, NULL);
    pg_describe(fd, 'S', "desc_test");
    pg_sync(fd);

    /* Read responses: ParseComplete ('1'), ParameterDescription ('t'),
     * RowDescription ('T') or NoData ('n'), ReadyForQuery ('Z') */
    int got_parse_complete = 0;
    int got_param_desc = 0;
    int got_row_desc = 0;
    int param_count = -1;

    for (;;) {
        uint8_t type;
        uint8_t *body = NULL;
        uint32_t body_len = 0;
        if (pg_read_msg(fd, &type, &body, &body_len) != 0) break;

        if (type == '1') got_parse_complete = 1;
        if (type == 't' && body && body_len >= 2) {
            got_param_desc = 1;
            param_count = get_i16(body);
        }
        if (type == 'T') got_row_desc = 1;

        free(body);
        if (type == 'Z') break;
    }

    check("desc_stmt: got ParseComplete", got_parse_complete);
    check("desc_stmt: got ParameterDescription", got_param_desc);
    check("desc_stmt: param count is 1", param_count == 1);
    check("desc_stmt: got RowDescription", got_row_desc);

    pg_close_stmt(fd, 'S', "desc_test");
    pg_sync(fd);
    pg_read_until_ready(fd, NULL, 0);

    pg_query(fd, "DROP TABLE t_desc", NULL, 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 16: Error in extended protocol skips to Sync                  */
/* ------------------------------------------------------------------ */

static void test_error_recovery(void)
{
    printf("  test: error_recovery\n");

    int fd = tcp_connect();
    if (fd < 0) { check("err_recov: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("err_recov: startup", 0); close(fd); return; }

    /* Send Parse+Bind+Execute+Sync with invalid SQL.
     * The error may come at Parse or Execute time depending on the server. */
    pg_parse(fd, "", "SELECT * FROM nonexistent_table_xyz_42", 0, NULL);
    pg_bind(fd, "", "", 0, NULL, NULL);
    pg_execute(fd, "", 0);
    pg_sync(fd);

    /* Should get ErrorResponse somewhere + ReadyForQuery */
    int got_error = 0;
    for (;;) {
        uint8_t type;
        uint8_t *body = NULL;
        uint32_t body_len = 0;
        if (pg_read_msg(fd, &type, &body, &body_len) != 0) break;
        if (type == 'E') got_error = 1;
        free(body);
        if (type == 'Z') break;
    }
    check("err_recov: got ErrorResponse for bad SQL", got_error);

    /* Connection should still be usable after the error */
    char data[256] = {0};
    int status = pg_query(fd, "SELECT 1", data, sizeof(data));
    check("err_recov: connection still usable", status > 0);
    check("err_recov: SELECT 1 works", strstr(data, "1") != NULL);

    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 17: Flush message                                             */
/* ------------------------------------------------------------------ */

static void test_flush(void)
{
    printf("  test: flush\n");

    int fd = tcp_connect();
    if (fd < 0) { check("flush: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("flush: startup", 0); close(fd); return; }

    /* Parse + Flush — should get ParseComplete immediately */
    pg_parse(fd, "", "SELECT 1", 0, NULL);
    pg_flush(fd);

    uint8_t type;
    uint8_t *body = NULL;
    uint32_t body_len = 0;
    int rc = pg_read_msg(fd, &type, &body, &body_len);
    check("flush: got response after Flush", rc == 0);
    check("flush: response is ParseComplete ('1')", type == '1');
    free(body);

    /* Now Bind + Execute + Sync to complete the cycle */
    pg_bind(fd, "", "", 0, NULL, NULL);
    pg_execute(fd, "", 0);
    pg_sync(fd);
    pg_read_until_ready(fd, NULL, 0);

    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  main                                                               */
/* ------------------------------------------------------------------ */

int main(void)
{
    signal(SIGPIPE, SIG_IGN);

    const char *port_env = getenv("MSKQL_TEST_PORT");
    if (port_env) SERVER_PORT = atoi(port_env);

    printf("mskql extended query protocol tests\n");
    printf("============================================================\n");

    start_server();

    test_param_select();
    test_multi_param();
    test_param_insert();
    test_null_param();
    test_param_update();
    test_param_delete();
    test_string_param_quotes();
    test_reuse_connection();
    test_batch_inserts();
    test_non_param_extended();
    test_float_param();
    test_bool_param();
    test_transaction_extended();
    test_named_statement();
    test_describe_statement();
    test_error_recovery();
    test_flush();

    stop_server();

    printf("============================================================\n");
    if (g_fail > 0) {
        printf("%d passed, %d FAILED\n", g_pass, g_fail);
        return 1;
    } else {
        printf("All %d tests passed\n", g_pass);
        return 0;
    }
}
