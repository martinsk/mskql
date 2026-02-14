/*
 * test_concurrent.c — concurrency and state-confusion tests for mskql pgwire server
 *
 * These tests exercise scenarios that cannot be tested with simple SQL files
 * because they require multiple simultaneous connections, raw protocol manipulation,
 * or precise timing of connect/disconnect events.
 *
 * The server must be running on 127.0.0.1:5433 before running these tests.
 *
 * Build:  make -C tests/cases/concurrent
 * Run:    ./tests/cases/concurrent/test_concurrent
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

#define SERVER_HOST "127.0.0.1"
static int SERVER_PORT = 15400;

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

static uint32_t read_u32(const uint8_t *buf)
{
    return ((uint32_t)buf[0] << 24) |
           ((uint32_t)buf[1] << 16) |
           ((uint32_t)buf[2] <<  8) |
           ((uint32_t)buf[3]);
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

/* Connect a TCP socket to the server */
static int tcp_connect(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_HOST, &addr.sin_addr);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }
    return fd;
}

/* Perform the PG startup handshake (protocol 3.0, no auth) */
static int pg_startup(int fd)
{
    /* StartupMessage: length(4) + version(4) + "user\0test\0\0" */
    const char params[] = "user\0test\0\0";
    uint32_t len = 4 + 4 + sizeof(params);
    uint8_t buf[64];
    put_u32(buf, len);
    put_u32(buf + 4, 196608); /* 3.0 */
    memcpy(buf + 8, params, sizeof(params));
    if (send_all(fd, buf, len) != 0) return -1;

    /* read messages until ReadyForQuery ('Z') */
    for (;;) {
        uint8_t type;
        if (read_all(fd, &type, 1) != 0) return -1;
        uint8_t lbuf[4];
        if (read_all(fd, lbuf, 4) != 0) return -1;
        uint32_t mlen = read_u32(lbuf);
        if (mlen < 4) return -1;
        uint32_t body_len = mlen - 4;
        if (body_len > 0) {
            uint8_t *body = malloc(body_len);
            if (read_all(fd, body, body_len) != 0) { free(body); return -1; }
            free(body);
        }
        if (type == 'Z') return 0; /* ReadyForQuery */
    }
}

/* Send a simple query and read all response messages until ReadyForQuery.
 * Returns the ReadyForQuery status byte ('I', 'T', or 'E').
 * If out_data is non-NULL, appends DataRow text fields to it (pipe-separated, newline per row).
 */
static int pg_query(int fd, const char *sql, char *out_data, size_t out_cap)
{
    size_t sql_len = strlen(sql) + 1; /* include NUL */
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
        uint32_t mlen = read_u32(lbuf);
        if (mlen < 4) return -1;
        uint32_t body_len = mlen - 4;
        uint8_t *body = NULL;
        if (body_len > 0) {
            body = malloc(body_len);
            if (read_all(fd, body, body_len) != 0) { free(body); return -1; }
        }

        if (type == 'D' && out_data && body) {
            /* DataRow: int16 num_fields, then for each: int32 len + data */
            uint16_t ncols = ((uint16_t)body[0] << 8) | body[1];
            size_t off = 2;
            for (uint16_t c = 0; c < ncols; c++) {
                if (off + 4 > body_len) break;
                int32_t flen = (int32_t)read_u32(body + off);
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
                    /* NULL */
                } else {
                    off += (uint32_t)flen;
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

/* Send Terminate message and close */
static void pg_close(int fd)
{
    uint8_t msg[5] = { 'X', 0, 0, 0, 4 };
    send_all(fd, msg, 5);
    close(fd);
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
        /* child: set port and exec the server */
        char port_str[16];
        snprintf(port_str, sizeof(port_str), "%d", SERVER_PORT);
        setenv("MSKQL_PORT", port_str, 1);
        execl("./build/mskql", "mskql", NULL);
        perror("execl");
        _exit(1);
    }
    /* parent: wait for server to be ready */
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
/*  Test 1: Transaction left open by disconnecting client              */
/*                                                                     */
/*  Client A: BEGIN, INSERT, disconnect (no COMMIT/ROLLBACK)           */
/*  Client B: SELECT should NOT see uncommitted row                    */
/*  Bug: server doesn't rollback on disconnect, so txn state leaks    */
/* ------------------------------------------------------------------ */

static void test_txn_disconnect_no_rollback(void)
{
    printf("  test: txn_disconnect_no_rollback\n");

    /* Connection A: create table, insert committed row */
    int a = tcp_connect();
    if (a < 0) { check("txn_disconnect_no_rollback: connect A", 0); return; }
    if (pg_startup(a) != 0) { check("txn_disconnect_no_rollback: startup A", 0); close(a); return; }

    pg_query(a, "CREATE TABLE txn_disc_c (id INT, val TEXT)", NULL, 0);
    pg_query(a, "INSERT INTO txn_disc_c (id, val) VALUES (1, 'committed')", NULL, 0);

    /* Begin transaction, insert uncommitted row, then disconnect WITHOUT commit */
    pg_query(a, "BEGIN", NULL, 0);
    pg_query(a, "INSERT INTO txn_disc_c (id, val) VALUES (2, 'uncommitted')", NULL, 0);

    /* Abrupt disconnect — no Terminate message, no ROLLBACK */
    close(a);

    /* Small delay to let server finish processing the closed connection */
    usleep(100000);

    /* Connection B: should only see the committed row */
    int b = tcp_connect();
    if (b < 0) { check("txn_disconnect_no_rollback: connect B", 0); return; }
    if (pg_startup(b) != 0) { check("txn_disconnect_no_rollback: startup B", 0); close(b); return; }

    char data[4096] = {0};
    pg_query(b, "SELECT * FROM txn_disc_c ORDER BY id", data, sizeof(data));

    /* The uncommitted row (id=2) should NOT be visible */
    int has_uncommitted = (strstr(data, "uncommitted") != NULL);
    check("txn_disconnect_no_rollback: uncommitted data should not be visible", !has_uncommitted);

    /* Also check that the database is not stuck in_transaction */
    char data2[4096] = {0};
    int status = pg_query(b, "INSERT INTO txn_disc_c (id, val) VALUES (3, 'after')", data2, sizeof(data2));
    pg_query(b, "SELECT * FROM txn_disc_c ORDER BY id", data2, sizeof(data2));
    int has_after = (strstr(data2, "after") != NULL);
    check("txn_disconnect_no_rollback: db not stuck in transaction", has_after);
    (void)status;

    pg_close(b);
}

/* ------------------------------------------------------------------ */
/*  Test 2: ReadyForQuery status byte reflects transaction state       */
/*                                                                     */
/*  After BEGIN, status should be 'T' (in transaction)                 */
/*  After COMMIT, status should be 'I' (idle)                         */
/*  Bug: server always sends 'I' regardless of transaction state      */
/* ------------------------------------------------------------------ */

static void test_ready_for_query_status(void)
{
    printf("  test: ready_for_query_status\n");

    int fd = tcp_connect();
    if (fd < 0) { check("rfq_status: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("rfq_status: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE rfq_test (id INT)", NULL, 0);

    /* After BEGIN, ReadyForQuery should report 'T' */
    int status = pg_query(fd, "BEGIN", NULL, 0);
    check("rfq_status: BEGIN should return status 'T'", status == 'T');

    /* While in transaction, queries should still report 'T' */
    status = pg_query(fd, "INSERT INTO rfq_test (id) VALUES (1)", NULL, 0);
    check("rfq_status: INSERT in txn should return status 'T'", status == 'T');

    /* After COMMIT, should go back to 'I' */
    status = pg_query(fd, "COMMIT", NULL, 0);
    check("rfq_status: COMMIT should return status 'I'", status == 'I');

    /* After ROLLBACK of a new txn, should be 'I' */
    pg_query(fd, "BEGIN", NULL, 0);
    status = pg_query(fd, "ROLLBACK", NULL, 0);
    check("rfq_status: ROLLBACK should return status 'I'", status == 'I');

    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 3: Second connection blocked while first is active            */
/*                                                                     */
/*  Since the server is single-threaded and synchronous, a second      */
/*  connection cannot be served until the first disconnects.           */
/*  This test verifies the server doesn't crash or corrupt state.      */
/* ------------------------------------------------------------------ */

static void test_sequential_connections(void)
{
    printf("  test: sequential_connections\n");

    /* Connection 1: create table and insert */
    int c1 = tcp_connect();
    if (c1 < 0) { check("seq_conn: connect 1", 0); return; }
    if (pg_startup(c1) != 0) { check("seq_conn: startup 1", 0); close(c1); return; }

    pg_query(c1, "CREATE TABLE seq_test (id INT, val TEXT)", NULL, 0);
    pg_query(c1, "INSERT INTO seq_test (id, val) VALUES (1, 'from_c1')", NULL, 0);
    pg_close(c1);

    /* Connection 2: should see data from connection 1 */
    int c2 = tcp_connect();
    if (c2 < 0) { check("seq_conn: connect 2", 0); return; }
    if (pg_startup(c2) != 0) { check("seq_conn: startup 2", 0); close(c2); return; }

    pg_query(c2, "INSERT INTO seq_test (id, val) VALUES (2, 'from_c2')", NULL, 0);

    char data[4096] = {0};
    pg_query(c2, "SELECT * FROM seq_test ORDER BY id", data, sizeof(data));

    check("seq_conn: c2 sees c1 data", strstr(data, "from_c1") != NULL);
    check("seq_conn: c2 sees own data", strstr(data, "from_c2") != NULL);
    pg_close(c2);

    /* Connection 3: should see data from both */
    int c3 = tcp_connect();
    if (c3 < 0) { check("seq_conn: connect 3", 0); return; }
    if (pg_startup(c3) != 0) { check("seq_conn: startup 3", 0); close(c3); return; }

    char data3[4096] = {0};
    pg_query(c3, "SELECT * FROM seq_test ORDER BY id", data3, sizeof(data3));
    check("seq_conn: c3 sees all data", strstr(data3, "from_c1") != NULL && strstr(data3, "from_c2") != NULL);
    pg_close(c3);
}

/* ------------------------------------------------------------------ */
/*  Test 4: Oversized message length (DoS protection)                  */
/*                                                                     */
/*  Send a Query message with an absurdly large length field.          */
/*  The server should reject it gracefully, not OOM or hang.           */
/* ------------------------------------------------------------------ */

static void test_oversized_message(void)
{
    printf("  test: oversized_message\n");

    int fd = tcp_connect();
    if (fd < 0) { check("oversized: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("oversized: startup", 0); close(fd); return; }

    /* Send a Query message with length = 1GB */
    uint8_t hdr[5];
    hdr[0] = 'Q';
    put_u32(hdr + 1, 0x40000000); /* 1GB */
    send_all(fd, hdr, 5);

    /* The server should close the connection (or at least not crash).
     * Try to read — we expect EOF or error. */
    usleep(500000);
    uint8_t buf[1];
    ssize_t n = read(fd, buf, 1);
    /* n <= 0 means server closed the connection or errored — that's acceptable */
    check("oversized: server did not crash (connection closed or errored)", n <= 0);

    close(fd);

    /* Verify server is still alive by making a new connection */
    usleep(200000);
    int fd2 = tcp_connect();
    if (fd2 < 0) {
        check("oversized: server still alive after oversized msg", 0);
        return;
    }
    if (pg_startup(fd2) != 0) {
        check("oversized: server still responsive", 0);
        close(fd2);
        return;
    }
    char data[256] = {0};
    int status = pg_query(fd2, "SELECT 1", data, sizeof(data));
    check("oversized: server still responsive", status > 0);
    pg_close(fd2);
}

/* ------------------------------------------------------------------ */
/*  Test 5: Zero-length query body                                     */
/*                                                                     */
/*  Send a Query message with body_len=0 (just the length field).     */
/*  The server should respond with EmptyQuery + ReadyForQuery,        */
/*  not silently swallow the message.                                  */
/* ------------------------------------------------------------------ */

static void test_zero_length_query(void)
{
    printf("  test: zero_length_query\n");

    int fd = tcp_connect();
    if (fd < 0) { check("zero_query: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("zero_query: startup", 0); close(fd); return; }

    /* Send Query with just the NUL terminator (empty string) */
    uint8_t msg[6];
    msg[0] = 'Q';
    put_u32(msg + 1, 5); /* length = 4 + 1 byte body */
    msg[5] = '\0';       /* empty SQL string */
    if (send_all(fd, msg, 6) != 0) {
        check("zero_query: send", 0);
        close(fd);
        return;
    }

    /* Should get EmptyQueryResponse + ReadyForQuery */
    int got_response = 0;
    for (int i = 0; i < 10; i++) {
        uint8_t type;
        if (read_all(fd, &type, 1) != 0) break;
        uint8_t lbuf[4];
        if (read_all(fd, lbuf, 4) != 0) break;
        uint32_t mlen = read_u32(lbuf);
        uint32_t body_len = mlen - 4;
        if (body_len > 0 && body_len < 65536) {
            uint8_t *body = malloc(body_len);
            if (read_all(fd, body, body_len) != 0) { free(body); break; }
            free(body);
        }
        if (type == 'Z') { got_response = 1; break; }
    }
    check("zero_query: server responds with ReadyForQuery", got_response);

    /* Verify the connection is still usable */
    char data[256] = {0};
    int status = pg_query(fd, "SELECT 1", data, sizeof(data));
    check("zero_query: connection still usable after empty query", status > 0);

    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 6: Rapid connect/disconnect stress                            */
/*                                                                     */
/*  Rapidly open and close connections to check for resource leaks     */
/*  or server crashes.                                                 */
/* ------------------------------------------------------------------ */

static void test_rapid_connect_disconnect(void)
{
    printf("  test: rapid_connect_disconnect\n");

    for (int i = 0; i < 50; i++) {
        int fd = tcp_connect();
        if (fd < 0) {
            check("rapid_conn: connect failed early", 0);
            return;
        }
        /* Some iterations: do full startup then close */
        if (i % 3 == 0) {
            if (pg_startup(fd) == 0) {
                pg_query(fd, "SELECT 1", NULL, 0);
            }
            pg_close(fd);
        }
        /* Some iterations: close immediately after connect (no startup) */
        else if (i % 3 == 1) {
            close(fd);
            usleep(10000);
        }
        /* Some iterations: startup but abrupt close (no Terminate) */
        else {
            pg_startup(fd);
            close(fd);
            usleep(10000);
        }
    }

    /* Verify server is still alive */
    usleep(200000);
    int fd = tcp_connect();
    if (fd < 0) { check("rapid_conn: server alive after stress", 0); return; }
    if (pg_startup(fd) != 0) { check("rapid_conn: server responsive", 0); close(fd); return; }
    char data[256] = {0};
    int status = pg_query(fd, "SELECT 1", data, sizeof(data));
    check("rapid_conn: server still works after 50 rapid connections", status > 0);
    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 7: Transaction state leaks across connections                 */
/*                                                                     */
/*  Connection A: BEGIN (opens transaction on the shared db)           */
/*  Connection A: disconnects                                          */
/*  Connection B: tries COMMIT — should get "no transaction" warning,  */
/*  not accidentally commit A's transaction.                           */
/* ------------------------------------------------------------------ */

static void test_txn_state_leak_across_connections(void)
{
    printf("  test: txn_state_leak_across_connections\n");

    /* Connection A: create table, begin transaction, disconnect */
    int a = tcp_connect();
    if (a < 0) { check("txn_leak: connect A", 0); return; }
    if (pg_startup(a) != 0) { check("txn_leak: startup A", 0); close(a); return; }

    pg_query(a, "CREATE TABLE txn_leak (id INT, val TEXT)", NULL, 0);
    pg_query(a, "INSERT INTO txn_leak (id, val) VALUES (1, 'base')", NULL, 0);
    pg_query(a, "BEGIN", NULL, 0);
    pg_query(a, "INSERT INTO txn_leak (id, val) VALUES (2, 'in_txn')", NULL, 0);
    close(a); /* abrupt disconnect */
    usleep(100000);

    /* Connection B: try to COMMIT — this should NOT succeed in committing A's txn */
    int b = tcp_connect();
    if (b < 0) { check("txn_leak: connect B", 0); return; }
    if (pg_startup(b) != 0) { check("txn_leak: startup B", 0); close(b); return; }

    pg_query(b, "COMMIT", NULL, 0);

    char data[4096] = {0};
    pg_query(b, "SELECT * FROM txn_leak ORDER BY id", data, sizeof(data));

    /* Should only see the base row, not the in_txn row */
    int has_in_txn = (strstr(data, "in_txn") != NULL);
    check("txn_leak: connection B cannot see/commit A's uncommitted data", !has_in_txn);

    pg_close(b);
}

/* ------------------------------------------------------------------ */
/*  Test 8: Multiple queries on same connection preserve state         */
/*                                                                     */
/*  Verify that CREATE + INSERT + SELECT on the same connection        */
/*  all work correctly in sequence.                                    */
/* ------------------------------------------------------------------ */

static void test_multi_query_same_connection(void)
{
    printf("  test: multi_query_same_connection\n");

    int fd = tcp_connect();
    if (fd < 0) { check("multi_q: connect", 0); return; }
    if (pg_startup(fd) != 0) { check("multi_q: startup", 0); close(fd); return; }

    pg_query(fd, "CREATE TABLE multi_q (id INT, name TEXT)", NULL, 0);

    for (int i = 0; i < 100; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO multi_q (id, name) VALUES (%d, 'row_%d')", i, i);
        pg_query(fd, sql, NULL, 0);
    }

    char data[65536] = {0};
    pg_query(fd, "SELECT COUNT(*) FROM multi_q", data, sizeof(data));
    check("multi_q: 100 rows inserted", strstr(data, "100") != NULL);

    pg_query(fd, "DELETE FROM multi_q WHERE id >= 50", NULL, 0);

    memset(data, 0, sizeof(data));
    pg_query(fd, "SELECT COUNT(*) FROM multi_q", data, sizeof(data));
    check("multi_q: 50 rows remain after delete", strstr(data, "50") != NULL);

    pg_close(fd);
}

/* ------------------------------------------------------------------ */
/*  Test 9: Per-connection transaction isolation                        */
/*                                                                     */
/*  Two simultaneous connections have independent transaction state.    */
/*  Connection A's BEGIN does not put connection B into a transaction.  */
/*  Connection B can read/write while A has an open transaction.        */
/* ------------------------------------------------------------------ */

static void test_per_connection_txn_isolation(void)
{
    printf("  test: per_connection_txn_isolation\n");

    /* Setup: create table via a temporary connection */
    int setup = tcp_connect();
    if (setup < 0) { check("txn_iso: connect setup", 0); return; }
    if (pg_startup(setup) != 0) { check("txn_iso: startup setup", 0); close(setup); return; }
    pg_query(setup, "CREATE TABLE txn_iso (id INT, val TEXT)", NULL, 0);
    pg_query(setup, "INSERT INTO txn_iso (id, val) VALUES (1, 'initial')", NULL, 0);
    pg_close(setup);

    /* Open two simultaneous connections */
    int a = tcp_connect();
    if (a < 0) { check("txn_iso: connect A", 0); return; }
    if (pg_startup(a) != 0) { check("txn_iso: startup A", 0); close(a); return; }

    int b = tcp_connect();
    if (b < 0) { check("txn_iso: connect B", 0); close(a); return; }
    if (pg_startup(b) != 0) { check("txn_iso: startup B", 0); close(a); close(b); return; }

    /* A: BEGIN transaction — only A should be in a transaction */
    int status_a = pg_query(a, "BEGIN", NULL, 0);
    check("txn_iso: A is in transaction after BEGIN", status_a == 'T');

    /* B: should NOT be in a transaction (A's BEGIN is independent) */
    int status_b = pg_query(b, "SELECT * FROM txn_iso", NULL, 0);
    check("txn_iso: B is idle (not in A's transaction)", status_b == 'I');

    /* B: can BEGIN its own transaction independently */
    status_b = pg_query(b, "BEGIN", NULL, 0);
    check("txn_iso: B can BEGIN independently", status_b == 'T');

    /* Both are now in transactions — verify status bytes */
    status_a = pg_query(a, "SELECT 1", NULL, 0);
    check("txn_iso: A still in transaction", status_a == 'T');
    status_b = pg_query(b, "SELECT 1", NULL, 0);
    check("txn_iso: B still in transaction", status_b == 'T');

    /* A: COMMIT — only A should leave transaction */
    status_a = pg_query(a, "COMMIT", NULL, 0);
    check("txn_iso: A is idle after COMMIT", status_a == 'I');

    /* B: should still be in its own transaction */
    status_b = pg_query(b, "SELECT 1", NULL, 0);
    check("txn_iso: B still in transaction after A committed", status_b == 'T');

    /* B: ROLLBACK — B leaves its transaction */
    status_b = pg_query(b, "ROLLBACK", NULL, 0);
    check("txn_iso: B is idle after ROLLBACK", status_b == 'I');

    /* Both idle — verify both can still query */
    char data_a[4096] = {0};
    status_a = pg_query(a, "SELECT * FROM txn_iso", data_a, sizeof(data_a));
    check("txn_iso: A can query after both committed", status_a == 'I');
    check("txn_iso: A sees data", strstr(data_a, "initial") != NULL);

    char data_b[4096] = {0};
    status_b = pg_query(b, "SELECT * FROM txn_iso", data_b, sizeof(data_b));
    check("txn_iso: B can query after both committed", status_b == 'I');
    check("txn_iso: B sees data", strstr(data_b, "initial") != NULL);

    pg_close(a);
    pg_close(b);
}

/* ------------------------------------------------------------------ */
/*  main                                                               */
/* ------------------------------------------------------------------ */

int main(void)
{
    signal(SIGPIPE, SIG_IGN);

    const char *port_env = getenv("MSKQL_TEST_PORT");
    if (port_env) SERVER_PORT = atoi(port_env);

    printf("mskql concurrent/state tests\n");
    printf("============================================================\n");

    start_server();

    test_sequential_connections();
    test_multi_query_same_connection();
    test_ready_for_query_status();
    test_txn_disconnect_no_rollback();
    test_txn_state_leak_across_connections();
    test_per_connection_txn_isolation();
    test_zero_length_query();
    test_rapid_connect_disconnect();
    test_oversized_message();

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
