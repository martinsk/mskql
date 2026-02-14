/*
 * bench_throughput.c — mskql throughput benchmark
 *
 * Connects to a running mskql server over pgwire, fires queries from
 * multiple threads, and reports QPS + latency percentiles.
 *
 * Build:  make bench-throughput
 * Run:    ./build/mskql_bench_tp [options]
 *
 * Options:
 *   --threads N     number of concurrent client threads (default: CPU count)
 *   --duration S    seconds per workload (default: 5)
 *   --port P        server port (default: 5433)
 *   --filter NAME   run only this workload
 *   --json FILE     write results in JSON format
 *   --no-server     don't auto-start server (connect to existing)
 *   --pg            also benchmark against PostgreSQL and show comparison
 *   --pg-port P     PostgreSQL port (default: 5432)
 *   --pg-db DB      PostgreSQL database (default: mskql_bench)
 *   --pg-user USER  PostgreSQL user (default: $USER)
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <stdatomic.h>

/* ------------------------------------------------------------------ */
/*  Configuration                                                      */
/* ------------------------------------------------------------------ */

static int    g_port       = 5433;
static int    g_nthreads   = 0;   /* 0 = auto-detect */
static double g_duration   = 5.0;
static const char *g_filter    = NULL;
static const char *g_json_path = NULL;
static int    g_auto_server = 1;
static int    g_compare_pg  = 0;
static int    g_pg_port     = 5432;
static const char *g_pg_db   = "mskql_bench";
static const char *g_pg_user = NULL; /* default: $USER */

#define SERVER_HOST "127.0.0.1"
#define MAX_THREADS 128
#define MAX_SAMPLES 2000000

/* ------------------------------------------------------------------ */
/*  Low-level pgwire helpers (same pattern as test_concurrent.c)       */
/* ------------------------------------------------------------------ */

static void put_u32(uint8_t *buf, uint32_t v)
{
    buf[0] = (v >> 24) & 0xff;
    buf[1] = (v >> 16) & 0xff;
    buf[2] = (v >>  8) & 0xff;
    buf[3] =  v        & 0xff;
}

static uint32_t get_u32(const uint8_t *buf)
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

static int tcp_connect_port(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    inet_pton(AF_INET, SERVER_HOST, &addr.sin_addr);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int tcp_connect(void) { return tcp_connect_port(g_port); }

/* Build startup message with user + optional database params.
 * PostgreSQL requires "database" param; mskql ignores it. */
static int pg_startup_params(int fd, const char *user, const char *database)
{
    uint8_t buf[512];
    size_t off = 8; /* skip length(4) + version(4) */
    /* user param */
    memcpy(buf + off, "user", 5); off += 5;
    size_t ulen = strlen(user) + 1;
    memcpy(buf + off, user, ulen); off += ulen;
    /* database param (if provided) */
    if (database) {
        memcpy(buf + off, "database", 9); off += 9;
        size_t dlen = strlen(database) + 1;
        memcpy(buf + off, database, dlen); off += dlen;
    }
    buf[off++] = '\0'; /* terminator */
    put_u32(buf, (uint32_t)off);
    put_u32(buf + 4, 196608); /* 3.0 */
    if (send_all(fd, buf, off) != 0) return -1;

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
        if (type == 'E') return -1; /* auth error etc. */
    }
}

/* Send a simple query and drain all response messages until ReadyForQuery.
 * Returns 0 on success, -1 on error. */
static int pg_query(int fd, const char *sql)
{
    size_t sql_len = strlen(sql) + 1;
    uint32_t msg_len = 4 + (uint32_t)sql_len;
    uint8_t hdr[5];
    hdr[0] = 'Q';
    put_u32(hdr + 1, msg_len);
    if (send_all(fd, hdr, 5) != 0) return -1;
    if (send_all(fd, sql, sql_len) != 0) return -1;

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
        /* Don't treat ErrorResponse as fatal — caller may want to continue */
    }
}

static void pg_close(int fd)
{
    uint8_t msg[5] = { 'X', 0, 0, 0, 4 };
    send_all(fd, msg, 5);
    close(fd);
}

/* ------------------------------------------------------------------ */
/*  Server management                                                  */
/* ------------------------------------------------------------------ */

static pid_t g_server_pid = 0;

static void start_server(void)
{
    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", g_port);

    g_server_pid = fork();
    if (g_server_pid == 0) {
        setenv("MSKQL_PORT", port_str, 1);
        /* redirect stdout/stderr to /dev/null */
        freopen("/dev/null", "w", stdout);
        freopen("/dev/null", "w", stderr);
        execl("./build/mskql", "mskql", NULL);
        _exit(1);
    }
    /* wait for server to accept connections */
    for (int i = 0; i < 40; i++) {
        usleep(250000);
        int fd = tcp_connect();
        if (fd >= 0) {
            close(fd);
            return;
        }
    }
    fprintf(stderr, "FATAL: server did not start on port %d\n", g_port);
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
/*  Timing                                                             */
/* ------------------------------------------------------------------ */

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

/* ------------------------------------------------------------------ */
/*  Thread context                                                     */
/* ------------------------------------------------------------------ */

/* Connection target descriptor — passed to threads and setup functions */
struct conn_target {
    int         port;
    const char *user;
    const char *database;  /* NULL for mskql */
};

struct thread_ctx {
    int                  thread_id;
    const char         **queries;      /* array of SQL to cycle through */
    int                  nqueries;
    double               duration;
    atomic_int          *stop_flag;
    struct conn_target   target;

    /* results */
    long          count;        /* queries completed */
    double       *latencies;    /* per-query latency in seconds */
    int           lat_cap;
    int           lat_count;
};

static void *worker_fn(void *arg)
{
    struct thread_ctx *ctx = arg;
    struct conn_target *t = &ctx->target;

    int fd = tcp_connect_port(t->port);
    if (fd < 0) { ctx->count = -1; return NULL; }
    if (pg_startup_params(fd, t->user, t->database) != 0) { close(fd); ctx->count = -1; return NULL; }

    int qi = ctx->thread_id % ctx->nqueries; /* start offset to spread queries */
    long completed = 0;
    int lat_i = 0;

    while (!atomic_load(ctx->stop_flag)) {
        double t0 = now_sec();
        if (pg_query(fd, ctx->queries[qi]) != 0) {
            /* reconnect on error */
            close(fd);
            fd = tcp_connect_port(t->port);
            if (fd < 0) break;
            if (pg_startup_params(fd, t->user, t->database) != 0) { close(fd); break; }
            continue;
        }
        double elapsed = now_sec() - t0;
        completed++;
        if (lat_i < ctx->lat_cap) {
            ctx->latencies[lat_i++] = elapsed;
        }
        qi = (qi + 1) % ctx->nqueries;
    }

    ctx->count = completed;
    ctx->lat_count = lat_i;
    pg_close(fd);
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Percentile helper                                                  */
/* ------------------------------------------------------------------ */

static int dbl_cmp(const void *a, const void *b)
{
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

static double pct(double *sorted, int n, double p)
{
    if (n <= 0) return 0.0;
    double idx = p / 100.0 * (n - 1);
    int lo = (int)idx;
    if (lo >= n - 1) return sorted[n - 1];
    double frac = idx - lo;
    return sorted[lo] * (1.0 - frac) + sorted[lo + 1] * frac;
}

/* ------------------------------------------------------------------ */
/*  Run a single workload                                              */
/* ------------------------------------------------------------------ */

struct bench_result {
    const char *name;
    double      qps;
    double      avg_ms;
    double      p50_ms;
    double      p95_ms;
    double      p99_ms;
    long        total_queries;
    double      elapsed_sec;
};

static void run_setup_on(struct conn_target *target, const char **setup_sqls, int n)
{
    int fd = tcp_connect_port(target->port);
    if (fd < 0) { fprintf(stderr, "setup: connect to port %d failed\n", target->port); return; }
    if (pg_startup_params(fd, target->user, target->database) != 0) {
        fprintf(stderr, "setup: startup on port %d failed\n", target->port);
        close(fd); return;
    }

    for (int i = 0; i < n; i++) {
        if (pg_query(fd, setup_sqls[i]) != 0) {
            /* continue — some DROP IF EXISTS may "fail" */
        }
    }
    pg_close(fd);
}

static struct bench_result run_workload_on(const char *name,
                                           struct conn_target *target,
                                           const char **setup_sqls, int nsetup,
                                           const char **hot_sqls, int nhot)
{
    struct bench_result res = { .name = name };

    /* setup phase */
    if (nsetup > 0) run_setup_on(target, setup_sqls, nsetup);

    /* allocate per-thread latency arrays */
    int per_thread_cap = MAX_SAMPLES / g_nthreads;
    if (per_thread_cap < 1000) per_thread_cap = 1000;

    atomic_int stop_flag = 0;
    struct thread_ctx ctxs[MAX_THREADS];
    pthread_t threads[MAX_THREADS];
    double *lat_bufs[MAX_THREADS];

    for (int i = 0; i < g_nthreads; i++) {
        lat_bufs[i] = malloc((size_t)per_thread_cap * sizeof(double));
        ctxs[i] = (struct thread_ctx){
            .thread_id = i,
            .queries   = hot_sqls,
            .nqueries  = nhot,
            .duration  = g_duration,
            .stop_flag = &stop_flag,
            .target    = *target,
            .count     = 0,
            .latencies = lat_bufs[i],
            .lat_cap   = per_thread_cap,
            .lat_count = 0,
        };
    }

    /* launch threads */
    double t0 = now_sec();
    for (int i = 0; i < g_nthreads; i++) {
        pthread_create(&threads[i], NULL, worker_fn, &ctxs[i]);
    }

    /* wait for duration */
    usleep((useconds_t)(g_duration * 1e6));
    atomic_store(&stop_flag, 1);

    /* join threads */
    for (int i = 0; i < g_nthreads; i++) {
        pthread_join(threads[i], NULL);
    }
    double elapsed = now_sec() - t0;

    /* aggregate results */
    long total = 0;
    int total_lat = 0;
    for (int i = 0; i < g_nthreads; i++) {
        if (ctxs[i].count > 0) total += ctxs[i].count;
        total_lat += ctxs[i].lat_count;
    }

    /* merge latency samples */
    double *all_lat = malloc((size_t)total_lat * sizeof(double));
    int pos = 0;
    for (int i = 0; i < g_nthreads; i++) {
        memcpy(all_lat + pos, ctxs[i].latencies, (size_t)ctxs[i].lat_count * sizeof(double));
        pos += ctxs[i].lat_count;
    }
    qsort(all_lat, (size_t)total_lat, sizeof(double), dbl_cmp);

    /* compute stats */
    double sum = 0;
    for (int i = 0; i < total_lat; i++) sum += all_lat[i];

    res.qps           = (double)total / elapsed;
    res.avg_ms        = total_lat > 0 ? (sum / total_lat) * 1e3 : 0;
    res.p50_ms        = pct(all_lat, total_lat, 50) * 1e3;
    res.p95_ms        = pct(all_lat, total_lat, 95) * 1e3;
    res.p99_ms        = pct(all_lat, total_lat, 99) * 1e3;
    res.total_queries = total;
    res.elapsed_sec   = elapsed;

    free(all_lat);
    for (int i = 0; i < g_nthreads; i++) free(lat_bufs[i]);

    return res;
}

static struct conn_target g_mskql_target;

/* ------------------------------------------------------------------ */
/*  Workload definitions                                               */
/* ------------------------------------------------------------------ */

/* Helper: generate INSERT statements into a static buffer pool */
#define MAX_SETUP_SQLS 20000
static char g_setup_buf[MAX_SETUP_SQLS][256];
static const char *g_setup_ptrs[MAX_SETUP_SQLS];
static int g_setup_count;

static void setup_reset(void) { g_setup_count = 0; }

static void setup_add(const char *sql)
{
    if (g_setup_count >= MAX_SETUP_SQLS) return;
    snprintf(g_setup_buf[g_setup_count], sizeof(g_setup_buf[0]), "%s", sql);
    g_setup_ptrs[g_setup_count] = g_setup_buf[g_setup_count];
    g_setup_count++;
}

static void setup_addf(const char *fmt, ...)
{
    if (g_setup_count >= MAX_SETUP_SQLS) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(g_setup_buf[g_setup_count], sizeof(g_setup_buf[0]), fmt, ap);
    va_end(ap);
    g_setup_ptrs[g_setup_count] = g_setup_buf[g_setup_count];
    g_setup_count++;
}

/* A workload definition: setup SQL + hot SQL, prepared by workload_*() */
struct workload_def {
    const char **setup_sqls;
    int          nsetup;
    const char **hot_sqls;
    int          nhot;
};

struct workload_entry {
    const char *name;
    struct workload_def (*prepare)(void);
};

/* --- point_read: indexed single-row lookup --- */

#define POINT_READ_QUERIES 100
static char g_point_read_buf[POINT_READ_QUERIES][128];
static const char *g_point_read_ptrs[POINT_READ_QUERIES];

static struct workload_def workload_point_read(void)
{
    setup_reset();
    setup_add("DROP TABLE IF EXISTS bench_pr");
    setup_add("CREATE TABLE bench_pr (id INT, val TEXT)");
    for (int i = 0; i < 10000; i++)
        setup_addf("INSERT INTO bench_pr VALUES (%d, 'value_%d')", i, i);
    setup_add("CREATE INDEX idx_bench_pr ON bench_pr (id)");

    for (int i = 0; i < POINT_READ_QUERIES; i++) {
        snprintf(g_point_read_buf[i], sizeof(g_point_read_buf[0]),
                 "SELECT * FROM bench_pr WHERE id = %d", i * 100);
        g_point_read_ptrs[i] = g_point_read_buf[i];
    }

    return (struct workload_def){
        .setup_sqls = g_setup_ptrs, .nsetup = g_setup_count,
        .hot_sqls = g_point_read_ptrs, .nhot = POINT_READ_QUERIES,
    };
}

/* --- full_scan: SELECT * from 5K rows --- */

static const char *g_full_scan_q[] = { "SELECT * FROM bench_fs" };

static struct workload_def workload_full_scan(void)
{
    setup_reset();
    setup_add("DROP TABLE IF EXISTS bench_fs");
    setup_add("CREATE TABLE bench_fs (id INT, name TEXT, val INT)");
    for (int i = 0; i < 5000; i++)
        setup_addf("INSERT INTO bench_fs VALUES (%d, 'row_%d', %d)", i, i, i * 7);

    return (struct workload_def){
        .setup_sqls = g_setup_ptrs, .nsetup = g_setup_count,
        .hot_sqls = g_full_scan_q, .nhot = 1,
    };
}

/* --- filtered_scan: SELECT with WHERE --- */

static const char *g_filtered_q[] = { "SELECT * FROM bench_fw WHERE amount > 2500" };

static struct workload_def workload_filtered_scan(void)
{
    setup_reset();
    setup_add("DROP TABLE IF EXISTS bench_fw");
    setup_add("CREATE TABLE bench_fw (id INT, category TEXT, amount INT)");
    for (int i = 0; i < 5000; i++)
        setup_addf("INSERT INTO bench_fw VALUES (%d, 'cat_%d', %d)", i, i % 10, i);

    return (struct workload_def){
        .setup_sqls = g_setup_ptrs, .nsetup = g_setup_count,
        .hot_sqls = g_filtered_q, .nhot = 1,
    };
}

/* --- aggregate: GROUP BY + SUM --- */

static const char *g_agg_q[] = {
    "SELECT region, SUM(amount) FROM bench_agg GROUP BY region"
};

static struct workload_def workload_aggregate(void)
{
    const char *regions[] = {"north", "south", "east", "west"};
    setup_reset();
    setup_add("DROP TABLE IF EXISTS bench_agg");
    setup_add("CREATE TABLE bench_agg (region TEXT, amount INT)");
    for (int i = 0; i < 5000; i++)
        setup_addf("INSERT INTO bench_agg VALUES ('%s', %d)",
                   regions[i % 4], (i * 13) % 1000);

    return (struct workload_def){
        .setup_sqls = g_setup_ptrs, .nsetup = g_setup_count,
        .hot_sqls = g_agg_q, .nhot = 1,
    };
}

/* --- join: equi-join between two tables --- */

static const char *g_join_q[] = {
    "SELECT users.name, orders.total FROM bench_users "
    "JOIN bench_orders ON bench_users.id = bench_orders.user_id"
};

static struct workload_def workload_join(void)
{
    setup_reset();
    setup_add("DROP TABLE IF EXISTS bench_orders");
    setup_add("DROP TABLE IF EXISTS bench_users");
    setup_add("CREATE TABLE bench_users (id INT, name TEXT)");
    setup_add("CREATE TABLE bench_orders (id INT, user_id INT, total INT)");
    for (int i = 0; i < 500; i++)
        setup_addf("INSERT INTO bench_users VALUES (%d, 'user_%d')", i, i);
    for (int i = 0; i < 2000; i++)
        setup_addf("INSERT INTO bench_orders VALUES (%d, %d, %d)",
                   i, i % 500, (i * 17) % 1000);

    return (struct workload_def){
        .setup_sqls = g_setup_ptrs, .nsetup = g_setup_count,
        .hot_sqls = g_join_q, .nhot = 1,
    };
}

/* --- insert: single-row INSERT throughput --- */

#define INSERT_QUERIES 200
static char g_insert_buf[INSERT_QUERIES][256];
static const char *g_insert_ptrs[INSERT_QUERIES];

static struct workload_def workload_insert(void)
{
    setup_reset();
    setup_add("DROP TABLE IF EXISTS bench_ins");
    setup_add("CREATE TABLE bench_ins (id INT, name TEXT, score FLOAT)");

    for (int i = 0; i < INSERT_QUERIES; i++) {
        snprintf(g_insert_buf[i], sizeof(g_insert_buf[0]),
                 "INSERT INTO bench_ins VALUES (%d, 'user_%d', %d.%d)",
                 i, i, i % 100, i % 10);
        g_insert_ptrs[i] = g_insert_buf[i];
    }

    return (struct workload_def){
        .setup_sqls = g_setup_ptrs, .nsetup = g_setup_count,
        .hot_sqls = g_insert_ptrs, .nhot = INSERT_QUERIES,
    };
}

/* --- mixed_rw: 80% reads / 20% writes --- */

#define MIXED_QUERIES 100
static char g_mixed_buf[MIXED_QUERIES][256];
static const char *g_mixed_ptrs[MIXED_QUERIES];

static struct workload_def workload_mixed_rw(void)
{
    setup_reset();
    setup_add("DROP TABLE IF EXISTS bench_mix");
    setup_add("CREATE TABLE bench_mix (id INT, val INT)");
    setup_add("CREATE INDEX idx_bench_mix ON bench_mix (id)");
    for (int i = 0; i < 5000; i++)
        setup_addf("INSERT INTO bench_mix VALUES (%d, %d)", i, i * 3);

    /* 80 reads, 20 writes */
    int qi = 0;
    for (int i = 0; i < 80; i++) {
        snprintf(g_mixed_buf[qi], sizeof(g_mixed_buf[0]),
                 "SELECT * FROM bench_mix WHERE id = %d", (i * 61) % 5000);
        g_mixed_ptrs[qi] = g_mixed_buf[qi];
        qi++;
    }
    for (int i = 0; i < 20; i++) {
        snprintf(g_mixed_buf[qi], sizeof(g_mixed_buf[0]),
                 "UPDATE bench_mix SET val = %d WHERE id = %d",
                 i * 7, (i * 251) % 5000);
        g_mixed_ptrs[qi] = g_mixed_buf[qi];
        qi++;
    }

    return (struct workload_def){
        .setup_sqls = g_setup_ptrs, .nsetup = g_setup_count,
        .hot_sqls = g_mixed_ptrs, .nhot = MIXED_QUERIES,
    };
}

/* ------------------------------------------------------------------ */
/*  Workload registry                                                  */
/* ------------------------------------------------------------------ */

static struct workload_entry workloads[] = {
    { "point_read",     workload_point_read },
    { "full_scan",      workload_full_scan },
    { "filtered_scan",  workload_filtered_scan },
    { "aggregate",      workload_aggregate },
    { "join",           workload_join },
    { "insert",         workload_insert },
    { "mixed_rw",       workload_mixed_rw },
};

static int nworkloads = (int)(sizeof(workloads) / sizeof(workloads[0]));

/* ------------------------------------------------------------------ */
/*  JSON output                                                        */
/* ------------------------------------------------------------------ */

struct comparison_result {
    const char        *name;
    struct bench_result mskql;
    struct bench_result pg;     /* zeroed if not comparing */
};

static void write_json(const char *path, struct comparison_result *results, int n)
{
    FILE *f = fopen(path, "w");
    if (!f) { perror(path); return; }
    fprintf(f, "[\n");
    int first = 1;
    for (int i = 0; i < n; i++) {
        struct bench_result *r = &results[i].mskql;
        if (!first) fprintf(f, ",\n");
        first = 0;
        fprintf(f, "  { \"name\": \"tp_%s\", \"unit\": \"queries/sec\", "
                "\"value\": %.1f, "
                "\"extra\": \"threads=%d  duration=%.1fs  total=%ld  "
                "avg=%.3fms  p50=%.3fms  p95=%.3fms  p99=%.3fms",
                results[i].name, r->qps,
                g_nthreads, r->elapsed_sec, r->total_queries,
                r->avg_ms, r->p50_ms, r->p95_ms, r->p99_ms);
        if (g_compare_pg && results[i].pg.qps > 0) {
            double ratio = r->qps / results[i].pg.qps;
            fprintf(f, "  pg_qps=%.1f  ratio=%.2fx", results[i].pg.qps, ratio);
        }
        fprintf(f, "\" }");
    }
    fprintf(f, "\n]\n");
    fclose(f);
    printf("wrote %s\n", path);
}

/* ------------------------------------------------------------------ */
/*  Detect CPU count                                                   */
/* ------------------------------------------------------------------ */

static int detect_cpus(void)
{
#ifdef _SC_NPROCESSORS_ONLN
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n > 0) return (int)n;
#endif
    return 4;
}

/* ------------------------------------------------------------------ */
/*  Format number with commas                                          */
/* ------------------------------------------------------------------ */

static const char *fmt_qps(double qps, char *buf, size_t bufsz)
{
    /* format integer part with comma separators */
    long v = (long)qps;
    char raw[32];
    snprintf(raw, sizeof(raw), "%ld", v);
    int rawlen = (int)strlen(raw);
    int commas = (rawlen - 1) / 3;
    int total = rawlen + commas;
    if ((size_t)total >= bufsz) { snprintf(buf, bufsz, "%ld", v); return buf; }
    buf[total] = '\0';
    int si = rawlen - 1, di = total - 1, grp = 0;
    while (si >= 0) {
        buf[di--] = raw[si--];
        grp++;
        if (grp == 3 && si >= 0) {
            buf[di--] = ',';
            grp = 0;
        }
    }
    return buf;
}

/* ------------------------------------------------------------------ */
/*  Main                                                               */
/* ------------------------------------------------------------------ */

/* Check if a PG database exists by querying pg_database on the "postgres" db.
 * Returns 1 if it exists, 0 otherwise. */
static int pg_database_exists(struct conn_target *t, const char *dbname)
{
    /* Connect to "postgres" database to check */
    struct conn_target tmp = *t;
    tmp.database = "postgres";
    int fd = tcp_connect_port(tmp.port);
    if (fd < 0) return 0;
    if (pg_startup_params(fd, tmp.user, tmp.database) != 0) { close(fd); return 0; }

    char sql[256];
    snprintf(sql, sizeof(sql),
             "SELECT 1 FROM pg_database WHERE datname = '%s'", dbname);
    /* We can't easily parse the result with our minimal client,
     * so just check if the query succeeds (it always does).
     * Instead, try connecting directly to the target database. */
    pg_close(fd);

    fd = tcp_connect_port(t->port);
    if (fd < 0) return 0;
    if (pg_startup_params(fd, t->user, t->database) != 0) {
        close(fd);
        return 0;
    }
    pg_close(fd);
    return 1;
}

/* Create a PG database by connecting to "postgres" and running CREATE DATABASE */
static int pg_create_database(struct conn_target *t, const char *dbname)
{
    struct conn_target tmp = *t;
    tmp.database = "postgres";
    int fd = tcp_connect_port(tmp.port);
    if (fd < 0) return -1;
    if (pg_startup_params(fd, tmp.user, tmp.database) != 0) { close(fd); return -1; }

    char sql[256];
    snprintf(sql, sizeof(sql), "CREATE DATABASE %s", dbname);
    int rc = pg_query(fd, sql);
    pg_close(fd);
    return rc;
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);

    /* default pg user to $USER */
    g_pg_user = getenv("USER");
    if (!g_pg_user) g_pg_user = "postgres";

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--threads") == 0 && i + 1 < argc)
            g_nthreads = atoi(argv[++i]);
        else if (strcmp(argv[i], "--duration") == 0 && i + 1 < argc)
            g_duration = atof(argv[++i]);
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc)
            g_port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--filter") == 0 && i + 1 < argc)
            g_filter = argv[++i];
        else if (strcmp(argv[i], "--json") == 0 && i + 1 < argc)
            g_json_path = argv[++i];
        else if (strcmp(argv[i], "--no-server") == 0)
            g_auto_server = 0;
        else if (strcmp(argv[i], "--pg") == 0)
            g_compare_pg = 1;
        else if (strcmp(argv[i], "--pg-port") == 0 && i + 1 < argc)
            g_pg_port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--pg-db") == 0 && i + 1 < argc)
            g_pg_db = argv[++i];
        else if (strcmp(argv[i], "--pg-user") == 0 && i + 1 < argc)
            g_pg_user = argv[++i];
        else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [options]\n"
                   "  --threads N      concurrent client threads (default: CPU count)\n"
                   "  --duration S     seconds per workload (default: 5)\n"
                   "  --port P         mskql server port (default: 5433)\n"
                   "  --filter NAME    run only this workload\n"
                   "  --json FILE      write JSON results\n"
                   "  --no-server      don't auto-start mskql server\n"
                   "  --pg             also benchmark against PostgreSQL\n"
                   "  --pg-port P      PostgreSQL port (default: 5432)\n"
                   "  --pg-db DB       PostgreSQL database (default: mskql_bench)\n"
                   "  --pg-user USER   PostgreSQL user (default: $USER)\n",
                   argv[0]);
            return 0;
        }
    }

    if (g_nthreads <= 0) g_nthreads = detect_cpus();
    if (g_nthreads > MAX_THREADS) g_nthreads = MAX_THREADS;

    /* initialize mskql connection target */
    g_mskql_target = (struct conn_target){
        .port = g_port, .user = "test", .database = NULL,
    };

    /* start mskql server if needed */
    if (g_auto_server) {
        int fd = tcp_connect();
        if (fd >= 0) {
            close(fd);
            g_auto_server = 0;
        } else {
            start_server();
        }
    }

    /* set up PG target if comparing */
    struct conn_target pg_target = {0};
    if (g_compare_pg) {
        pg_target = (struct conn_target){
            .port = g_pg_port, .user = g_pg_user, .database = g_pg_db,
        };
        /* verify PG connectivity */
        int fd = tcp_connect_port(g_pg_port);
        if (fd < 0) {
            fprintf(stderr, "ERROR: Cannot connect to PostgreSQL on port %d\n", g_pg_port);
            if (g_auto_server) stop_server();
            return 1;
        }
        close(fd);
        /* ensure benchmark database exists */
        if (!pg_database_exists(&pg_target, g_pg_db)) {
            printf("Creating PostgreSQL database '%s'...\n", g_pg_db);
            if (pg_create_database(&pg_target, g_pg_db) != 0) {
                fprintf(stderr, "ERROR: Failed to create PostgreSQL database '%s'\n", g_pg_db);
                if (g_auto_server) stop_server();
                return 1;
            }
        }
    }

    /* header */
    printf("mskql throughput benchmark (%d threads, %.0fs per workload)\n",
           g_nthreads, g_duration);
    printf("  mskql : %s:%d\n", SERVER_HOST, g_port);
    if (g_compare_pg)
        printf("  pg    : %s:%d (db: %s, user: %s)\n",
               SERVER_HOST, g_pg_port, g_pg_db, g_pg_user);
    printf("============================================================================\n");

    if (g_compare_pg) {
        printf("  %-18s %12s %12s %10s %10s %10s\n",
               "WORKLOAD", "mskql QPS", "pg QPS", "ratio", "mskql p50", "pg p50");
        printf("  %-18s %12s %12s %10s %10s %10s\n",
               "--------", "---------", "------", "-----", "---------", "------");
    } else {
        printf("  %-20s %12s %10s %10s %10s %10s\n",
               "WORKLOAD", "QPS", "avg(ms)", "p50(ms)", "p95(ms)", "p99(ms)");
        printf("  %-20s %12s %10s %10s %10s %10s\n",
               "--------", "---", "-------", "-------", "-------", "-------");
    }

    struct comparison_result results[32];
    int ran = 0;

    for (int i = 0; i < nworkloads; i++) {
        if (g_filter && strcmp(g_filter, workloads[i].name) != 0)
            continue;

        /* prepare workload (generates setup + hot SQL) */
        struct workload_def wd = workloads[i].prepare();

        if (g_compare_pg) {
            printf("  %-18s ", workloads[i].name);
            fflush(stdout);

            /* run against mskql */
            struct bench_result mr = run_workload_on(workloads[i].name,
                &g_mskql_target, wd.setup_sqls, wd.nsetup, wd.hot_sqls, wd.nhot);

            /* run against PostgreSQL */
            struct bench_result pr = run_workload_on(workloads[i].name,
                &pg_target, wd.setup_sqls, wd.nsetup, wd.hot_sqls, wd.nhot);

            results[ran] = (struct comparison_result){
                .name = workloads[i].name, .mskql = mr, .pg = pr,
            };
            ran++;

            char mq[32], pq[32];
            double ratio = pr.qps > 0 ? mr.qps / pr.qps : 0;
            printf("%12s %12s %9.2fx %9.3f %9.3f\n",
                   fmt_qps(mr.qps, mq, sizeof(mq)),
                   fmt_qps(pr.qps, pq, sizeof(pq)),
                   ratio, mr.p50_ms, pr.p50_ms);
        } else {
            printf("  %-20s ", workloads[i].name);
            fflush(stdout);

            struct bench_result r = run_workload_on(workloads[i].name,
                &g_mskql_target, wd.setup_sqls, wd.nsetup, wd.hot_sqls, wd.nhot);

            results[ran] = (struct comparison_result){
                .name = workloads[i].name, .mskql = r,
            };
            ran++;

            char qps_buf[32];
            printf("%12s %10.3f %10.3f %10.3f %10.3f\n",
                   fmt_qps(r.qps, qps_buf, sizeof(qps_buf)),
                   r.avg_ms, r.p50_ms, r.p95_ms, r.p99_ms);
        }
        fflush(stdout);
    }

    if (ran == 0 && g_filter) {
        fprintf(stderr, "unknown workload: %s\navailable:", g_filter);
        for (int i = 0; i < nworkloads; i++)
            fprintf(stderr, " %s", workloads[i].name);
        fprintf(stderr, "\n");
        if (g_auto_server) stop_server();
        return 1;
    }

    printf("============================================================================\n");
    if (g_compare_pg)
        printf("  ratio = mskql / pg  (> 1.0 means mskql has higher throughput)\n");
    printf("done (%d workloads)\n", ran);

    if (g_json_path)
        write_json(g_json_path, results, ran);

    if (g_auto_server) stop_server();

    return 0;
}
