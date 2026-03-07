/*
 * bench.c — mskql micro-benchmarks
 *
 * Builds as a standalone executable that links against the library .o files
 * (everything except main.o).  Each benchmark function sets up a fresh
 * database, runs a workload, and returns wall-clock elapsed time in ms.
 *
 * When --json <file> is passed, writes results in the
 * "customSmallerIsBetter" format for github-action-benchmark.
 *
 * Usage:  ./build/mskql_bench                          (run all, console)
 *         ./build/mskql_bench --json results.json      (run all + JSON)
 *         ./build/mskql_bench <name>                   (single benchmark)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <mach/mach_time.h>

#include "../src/database.h"
#include "../src/parser.h"

/* ------------------------------------------------------------------ */
/*  Timing helpers                                                     */
/* ------------------------------------------------------------------ */

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

#define MAX_ITERS 100000
static double g_iter_ms[MAX_ITERS];
static int    g_niter;
static double g_ns_per_iter;

static int dbl_cmp(const void *a, const void *b)
{
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

static double percentile(double *sorted, int n, double p)
{
    if (n <= 0) return 0.0;
    double idx = p / 100.0 * (n - 1);
    int lo = (int)idx;
    if (lo >= n - 1) return sorted[n - 1];
    double frac = idx - lo;
    return sorted[lo] * (1.0 - frac) + sorted[lo + 1] * frac;
}

/* ------------------------------------------------------------------ */
/*  Helper: execute SQL, discard result, abort on error                */
/* ------------------------------------------------------------------ */

static void exec(struct database *db, const char *sql)
{
    struct rows r = {0};
    int rc = db_exec_sql(db, sql, &r);
    rows_free(&r);
    if (rc < 0) {
        fprintf(stderr, "FATAL (rc=%d): %s\n", rc, sql);
        fflush(stderr);
        exit(1);
    }
}

/* ------------------------------------------------------------------ */
/*  Benchmark: bulk INSERT                                             */
/* ------------------------------------------------------------------ */

static double bench_insert_bulk(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, name TEXT, score FLOAT)");

    const int N = 10000;
    char sql[256];
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, 'user_%d', %d.%d)",
                 i, i, i % 100, i % 10);
        exec(&db, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: SELECT full scan                                        */
/* ------------------------------------------------------------------ */

static double bench_select_full_scan(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, name TEXT, val INT)");

    for (int i = 0; i < 5000; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, 'row_%d', %d)", i, i, i * 7);
        exec(&db, sql);
    }

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM t");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: SELECT with WHERE filter                                */
/* ------------------------------------------------------------------ */

static double bench_select_where(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, category TEXT, amount INT)");

    for (int i = 0; i < 5000; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, 'cat_%d', %d)",
                 i, i % 10, i);
        exec(&db, sql);
    }

    const int N = 500;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM t WHERE amount > 2500");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: aggregate (SUM + GROUP BY)                              */
/* ------------------------------------------------------------------ */

static double bench_aggregate(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE sales (region TEXT, amount INT)");

    const char *regions[] = {"north", "south", "east", "west"};
    for (int i = 0; i < 5000; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO sales VALUES ('%s', %d)",
                 regions[i % 4], (i * 13) % 1000);
        exec(&db, sql);
    }

    const int N = 500;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT region, SUM(amount) FROM sales GROUP BY region");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: ORDER BY                                                */
/* ------------------------------------------------------------------ */

static double bench_order_by(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, val INT)");

    for (int i = 0; i < 5000; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, %d)", i, (i * 31337) % 100000);
        exec(&db, sql);
    }

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM t ORDER BY val DESC");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: JOIN                                                    */
/* ------------------------------------------------------------------ */

static double bench_join(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE users (id INT, name TEXT)");
    exec(&db, "CREATE TABLE orders (id INT, user_id INT, total INT)");

    for (int i = 0; i < 500; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO users VALUES (%d, 'user_%d')", i, i);
        exec(&db, sql);
    }
    for (int i = 0; i < 2000; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO orders VALUES (%d, %d, %d)",
                 i, i % 500, (i * 17) % 1000);
        exec(&db, sql);
    }

    const int N = 50;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT users.name, orders.total FROM users "
            "JOIN orders ON users.id = orders.user_id");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: UPDATE                                                  */
/* ------------------------------------------------------------------ */

static double bench_update(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, val INT)");

    for (int i = 0; i < 5000; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO t VALUES (%d, %d)", i, i);
        exec(&db, sql);
    }

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        exec(&db, "UPDATE t SET val = 0 WHERE id < 1000");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: DELETE                                                   */
/* ------------------------------------------------------------------ */

static double bench_delete(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, val INT)");

    const int N = 50;
    g_niter = N;
    double t0 = now_sec();
    for (int iter = 0; iter < N; iter++) {
        double it0 = now_sec();
        exec(&db, "DROP TABLE t");
        exec(&db, "CREATE TABLE t (id INT, val INT)");
        for (int i = 0; i < 2000; i++) {
            char sql[128];
            snprintf(sql, sizeof(sql), "INSERT INTO t VALUES (%d, %d)", i, i);
            exec(&db, sql);
        }
        exec(&db, "DELETE FROM t WHERE id >= 1000");
        g_iter_ms[iter] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: parser throughput                                       */
/* ------------------------------------------------------------------ */

static double bench_parser(void)
{
    const char *queries[] = {
        "SELECT * FROM t",
        "SELECT a, b, c FROM t WHERE x > 10 ORDER BY a DESC LIMIT 100",
        "INSERT INTO t VALUES (1, 'hello', 3.14)",
        "UPDATE t SET a = 1, b = 'foo' WHERE id = 42",
        "DELETE FROM t WHERE id IN (1, 2, 3)",
        "SELECT a, SUM(b) FROM t GROUP BY a",
        "CREATE TABLE t (id INT, name TEXT, score FLOAT)",
        "SELECT * FROM a JOIN b ON a.id = b.a_id WHERE a.x > 5",
    };
    int nq = (int)(sizeof(queries) / sizeof(queries[0]));

    const int N = 50000;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        struct query q = {0};
        if (query_parse(queries[i % nq], &q) == 0)
            query_free(&q);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: index lookup                                            */
/* ------------------------------------------------------------------ */

static double bench_index_lookup(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, val TEXT)");

    for (int i = 0; i < 10000; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, 'value_%d')", i, i);
        exec(&db, sql);
    }
    exec(&db, "CREATE INDEX idx_id ON t (id)");

    const int N = 2000;
    char sql[128];
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM t WHERE id = %d", i % 10000);
        db_exec_sql_discard(&db, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: transaction (BEGIN / INSERT / COMMIT)                   */
/* ------------------------------------------------------------------ */

static double bench_transaction(void)
{
    struct database db;
    db_init(&db, "bench");
    struct txn_state txn = {0};
    db.active_txn = &txn;
    exec(&db, "CREATE TABLE t (id INT, val INT)");

    for (int i = 0; i < 1000; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO t VALUES (%d, %d)", i, i);
        exec(&db, sql);
    }

    const int N = 100;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        exec(&db, "BEGIN");
        for (int j = 0; j < 50; j++) {
            char sql[128];
            snprintf(sql, sizeof(sql),
                     "INSERT INTO t VALUES (%d, %d)", 10000 + i * 50 + j, j);
            exec(&db, sql);
        }
        exec(&db, "COMMIT");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: window functions                                        */
/* ------------------------------------------------------------------ */

static double bench_window_functions(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, grp INT, val INT)");

    for (int i = 0; i < 5000; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, %d, %d)",
                 i, i % 20, (i * 7) % 1000);
        exec(&db, sql);
    }

    const int N = 20;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT id, grp, val, "
            "ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) FROM t");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: DISTINCT                                                */
/* ------------------------------------------------------------------ */

static double bench_distinct(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, category TEXT, val INT)");

    for (int i = 0; i < 5000; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, 'cat_%d', %d)",
                 i, i % 100, i);
        exec(&db, sql);
    }

    const int N = 500;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT DISTINCT category FROM t");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: subquery (WHERE IN)                                     */
/* ------------------------------------------------------------------ */

static double bench_subquery(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t1 (id INT, val INT)");
    exec(&db, "CREATE TABLE t2 (id INT, flag INT)");

    for (int i = 0; i < 5000; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t1 VALUES (%d, %d)", i, i * 3);
        exec(&db, sql);
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t2 VALUES (%d, %d)", i, i % 1000);
        exec(&db, sql);
    }

    const int N = 50;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT * FROM t1 WHERE id IN "
            "(SELECT id FROM t2 WHERE flag > 500)");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: CTE                                                     */
/* ------------------------------------------------------------------ */

static double bench_cte(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, val INT, category TEXT)");

    for (int i = 0; i < 5000; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, %d, 'cat_%d')",
                 i, i * 3, i % 10);
        exec(&db, sql);
    }

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "WITH filtered AS (SELECT * FROM t WHERE val > 500) "
            "SELECT * FROM filtered WHERE category = 'cat_3'");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: generate_series                                         */
/* ------------------------------------------------------------------ */

static double bench_generate_series(void)
{
    struct database db;
    db_init(&db, "bench");

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM generate_series(1, 10000)");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: scalar functions                                        */
/* ------------------------------------------------------------------ */

static double bench_scalar_functions(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, name TEXT, score FLOAT)");

    for (int i = 0; i < 5000; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, 'user_%d', %d.%d)",
                 i, i, i % 100, i % 10);
        exec(&db, sql);
    }

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT UPPER(name), LENGTH(name), ABS(score), "
            "ROUND(score, 2) FROM t");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: expression-based aggregate                              */
/* ------------------------------------------------------------------ */

static double bench_expression_agg(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, category TEXT, price INT, quantity INT)");

    const char *cats[] = {"electronics", "clothing", "food", "books", "toys"};
    for (int i = 0; i < 5000; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, '%s', %d, %d)",
                 i, cats[i % 5], (i * 17) % 500, (i * 7) % 100);
        exec(&db, sql);
    }

    const int N = 500;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT category, SUM(price * quantity) FROM t GROUP BY category");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: multi-column ORDER BY                                   */
/* ------------------------------------------------------------------ */

static double bench_multi_sort(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, a INT, b INT)");

    for (int i = 0; i < 5000; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, %d, %d)",
                 i, (i * 31337) % 1000, (i * 7919) % 1000);
        exec(&db, sql);
    }

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM t ORDER BY a DESC, b ASC");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: set operations (UNION)                                  */
/* ------------------------------------------------------------------ */

static double bench_set_ops(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t1 (id INT, val TEXT)");
    exec(&db, "CREATE TABLE t2 (id INT, val TEXT)");

    for (int i = 0; i < 2000; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t1 VALUES (%d, 'a_%d')", i, i);
        exec(&db, sql);
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t2 VALUES (%d, 'b_%d')", i + 1000, i + 1000);
        exec(&db, sql);
    }

    const int N = 50;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT id, val FROM t1 UNION SELECT id, val FROM t2");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Bootstrap dataset for complex benchmarks                           */
/* ------------------------------------------------------------------ */

static void setup_bootstrap(struct database *db)
{
    const char *regions[] = {"north", "south", "east", "west"};
    const char *tiers[] = {"basic", "premium", "enterprise"};
    const char *categories[] = {"electronics", "clothing", "food", "books", "toys"};
    char sql[512];

    /* dimension tables (TEXT columns, small — individual INSERTs) */
    exec(db, "CREATE TABLE customers (id INT, name TEXT, region TEXT, tier TEXT)");
    exec(db, "CREATE TABLE products (id INT, name TEXT, category TEXT, price INT)");
    /* fact tables (all-INT, bulk-loaded via INSERT...SELECT generate_series) */
    exec(db, "CREATE TABLE orders (id INT, customer_id INT, product_id INT, quantity INT, amount INT)");
    exec(db, "CREATE TABLE events (id INT, user_id INT, event_type INT, amount INT, score INT)");
    exec(db, "CREATE TABLE metrics (id INT, sensor_id INT, v1 INT, v2 INT, v3 INT, v4 INT, v5 INT)");
    exec(db, "CREATE TABLE sales (id INT, rep_id INT, region_id INT, amount INT)");

    for (int i = 0; i < 2000; i++) {
        snprintf(sql, sizeof(sql),
                 "INSERT INTO customers VALUES (%d, 'cust_%d', '%s', '%s')",
                 i, i, regions[i % 4], tiers[i % 3]);
        exec(db, sql);
    }
    for (int i = 0; i < 500; i++) {
        snprintf(sql, sizeof(sql),
                 "INSERT INTO products VALUES (%d, 'prod_%d', '%s', %d)",
                 i, i, categories[i % 5], 10 + (i * 17) % 490);
        exec(db, sql);
    }

    /* bulk-load fact tables */
    exec(db, "INSERT INTO orders SELECT n, n % 2000, n % 500, "
             "1 + (n * 7) % 20, 10 + (n * 13) % 990 "
             "FROM generate_series(0, 49999) AS g(n)");
    exec(db, "INSERT INTO events SELECT n, n % 2000, n % 5, "
             "(n * 11) % 1000, (n * 31337) % 10000 "
             "FROM generate_series(0, 49999) AS g(n)");
    exec(db, "INSERT INTO metrics SELECT n, n % 100, "
             "(n * 7) % 1000, (n * 13) % 1000, (n * 19) % 1000, "
             "(n * 23) % 1000, (n * 29) % 1000 "
             "FROM generate_series(0, 49999) AS g(n)");
    exec(db, "INSERT INTO sales SELECT n, n % 200, n % 4, "
             "10 + (n * 17) % 990 "
             "FROM generate_series(0, 49999) AS g(n)");
}

/* ------------------------------------------------------------------ */
/*  Benchmark: multi_join (3-table join + aggregate)                   */
/* ------------------------------------------------------------------ */

static double bench_multi_join(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_bootstrap(&db);

    const int N = 5;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT c.region, p.category, SUM(o.quantity * p.price) "
            "FROM orders o "
            "JOIN customers c ON o.customer_id = c.id "
            "JOIN products p ON o.product_id = p.id "
            "GROUP BY c.region, p.category "
            "ORDER BY c.region, p.category");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: analytical_cte (multi-CTE pipeline)                     */
/* ------------------------------------------------------------------ */

static double bench_analytical_cte(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_bootstrap(&db);

    const int N = 20;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "WITH user_totals AS ("
            "SELECT user_id, SUM(amount) AS total "
            "FROM events WHERE event_type = 0 "
            "GROUP BY user_id "
            "HAVING SUM(amount) > 500"
            ") SELECT * FROM user_totals ORDER BY total DESC LIMIT 100");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: wide_agg (many-column aggregation)                      */
/* ------------------------------------------------------------------ */

static double bench_wide_agg(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_bootstrap(&db);

    const int N = 20;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT sensor_id, COUNT(*), AVG(v1), SUM(v2), MIN(v3), MAX(v4), AVG(v5) "
            "FROM metrics GROUP BY sensor_id ORDER BY sensor_id");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: large_sort (sorting 50K rows)                           */
/* ------------------------------------------------------------------ */

static double bench_large_sort(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_bootstrap(&db);

    const int N = 10;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT * FROM events ORDER BY score DESC, id ASC");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: subquery_complex (semi-join + filter + sort + limit)     */
/* ------------------------------------------------------------------ */

static double bench_subquery_complex(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_bootstrap(&db);

    const int N = 20;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT * FROM events "
            "WHERE user_id IN (SELECT id FROM customers WHERE tier = 'premium') "
            "AND amount > 100 "
            "ORDER BY amount DESC LIMIT 500");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: window_rank (window function on large dataset)          */
/* ------------------------------------------------------------------ */

static double bench_window_rank(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_bootstrap(&db);

    const int N = 5;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT rep_id, region_id, amount, "
            "RANK() OVER (PARTITION BY region_id ORDER BY amount DESC) "
            "FROM sales");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: mixed_analytical (CTE + join + agg + sort)              */
/* ------------------------------------------------------------------ */

static double bench_mixed_analytical(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_bootstrap(&db);

    const int N = 5;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "WITH order_summary AS ("
            "SELECT o.customer_id, SUM(o.quantity * p.price) AS total "
            "FROM orders o JOIN products p ON o.product_id = p.id "
            "GROUP BY o.customer_id"
            ") SELECT c.region, COUNT(*) AS num_customers, SUM(os.total) AS revenue "
            "FROM order_summary os JOIN customers c ON os.customer_id = c.id "
            "GROUP BY c.region ORDER BY revenue DESC");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: vector_insert (INSERT 5000 rows with VECTOR(4))         */
/* ------------------------------------------------------------------ */

static double bench_vector_insert(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t_vec (id INT, label TEXT, v VECTOR(4))");

    const int N = 5000;
    g_niter = N;
    char sql[256];
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t_vec VALUES (%d, 'item_%d', '[%d.1, %d.2, %d.3, %d.4]')",
                 i, i, i % 100, i % 50, i % 30, i % 20);
        exec(&db, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: vector_scan (SELECT * with VECTOR(4) column, 5000 rows) */
/* ------------------------------------------------------------------ */

static double bench_vector_scan(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t_vec (id INT, v VECTOR(4))");

    char sql[256];
    for (int i = 0; i < 5000; i++) {
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t_vec VALUES (%d, '[%d.1, %d.2, %d.3, %d.4]')",
                 i, i % 100, i % 50, i % 30, i % 20);
        exec(&db, sql);
    }

    const int N = 100;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM t_vec");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: vector_wide (VECTOR(128) — scan + sort, 2000 rows)      */
/* ------------------------------------------------------------------ */

static double bench_vector_wide(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t_wide (id INT, score FLOAT, v VECTOR(128))");

    /* Build a 128-dim vector literal */
    char vec_buf[128 * 8 + 4]; /* "[0.01,0.02,...,1.28]" */
    char sql[128 * 8 + 256];
    for (int i = 0; i < 2000; i++) {
        char *p = vec_buf;
        *p++ = '[';
        for (int d = 0; d < 128; d++) {
            if (d > 0) *p++ = ',';
            p += snprintf(p, 16, "%.2f", (double)((i * 128 + d) % 10000) / 100.0);
        }
        *p++ = ']';
        *p = '\0';
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t_wide VALUES (%d, %d.%d, '%s')",
                 i, (i * 31337) % 1000, i % 10, vec_buf);
        exec(&db, sql);
    }

    const int N = 20;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT id, score, v FROM t_wide ORDER BY score DESC LIMIT 100");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Benchmark: vector_filter (WHERE on table with VECTOR(128), 2000 rows) */
/* ------------------------------------------------------------------ */

static double bench_vector_filter(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t_vfilt (id INT, category INT, v VECTOR(128))");

    char vec_buf[128 * 8 + 4];
    char sql[128 * 8 + 256];
    for (int i = 0; i < 2000; i++) {
        char *p = vec_buf;
        *p++ = '[';
        for (int d = 0; d < 128; d++) {
            if (d > 0) *p++ = ',';
            p += snprintf(p, 16, "%.2f", (double)((i + d) % 100) / 10.0);
        }
        *p++ = ']';
        *p = '\0';
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t_vfilt VALUES (%d, %d, '%s')",
                 i, i % 10, vec_buf);
        exec(&db, sql);
    }

    const int N = 50;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT id, v FROM t_vfilt WHERE category = 5");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmarks: shared setup                                      */
/* ------------------------------------------------------------------ */

static void setup_node_int_table(struct database *db, int nrows)
{
    exec(db, "CREATE TABLE t (id INT, grp INT, val INT)");
    char sql[256];
    snprintf(sql, sizeof(sql),
             "INSERT INTO t SELECT n, n %% 100, (n * 7) %% %d "
             "FROM generate_series(1, %d) AS g(n)", nrows, nrows);
    exec(db, sql);
}

static void setup_node_float_table(struct database *db, int nrows)
{
    exec(db, "CREATE TABLE tf (id INT, grp INT, val FLOAT)");
    char sql[256];
    snprintf(sql, sizeof(sql),
             "INSERT INTO tf SELECT n, n %% 100, ((n * 7) %% %d) * 1.0 "
             "FROM generate_series(1, %d) AS g(n)", nrows, nrows);
    exec(db, sql);
}

static void setup_node_text_table(struct database *db, int nrows)
{
    exec(db, "CREATE TABLE tt (id INT, name TEXT, val INT)");
    char sql[256];
    snprintf(sql, sizeof(sql),
             "INSERT INTO tt SELECT n, 'name_' || n, (n * 7) %% %d "
             "FROM generate_series(1, %d) AS g(n)", nrows, nrows);
    exec(db, sql);
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: seq_scan (pure memcpy throughput)                   */
/* ------------------------------------------------------------------ */

static double bench_node_seq_scan(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM t");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: filter_int (~50% selectivity, vectorized compare)   */
/* ------------------------------------------------------------------ */

static double bench_node_filter_int(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM t WHERE val > 50000");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: filter_low_sel (~5% selectivity, compact pass)      */
/* ------------------------------------------------------------------ */

static double bench_node_filter_low_sel(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM t WHERE val > 95000");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: filter_compound (AND-chained columnar filter)       */
/* ------------------------------------------------------------------ */

static double bench_node_filter_compound(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT * FROM t WHERE val > 30000 AND grp < 50");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: project (column pruning 3→2)                       */
/* ------------------------------------------------------------------ */

static double bench_node_project(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT id, val FROM t");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: vec_project_int (VEC_COL_OP_LIT i32, auto-vec)     */
/* ------------------------------------------------------------------ */

static double bench_node_vec_project_int(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT val * 2 + 1 FROM t");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: vec_project_float (SIMD float throughput)           */
/* ------------------------------------------------------------------ */

static double bench_node_vec_project_float(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_float_table(&db, 100000);

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT val * 2.5 + 1.0 FROM tf");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: vec_project_col_col (VEC_COL_OP_COL bandwidth)     */
/* ------------------------------------------------------------------ */

static double bench_node_vec_project_col_col(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT id + val FROM t");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: vec_project_multi (4 vec ops per block)            */
/* ------------------------------------------------------------------ */

static double bench_node_vec_project_multi(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT val * 2, val + 1, val - 3, val / 2 FROM t");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: vec_project_text (UPPER — string, not SIMD)        */
/* ------------------------------------------------------------------ */

static double bench_node_vec_project_text(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_text_table(&db, 50000);

    const int N = 100;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT UPPER(name) FROM tt");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: sort_int (single INT key — radix sort path)        */
/* ------------------------------------------------------------------ */

static double bench_node_sort_int(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 50000);

    const int N = 50;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM t ORDER BY val");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: sort_float (FLOAT key — radix_sort_f64)            */
/* ------------------------------------------------------------------ */

static double bench_node_sort_float(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_float_table(&db, 50000);

    const int N = 50;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM tf ORDER BY val");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: sort_text (TEXT key — pdqsort + strcmp)             */
/* ------------------------------------------------------------------ */

static double bench_node_sort_text(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_text_table(&db, 50000);

    const int N = 20;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM tt ORDER BY name");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: sort_multi (multi-key — composite radix or pdq)    */
/* ------------------------------------------------------------------ */

static double bench_node_sort_multi(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 50000);

    const int N = 50;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM t ORDER BY grp, val");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: simple_agg (COUNT/SUM/AVG, no GROUP BY)            */
/* ------------------------------------------------------------------ */

static double bench_node_simple_agg(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 500;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT COUNT(*), SUM(val), AVG(val) FROM t");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: hash_agg_few (100 groups — L1-resident HT)        */
/* ------------------------------------------------------------------ */

static double bench_node_hash_agg_few(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT grp, SUM(val) FROM t GROUP BY grp");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: hash_agg_many (100K groups — cache-miss-heavy)     */
/* ------------------------------------------------------------------ */

static double bench_node_hash_agg_many(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 50;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT id, SUM(val) FROM t GROUP BY id");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: hash_agg_multi (multi-agg accumulation)            */
/* ------------------------------------------------------------------ */

static double bench_node_hash_agg_multi(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT grp, SUM(val), COUNT(*), AVG(val) FROM t GROUP BY grp");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: hash_join (100K × 1K — L1-resident build side)     */
/* ------------------------------------------------------------------ */

static double bench_node_hash_join(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);
    exec(&db, "CREATE TABLE t_inner (id INT, label INT)");
    exec(&db, "INSERT INTO t_inner SELECT n, n * 3 "
              "FROM generate_series(1, 1000) AS g(n)");

    const int N = 50;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT t.id, t.val, t_inner.label "
            "FROM t JOIN t_inner ON t.grp = t_inner.id");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: hash_join_large (100K × 50K — L2/L3 pressure)     */
/* ------------------------------------------------------------------ */

static double bench_node_hash_join_large(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);
    exec(&db, "CREATE TABLE t_big (id INT, payload INT)");
    exec(&db, "INSERT INTO t_big SELECT n, n * 11 "
              "FROM generate_series(1, 50000) AS g(n)");

    const int N = 10;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT t.id, t_big.payload "
            "FROM t JOIN t_big ON t.val = t_big.id");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: semi_join (hash semi-join via IN subquery)         */
/* ------------------------------------------------------------------ */

static double bench_node_semi_join(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 50000);
    exec(&db, "CREATE TABLE t_filter (id INT, flag INT)");
    exec(&db, "INSERT INTO t_filter SELECT n, n % 2 "
              "FROM generate_series(1, 5000) AS g(n)");

    const int N = 50;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT * FROM t WHERE grp IN "
            "(SELECT id FROM t_filter WHERE flag = 1)");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: limit (early termination from 100K)                */
/* ------------------------------------------------------------------ */

static double bench_node_limit(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 2000;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT * FROM t LIMIT 100");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: top_n (heap-based ORDER BY ... LIMIT)              */
/* ------------------------------------------------------------------ */

static double bench_node_top_n(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 100;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT * FROM t ORDER BY val LIMIT 100");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: window (ROW_NUMBER OVER PARTITION BY)              */
/* ------------------------------------------------------------------ */

static double bench_node_window(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 50000);

    const int N = 10;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT id, grp, val, "
            "ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) FROM t");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: distinct (hash dedup, 100K→100)                    */
/* ------------------------------------------------------------------ */

static double bench_node_distinct(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db, "SELECT DISTINCT grp FROM t");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: set_op (UNION of two 50K tables)                   */
/* ------------------------------------------------------------------ */

static double bench_node_set_op(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t_a (id INT, val INT)");
    exec(&db, "CREATE TABLE t_b (id INT, val INT)");
    exec(&db, "INSERT INTO t_a SELECT n, n * 3 "
              "FROM generate_series(1, 50000) AS g(n)");
    exec(&db, "INSERT INTO t_b SELECT n + 25000, n * 7 "
              "FROM generate_series(1, 50000) AS g(n)");

    const int N = 10;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT id, val FROM t_a UNION SELECT id, val FROM t_b");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: gen_series (virtual table scan throughput)          */
/* ------------------------------------------------------------------ */

static double bench_node_gen_series(void)
{
    struct database db;
    db_init(&db, "bench");

    const int N = 200;
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        db_exec_sql_discard(&db,
            "SELECT * FROM generate_series(1, 100000)");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Node benchmark: index_scan (B-tree point lookups)                  */
/* ------------------------------------------------------------------ */

static double bench_node_index_scan(void)
{
    struct database db;
    db_init(&db, "bench");
    setup_node_int_table(&db, 100000);
    exec(&db, "CREATE INDEX idx_t_id ON t (id)");

    const int N = 1000;
    char sql[128];
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM t WHERE id = %d", (i * 7919) % 100000 + 1);
        db_exec_sql_discard(&db, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    double elapsed_ms = (now_sec() - t0) * 1e3;

    db_free(&db);
    return elapsed_ms;
}

/* ------------------------------------------------------------------ */
/*  Registry                                                           */
/* ------------------------------------------------------------------ */

struct bench_entry {
    const char *name;
    double (*fn)(void);
};

static struct bench_entry benchmarks[] = {
    { "insert_bulk",          bench_insert_bulk },
    { "select_full_scan",     bench_select_full_scan },
    { "select_where",         bench_select_where },
    { "aggregate",            bench_aggregate },
    { "order_by",             bench_order_by },
    { "join",                 bench_join },
    { "update",               bench_update },
    { "delete",               bench_delete },
    { "parser",               bench_parser },
    { "index_lookup",         bench_index_lookup },
    { "transaction",          bench_transaction },
    { "window_functions",     bench_window_functions },
    { "distinct",             bench_distinct },
    { "subquery",             bench_subquery },
    { "cte",                  bench_cte },
    { "generate_series",      bench_generate_series },
    { "scalar_functions",     bench_scalar_functions },
    { "expression_agg",       bench_expression_agg },
    { "multi_sort",           bench_multi_sort },
    { "set_ops",              bench_set_ops },
    { "multi_join",           bench_multi_join },
    { "analytical_cte",       bench_analytical_cte },
    { "wide_agg",             bench_wide_agg },
    { "large_sort",           bench_large_sort },
    { "subquery_complex",     bench_subquery_complex },
    { "window_rank",          bench_window_rank },
    { "mixed_analytical",     bench_mixed_analytical },
    { "vector_insert",        bench_vector_insert },
    { "vector_scan",          bench_vector_scan },
    { "vector_wide",          bench_vector_wide },
    { "vector_filter",        bench_vector_filter },
    { "node_seq_scan",        bench_node_seq_scan },
    { "node_filter_int",      bench_node_filter_int },
    { "node_filter_low_sel",  bench_node_filter_low_sel },
    { "node_filter_compound", bench_node_filter_compound },
    { "node_project",         bench_node_project },
    { "node_vec_project_int", bench_node_vec_project_int },
    { "node_vec_project_float", bench_node_vec_project_float },
    { "node_vec_project_col_col", bench_node_vec_project_col_col },
    { "node_vec_project_multi", bench_node_vec_project_multi },
    { "node_vec_project_text", bench_node_vec_project_text },
    { "node_sort_int",        bench_node_sort_int },
    { "node_sort_float",      bench_node_sort_float },
    { "node_sort_text",       bench_node_sort_text },
    { "node_sort_multi",      bench_node_sort_multi },
    { "node_simple_agg",      bench_node_simple_agg },
    { "node_hash_agg_few",    bench_node_hash_agg_few },
    { "node_hash_agg_many",   bench_node_hash_agg_many },
    { "node_hash_agg_multi",  bench_node_hash_agg_multi },
    { "node_hash_join",       bench_node_hash_join },
    { "node_hash_join_large", bench_node_hash_join_large },
    { "node_semi_join",       bench_node_semi_join },
    { "node_limit",           bench_node_limit },
    { "node_top_n",           bench_node_top_n },
    { "node_window",          bench_node_window },
    { "node_distinct",        bench_node_distinct },
    { "node_set_op",          bench_node_set_op },
    { "node_gen_series",      bench_node_gen_series },
    { "node_index_scan",      bench_node_index_scan },
};

static int nbench = (int)(sizeof(benchmarks) / sizeof(benchmarks[0]));

struct bench_result {
    const char *name;
    double ms;
    double p_min, p50, p95, p99, p_max;
    int niter;
    double ns_per_iter;
};

static void write_json(const char *path, struct bench_result *results, int n)
{
    FILE *f = fopen(path, "w");
    if (!f) { perror(path); return; }
    fprintf(f, "[\n");
    for (int i = 0; i < n; i++) {
        fprintf(f, "  { \"name\": \"%s\", \"unit\": \"ms\", \"value\": %.3f, "
                "\"extra\": \"iters=%d  min=%.3f  p50=%.3f  p95=%.3f  p99=%.3f  max=%.3f\" }",
                results[i].name, results[i].ms, results[i].niter,
                results[i].p_min, results[i].p50, results[i].p95,
                results[i].p99, results[i].p_max);
        fprintf(f, "%s\n", i + 1 < n ? "," : "");
    }
    fprintf(f, "]\n");
    fclose(f);
    printf("wrote %s\n", path);
}

int main(int argc, char **argv)
{
    const char *filter = NULL;
    const char *json_path = NULL;

    const char *prefix = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--json") == 0 && i + 1 < argc) {
            json_path = argv[++i];
        } else if (strcmp(argv[i], "--prefix") == 0 && i + 1 < argc) {
            prefix = argv[++i];
        } else {
            filter = argv[i];
        }
    }

    printf("mskql benchmarks\n");
    printf("=========================================="
           "======================================\n");

    struct bench_result results[128];
    int ran = 0;
    for (int i = 0; i < nbench; i++) {
        if (filter && strcmp(filter, benchmarks[i].name) != 0)
            continue;
        if (prefix && strncmp(prefix, benchmarks[i].name, strlen(prefix)) != 0)
            continue;
        printf("  running %-30s ...", benchmarks[i].name);
        fflush(stdout);
        g_niter = 0;
        g_ns_per_iter = 0.0;

        mach_timebase_info_data_t tb;
        mach_timebase_info(&tb);
        uint64_t mach_t0 = mach_absolute_time();
        double ms = benchmarks[i].fn();
        uint64_t mach_t1 = mach_absolute_time();
        uint64_t mach_elapsed = mach_t1 - mach_t0;
        double total_ns = (double)mach_elapsed * (double)tb.numer / (double)tb.denom;
        if (g_niter > 0)
            g_ns_per_iter = total_ns / (double)g_niter;
        results[ran].name = benchmarks[i].name;
        results[ran].ms = ms;
        results[ran].niter = g_niter;
        results[ran].ns_per_iter = g_ns_per_iter;
        if (g_niter > 0) {
            qsort(g_iter_ms, (size_t)g_niter, sizeof(double), dbl_cmp);
            results[ran].p_min = g_iter_ms[0];
            results[ran].p50   = percentile(g_iter_ms, g_niter, 50);
            results[ran].p95   = percentile(g_iter_ms, g_niter, 95);
            results[ran].p99   = percentile(g_iter_ms, g_niter, 99);
            results[ran].p_max = g_iter_ms[g_niter - 1];
        }
        printf("  %9.3f ms", ms);
        if (g_niter > 0)
            printf("  [p50=%.3f p95=%.3f p99=%.3f]",
                   results[ran].p50, results[ran].p95, results[ran].p99);
        if (g_ns_per_iter > 0)
            printf("  ~%.0f ns/iter", g_ns_per_iter);
        printf("\n");
        fflush(stdout);
        ran++;
    }

    if (ran == 0 && filter) {
        fprintf(stderr, "unknown benchmark: %s\n", filter);
        fprintf(stderr, "available:");
        for (int i = 0; i < nbench; i++)
            fprintf(stderr, " %s", benchmarks[i].name);
        fprintf(stderr, "\n");
        return 1;
    }

    printf("=========================================="
           "======================================\n");
    printf("done (%d benchmarks)\n", ran);

    if (json_path)
        write_json(json_path, results, ran);

    return 0;
}
