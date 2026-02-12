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
    if (rc != 0) {
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
        struct rows r = {0};
        db_exec_sql(&db, "SELECT * FROM t", &r);
        rows_free(&r);
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
        struct rows r = {0};
        db_exec_sql(&db, "SELECT * FROM t WHERE amount > 2500", &r);
        rows_free(&r);
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
        struct rows r = {0};
        db_exec_sql(&db,
            "SELECT region, SUM(amount) FROM sales GROUP BY region", &r);
        rows_free(&r);
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
        struct rows r = {0};
        db_exec_sql(&db, "SELECT * FROM t ORDER BY val DESC", &r);
        rows_free(&r);
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
        struct rows r = {0};
        db_exec_sql(&db,
            "SELECT users.name, orders.total FROM users "
            "JOIN orders ON users.id = orders.user_id", &r);
        rows_free(&r);
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
        struct rows r = {0};
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM t WHERE id = %d", i % 10000);
        db_exec_sql(&db, sql, &r);
        rows_free(&r);
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
        struct rows r = {0};
        db_exec_sql(&db,
            "SELECT id, grp, val, "
            "ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) FROM t", &r);
        rows_free(&r);
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
        struct rows r = {0};
        db_exec_sql(&db, "SELECT DISTINCT category FROM t", &r);
        rows_free(&r);
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
        struct rows r = {0};
        db_exec_sql(&db,
            "SELECT * FROM t1 WHERE id IN "
            "(SELECT id FROM t2 WHERE flag > 500)", &r);
        rows_free(&r);
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
        struct rows r = {0};
        db_exec_sql(&db,
            "WITH filtered AS (SELECT * FROM t WHERE val > 500) "
            "SELECT * FROM filtered WHERE category = 'cat_3'", &r);
        rows_free(&r);
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
        struct rows r = {0};
        db_exec_sql(&db, "SELECT * FROM generate_series(1, 10000)", &r);
        rows_free(&r);
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
        struct rows r = {0};
        db_exec_sql(&db,
            "SELECT UPPER(name), LENGTH(name), ABS(score), "
            "ROUND(score, 2) FROM t", &r);
        rows_free(&r);
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
        struct rows r = {0};
        db_exec_sql(&db,
            "SELECT category, SUM(price * quantity) FROM t GROUP BY category", &r);
        rows_free(&r);
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
        struct rows r = {0};
        db_exec_sql(&db, "SELECT * FROM t ORDER BY a DESC, b ASC", &r);
        rows_free(&r);
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
        struct rows r = {0};
        db_exec_sql(&db,
            "SELECT id, val FROM t1 UNION SELECT id, val FROM t2", &r);
        rows_free(&r);
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
};

static int nbench = (int)(sizeof(benchmarks) / sizeof(benchmarks[0]));

struct bench_result {
    const char *name;
    double ms;
    double p_min, p50, p95, p99, p_max;
    int niter;
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

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--json") == 0 && i + 1 < argc) {
            json_path = argv[++i];
        } else {
            filter = argv[i];
        }
    }

    printf("mskql benchmarks\n");
    printf("=========================================="
           "======================================\n");

    struct bench_result results[64];
    int ran = 0;
    for (int i = 0; i < nbench; i++) {
        if (filter && strcmp(filter, benchmarks[i].name) != 0)
            continue;
        printf("  running %-30s ...", benchmarks[i].name);
        fflush(stdout);
        g_niter = 0;
        double ms = benchmarks[i].fn();
        results[ran].name = benchmarks[i].name;
        results[ran].ms = ms;
        results[ran].niter = g_niter;
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
