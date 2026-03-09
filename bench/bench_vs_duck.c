/*
 * bench_vs_duck.c — in-process mskql vs DuckDB benchmark harness
 *
 * Links against libmskql.a and libduckdb to compare pure query engine
 * performance with zero wire/CLI overhead.
 *
 * Build:  make bench-vs-duck
 * Usage:  ./build/mskql_bench_vs_duck [--filter <name>] [--json <file>]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../src/mskql.h"
#include <duckdb.h>

/* ── timing ──────────────────────────────────────────────────────── */

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

/* ── DuckDB helpers ──────────────────────────────────────────────── */

static void duck_exec(duckdb_connection conn, const char *sql)
{
    duckdb_result result;
    duckdb_state st = duckdb_query(conn, sql, &result);
    if (st == DuckDBError) {
        const char *err = duckdb_result_error(&result);
        fprintf(stderr, "DuckDB FATAL: %s\nSQL: %s\n", err ? err : "?", sql);
        duckdb_destroy_result(&result);
        exit(1);
    }
    duckdb_destroy_result(&result);
}

static void duck_exec_discard(duckdb_connection conn, const char *sql)
{
    duckdb_result result;
    duckdb_state st = duckdb_query(conn, sql, &result);
    if (st == DuckDBError) {
        const char *err = duckdb_result_error(&result);
        fprintf(stderr, "DuckDB FATAL: %s\nSQL: %s\n", err ? err : "?", sql);
        duckdb_destroy_result(&result);
        exit(1);
    }
    duckdb_destroy_result(&result);
}

/* ── mskql helpers ───────────────────────────────────────────────── */

static void msk_exec(mskql_db *db, const char *sql)
{
    int rc = mskql_exec(db, sql);
    if (rc < 0) {
        fprintf(stderr, "mskql FATAL (rc=%d): %s\n", rc, sql);
        exit(1);
    }
}

static void msk_exec_discard(mskql_db *db, const char *sql)
{
    int rc = mskql_exec_discard(db, sql);
    if (rc < 0) {
        fprintf(stderr, "mskql FATAL (rc=%d): %s\n", rc, sql);
        exit(1);
    }
}

/* ── benchmark definition ────────────────────────────────────────── */

struct bench_times {
    double mskql_ms;
    double duck_ms;
};

/*
 * Each benchmark provides:
 *   - setup SQL (executed once, timed separately)
 *   - benchmark SQL array (executed N times, timed)
 *   - iteration count
 */

typedef void (*setup_fn)(mskql_db *mdb, duckdb_connection dconn);
typedef void (*bench_fn)(mskql_db *mdb, duckdb_connection dconn,
                         struct bench_times *out);

/* ── individual benchmarks ───────────────────────────────────────── */

/* ---------- select_full_scan ---------- */

static void setup_full_scan(mskql_db *mdb, duckdb_connection dconn)
{
    msk_exec(mdb, "CREATE TABLE t (id INT, name TEXT, score FLOAT)");
    duck_exec(dconn, "CREATE TABLE t (id INT, name TEXT, score FLOAT)");

    char sql[256];
    for (int i = 0; i < 10000; i++) {
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, 'user_%d', %d.%d)",
                 i, i, i % 100, i % 10);
        msk_exec(mdb, sql);
        duck_exec(dconn, sql);
    }
}

static void bench_full_scan(mskql_db *mdb, duckdb_connection dconn,
                            struct bench_times *out)
{
    const int N = 100;

    /* mskql */
    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, "SELECT * FROM t");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    /* duckdb */
    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, "SELECT * FROM t");
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- select_where ---------- */

static void setup_where(mskql_db *mdb, duckdb_connection dconn)
{
    setup_full_scan(mdb, dconn);  /* reuse same 10K row table */
}

static void bench_where(mskql_db *mdb, duckdb_connection dconn,
                        struct bench_times *out)
{
    const int N = 200;

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, "SELECT * FROM t WHERE score > 50.0");
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, "SELECT * FROM t WHERE score > 50.0");
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- aggregate ---------- */

static void setup_aggregate(mskql_db *mdb, duckdb_connection dconn)
{
    setup_full_scan(mdb, dconn);
}

static void bench_aggregate(mskql_db *mdb, duckdb_connection dconn,
                            struct bench_times *out)
{
    const int N = 200;
    const char *sql = "SELECT COUNT(*), SUM(score), AVG(score) FROM t";

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- order_by ---------- */

static void setup_order_by(mskql_db *mdb, duckdb_connection dconn)
{
    setup_full_scan(mdb, dconn);
}

static void bench_order_by(mskql_db *mdb, duckdb_connection dconn,
                           struct bench_times *out)
{
    const int N = 50;
    const char *sql = "SELECT * FROM t ORDER BY score DESC";

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- join ---------- */

static void setup_join(mskql_db *mdb, duckdb_connection dconn)
{
    const char *sql1 = "CREATE TABLE t_left (id INT, grp INT, val INT)";
    const char *sql2 = "CREATE TABLE t_right (id INT, label TEXT)";
    msk_exec(mdb, sql1);
    msk_exec(mdb, sql2);
    duck_exec(dconn, sql1);
    duck_exec(dconn, sql2);

    char sql[256];
    for (int i = 0; i < 10000; i++) {
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t_left VALUES (%d, %d, %d)",
                 i, i % 100, (i * 7) % 10000);
        msk_exec(mdb, sql);
        duck_exec(dconn, sql);
    }
    for (int i = 0; i < 1000; i++) {
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t_right VALUES (%d, 'label_%d')", i, i);
        msk_exec(mdb, sql);
        duck_exec(dconn, sql);
    }
}

static void bench_join(mskql_db *mdb, duckdb_connection dconn,
                       struct bench_times *out)
{
    const int N = 50;
    const char *sql =
        "SELECT t_left.id, t_left.val, t_right.label "
        "FROM t_left JOIN t_right ON t_left.grp = t_right.id";

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- group_by ---------- */

static void setup_group_by(mskql_db *mdb, duckdb_connection dconn)
{
    const char *ddl = "CREATE TABLE t_gb (id INT, grp INT, val INT)";
    msk_exec(mdb, ddl);
    duck_exec(dconn, ddl);

    char sql[256];
    for (int i = 0; i < 50000; i++) {
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t_gb VALUES (%d, %d, %d)",
                 i, i % 100, (i * 7) % 50000);
        msk_exec(mdb, sql);
        duck_exec(dconn, sql);
    }
}

static void bench_group_by(mskql_db *mdb, duckdb_connection dconn,
                           struct bench_times *out)
{
    const int N = 100;
    const char *sql =
        "SELECT grp, SUM(val), COUNT(*), AVG(val) FROM t_gb GROUP BY grp";

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- insert_bulk ---------- */

static void bench_insert_bulk(mskql_db *mdb, duckdb_connection dconn,
                              struct bench_times *out)
{
    msk_exec(mdb, "CREATE TABLE t_ins (id INT, name TEXT, score FLOAT)");
    duck_exec(dconn, "CREATE TABLE t_ins (id INT, name TEXT, score FLOAT)");

    const int N = 10000;
    char sql[256];

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t_ins VALUES (%d, 'user_%d', %d.%d)",
                 i, i, i % 100, i % 10);
        msk_exec(mdb, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t_ins VALUES (%d, 'user_%d', %d.%d)",
                 i, i, i % 100, i % 10);
        duck_exec(dconn, sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- generate_series scan ---------- */

static void bench_generate_series(mskql_db *mdb, duckdb_connection dconn,
                                  struct bench_times *out)
{
    const int N = 50;
    const char *msk_sql = "SELECT * FROM generate_series(1, 100000) AS g(n)";
    const char *duck_sql = "SELECT * FROM generate_series(1, 100000)";

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, msk_sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, duck_sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- large sort (100K rows) ---------- */

static void setup_large_sort(mskql_db *mdb, duckdb_connection dconn)
{
    msk_exec(mdb, "CREATE TABLE t_ls (id INT, grp INT, val INT)");
    duck_exec(dconn, "CREATE TABLE t_ls (id INT, grp INT, val INT)");

    /* bulk insert via generate_series where possible */
    msk_exec(mdb,
        "INSERT INTO t_ls SELECT n, n % 100, (n * 7) % 100000 "
        "FROM generate_series(1, 100000) AS g(n)");
    duck_exec(dconn,
        "INSERT INTO t_ls SELECT generate_series AS n, "
        "generate_series % 100, (generate_series * 7) % 100000 "
        "FROM generate_series(1, 100000)");
}

static void bench_large_sort(mskql_db *mdb, duckdb_connection dconn,
                             struct bench_times *out)
{
    const int N = 20;
    const char *sql = "SELECT * FROM t_ls ORDER BY val DESC";

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- large join (100K × 1K) ---------- */

static void setup_large_join(mskql_db *mdb, duckdb_connection dconn)
{
    msk_exec(mdb, "CREATE TABLE t_lj (id INT, grp INT, val INT)");
    msk_exec(mdb, "CREATE TABLE t_lj_inner (id INT, label INT)");
    duck_exec(dconn, "CREATE TABLE t_lj (id INT, grp INT, val INT)");
    duck_exec(dconn, "CREATE TABLE t_lj_inner (id INT, label INT)");

    msk_exec(mdb,
        "INSERT INTO t_lj SELECT n, n % 100, (n * 7) % 100000 "
        "FROM generate_series(1, 100000) AS g(n)");
    msk_exec(mdb,
        "INSERT INTO t_lj_inner SELECT n, n * 3 "
        "FROM generate_series(1, 1000) AS g(n)");

    duck_exec(dconn,
        "INSERT INTO t_lj SELECT generate_series, "
        "generate_series % 100, (generate_series * 7) % 100000 "
        "FROM generate_series(1, 100000)");
    duck_exec(dconn,
        "INSERT INTO t_lj_inner SELECT generate_series, "
        "generate_series * 3 FROM generate_series(1, 1000)");
}

static void bench_large_join(mskql_db *mdb, duckdb_connection dconn,
                             struct bench_times *out)
{
    const int N = 50;
    const char *sql =
        "SELECT t_lj.id, t_lj.val, t_lj_inner.label "
        "FROM t_lj JOIN t_lj_inner ON t_lj.grp = t_lj_inner.id";

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- distinct ---------- */

static void setup_distinct(mskql_db *mdb, duckdb_connection dconn)
{
    msk_exec(mdb, "CREATE TABLE t_dist (id INT, grp INT, val INT)");
    duck_exec(dconn, "CREATE TABLE t_dist (id INT, grp INT, val INT)");

    msk_exec(mdb,
        "INSERT INTO t_dist SELECT n, n % 100, (n * 7) % 100000 "
        "FROM generate_series(1, 100000) AS g(n)");
    duck_exec(dconn,
        "INSERT INTO t_dist SELECT generate_series, "
        "generate_series % 100, (generate_series * 7) % 100000 "
        "FROM generate_series(1, 100000)");
}

static void bench_distinct(mskql_db *mdb, duckdb_connection dconn,
                           struct bench_times *out)
{
    const int N = 200;
    const char *sql = "SELECT DISTINCT grp FROM t_dist";

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- window function ---------- */

static void setup_window(mskql_db *mdb, duckdb_connection dconn)
{
    msk_exec(mdb, "CREATE TABLE t_win (id INT, grp INT, val INT)");
    duck_exec(dconn, "CREATE TABLE t_win (id INT, grp INT, val INT)");

    msk_exec(mdb,
        "INSERT INTO t_win SELECT n, n % 100, (n * 7) % 50000 "
        "FROM generate_series(1, 50000) AS g(n)");
    duck_exec(dconn,
        "INSERT INTO t_win SELECT generate_series, "
        "generate_series % 100, (generate_series * 7) % 50000 "
        "FROM generate_series(1, 50000)");
}

static void bench_window(mskql_db *mdb, duckdb_connection dconn,
                         struct bench_times *out)
{
    const int N = 10;
    const char *sql =
        "SELECT id, grp, val, "
        "ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) FROM t_win";

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- CTE ---------- */

static void setup_cte(mskql_db *mdb, duckdb_connection dconn)
{
    msk_exec(mdb, "CREATE TABLE t_cte (id INT, grp INT, val INT)");
    duck_exec(dconn, "CREATE TABLE t_cte (id INT, grp INT, val INT)");

    msk_exec(mdb,
        "INSERT INTO t_cte SELECT n, n % 100, (n * 7) % 50000 "
        "FROM generate_series(1, 50000) AS g(n)");
    duck_exec(dconn,
        "INSERT INTO t_cte SELECT generate_series, "
        "generate_series % 100, (generate_series * 7) % 50000 "
        "FROM generate_series(1, 50000)");
}

static void bench_cte(mskql_db *mdb, duckdb_connection dconn,
                      struct bench_times *out)
{
    const int N = 50;
    const char *sql =
        "WITH grp_totals AS ("
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt "
        "FROM t_cte GROUP BY grp"
        ") SELECT * FROM grp_totals WHERE total > 100000 ORDER BY total DESC";

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- scalar_functions ---------- */

static void setup_scalar_functions(mskql_db *mdb, duckdb_connection dconn)
{
    msk_exec(mdb, "CREATE TABLE t_sf (id INT, name TEXT, val FLOAT)");
    duck_exec(dconn, "CREATE TABLE t_sf (id INT, name TEXT, val FLOAT)");

    char sql[256];
    for (int i = 0; i < 10000; i++) {
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t_sf VALUES (%d, 'name_%d', %d.%d)",
                 i, i, i % 100, i % 10);
        msk_exec(mdb, sql);
        duck_exec(dconn, sql);
    }
}

static void bench_scalar_functions(mskql_db *mdb, duckdb_connection dconn,
                                   struct bench_times *out)
{
    const int N = 100;
    const char *sql =
        "SELECT id, UPPER(name), ABS(val), val * 2 + 1 FROM t_sf";

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- top_n (ORDER BY + LIMIT) ---------- */

static void setup_top_n(mskql_db *mdb, duckdb_connection dconn)
{
    msk_exec(mdb, "CREATE TABLE t_topn (id INT, grp INT, val INT)");
    duck_exec(dconn, "CREATE TABLE t_topn (id INT, grp INT, val INT)");

    msk_exec(mdb,
        "INSERT INTO t_topn SELECT n, n % 100, (n * 7) % 100000 "
        "FROM generate_series(1, 100000) AS g(n)");
    duck_exec(dconn,
        "INSERT INTO t_topn SELECT generate_series, "
        "generate_series % 100, (generate_series * 7) % 100000 "
        "FROM generate_series(1, 100000)");
}

static void bench_top_n(mskql_db *mdb, duckdb_connection dconn,
                        struct bench_times *out)
{
    const int N = 100;
    const char *sql =
        "SELECT * FROM t_topn ORDER BY val DESC LIMIT 100";

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ---------- expression aggregate ---------- */

static void setup_expression_agg(mskql_db *mdb, duckdb_connection dconn)
{
    msk_exec(mdb, "CREATE TABLE t_ea (id INT, quantity INT, price INT)");
    duck_exec(dconn, "CREATE TABLE t_ea (id INT, quantity INT, price INT)");

    msk_exec(mdb,
        "INSERT INTO t_ea SELECT n, 1 + n % 20, 10 + (n * 13) % 990 "
        "FROM generate_series(1, 50000) AS g(n)");
    duck_exec(dconn,
        "INSERT INTO t_ea SELECT generate_series, "
        "1 + generate_series % 20, 10 + (generate_series * 13) % 990 "
        "FROM generate_series(1, 50000)");
}

static void bench_expression_agg(mskql_db *mdb, duckdb_connection dconn,
                                 struct bench_times *out)
{
    const int N = 100;
    const char *sql =
        "SELECT SUM(quantity * price), AVG(quantity * price), COUNT(*) "
        "FROM t_ea";

    g_niter = N;
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        double it0 = now_sec();
        msk_exec_discard(mdb, sql);
        g_iter_ms[i] = (now_sec() - it0) * 1e3;
    }
    out->mskql_ms = (now_sec() - t0) * 1e3;

    t0 = now_sec();
    for (int i = 0; i < N; i++) {
        duck_exec_discard(dconn, sql);
    }
    out->duck_ms = (now_sec() - t0) * 1e3;
}

/* ── registry ────────────────────────────────────────────────────── */

struct bench_entry {
    const char *name;
    setup_fn   setup;   /* NULL if bench handles its own setup */
    bench_fn   bench;
};

static struct bench_entry benchmarks[] = {
    { "insert_bulk",       NULL,                     (bench_fn)bench_insert_bulk },
    { "select_full_scan",  setup_full_scan,          bench_full_scan },
    { "select_where",      setup_where,              bench_where },
    { "aggregate",         setup_aggregate,          bench_aggregate },
    { "order_by",          setup_order_by,           bench_order_by },
    { "join",              setup_join,               bench_join },
    { "group_by",          setup_group_by,           bench_group_by },
    { "generate_series",   NULL,                     (bench_fn)bench_generate_series },
    { "large_sort",        setup_large_sort,         bench_large_sort },
    { "large_join",        setup_large_join,         bench_large_join },
    { "distinct",          setup_distinct,           bench_distinct },
    { "window",            setup_window,             bench_window },
    { "cte",               setup_cte,                bench_cte },
    { "scalar_functions",  setup_scalar_functions,   bench_scalar_functions },
    { "top_n",             setup_top_n,              bench_top_n },
    { "expression_agg",    setup_expression_agg,     bench_expression_agg },
};

static int nbench = (int)(sizeof(benchmarks) / sizeof(benchmarks[0]));

/* ── result reporting ────────────────────────────────────────────── */

struct bench_result {
    const char *name;
    double mskql_ms;
    double duck_ms;
    double ratio;     /* mskql / duck  (< 1 = mskql faster) */
    double p_min, p50, p95, p99, p_max;
    int    niter;
};

static void write_json(const char *path, struct bench_result *results, int n)
{
    FILE *f = fopen(path, "w");
    if (!f) { perror(path); return; }
    fprintf(f, "[\n");
    for (int i = 0; i < n; i++) {
        fprintf(f, "  { \"name\": \"%s\", \"mskql_ms\": %.3f, \"duck_ms\": %.3f, "
                "\"ratio\": %.3f, "
                "\"extra\": \"iters=%d  min=%.3f  p50=%.3f  p95=%.3f  p99=%.3f  max=%.3f\" }",
                results[i].name, results[i].mskql_ms, results[i].duck_ms,
                results[i].ratio, results[i].niter,
                results[i].p_min, results[i].p50, results[i].p95,
                results[i].p99, results[i].p_max);
        fprintf(f, "%s\n", i + 1 < n ? "," : "");
    }
    fprintf(f, "]\n");
    fclose(f);
}

/* ── main ────────────────────────────────────────────────────────── */

int main(int argc, char **argv)
{
    const char *filter = NULL;
    const char *json_path = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--filter") == 0 && i + 1 < argc) {
            filter = argv[++i];
        } else if (strcmp(argv[i], "--json") == 0 && i + 1 < argc) {
            json_path = argv[++i];
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [--filter <name>] [--json <file>]\n", argv[0]);
            return 0;
        }
    }

    /* ANSI colors */
    const char *GREEN  = "\033[32m";
    const char *RED    = "\033[31m";
    const char *YELLOW = "\033[33m";
    const char *RESET  = "\033[0m";

    printf("=======================================================================\n");
    printf("  mskql vs DuckDB — in-process benchmark\n");
    printf("  (both engines in-memory, zero wire/CLI overhead)\n");
    printf("=======================================================================\n\n");

    struct bench_result results[sizeof(benchmarks) / sizeof(benchmarks[0])];
    int nresults = 0;

    for (int b = 0; b < nbench; b++) {
        if (filter && strcmp(filter, benchmarks[b].name) != 0)
            continue;

        printf("  %-25s ", benchmarks[b].name);
        fflush(stdout);

        /* fresh databases for each benchmark */
        mskql_db *mdb = mskql_open("bench");
        if (!mdb) {
            fprintf(stderr, "mskql_open failed\n");
            return 1;
        }

        duckdb_database ddb;
        duckdb_connection dconn;
        if (duckdb_open(NULL, &ddb) == DuckDBError) {
            fprintf(stderr, "duckdb_open failed\n");
            mskql_close(mdb);
            return 1;
        }
        if (duckdb_connect(ddb, &dconn) == DuckDBError) {
            fprintf(stderr, "duckdb_connect failed\n");
            duckdb_close(&ddb);
            mskql_close(mdb);
            return 1;
        }

        /* setup */
        if (benchmarks[b].setup)
            benchmarks[b].setup(mdb, dconn);

        /* run */
        struct bench_times times = {0};
        g_niter = 0;
        benchmarks[b].bench(mdb, dconn, &times);

        /* compute percentiles from mskql iter times */
        double p_min = 0, p50 = 0, p95 = 0, p99 = 0, p_max = 0;
        if (g_niter > 0) {
            qsort(g_iter_ms, (size_t)g_niter, sizeof(double), dbl_cmp);
            p_min = g_iter_ms[0];
            p50   = percentile(g_iter_ms, g_niter, 50);
            p95   = percentile(g_iter_ms, g_niter, 95);
            p99   = percentile(g_iter_ms, g_niter, 99);
            p_max = g_iter_ms[g_niter - 1];
        }

        double ratio = (times.duck_ms > 0)
            ? times.mskql_ms / times.duck_ms : 0;

        results[nresults++] = (struct bench_result){
            .name     = benchmarks[b].name,
            .mskql_ms = times.mskql_ms,
            .duck_ms  = times.duck_ms,
            .ratio    = ratio,
            .p_min    = p_min,
            .p50      = p50,
            .p95      = p95,
            .p99      = p99,
            .p_max    = p_max,
            .niter    = g_niter,
        };

        const char *color = (ratio < 1.0) ? GREEN
                          : (ratio < 1.5) ? YELLOW
                          : RED;
        printf("mskql=%7.1fms  duck=%7.1fms  %s%.2fx%s\n",
               times.mskql_ms, times.duck_ms, color, ratio, RESET);

        /* cleanup */
        duckdb_disconnect(&dconn);
        duckdb_close(&ddb);
        mskql_close(mdb);
    }

    /* summary table */
    printf("\n  %-25s %12s %12s %8s  fastest\n",
           "BENCHMARK", "mskql (ms)", "duck (ms)", "ratio");
    printf("  %-25s %12s %12s %8s  -------\n",
           "-------------------------", "----------", "----------", "------");

    int mskql_wins = 0;
    for (int i = 0; i < nresults; i++) {
        const char *winner;
        const char *color;
        if (results[i].ratio < 1.0) {
            winner = "mskql";
            color = GREEN;
            mskql_wins++;
        } else {
            winner = "duck";
            color = RED;
        }
        printf("  %-25s %12.1f %12.1f %s%7.2fx%s  %s%s%s\n",
               results[i].name,
               results[i].mskql_ms,
               results[i].duck_ms,
               color, results[i].ratio, RESET,
               color, winner, RESET);
    }

    printf("\n  %d benchmarks, mskql fastest in %d\n\n", nresults, mskql_wins);

    if (json_path)
        write_json(json_path, results, nresults);

    return 0;
}
