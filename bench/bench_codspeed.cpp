/*
 * bench_codspeed.cpp — mskql benchmarks for CodSpeed via Google Benchmark
 *
 * Thin C++ wrapper that calls into the C library through extern "C".
 * Each benchmark uses the Google Benchmark state loop so CodSpeed can
 * detect and measure individual benchmarks.
 */

#include <benchmark/benchmark.h>
#include <cstdio>
#include <cstring>

extern "C" {
#include "database.h"
#include "parser.h"
}

/* ------------------------------------------------------------------ */
/*  Helper: execute SQL, discard result, abort on error                */
/* ------------------------------------------------------------------ */

static void exec(struct database *db, const char *sql)
{
    struct rows r = {};
    if (db_exec_sql(db, sql, &r) != 0) {
        fprintf(stderr, "FATAL: %s\n", sql);
        exit(1);
    }
    rows_free(&r);
}

/* ------------------------------------------------------------------ */
/*  Benchmark: bulk INSERT                                             */
/* ------------------------------------------------------------------ */

static void BM_InsertBulk(benchmark::State &state)
{
    for (auto _ : state) {
        struct database db;
        db_init(&db, "bench");
        exec(&db, "CREATE TABLE t (id INT, name TEXT, score FLOAT)");

        char sql[256];
        for (int i = 0; i < 10000; i++) {
            snprintf(sql, sizeof(sql),
                     "INSERT INTO t VALUES (%d, 'user_%d', %d.%d)",
                     i, i, i % 100, i % 10);
            exec(&db, sql);
        }
        db_free(&db);
    }
}
BENCHMARK(BM_InsertBulk)->Unit(benchmark::kMillisecond);

/* ------------------------------------------------------------------ */
/*  Benchmark: SELECT full scan                                        */
/* ------------------------------------------------------------------ */

static void BM_SelectFullScan(benchmark::State &state)
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

    for (auto _ : state) {
        struct rows r = {};
        db_exec_sql(&db, "SELECT * FROM t", &r);
        benchmark::DoNotOptimize(r.count);
        rows_free(&r);
    }
    db_free(&db);
}
BENCHMARK(BM_SelectFullScan)->Unit(benchmark::kMillisecond);

/* ------------------------------------------------------------------ */
/*  Benchmark: SELECT with WHERE filter                                */
/* ------------------------------------------------------------------ */

static void BM_SelectWhere(benchmark::State &state)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, category TEXT, amount INT)");
    for (int i = 0; i < 5000; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, 'cat_%d', %d)", i, i % 10, i);
        exec(&db, sql);
    }

    for (auto _ : state) {
        struct rows r = {};
        db_exec_sql(&db, "SELECT * FROM t WHERE amount > 2500", &r);
        benchmark::DoNotOptimize(r.count);
        rows_free(&r);
    }
    db_free(&db);
}
BENCHMARK(BM_SelectWhere)->Unit(benchmark::kMillisecond);

/* ------------------------------------------------------------------ */
/*  Benchmark: aggregate (SUM + GROUP BY)                              */
/* ------------------------------------------------------------------ */

static void BM_Aggregate(benchmark::State &state)
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

    for (auto _ : state) {
        struct rows r = {};
        db_exec_sql(&db,
            "SELECT region, SUM(amount) FROM sales GROUP BY region", &r);
        benchmark::DoNotOptimize(r.count);
        rows_free(&r);
    }
    db_free(&db);
}
BENCHMARK(BM_Aggregate)->Unit(benchmark::kMillisecond);

/* ------------------------------------------------------------------ */
/*  Benchmark: ORDER BY                                                */
/* ------------------------------------------------------------------ */

static void BM_OrderBy(benchmark::State &state)
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

    for (auto _ : state) {
        struct rows r = {};
        db_exec_sql(&db, "SELECT * FROM t ORDER BY val DESC", &r);
        benchmark::DoNotOptimize(r.count);
        rows_free(&r);
    }
    db_free(&db);
}
BENCHMARK(BM_OrderBy)->Unit(benchmark::kMillisecond);

/* ------------------------------------------------------------------ */
/*  Benchmark: JOIN                                                    */
/* ------------------------------------------------------------------ */

static void BM_Join(benchmark::State &state)
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

    for (auto _ : state) {
        struct rows r = {};
        db_exec_sql(&db,
            "SELECT users.name, orders.total FROM users "
            "JOIN orders ON users.id = orders.user_id", &r);
        benchmark::DoNotOptimize(r.count);
        rows_free(&r);
    }
    db_free(&db);
}
BENCHMARK(BM_Join)->Unit(benchmark::kMillisecond);

/* ------------------------------------------------------------------ */
/*  Benchmark: UPDATE                                                  */
/* ------------------------------------------------------------------ */

static void BM_Update(benchmark::State &state)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, val INT)");
    for (int i = 0; i < 5000; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO t VALUES (%d, %d)", i, i);
        exec(&db, sql);
    }

    for (auto _ : state) {
        exec(&db, "UPDATE t SET val = 0 WHERE id < 1000");
    }
    db_free(&db);
}
BENCHMARK(BM_Update)->Unit(benchmark::kMillisecond);

/* ------------------------------------------------------------------ */
/*  Benchmark: DELETE                                                   */
/* ------------------------------------------------------------------ */

static void BM_Delete(benchmark::State &state)
{
    for (auto _ : state) {
        struct database db;
        db_init(&db, "bench");
        exec(&db, "CREATE TABLE t (id INT, val INT)");
        for (int i = 0; i < 2000; i++) {
            char sql[128];
            snprintf(sql, sizeof(sql), "INSERT INTO t VALUES (%d, %d)", i, i);
            exec(&db, sql);
        }
        exec(&db, "DELETE FROM t WHERE id >= 1000");
        db_free(&db);
    }
}
BENCHMARK(BM_Delete)->Unit(benchmark::kMillisecond);

/* ------------------------------------------------------------------ */
/*  Benchmark: parser throughput                                       */
/* ------------------------------------------------------------------ */

static void BM_Parser(benchmark::State &state)
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
    int i = 0;

    for (auto _ : state) {
        struct query q = {};
        if (query_parse(queries[i % nq], &q) == 0)
            query_free(&q);
        i++;
    }
}
BENCHMARK(BM_Parser);

/* ------------------------------------------------------------------ */
/*  Benchmark: index lookup                                            */
/* ------------------------------------------------------------------ */

static void BM_IndexLookup(benchmark::State &state)
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

    int i = 0;
    char sql[128];
    for (auto _ : state) {
        struct rows r = {};
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM t WHERE id = %d", i % 10000);
        db_exec_sql(&db, sql, &r);
        benchmark::DoNotOptimize(r.count);
        rows_free(&r);
        i++;
    }
    db_free(&db);
}
BENCHMARK(BM_IndexLookup);

/* ------------------------------------------------------------------ */
/*  Benchmark: transaction (BEGIN / INSERT / ROLLBACK)                 */
/* ------------------------------------------------------------------ */

static void BM_Transaction(benchmark::State &state)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, val INT)");
    for (int i = 0; i < 1000; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO t VALUES (%d, %d)", i, i);
        exec(&db, sql);
    }

    for (auto _ : state) {
        exec(&db, "BEGIN");
        for (int j = 0; j < 100; j++) {
            char sql[128];
            snprintf(sql, sizeof(sql),
                     "INSERT INTO t VALUES (%d, %d)", 1000 + j, j);
            exec(&db, sql);
        }
        exec(&db, "ROLLBACK");
    }
    db_free(&db);
}
BENCHMARK(BM_Transaction)->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
