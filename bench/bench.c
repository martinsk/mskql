/*
 * bench.c — mskql micro-benchmarks
 *
 * Builds as a standalone executable that links against the library .o files
 * (everything except main.o).  Each benchmark function sets up a fresh
 * database, runs a workload, and prints wall-clock elapsed time.
 *
 * Usage:  ./build/mskql_bench              (run all benchmarks)
 *         ./build/mskql_bench <name>       (run only the named benchmark)
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

#define BENCH_START  double _t0 = now_sec()
#define BENCH_END(label, iters)                                         \
    do {                                                                \
        double _elapsed = now_sec() - _t0;                              \
        printf("%-40s  %8d iters  %9.3f ms  (%7.1f ops/s)\n",          \
               (label), (int)(iters), _elapsed * 1e3,                   \
               (double)(iters) / _elapsed);                             \
    } while (0)

/* ------------------------------------------------------------------ */
/*  Helper: execute SQL, discard result, abort on error                */
/* ------------------------------------------------------------------ */

static void exec(struct database *db, const char *sql)
{
    struct rows r = {0};
    if (db_exec_sql(db, sql, &r) != 0) {
        fprintf(stderr, "FATAL: %s\n", sql);
        exit(1);
    }
    rows_free(&r);
}

/* ------------------------------------------------------------------ */
/*  Benchmark: bulk INSERT                                             */
/* ------------------------------------------------------------------ */

static void bench_insert_bulk(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, name TEXT, score FLOAT)");

    const int N = 10000;
    char sql[256];
    BENCH_START;
    for (int i = 0; i < N; i++) {
        snprintf(sql, sizeof(sql),
                 "INSERT INTO t VALUES (%d, 'user_%d', %d.%d)",
                 i, i, i % 100, i % 10);
        exec(&db, sql);
    }
    BENCH_END("insert_bulk (10k rows)", N);

    db_free(&db);
}

/* ------------------------------------------------------------------ */
/*  Benchmark: SELECT full scan                                        */
/* ------------------------------------------------------------------ */

static void bench_select_full_scan(void)
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
    BENCH_START;
    for (int i = 0; i < N; i++) {
        struct rows r = {0};
        db_exec_sql(&db, "SELECT * FROM t", &r);
        rows_free(&r);
    }
    BENCH_END("select_full_scan (5k rows x200)", N);

    db_free(&db);
}

/* ------------------------------------------------------------------ */
/*  Benchmark: SELECT with WHERE filter                                */
/* ------------------------------------------------------------------ */

static void bench_select_where(void)
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
    BENCH_START;
    for (int i = 0; i < N; i++) {
        struct rows r = {0};
        db_exec_sql(&db, "SELECT * FROM t WHERE amount > 2500", &r);
        rows_free(&r);
    }
    BENCH_END("select_where (5k rows x500)", N);

    db_free(&db);
}

/* ------------------------------------------------------------------ */
/*  Benchmark: aggregate (SUM + GROUP BY)                              */
/* ------------------------------------------------------------------ */

static void bench_aggregate(void)
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
    BENCH_START;
    for (int i = 0; i < N; i++) {
        struct rows r = {0};
        db_exec_sql(&db,
            "SELECT region, SUM(amount) FROM sales GROUP BY region", &r);
        rows_free(&r);
    }
    BENCH_END("aggregate_group_by (5k rows x500)", N);

    db_free(&db);
}

/* ------------------------------------------------------------------ */
/*  Benchmark: ORDER BY                                                */
/* ------------------------------------------------------------------ */

static void bench_order_by(void)
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
    BENCH_START;
    for (int i = 0; i < N; i++) {
        struct rows r = {0};
        db_exec_sql(&db, "SELECT * FROM t ORDER BY val DESC", &r);
        rows_free(&r);
    }
    BENCH_END("order_by (5k rows x200)", N);

    db_free(&db);
}

/* ------------------------------------------------------------------ */
/*  Benchmark: JOIN                                                    */
/* ------------------------------------------------------------------ */

static void bench_join(void)
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
    BENCH_START;
    for (int i = 0; i < N; i++) {
        struct rows r = {0};
        db_exec_sql(&db,
            "SELECT users.name, orders.total FROM users "
            "JOIN orders ON users.id = orders.user_id", &r);
        rows_free(&r);
    }
    BENCH_END("join (500x2000 rows x50)", N);

    db_free(&db);
}

/* ------------------------------------------------------------------ */
/*  Benchmark: UPDATE                                                  */
/* ------------------------------------------------------------------ */

static void bench_update(void)
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
    BENCH_START;
    for (int i = 0; i < N; i++) {
        exec(&db, "UPDATE t SET val = 0 WHERE id < 1000");
    }
    BENCH_END("update_where (5k rows x200)", N);

    db_free(&db);
}

/* ------------------------------------------------------------------ */
/*  Benchmark: DELETE                                                   */
/* ------------------------------------------------------------------ */

static void bench_delete(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, val INT)");

    /* insert, delete half, re-insert — measures delete performance */
    const int N = 50;
    BENCH_START;
    for (int iter = 0; iter < N; iter++) {
        /* fresh table each iteration */
        exec(&db, "DROP TABLE t");
        exec(&db, "CREATE TABLE t (id INT, val INT)");
        for (int i = 0; i < 2000; i++) {
            char sql[128];
            snprintf(sql, sizeof(sql), "INSERT INTO t VALUES (%d, %d)", i, i);
            exec(&db, sql);
        }
        exec(&db, "DELETE FROM t WHERE id >= 1000");
    }
    BENCH_END("delete_where (2k rows x50)", N);

    db_free(&db);
}

/* ------------------------------------------------------------------ */
/*  Benchmark: parser throughput                                       */
/* ------------------------------------------------------------------ */

static void bench_parser(void)
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
    BENCH_START;
    for (int i = 0; i < N; i++) {
        struct query q = {0};
        if (query_parse(queries[i % nq], &q) == 0)
            query_free(&q);
    }
    BENCH_END("parser (50k parses)", N);
}

/* ------------------------------------------------------------------ */
/*  Benchmark: index lookup                                            */
/* ------------------------------------------------------------------ */

static void bench_index_lookup(void)
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
    BENCH_START;
    for (int i = 0; i < N; i++) {
        struct rows r = {0};
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM t WHERE id = %d", i % 10000);
        db_exec_sql(&db, sql, &r);
        rows_free(&r);
    }
    BENCH_END("index_lookup (10k rows x2000)", N);

    db_free(&db);
}

/* ------------------------------------------------------------------ */
/*  Benchmark: transaction (BEGIN / INSERT / ROLLBACK)                 */
/* ------------------------------------------------------------------ */

static void bench_transaction(void)
{
    struct database db;
    db_init(&db, "bench");
    exec(&db, "CREATE TABLE t (id INT, val INT)");

    for (int i = 0; i < 1000; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "INSERT INTO t VALUES (%d, %d)", i, i);
        exec(&db, sql);
    }

    const int N = 200;
    BENCH_START;
    for (int i = 0; i < N; i++) {
        exec(&db, "BEGIN");
        for (int j = 0; j < 100; j++) {
            char sql[128];
            snprintf(sql, sizeof(sql),
                     "INSERT INTO t VALUES (%d, %d)", 1000 + j, j);
            exec(&db, sql);
        }
        exec(&db, "ROLLBACK");
    }
    BENCH_END("transaction_rollback (1k base x200)", N);

    db_free(&db);
}

/* ------------------------------------------------------------------ */
/*  Registry                                                           */
/* ------------------------------------------------------------------ */

struct bench_entry {
    const char *name;
    void (*fn)(void);
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
};

static int nbench = (int)(sizeof(benchmarks) / sizeof(benchmarks[0]));

int main(int argc, char **argv)
{
    const char *filter = argc > 1 ? argv[1] : NULL;

    printf("mskql benchmarks\n");
    printf("=========================================="
           "======================================\n");

    int ran = 0;
    for (int i = 0; i < nbench; i++) {
        if (filter && strcmp(filter, benchmarks[i].name) != 0)
            continue;
        benchmarks[i].fn();
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
    return 0;
}
