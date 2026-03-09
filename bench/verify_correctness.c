/*
 * verify_correctness.c — compare mskql vs DuckDB results for benchmark queries
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "../src/mskql.h"
#include <duckdb.h>

static int row_str_cmp(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

static void duck_exec(duckdb_connection c, const char *sql) {
    duckdb_result r;
    if (duckdb_query(c, sql, &r) == DuckDBError) {
        fprintf(stderr, "DuckDB: %s\nSQL: %s\n", duckdb_result_error(&r), sql);
        duckdb_destroy_result(&r); exit(1);
    }
    duckdb_destroy_result(&r);
}

static void msk_exec(mskql_db *db, const char *sql) {
    if (mskql_exec(db, sql) < 0) { fprintf(stderr, "mskql: %s\n", sql); exit(1); }
}

static int g_pass = 0, g_fail = 0;

static void verify(const char *name, const char *sql,
                   mskql_db *mdb, duckdb_connection dc, int ordered)
{
    /* mskql */
    mskql_result *mr = NULL;
    int rc = mskql_query(mdb, sql, &mr);
    if (rc < 0) { printf("  FAIL %-22s mskql rc=%d\n", name, rc); g_fail++; return; }
    int mnr = mskql_result_nrows(mr), mnc = mskql_result_ncols(mr);

    /* DuckDB */
    duckdb_result dr;
    if (duckdb_query(dc, sql, &dr) == DuckDBError) {
        printf("  FAIL %-22s duck err\n", name);
        mskql_result_free(mr); duckdb_destroy_result(&dr); g_fail++; return;
    }
    int dnr = (int)duckdb_row_count(&dr), dnc = (int)duckdb_column_count(&dr);

    int ok = 1;
    if (mnr != dnr) { printf("  FAIL %-22s rows mskql=%d duck=%d\n", name, mnr, dnr); ok = 0; }
    if (mnc != dnc) { printf("  FAIL %-22s cols mskql=%d duck=%d\n", name, mnc, dnc); ok = 0; }

    if (ok && mnr > 0) {
        /* Build row strings for both engines */
        char **mrows = calloc(mnr, sizeof(char*));
        char **drows = calloc(dnr, sizeof(char*));
        for (int r = 0; r < mnr; r++) {
            char buf[2048]; int off = 0;
            for (int c = 0; c < mnc; c++) {
                const char *v = mskql_result_value(mr, r, c);
                off += snprintf(buf+off, sizeof(buf)-off, "%s%s", c?"|":"", v?v:"NULL");
            }
            mrows[r] = strdup(buf);
        }
        for (int r = 0; r < dnr; r++) {
            char buf[2048]; int off = 0;
            for (int c = 0; c < dnc; c++) {
                char *s = duckdb_value_varchar(&dr, (idx_t)c, (idx_t)r);
                off += snprintf(buf+off, sizeof(buf)-off, "%s%s", c?"|":"", s?s:"NULL");
                if (s) duckdb_free(s);
            }
            drows[r] = strdup(buf);
        }

        if (!ordered) {
            qsort(mrows, mnr, sizeof(char*), row_str_cmp);
            qsort(drows, dnr, sizeof(char*), row_str_cmp);
        }

        int mismatches = 0;
        for (int r = 0; r < mnr && mismatches < 5; r++) {
            if (strcmp(mrows[r], drows[r]) != 0) {
                /* Try numeric-tolerant comparison */
                int close = 1;
                char *ma = strdup(mrows[r]), *da = strdup(drows[r]);
                char *mt = ma, *dt = da;
                char *ms, *ds;
                while ((ms = strsep(&mt, "|")) && (ds = strsep(&dt, "|"))) {
                    char *me, *de;
                    double mv = strtod(ms, &me), dv = strtod(ds, &de);
                    if (me != ms && de != ds && *me == '\0' && *de == '\0') {
                        double diff = fabs(mv - dv);
                        double mag = fmax(fabs(mv), fabs(dv));
                        if (mag > 1e-9 && diff/mag > 1e-4) { close = 0; break; }
                        else if (mag <= 1e-9 && diff > 1e-9) { close = 0; break; }
                    } else if (strcmp(ms, ds) != 0) {
                        close = 0; break;
                    }
                }
                free(ma); free(da);
                if (!close) {
                    if (mismatches == 0)
                        printf("  FAIL %-22s row %d:\n       mskql: %s\n       duck:  %s\n",
                               name, r, mrows[r], drows[r]);
                    mismatches++;
                }
            }
        }
        if (mismatches > 0) ok = 0;

        for (int r = 0; r < mnr; r++) free(mrows[r]);
        for (int r = 0; r < dnr; r++) free(drows[r]);
        free(mrows); free(drows);
    }

    if (ok) { printf("  OK   %-22s rows=%d cols=%d\n", name, mnr, mnc); g_pass++; }
    else g_fail++;

    mskql_result_free(mr);
    duckdb_destroy_result(&dr);
}

int main(void)
{
    mskql_db *mdb = mskql_open("verify");
    duckdb_database ddb; duckdb_connection dc;
    duckdb_open(NULL, &ddb); duckdb_connect(ddb, &dc);

    printf("=== Correctness verification: mskql vs DuckDB ===\n\n");

    /* --- Setup tables (same as benchmark) --- */
    /* t (10K rows) — full_scan, where, aggregate, order_by */
    msk_exec(mdb, "CREATE TABLE t (id INT, name TEXT, score FLOAT)");
    duck_exec(dc, "CREATE TABLE t (id INT, name TEXT, score FLOAT)");
    { char sql[256];
      for (int i = 0; i < 10000; i++) {
        snprintf(sql, sizeof(sql), "INSERT INTO t VALUES (%d, 'user_%d', %d.%d)",
                 i, i, i % 100, i % 10);
        msk_exec(mdb, sql); duck_exec(dc, sql);
    }}

    /* t_left, t_right — join */
    msk_exec(mdb, "CREATE TABLE t_left (id INT, grp INT, val INT)");
    msk_exec(mdb, "CREATE TABLE t_right (id INT, label TEXT)");
    duck_exec(dc, "CREATE TABLE t_left (id INT, grp INT, val INT)");
    duck_exec(dc, "CREATE TABLE t_right (id INT, label TEXT)");
    { char sql[256];
      for (int i = 0; i < 10000; i++) {
        snprintf(sql, sizeof(sql), "INSERT INTO t_left VALUES (%d, %d, %d)",
                 i, i % 100, (i * 7) % 10000);
        msk_exec(mdb, sql); duck_exec(dc, sql);
      }
      for (int i = 0; i < 1000; i++) {
        snprintf(sql, sizeof(sql), "INSERT INTO t_right VALUES (%d, 'label_%d')", i, i);
        msk_exec(mdb, sql); duck_exec(dc, sql);
    }}

    /* t_gb — group_by */
    msk_exec(mdb, "CREATE TABLE t_gb (id INT, grp INT, val INT)");
    duck_exec(dc, "CREATE TABLE t_gb (id INT, grp INT, val INT)");
    { char sql[256];
      for (int i = 0; i < 50000; i++) {
        snprintf(sql, sizeof(sql), "INSERT INTO t_gb VALUES (%d, %d, %d)",
                 i, i % 100, (i * 7) % 50000);
        msk_exec(mdb, sql); duck_exec(dc, sql);
    }}

    /* t_lj, t_lj_inner — large_join */
    msk_exec(mdb, "CREATE TABLE t_lj (id INT, grp INT, val INT)");
    msk_exec(mdb, "CREATE TABLE t_lj_inner (id INT, label INT)");
    duck_exec(dc, "CREATE TABLE t_lj (id INT, grp INT, val INT)");
    duck_exec(dc, "CREATE TABLE t_lj_inner (id INT, label INT)");
    msk_exec(mdb, "INSERT INTO t_lj SELECT n, n % 100, (n * 7) % 100000 FROM generate_series(1, 100000) AS g(n)");
    msk_exec(mdb, "INSERT INTO t_lj_inner SELECT n, n * 3 FROM generate_series(1, 1000) AS g(n)");
    duck_exec(dc, "INSERT INTO t_lj SELECT generate_series, generate_series % 100, (generate_series * 7) % 100000 FROM generate_series(1, 100000)");
    duck_exec(dc, "INSERT INTO t_lj_inner SELECT generate_series, generate_series * 3 FROM generate_series(1, 1000)");

    /* t_dist — distinct */
    msk_exec(mdb, "CREATE TABLE t_dist (id INT, grp INT, val INT)");
    duck_exec(dc, "CREATE TABLE t_dist (id INT, grp INT, val INT)");
    msk_exec(mdb, "INSERT INTO t_dist SELECT n, n % 100, (n * 7) % 100000 FROM generate_series(1, 100000) AS g(n)");
    duck_exec(dc, "INSERT INTO t_dist SELECT generate_series, generate_series % 100, (generate_series * 7) % 100000 FROM generate_series(1, 100000)");

    /* t_win — window */
    msk_exec(mdb, "CREATE TABLE t_win (id INT, grp INT, val INT)");
    duck_exec(dc, "CREATE TABLE t_win (id INT, grp INT, val INT)");
    msk_exec(mdb, "INSERT INTO t_win SELECT n, n % 100, (n * 7) % 50000 FROM generate_series(1, 50000) AS g(n)");
    duck_exec(dc, "INSERT INTO t_win SELECT generate_series, generate_series % 100, (generate_series * 7) % 50000 FROM generate_series(1, 50000)");

    /* t_cte — cte */
    msk_exec(mdb, "CREATE TABLE t_cte (id INT, grp INT, val INT)");
    duck_exec(dc, "CREATE TABLE t_cte (id INT, grp INT, val INT)");
    msk_exec(mdb, "INSERT INTO t_cte SELECT n, n % 100, (n * 7) % 50000 FROM generate_series(1, 50000) AS g(n)");
    duck_exec(dc, "INSERT INTO t_cte SELECT generate_series, generate_series % 100, (generate_series * 7) % 50000 FROM generate_series(1, 50000)");

    /* t_sf — scalar_functions */
    msk_exec(mdb, "CREATE TABLE t_sf (id INT, name TEXT, val FLOAT)");
    duck_exec(dc, "CREATE TABLE t_sf (id INT, name TEXT, val FLOAT)");
    { char sql[256];
      for (int i = 0; i < 10000; i++) {
        snprintf(sql, sizeof(sql), "INSERT INTO t_sf VALUES (%d, 'name_%d', %d.%d)",
                 i, i, i % 100, i % 10);
        msk_exec(mdb, sql); duck_exec(dc, sql);
    }}

    /* t_topn — top_n */
    msk_exec(mdb, "CREATE TABLE t_topn (id INT, grp INT, val INT)");
    duck_exec(dc, "CREATE TABLE t_topn (id INT, grp INT, val INT)");
    msk_exec(mdb, "INSERT INTO t_topn SELECT n, n % 100, (n * 7) % 100000 FROM generate_series(1, 100000) AS g(n)");
    duck_exec(dc, "INSERT INTO t_topn SELECT generate_series, generate_series % 100, (generate_series * 7) % 100000 FROM generate_series(1, 100000)");

    /* t_ea — expression_agg */
    msk_exec(mdb, "CREATE TABLE t_ea (id INT, quantity INT, price INT)");
    duck_exec(dc, "CREATE TABLE t_ea (id INT, quantity INT, price INT)");
    msk_exec(mdb, "INSERT INTO t_ea SELECT n, 1 + n % 20, 10 + (n * 13) % 990 FROM generate_series(1, 50000) AS g(n)");
    duck_exec(dc, "INSERT INTO t_ea SELECT generate_series, 1 + generate_series % 20, 10 + (generate_series * 13) % 990 FROM generate_series(1, 50000)");

    /* t_ins — insert_bulk (just verify counts match) */
    msk_exec(mdb, "CREATE TABLE t_ins (id INT, name TEXT, score FLOAT)");
    duck_exec(dc, "CREATE TABLE t_ins (id INT, name TEXT, score FLOAT)");
    { char sql[256];
      for (int i = 0; i < 100; i++) {
        snprintf(sql, sizeof(sql), "INSERT INTO t_ins VALUES (%d, 'user_%d', %d.%d)",
                 i, i, i % 100, i % 10);
        msk_exec(mdb, sql); duck_exec(dc, sql);
    }}

    printf("--- Setup complete, verifying queries ---\n\n");

    /* 1. select_full_scan */
    verify("select_full_scan", "SELECT * FROM t", mdb, dc, 0);

    /* 2. select_where */
    verify("select_where", "SELECT * FROM t WHERE score > 50.0", mdb, dc, 0);

    /* 3. aggregate */
    verify("aggregate", "SELECT COUNT(*), SUM(score), AVG(score) FROM t", mdb, dc, 1);

    /* 4. order_by — add id tiebreaker to avoid sort-stability differences */
    verify("order_by", "SELECT * FROM t ORDER BY score DESC, id ASC", mdb, dc, 1);

    /* 5. join */
    verify("join",
        "SELECT t_left.id, t_left.val, t_right.label "
        "FROM t_left JOIN t_right ON t_left.grp = t_right.id", mdb, dc, 0);

    /* 6. group_by */
    verify("group_by",
        "SELECT grp, SUM(val), COUNT(*), AVG(val) FROM t_gb GROUP BY grp",
        mdb, dc, 0);

    /* 7. large_join */
    verify("large_join",
        "SELECT t_lj.id, t_lj.val, t_lj_inner.label "
        "FROM t_lj JOIN t_lj_inner ON t_lj.grp = t_lj_inner.id", mdb, dc, 0);

    /* 8. distinct */
    verify("distinct", "SELECT DISTINCT grp FROM t_dist", mdb, dc, 0);

    /* 9. window */
    verify("window",
        "SELECT id, grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) FROM t_win",
        mdb, dc, 0);

    /* 10. cte */
    verify("cte",
        "WITH grp_totals AS ("
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt "
        "FROM t_cte GROUP BY grp"
        ") SELECT * FROM grp_totals WHERE total > 100000 ORDER BY total DESC",
        mdb, dc, 1);

    /* 11. scalar_functions */
    verify("scalar_functions",
        "SELECT id, UPPER(name), ABS(val), val * 2 + 1 FROM t_sf", mdb, dc, 0);

    /* 12. top_n */
    verify("top_n",
        "SELECT * FROM t_topn ORDER BY val DESC LIMIT 100", mdb, dc, 1);

    /* 13. expression_agg */
    verify("expression_agg",
        "SELECT SUM(quantity * price), AVG(quantity * price), COUNT(*) FROM t_ea",
        mdb, dc, 1);

    /* 14. insert_bulk count */
    verify("insert_bulk",
        "SELECT COUNT(*) FROM t_ins", mdb, dc, 1);

    /* 15. large_sort - just check row count + first/last via aggregate */
    verify("large_sort_count",
        "SELECT COUNT(*) FROM t_topn", mdb, dc, 1);

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);

    mskql_close(mdb);
    duckdb_disconnect(&dc);
    duckdb_close(&ddb);
    return g_fail > 0 ? 1 : 0;
}
