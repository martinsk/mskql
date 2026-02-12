#!/usr/bin/env python3
"""
bench_vs_pg.py — generate SQL workloads and time them against mskql and PostgreSQL.

Mirrors the workloads in bench/bench.c but runs them over the wire via psql
so the comparison with PostgreSQL is apples-to-apples.
"""

import argparse
import os
import subprocess
import sys
import tempfile
import time


# ── helpers ──────────────────────────────────────────────────────────────────

def run_psql(host, port, user, db, sql_file):
    """Run a SQL file through psql. Returns (returncode, stdout)."""
    cmd = [
        "psql", "-h", host, "-p", str(port), "-U", user, "-d", db,
        "-X", "-q", "-A", "-t", "-f", sql_file,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return r.returncode, r.stdout


def run_psql_cmd(host, port, user, db, sql):
    """Run a single SQL command through psql."""
    cmd = [
        "psql", "-h", host, "-p", str(port), "-U", user, "-d", db,
        "-X", "-q", "-A", "-t", "-c", sql,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return r.returncode, r.stdout


def write_sql(path, lines):
    """Write SQL lines to a file."""
    with open(path, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line)
            f.write("\n")


def time_psql(host, port, user, db, sql_file):
    """Time a psql -f invocation, return elapsed milliseconds."""
    t0 = time.monotonic()
    rc, _ = run_psql(host, port, user, db, sql_file)
    elapsed = (time.monotonic() - t0) * 1000.0
    if rc != 0:
        return -1.0
    return elapsed


# ── benchmark definitions ────────────────────────────────────────────────────
# Each benchmark returns (setup_lines, bench_lines).

def bench_insert_bulk():
    N = 10000
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INT, name TEXT, score FLOAT);",
    ]
    bench = []
    for i in range(N):
        bench.append(f"INSERT INTO t VALUES ({i}, 'user_{i}', {i % 100}.{i % 10});")
    return setup, bench


def bench_select_full_scan():
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INT, name TEXT, val INT);",
    ]
    for i in range(5000):
        setup.append(f"INSERT INTO t VALUES ({i}, 'row_{i}', {i * 7});")

    bench = ["SELECT * FROM t;"] * 200
    return setup, bench


def bench_select_where():
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INT, category TEXT, amount INT);",
    ]
    for i in range(5000):
        setup.append(f"INSERT INTO t VALUES ({i}, 'cat_{i % 10}', {i});")

    bench = ["SELECT * FROM t WHERE amount > 2500;"] * 500
    return setup, bench


def bench_aggregate():
    regions = ["north", "south", "east", "west"]
    setup = [
        "DROP TABLE IF EXISTS sales;",
        "CREATE TABLE sales (region TEXT, amount INT);",
    ]
    for i in range(5000):
        setup.append(f"INSERT INTO sales VALUES ('{regions[i % 4]}', {(i * 13) % 1000});")

    bench = ["SELECT region, SUM(amount) FROM sales GROUP BY region;"] * 500
    return setup, bench


def bench_order_by():
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INT, val INT);",
    ]
    for i in range(5000):
        setup.append(f"INSERT INTO t VALUES ({i}, {(i * 31337) % 100000});")

    bench = ["SELECT * FROM t ORDER BY val DESC;"] * 200
    return setup, bench


def bench_join():
    setup = [
        "DROP TABLE IF EXISTS orders;",
        "DROP TABLE IF EXISTS users;",
        "CREATE TABLE users (id INT, name TEXT);",
        "CREATE TABLE orders (id INT, user_id INT, total INT);",
    ]
    for i in range(500):
        setup.append(f"INSERT INTO users VALUES ({i}, 'user_{i}');")
    for i in range(2000):
        setup.append(f"INSERT INTO orders VALUES ({i}, {i % 500}, {(i * 17) % 1000});")

    bench = [
        "SELECT users.name, orders.total FROM users "
        "JOIN orders ON users.id = orders.user_id;"
    ] * 50
    return setup, bench


def bench_update():
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INT, val INT);",
    ]
    for i in range(5000):
        setup.append(f"INSERT INTO t VALUES ({i}, {i});")

    bench = ["UPDATE t SET val = 0 WHERE id < 1000;"] * 200
    return setup, bench


def bench_index_lookup():
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INT, val TEXT);",
    ]
    for i in range(10000):
        setup.append(f"INSERT INTO t VALUES ({i}, 'value_{i}');")
    setup.append("CREATE INDEX idx_id ON t (id);")

    bench = []
    for i in range(2000):
        bench.append(f"SELECT * FROM t WHERE id = {i % 10000};")
    return setup, bench


def bench_delete():
    setup = []
    bench = []
    for _iter in range(50):
        bench.append("DROP TABLE IF EXISTS t;")
        bench.append("CREATE TABLE t (id INT, val INT);")
        for i in range(2000):
            bench.append(f"INSERT INTO t VALUES ({i}, {i});")
        bench.append("DELETE FROM t WHERE id >= 1000;")
    return setup, bench


def bench_transaction():
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INT, val INT);",
    ]
    for i in range(1000):
        setup.append(f"INSERT INTO t VALUES ({i}, {i});")

    bench = []
    for i in range(100):
        bench.append("BEGIN;")
        for j in range(50):
            bench.append(f"INSERT INTO t VALUES ({10000 + i * 50 + j}, {j});")
        bench.append("COMMIT;")
    return setup, bench


def bench_window_functions():
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INT, grp INT, val INT);",
    ]
    for i in range(5000):
        setup.append(f"INSERT INTO t VALUES ({i}, {i % 20}, {(i * 7) % 1000});")

    bench = [
        "SELECT id, grp, val, "
        "ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) FROM t;"
    ] * 20
    return setup, bench


def bench_distinct():
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INT, category TEXT, val INT);",
    ]
    for i in range(5000):
        setup.append(f"INSERT INTO t VALUES ({i}, 'cat_{i % 100}', {i});")

    bench = ["SELECT DISTINCT category FROM t;"] * 500
    return setup, bench


def bench_subquery():
    setup = [
        "DROP TABLE IF EXISTS t2;",
        "DROP TABLE IF EXISTS t1;",
        "CREATE TABLE t1 (id INT, val INT);",
        "CREATE TABLE t2 (id INT, flag INT);",
    ]
    for i in range(5000):
        setup.append(f"INSERT INTO t1 VALUES ({i}, {i * 3});")
        setup.append(f"INSERT INTO t2 VALUES ({i}, {i % 1000});")

    bench = [
        "SELECT * FROM t1 WHERE id IN "
        "(SELECT id FROM t2 WHERE flag > 500);"
    ] * 50
    return setup, bench


def bench_cte():
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INT, val INT, category TEXT);",
    ]
    for i in range(5000):
        setup.append(f"INSERT INTO t VALUES ({i}, {i * 3}, 'cat_{i % 10}');")

    bench = [
        "WITH filtered AS (SELECT * FROM t WHERE val > 500) "
        "SELECT * FROM filtered WHERE category = 'cat_3';"
    ] * 200
    return setup, bench


def bench_generate_series():
    setup = []
    bench = ["SELECT * FROM generate_series(1, 10000);"] * 200
    return setup, bench


def bench_scalar_functions():
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INT, name TEXT, score FLOAT);",
    ]
    for i in range(5000):
        setup.append(f"INSERT INTO t VALUES ({i}, 'user_{i}', {i % 100}.{i % 10});")

    bench = [
        "SELECT UPPER(name), LENGTH(name), ABS(score), "
        "ROUND(score::numeric, 2) FROM t;"
    ] * 200
    return setup, bench


def bench_expression_agg():
    cats = ["electronics", "clothing", "food", "books", "toys"]
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INT, category TEXT, price INT, quantity INT);",
    ]
    for i in range(5000):
        setup.append(
            f"INSERT INTO t VALUES ({i}, '{cats[i % 5]}', "
            f"{(i * 17) % 500}, {(i * 7) % 100});"
        )

    bench = [
        "SELECT category, SUM(price * quantity) FROM t GROUP BY category;"
    ] * 500
    return setup, bench


def bench_multi_sort():
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INT, a INT, b INT);",
    ]
    for i in range(5000):
        setup.append(
            f"INSERT INTO t VALUES ({i}, {(i * 31337) % 1000}, {(i * 7919) % 1000});"
        )

    bench = ["SELECT * FROM t ORDER BY a DESC, b ASC;"] * 200
    return setup, bench


def bench_set_ops():
    setup = [
        "DROP TABLE IF EXISTS t2;",
        "DROP TABLE IF EXISTS t1;",
        "CREATE TABLE t1 (id INT, val TEXT);",
        "CREATE TABLE t2 (id INT, val TEXT);",
    ]
    for i in range(2000):
        setup.append(f"INSERT INTO t1 VALUES ({i}, 'a_{i}');")
        setup.append(f"INSERT INTO t2 VALUES ({i + 1000}, 'b_{i + 1000}');")

    bench = [
        "SELECT id, val FROM t1 UNION SELECT id, val FROM t2;"
    ] * 50
    return setup, bench


BENCHMARKS = [
    ("insert_bulk",      bench_insert_bulk),
    ("select_full_scan", bench_select_full_scan),
    ("select_where",     bench_select_where),
    ("aggregate",        bench_aggregate),
    ("order_by",         bench_order_by),
    ("join",             bench_join),
    ("update",           bench_update),
    ("index_lookup",     bench_index_lookup),
    ("delete",           bench_delete),
    ("transaction",      bench_transaction),
    ("window_functions", bench_window_functions),
    ("distinct",         bench_distinct),
    ("subquery",         bench_subquery),
    ("cte",              bench_cte),
    ("generate_series",  bench_generate_series),
    ("scalar_functions", bench_scalar_functions),
    ("expression_agg",   bench_expression_agg),
    ("multi_sort",       bench_multi_sort),
    ("set_ops",          bench_set_ops),
]


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="mskql vs PostgreSQL benchmark")
    p.add_argument("--mskql-port", type=int, default=5433)
    p.add_argument("--pg-port", type=int, default=5432)
    p.add_argument("--pg-db", default="mskql_bench")
    p.add_argument("--pg-user", default=os.environ.get("USER", "postgres"))
    p.add_argument("--filter", default=None, help="run only this benchmark")
    p.add_argument("--markdown", action="store_true", help="output markdown table")
    args = p.parse_args()

    host = "127.0.0.1"

    print("=" * 78)
    print("  mskql vs PostgreSQL benchmark")
    print(f"  mskql  : {host}:{args.mskql_port}")
    print(f"  postgres: {host}:{args.pg_port} (db: {args.pg_db})")
    print("=" * 78)
    print()

    # ── verify connectivity ──
    rc, _ = run_psql_cmd(host, args.mskql_port, args.pg_user, "mskql", "SELECT 1")
    if rc != 0:
        print(f"ERROR: Cannot connect to mskql on port {args.mskql_port}")
        print("       Start it with: ./build/mskql")
        sys.exit(1)

    rc, _ = run_psql_cmd(host, args.pg_port, args.pg_user, "postgres", "SELECT 1")
    if rc != 0:
        print(f"ERROR: Cannot connect to PostgreSQL on port {args.pg_port}")
        sys.exit(1)

    # ── ensure pg benchmark database exists ──
    rc, out = run_psql_cmd(
        host, args.pg_port, args.pg_user, "postgres",
        f"SELECT 1 FROM pg_database WHERE datname = '{args.pg_db}'"
    )
    if "1" not in out:
        print(f"Creating PostgreSQL database '{args.pg_db}'...")
        run_psql_cmd(host, args.pg_port, args.pg_user, "postgres",
                     f"CREATE DATABASE {args.pg_db}")

    # ── header ──
    print(f"  {'BENCHMARK':<25s} {'mskql (ms)':>12s} {'pg (ms)':>12s} {'ratio':>10s}")
    print(f"  {'-' * 63}")

    results = []

    for name, fn in BENCHMARKS:
        if args.filter and args.filter != name:
            continue

        # Generate SQL
        setup_lines, bench_lines = fn()

        # Write to temp files
        setup_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".sql", prefix=f"bench_setup_{name}_", delete=False
        )
        bench_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".sql", prefix=f"bench_run_{name}_", delete=False
        )
        setup_file.write("\n".join(setup_lines) + "\n")
        setup_file.close()
        bench_file.write("\n".join(bench_lines) + "\n")
        bench_file.close()

        print(f"  {name:<25s} ", end="", flush=True)

        # ── mskql ──
        run_psql(host, args.mskql_port, args.pg_user, "mskql", setup_file.name)
        mskql_ms = time_psql(host, args.mskql_port, args.pg_user, "mskql", bench_file.name)

        # ── postgres ──
        run_psql(host, args.pg_port, args.pg_user, args.pg_db, setup_file.name)
        pg_ms = time_psql(host, args.pg_port, args.pg_user, args.pg_db, bench_file.name)

        # Cleanup
        os.unlink(setup_file.name)
        os.unlink(bench_file.name)

        # Ratio
        if pg_ms > 0 and mskql_ms >= 0:
            ratio = mskql_ms / pg_ms
            ratio_str = f"{ratio:.2f}x"
        else:
            ratio_str = "n/a"

        if mskql_ms < 0:
            mskql_str = "ERROR"
        else:
            mskql_str = f"{mskql_ms:.1f}"

        if pg_ms < 0:
            pg_str = "ERROR"
        else:
            pg_str = f"{pg_ms:.1f}"

        print(f"{mskql_str:>12s} {pg_str:>12s} {ratio_str:>10s}")

        results.append((name, mskql_ms, pg_ms))

    print(f"  {'-' * 63}")
    print()
    print("  ratio = mskql / pg  (< 1.0 means mskql is faster)")
    print()
    print(f"done. ({len(results)} benchmarks)")

    if args.markdown and results:
        print()
        print("| Benchmark | mskql (ms) | pg (ms) | ratio |")
        print("|-----------|-----------|---------|-------|")
        for name, m_ms, p_ms in results:
            m_str = f"{m_ms:.1f}" if m_ms >= 0 else "ERROR"
            p_str = f"{p_ms:.1f}" if p_ms >= 0 else "ERROR"
            if p_ms > 0 and m_ms >= 0:
                r_str = f"{m_ms / p_ms:.2f}x"
            else:
                r_str = "n/a"
            print(f"| {name} | {m_str} | {p_str} | {r_str} |")


if __name__ == "__main__":
    main()
