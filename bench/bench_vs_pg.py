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


# ── complex benchmarks (shared bootstrap dataset) ───────────────────────────

def bootstrap_setup():
    """Generate setup SQL for the shared star-schema bootstrap dataset.

    Dimension tables (customers, products) use TEXT and individual INSERTs.
    Fact tables (orders, events, metrics, sales) are all-INT and bulk-loaded
    via INSERT INTO ... SELECT ... FROM generate_series() for speed.
    """
    regions = ["north", "south", "east", "west"]
    tiers = ["basic", "premium", "enterprise"]
    categories = ["electronics", "clothing", "food", "books", "toys"]

    setup = [
        "DROP TABLE IF EXISTS sales;",
        "DROP TABLE IF EXISTS metrics;",
        "DROP TABLE IF EXISTS events;",
        "DROP TABLE IF EXISTS orders;",
        "DROP TABLE IF EXISTS products;",
        "DROP TABLE IF EXISTS customers;",
        # dimension tables (TEXT columns, small)
        "CREATE TABLE customers (id INT, name TEXT, region TEXT, tier TEXT);",
        "CREATE TABLE products (id INT, name TEXT, category TEXT, price INT);",
        # fact tables (all-INT, bulk-loaded)
        "CREATE TABLE orders (id INT, customer_id INT, product_id INT, quantity INT, amount INT);",
        "CREATE TABLE events (id INT, user_id INT, event_type INT, amount INT, score INT);",
        "CREATE TABLE metrics (id INT, sensor_id INT, v1 INT, v2 INT, v3 INT, v4 INT, v5 INT);",
        "CREATE TABLE sales (id INT, rep_id INT, region_id INT, amount INT);",
    ]

    # customers: 2000 rows (individual INSERTs — small table)
    for i in range(2000):
        setup.append(
            f"INSERT INTO customers VALUES ({i}, 'cust_{i}', "
            f"'{regions[i % 4]}', '{tiers[i % 3]}');"
        )

    # products: 500 rows (individual INSERTs — small table)
    for i in range(500):
        setup.append(
            f"INSERT INTO products VALUES ({i}, 'prod_{i}', "
            f"'{categories[i % 5]}', {10 + (i * 17) % 490});"
        )

    # fact tables: 50K rows each via bulk INSERT...SELECT from generate_series
    setup.append(
        "INSERT INTO orders SELECT n, n % 2000, n % 500, "
        "1 + (n * 7) % 20, 10 + (n * 13) % 990 "
        "FROM generate_series(0, 49999) AS g(n);"
    )
    setup.append(
        "INSERT INTO events SELECT n, n % 2000, n % 5, "
        "(n * 11) % 1000, (n * 31337) % 10000 "
        "FROM generate_series(0, 49999) AS g(n);"
    )
    setup.append(
        "INSERT INTO metrics SELECT n, n % 100, "
        "(n * 7) % 1000, (n * 13) % 1000, (n * 19) % 1000, "
        "(n * 23) % 1000, (n * 29) % 1000 "
        "FROM generate_series(0, 49999) AS g(n);"
    )
    setup.append(
        "INSERT INTO sales SELECT n, n % 200, n % 4, "
        "10 + (n * 17) % 990 "
        "FROM generate_series(0, 49999) AS g(n);"
    )

    return setup


def bench_multi_join():
    setup = bootstrap_setup()
    bench = [
        "SELECT c.region, p.category, SUM(o.quantity * p.price) "
        "FROM orders o "
        "JOIN customers c ON o.customer_id = c.id "
        "JOIN products p ON o.product_id = p.id "
        "GROUP BY c.region, p.category "
        "ORDER BY c.region, p.category;"
    ] * 5
    return setup, bench


def bench_analytical_cte():
    setup = bootstrap_setup()
    bench = [
        "WITH user_totals AS ("
        "SELECT user_id, SUM(amount) AS total "
        "FROM events WHERE event_type = 0 "
        "GROUP BY user_id "
        "HAVING SUM(amount) > 500"
        ") SELECT * FROM user_totals ORDER BY total DESC LIMIT 100;"
    ] * 20
    return setup, bench


def bench_wide_agg():
    setup = bootstrap_setup()
    bench = [
        "SELECT sensor_id, COUNT(*), AVG(v1), SUM(v2), MIN(v3), MAX(v4), AVG(v5) "
        "FROM metrics GROUP BY sensor_id ORDER BY sensor_id;"
    ] * 20
    return setup, bench


def bench_large_sort():
    setup = bootstrap_setup()
    bench = [
        "SELECT * FROM events ORDER BY score DESC, id ASC;"
    ] * 10
    return setup, bench


def bench_subquery_complex():
    setup = bootstrap_setup()
    bench = [
        "SELECT * FROM events "
        "WHERE user_id IN (SELECT id FROM customers WHERE tier = 'premium') "
        "AND amount > 100 "
        "ORDER BY amount DESC LIMIT 500;"
    ] * 20
    return setup, bench


def bench_window_rank():
    setup = bootstrap_setup()
    bench = [
        "SELECT rep_id, region_id, amount, "
        "RANK() OVER (PARTITION BY region_id ORDER BY amount DESC) "
        "FROM sales;"
    ] * 5
    return setup, bench


def bench_mixed_analytical():
    setup = bootstrap_setup()
    bench = [
        "WITH order_summary AS ("
        "SELECT o.customer_id, SUM(o.quantity * p.price) AS total "
        "FROM orders o JOIN products p ON o.product_id = p.id "
        "GROUP BY o.customer_id"
        ") SELECT c.region, COUNT(*) AS num_customers, SUM(os.total) AS revenue "
        "FROM order_summary os JOIN customers c ON os.customer_id = c.id "
        "GROUP BY c.region ORDER BY revenue DESC;"
    ] * 5
    return setup, bench


BENCHMARKS = [
    ("insert_bulk",        bench_insert_bulk),
    ("select_full_scan",   bench_select_full_scan),
    ("select_where",       bench_select_where),
    ("aggregate",          bench_aggregate),
    ("order_by",           bench_order_by),
    ("join",               bench_join),
    ("update",             bench_update),
    ("index_lookup",       bench_index_lookup),
    ("delete",             bench_delete),
    ("transaction",        bench_transaction),
    ("window_functions",   bench_window_functions),
    ("distinct",           bench_distinct),
    ("subquery",           bench_subquery),
    ("cte",                bench_cte),
    ("generate_series",    bench_generate_series),
    ("scalar_functions",   bench_scalar_functions),
    ("expression_agg",     bench_expression_agg),
    ("multi_sort",         bench_multi_sort),
    ("set_ops",            bench_set_ops),
    ("multi_join",         bench_multi_join),
    ("analytical_cte",     bench_analytical_cte),
    ("wide_agg",           bench_wide_agg),
    ("large_sort",         bench_large_sort),
    ("subquery_complex",   bench_subquery_complex),
    ("window_rank",        bench_window_rank),
    ("mixed_analytical",   bench_mixed_analytical),
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
