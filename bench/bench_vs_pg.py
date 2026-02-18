#!/usr/bin/env python3
"""
bench_vs_pg.py — generate SQL workloads and time them against mskql and PostgreSQL.

Mirrors the workloads in bench/bench.c but runs them over the wire via psql
so the comparison with PostgreSQL is apples-to-apples.
"""

import argparse
import os
import re
import shutil
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


def run_duckdb(duckdb_bin, db_path, sql_file):
    """Run a SQL file through the DuckDB CLI. Returns (returncode, stdout)."""
    with open(sql_file, "r", encoding="utf-8") as f:
        r = subprocess.run(
            [duckdb_bin, db_path],
            stdin=f, capture_output=True, text=True, check=False,
        )
    return r.returncode, r.stdout


def time_duckdb(duckdb_bin, db_path, sql_file):
    """Time a DuckDB CLI invocation, return elapsed milliseconds."""
    t0 = time.monotonic()
    rc, _ = run_duckdb(duckdb_bin, db_path, sql_file)
    elapsed = (time.monotonic() - t0) * 1000.0
    if rc != 0:
        return -1.0
    return elapsed


def duckdb_fixup(lines):
    """Apply DuckDB SQL dialect fixups to a list of SQL lines."""
    return [re.sub(r"::numeric", "::decimal", line, flags=re.IGNORECASE) for line in lines]


# ── no-cache variant helper ──────────────────────────────────────────────────
# Benchmarks that are inherently write-heavy or already vary parameters per query.
CACHE_RESISTANT = {"insert_bulk", "update", "delete", "transaction", "index_lookup", "composite_index_lookup"}


def nocache_lines(bench_lines):
    """Append a unique SQL comment to each line to bust the result cache."""
    out = []
    for i, line in enumerate(bench_lines):
        stripped = line.rstrip()
        if stripped.endswith(";"):
            out.append(f"{stripped[:-1]} /* nc{i} */;")
        else:
            out.append(f"{stripped} /* nc{i} */")
    return out


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


def bench_composite_index_lookup():
    setup = [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (a INT, b INT, val TEXT);",
    ]
    for i in range(10000):
        setup.append(f"INSERT INTO t VALUES ({i % 100}, {i // 100}, 'value_{i}');")
    setup.append("CREATE INDEX idx_ab ON t (a, b);")

    bench = []
    for i in range(2000):
        a = i % 100
        b = (i * 7) % 100
        bench.append(f"SELECT * FROM t WHERE a = {a} AND b = {b};")
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


# ── worst-case benchmarks (scenarios where PostgreSQL is expected to win) ─────

def bench_wc_large_dataset():
    setup = [
        "DROP TABLE IF EXISTS big;",
        "CREATE TABLE big (id INT, a INT, b INT, c TEXT, d INT);",
        "INSERT INTO big SELECT n, n % 1000, (n * 31337) % 100000, "
        "'row_' || CAST(n % 5000 AS TEXT), (n * 7) % 10000 "
        "FROM generate_series(0, 199999) AS g(n);",
    ]
    bench = [
        "SELECT a, SUM(b), AVG(d) FROM big GROUP BY a ORDER BY a;",
    ] * 5
    return setup, bench


def bench_wc_join_reorder():
    setup = bootstrap_setup()
    bench = [
        "SELECT o.id, o.amount, c.name, c.region "
        "FROM orders o "
        "JOIN events e ON o.customer_id = e.user_id "
        "JOIN customers c ON o.customer_id = c.id "
        "WHERE c.tier = 'premium' AND e.event_type = 0 "
        "ORDER BY o.amount DESC LIMIT 100;",
    ] * 5
    return setup, bench


def bench_wc_correlated_subquery():
    setup = [
        "DROP TABLE IF EXISTS csq_detail;",
        "DROP TABLE IF EXISTS csq_master;",
        "CREATE TABLE csq_master (id INT, val INT);",
        "CREATE TABLE csq_detail (id INT, ref_id INT, score INT);",
        "INSERT INTO csq_master SELECT n, n * 3 "
        "FROM generate_series(0, 1999) AS g(n);",
        "INSERT INTO csq_detail SELECT n, n % 2000, (n * 17) % 1000 "
        "FROM generate_series(0, 9999) AS g(n);",
    ]
    bench = [
        "SELECT csq_master.id, csq_master.val, "
        "(SELECT MAX(csq_detail.score) FROM csq_detail "
        "WHERE csq_detail.ref_id = csq_master.id) AS max_score "
        "FROM csq_master WHERE csq_master.val > 3000;",
    ] * 10
    return setup, bench


def bench_wc_multi_index():
    setup = [
        "DROP TABLE IF EXISTS midx;",
        "CREATE TABLE midx (id INT, status INT, category INT, "
        "region INT, score INT, name TEXT);",
        "INSERT INTO midx SELECT n, n % 5, n % 50, n % 4, "
        "(n * 31337) % 10000, 'user_' || CAST(n AS TEXT) "
        "FROM generate_series(0, 99999) AS g(n);",
        "CREATE INDEX idx_midx_status ON midx (status);",
        "CREATE INDEX idx_midx_category ON midx (category);",
    ]
    bench = [
        "SELECT * FROM midx WHERE status = 2 AND category = 17 "
        "AND score > 5000 ORDER BY score DESC LIMIT 50;",
    ] * 50
    return setup, bench


def bench_wc_string_heavy():
    setup = [
        "DROP TABLE IF EXISTS strtbl;",
        "CREATE TABLE strtbl (id INT, first_name TEXT, last_name TEXT, email TEXT);",
        "INSERT INTO strtbl SELECT n, "
        "'FirstName_' || CAST(n AS TEXT), "
        "'LastName_' || CAST(n AS TEXT), "
        "CONCAT('user_', CAST(n AS TEXT), '@example.com') "
        "FROM generate_series(0, 9999) AS g(n);",
    ]
    bench = [
        "SELECT id, "
        "CONCAT(UPPER(first_name), ' ', UPPER(last_name)) AS full_name, "
        "LENGTH(email), "
        "REPLACE(email, 'example.com', 'test.org') "
        "FROM strtbl WHERE first_name LIKE 'FirstName_1%' "
        "ORDER BY id;",
    ] * 20
    return setup, bench


def bench_wc_nested_cte():
    setup = bootstrap_setup()
    bench = [
        "WITH "
        "active_customers AS ("
        "SELECT id, name, region FROM customers WHERE tier != 'basic'"
        "), "
        "customer_orders AS ("
        "SELECT o.customer_id, COUNT(*) AS order_count, SUM(o.amount) AS total "
        "FROM orders o "
        "JOIN active_customers ac ON o.customer_id = ac.id "
        "GROUP BY o.customer_id"
        "), "
        "ranked AS ("
        "SELECT co.customer_id, co.order_count, co.total, "
        "ac.name, ac.region "
        "FROM customer_orders co "
        "JOIN active_customers ac ON co.customer_id = ac.id "
        "WHERE co.total > 500"
        ") "
        "SELECT region, COUNT(*) AS n, SUM(total) AS revenue, "
        "AVG(order_count) AS avg_orders "
        "FROM ranked GROUP BY region ORDER BY revenue DESC;",
    ] * 5
    return setup, bench


def bench_wc_wide_output():
    setup = [
        "DROP TABLE IF EXISTS wide;",
        "CREATE TABLE wide (c0 INT, c1 INT, c2 INT, c3 INT, c4 INT, "
        "c5 INT, c6 INT, c7 INT, c8 INT, c9 INT);",
        "INSERT INTO wide SELECT n, (n*3)%10000, (n*7)%10000, "
        "(n*11)%10000, (n*13)%10000, (n*17)%10000, "
        "(n*19)%10000, (n*23)%10000, (n*29)%10000, (n*31)%10000 "
        "FROM generate_series(0, 49999) AS g(n);",
    ]
    bench = [
        "SELECT * FROM wide ORDER BY c1 DESC;",
    ] * 5
    return setup, bench


# ── Parquet benchmarks (mskql foreign tables vs DuckDB native parquet) ────────
# These return a dict: {"mskql": (setup, bench), "duck": (setup, bench), "pg": None}
# pg=None means skip PostgreSQL (show "-").

PARQUET_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "parquet_fixtures")


def _pq(name):
    return os.path.join(PARQUET_DIR, name)


def _mskql_foreign(table_name, parquet_file):
    return f"CREATE FOREIGN TABLE {table_name} OPTIONS (FILENAME '{_pq(parquet_file)}');"


def _duck_from_pq(table_name, parquet_file):
    return f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{_pq(parquet_file)}');"


def bench_pq_full_scan():
    """Full scan of 50K-row Parquet file."""
    mskql_setup = [_mskql_foreign("orders", "orders.parquet")]
    duck_setup = [_duck_from_pq("orders", "orders.parquet")]
    bench = ["SELECT * FROM orders;"] * 20
    return {"mskql": (mskql_setup, bench), "duck": (duck_setup, bench), "pg": None}


def bench_pq_where():
    """Filtered scan on 50K-row Parquet file."""
    mskql_setup = [_mskql_foreign("events", "events.parquet")]
    duck_setup = [_duck_from_pq("events", "events.parquet")]
    bench = ["SELECT * FROM events WHERE amount > 500 AND score > 5000;"] * 50
    return {"mskql": (mskql_setup, bench), "duck": (duck_setup, bench), "pg": None}


def bench_pq_aggregate():
    """GROUP BY aggregate on 50K-row Parquet file."""
    mskql_setup = [_mskql_foreign("events", "events.parquet")]
    duck_setup = [_duck_from_pq("events", "events.parquet")]
    bench = [
        "SELECT event_type, COUNT(*), SUM(amount), AVG(score) "
        "FROM events GROUP BY event_type;"
    ] * 100
    return {"mskql": (mskql_setup, bench), "duck": (duck_setup, bench), "pg": None}


def bench_pq_order_by():
    """Sort 50K-row Parquet file."""
    mskql_setup = [_mskql_foreign("events", "events.parquet")]
    duck_setup = [_duck_from_pq("events", "events.parquet")]
    bench = ["SELECT * FROM events ORDER BY score DESC;"] * 10
    return {"mskql": (mskql_setup, bench), "duck": (duck_setup, bench), "pg": None}


def bench_pq_wide_agg():
    """Wide aggregate on 50K-row 7-column Parquet file."""
    mskql_setup = [_mskql_foreign("metrics", "metrics.parquet")]
    duck_setup = [_duck_from_pq("metrics", "metrics.parquet")]
    bench = [
        "SELECT sensor_id, COUNT(*), AVG(v1), SUM(v2), MIN(v3), MAX(v4), AVG(v5) "
        "FROM metrics GROUP BY sensor_id ORDER BY sensor_id;"
    ] * 20
    return {"mskql": (mskql_setup, bench), "duck": (duck_setup, bench), "pg": None}


def bench_pq_join_two():
    """Join two Parquet files: orders (50K) x customers (2K)."""
    mskql_setup = [
        _mskql_foreign("pq_orders", "orders.parquet"),
        _mskql_foreign("pq_customers", "customers.parquet"),
    ]
    duck_setup = [
        _duck_from_pq("pq_orders", "orders.parquet"),
        _duck_from_pq("pq_customers", "customers.parquet"),
    ]
    bench = [
        "SELECT pq_customers.region, COUNT(*), SUM(pq_orders.amount) "
        "FROM pq_orders "
        "JOIN pq_customers ON pq_orders.customer_id = pq_customers.id "
        "GROUP BY pq_customers.region ORDER BY pq_customers.region;"
    ] * 10
    return {"mskql": (mskql_setup, bench), "duck": (duck_setup, bench), "pg": None}


def bench_pq_join_three():
    """Three-way join: orders x customers x products (all Parquet)."""
    mskql_setup = [
        _mskql_foreign("pq_orders", "orders.parquet"),
        _mskql_foreign("pq_customers", "customers.parquet"),
        _mskql_foreign("pq_products", "products.parquet"),
    ]
    duck_setup = [
        _duck_from_pq("pq_orders", "orders.parquet"),
        _duck_from_pq("pq_customers", "customers.parquet"),
        _duck_from_pq("pq_products", "products.parquet"),
    ]
    bench = [
        "SELECT pq_customers.region, pq_products.category, "
        "SUM(pq_orders.quantity * pq_products.price) "
        "FROM pq_orders "
        "JOIN pq_customers ON pq_orders.customer_id = pq_customers.id "
        "JOIN pq_products ON pq_orders.product_id = pq_products.id "
        "GROUP BY pq_customers.region, pq_products.category "
        "ORDER BY pq_customers.region, pq_products.category;"
    ] * 5
    return {"mskql": (mskql_setup, bench), "duck": (duck_setup, bench), "pg": None}


def bench_pq_mixed_join():
    """Join Parquet file with a regular in-memory table."""
    common_setup = [
        "DROP TABLE IF EXISTS local_regions;",
        "CREATE TABLE local_regions (id INT, region_name TEXT, tax_rate INT);",
        "INSERT INTO local_regions VALUES (0, 'north', 10);",
        "INSERT INTO local_regions VALUES (1, 'south', 8);",
        "INSERT INTO local_regions VALUES (2, 'east', 12);",
        "INSERT INTO local_regions VALUES (3, 'west', 9);",
    ]
    mskql_setup = common_setup + [_mskql_foreign("pq_sales", "sales.parquet")]
    duck_setup = common_setup + [_duck_from_pq("pq_sales", "sales.parquet")]
    bench = [
        "SELECT lr.region_name, SUM(pq_sales.amount), "
        "SUM(pq_sales.amount * lr.tax_rate / 100) "
        "FROM pq_sales "
        "JOIN local_regions lr ON pq_sales.region_id = lr.id "
        "GROUP BY lr.region_name ORDER BY lr.region_name;"
    ] * 20
    return {"mskql": (mskql_setup, bench), "duck": (duck_setup, bench), "pg": None}


def bench_pq_subquery():
    """Subquery: filter Parquet events by customers from another Parquet file."""
    mskql_setup = [
        _mskql_foreign("pq_events", "events.parquet"),
        _mskql_foreign("pq_customers", "customers.parquet"),
    ]
    duck_setup = [
        _duck_from_pq("pq_events", "events.parquet"),
        _duck_from_pq("pq_customers", "customers.parquet"),
    ]
    bench = [
        "SELECT * FROM pq_events "
        "WHERE user_id IN (SELECT id FROM pq_customers WHERE tier = 'premium') "
        "AND amount > 500 "
        "ORDER BY score DESC LIMIT 100;"
    ] * 20
    return {"mskql": (mskql_setup, bench), "duck": (duck_setup, bench), "pg": None}


def bench_pq_lineitem_agg():
    """Aggregate on 200K-row lineitem Parquet file."""
    mskql_setup = [
        _mskql_foreign("pq_lineitem", "lineitem.parquet"),
        _mskql_foreign("pq_products", "products.parquet"),
    ]
    duck_setup = [
        _duck_from_pq("pq_lineitem", "lineitem.parquet"),
        _duck_from_pq("pq_products", "products.parquet"),
    ]
    bench = [
        "SELECT pq_products.category, "
        "COUNT(*), SUM(pq_lineitem.quantity * pq_lineitem.unit_price) "
        "FROM pq_lineitem "
        "JOIN pq_products ON pq_lineitem.product_id = pq_products.id "
        "GROUP BY pq_products.category ORDER BY pq_products.category;"
    ] * 5
    return {"mskql": (mskql_setup, bench), "duck": (duck_setup, bench), "pg": None}


def bench_pq_analytical():
    """CTE + join + aggregate across Parquet files."""
    mskql_setup = [
        _mskql_foreign("pq_orders", "orders.parquet"),
        _mskql_foreign("pq_customers", "customers.parquet"),
        _mskql_foreign("pq_products", "products.parquet"),
    ]
    duck_setup = [
        _duck_from_pq("pq_orders", "orders.parquet"),
        _duck_from_pq("pq_customers", "customers.parquet"),
        _duck_from_pq("pq_products", "products.parquet"),
    ]
    bench = [
        "WITH order_totals AS ("
        "SELECT pq_orders.customer_id, "
        "SUM(pq_orders.quantity * pq_products.price) AS total "
        "FROM pq_orders "
        "JOIN pq_products ON pq_orders.product_id = pq_products.id "
        "GROUP BY pq_orders.customer_id"
        ") SELECT pq_customers.region, COUNT(*), SUM(ot.total) "
        "FROM order_totals ot "
        "JOIN pq_customers ON ot.customer_id = pq_customers.id "
        "GROUP BY pq_customers.region ORDER BY pq_customers.region;"
    ] * 5
    return {"mskql": (mskql_setup, bench), "duck": (duck_setup, bench), "pg": None}


_BASE_BENCHMARKS = [
    ("insert_bulk",        bench_insert_bulk),
    ("select_full_scan",   bench_select_full_scan),
    ("select_where",       bench_select_where),
    ("aggregate",          bench_aggregate),
    ("order_by",           bench_order_by),
    ("join",               bench_join),
    ("update",             bench_update),
    ("index_lookup",       bench_index_lookup),
    ("composite_index_lookup", bench_composite_index_lookup),
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
    ("wc_large_dataset",   bench_wc_large_dataset),
    ("wc_join_reorder",    bench_wc_join_reorder),
    ("wc_correlated_subq", bench_wc_correlated_subquery),
    ("wc_multi_index",     bench_wc_multi_index),
    ("wc_string_heavy",    bench_wc_string_heavy),
    ("wc_nested_cte",      bench_wc_nested_cte),
    ("wc_wide_output",     bench_wc_wide_output),
    # ── Parquet benchmarks (mskql vs DuckDB, no PostgreSQL) ──
    ("pq_full_scan",       bench_pq_full_scan),
    ("pq_where",           bench_pq_where),
    ("pq_aggregate",       bench_pq_aggregate),
    ("pq_order_by",        bench_pq_order_by),
    ("pq_wide_agg",        bench_pq_wide_agg),
    ("pq_join_two",        bench_pq_join_two),
    ("pq_join_three",      bench_pq_join_three),
    ("pq_mixed_join",      bench_pq_mixed_join),
    ("pq_subquery",        bench_pq_subquery),
    ("pq_lineitem_agg",    bench_pq_lineitem_agg),
    ("pq_analytical",      bench_pq_analytical),
]


def _make_nc_wrapper(fn):
    """Create a no-cache wrapper for a benchmark function."""
    def wrapper():
        result = fn()
        if isinstance(result, dict):
            # Parquet-style: {"mskql": (setup, bench), "duck": (setup, bench), ...}
            out = {}
            for key, spec in result.items():
                if spec is None:
                    out[key] = None
                else:
                    out[key] = (spec[0], nocache_lines(spec[1]))
            return out
        else:
            setup, bench = result
            return setup, nocache_lines(bench)
    return wrapper


# Build final list: each cache-susceptible benchmark gets a _nc variant right after it.
BENCHMARKS = []
for _name, _fn in _BASE_BENCHMARKS:
    BENCHMARKS.append((_name, _fn))
    if _name not in CACHE_RESISTANT:
        BENCHMARKS.append((_name + "_nc", _make_nc_wrapper(_fn)))


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="mskql vs PostgreSQL benchmark")
    p.add_argument("--mskql-port", type=int, default=5433)
    p.add_argument("--pg-port", type=int, default=5432)
    p.add_argument("--pg-db", default="mskql_bench")
    p.add_argument("--pg-user", default=os.environ.get("USER", "postgres"))
    p.add_argument("--duckdb-bin", default="duckdb", help="path to DuckDB CLI")
    p.add_argument("--filter", default=None, help="run only this benchmark")
    p.add_argument("--markdown", action="store_true", help="output markdown table")
    args = p.parse_args()

    host = "127.0.0.1"

    duckdb_bin = shutil.which(args.duckdb_bin)
    if not duckdb_bin:
        print(f"ERROR: DuckDB CLI not found (tried '{args.duckdb_bin}')")
        print("       Install DuckDB or pass --duckdb-bin /path/to/duckdb")
        sys.exit(1)

    print("=" * 90)
    print("  mskql vs PostgreSQL vs DuckDB benchmark")
    print(f"  mskql   : {host}:{args.mskql_port}")
    print(f"  postgres: {host}:{args.pg_port} (db: {args.pg_db})")
    print(f"  duckdb  : {duckdb_bin} (file-backed, in-process — no client/server overhead)")
    print("=" * 90)
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

    results = []
    bench_count = sum(1 for n, _ in BENCHMARKS if not args.filter or args.filter == n)

    bench_idx = 0
    for name, fn in BENCHMARKS:
        if args.filter and args.filter != name:
            continue
        bench_idx += 1

        print(f"  [{bench_idx}/{bench_count}] {name:<25s} ...", end="", flush=True)

        # Generate SQL — benchmark functions return either:
        #   (setup_lines, bench_lines)  — shared SQL for all engines
        #   {"mskql": (setup, bench), "duck": (setup, bench), "pg": None|(...)}
        result = fn()
        if isinstance(result, dict):
            is_parquet = True
            mskql_spec = result["mskql"]
            duck_spec = result["duck"]
            pg_spec = result.get("pg")
        else:
            is_parquet = False
            setup_lines, bench_lines = result
            mskql_spec = (setup_lines, bench_lines)
            duck_spec = (setup_lines, bench_lines)
            pg_spec = (setup_lines, bench_lines)

        # ── mskql ──
        m_setup_f = tempfile.NamedTemporaryFile(
            mode="w", suffix=".sql", prefix=f"bench_setup_{name}_m_", delete=False
        )
        m_bench_f = tempfile.NamedTemporaryFile(
            mode="w", suffix=".sql", prefix=f"bench_run_{name}_m_", delete=False
        )
        m_setup_f.write("\n".join(mskql_spec[0]) + "\n")
        m_setup_f.close()
        m_bench_f.write("\n".join(mskql_spec[1]) + "\n")
        m_bench_f.close()
        run_psql(host, args.mskql_port, args.pg_user, "mskql", m_setup_f.name)
        mskql_ms = time_psql(host, args.mskql_port, args.pg_user, "mskql", m_bench_f.name)
        os.unlink(m_setup_f.name)
        os.unlink(m_bench_f.name)

        # ── postgres ──
        if pg_spec is not None:
            p_setup_f = tempfile.NamedTemporaryFile(
                mode="w", suffix=".sql", prefix=f"bench_setup_{name}_p_", delete=False
            )
            p_bench_f = tempfile.NamedTemporaryFile(
                mode="w", suffix=".sql", prefix=f"bench_run_{name}_p_", delete=False
            )
            p_setup_f.write("\n".join(pg_spec[0]) + "\n")
            p_setup_f.close()
            p_bench_f.write("\n".join(pg_spec[1]) + "\n")
            p_bench_f.close()
            run_psql(host, args.pg_port, args.pg_user, args.pg_db, p_setup_f.name)
            pg_ms = time_psql(host, args.pg_port, args.pg_user, args.pg_db, p_bench_f.name)
            os.unlink(p_setup_f.name)
            os.unlink(p_bench_f.name)
        else:
            pg_ms = -2.0  # sentinel: not applicable

        # ── duckdb ──
        duck_db = tempfile.mktemp(suffix=".duckdb", prefix=f"bench_{name}_")
        d_setup_lines = duck_spec[0] if is_parquet else duckdb_fixup(duck_spec[0])
        d_bench_lines = duck_spec[1] if is_parquet else duckdb_fixup(duck_spec[1])
        d_setup_f = tempfile.NamedTemporaryFile(
            mode="w", suffix=".sql", prefix=f"duck_setup_{name}_", delete=False
        )
        d_bench_f = tempfile.NamedTemporaryFile(
            mode="w", suffix=".sql", prefix=f"duck_run_{name}_", delete=False
        )
        d_setup_f.write("\n".join(d_setup_lines) + "\n")
        d_setup_f.close()
        d_bench_f.write("\n".join(d_bench_lines) + "\n")
        d_bench_f.close()
        run_duckdb(duckdb_bin, duck_db, d_setup_f.name)
        duck_ms = time_duckdb(duckdb_bin, duck_db, d_bench_f.name)
        os.unlink(d_setup_f.name)
        os.unlink(d_bench_f.name)
        if os.path.exists(duck_db):
            os.unlink(duck_db)

        results.append((name, mskql_ms, pg_ms, duck_ms))
        tag = f" mskql={mskql_ms:.0f}ms" if mskql_ms >= 0 else " ERROR"
        print(tag, flush=True)

    # ── sort: mskql wins (fastest) first, losses last ──
    def sort_key(r):
        _name, m, p, d = r
        best = min(t for t in (m, p, d) if t >= 0) if any(t >= 0 for t in (m, p, d)) else 0
        if m >= 0 and m <= best:
            return (0, m / max(best, 0.001))
        return (1, m / max(best, 0.001) if m >= 0 else 999)

    results.sort(key=sort_key)

    # ── ANSI colors ──
    GREEN = "\033[32m"
    RED = "\033[31m"
    YELLOW = "\033[33m"
    RESET = "\033[0m"

    # ── print sorted results ──
    print()
    print(f"  {'BENCHMARK':<25s} {'mskql (ms)':>12s} {'pg (ms)':>12s} {'duck (ms)':>12s} {'ms/pg':>8s} {'ms/duck':>8s}  fastest")
    print(f"  {'-' * 90}")

    for name, mskql_ms, pg_ms, duck_ms in results:
        mskql_str = f"{mskql_ms:.1f}" if mskql_ms >= 0 else "ERROR"
        pg_str = "-" if pg_ms == -2.0 else (f"{pg_ms:.1f}" if pg_ms >= 0 else "ERROR")
        duck_str = f"{duck_ms:.1f}" if duck_ms >= 0 else "ERROR"
        ms_pg_str = f"{mskql_ms / pg_ms:.2f}x" if pg_ms > 0 and mskql_ms >= 0 else "-"
        ms_duck_str = f"{mskql_ms / duck_ms:.2f}x" if duck_ms > 0 and mskql_ms >= 0 else "n/a"

        times = {"mskql": mskql_ms, "duck": duck_ms}
        if pg_ms >= 0:
            times["pg"] = pg_ms
        valid = {k: v for k, v in times.items() if v >= 0}
        if valid:
            winner = min(valid, key=valid.get)
        else:
            winner = "?"

        if winner == "mskql":
            tag = f"{GREEN}mskql{RESET}"
        elif winner == "pg":
            tag = f"{RED}pg{RESET}"
        elif winner == "duck":
            tag = f"{YELLOW}duck{RESET}"
        else:
            tag = "?"

        print(f"  {name:<25s} {mskql_str:>12s} {pg_str:>12s} {duck_str:>12s} {ms_pg_str:>8s} {ms_duck_str:>8s}  {tag}")

    print(f"  {'-' * 90}")
    print()
    print("  ratio = mskql / X  (< 1.0 means mskql is faster)")
    print()
    mskql_wins = sum(1 for _, m, p, d in results
                     if m >= 0 and all(m <= t for t in (p, d) if t >= 0))
    print(f"done. ({len(results)} benchmarks, mskql fastest in {mskql_wins})")

    if args.markdown and results:
        print()
        print("| Benchmark | mskql (ms) | pg (ms) | duck (ms) | ms/pg | ms/duck | fastest |")
        print("|-----------|-----------|---------|-----------|-------|---------|---------|")
        for name, m_ms, p_ms, d_ms in results:
            m_str = f"{m_ms:.1f}" if m_ms >= 0 else "ERROR"
            p_str = "-" if p_ms == -2.0 else (f"{p_ms:.1f}" if p_ms >= 0 else "ERROR")
            d_str = f"{d_ms:.1f}" if d_ms >= 0 else "ERROR"
            mp = f"{m_ms / p_ms:.2f}x" if p_ms > 0 and m_ms >= 0 else "-"
            md = f"{m_ms / d_ms:.2f}x" if d_ms > 0 and m_ms >= 0 else "n/a"
            times = {"mskql": m_ms, "duck": d_ms}
            if p_ms >= 0:
                times["pg"] = p_ms
            valid = {k: v for k, v in times.items() if v >= 0}
            winner = min(valid, key=valid.get) if valid else "?"
            print(f"| {name} | {m_str} | {p_str} | {d_str} | {mp} | {md} | {winner} |")


if __name__ == "__main__":
    main()
