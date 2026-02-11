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


BENCHMARKS = [
    ("insert_bulk",      bench_insert_bulk),
    ("select_full_scan", bench_select_full_scan),
    ("select_where",     bench_select_where),
    ("aggregate",        bench_aggregate),
    ("order_by",         bench_order_by),
    ("join",             bench_join),
    ("update",           bench_update),
    ("index_lookup",     bench_index_lookup),
]


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="mskql vs PostgreSQL benchmark")
    p.add_argument("--mskql-port", type=int, default=5433)
    p.add_argument("--pg-port", type=int, default=5432)
    p.add_argument("--pg-db", default="mskql_bench")
    p.add_argument("--pg-user", default=os.environ.get("USER", "postgres"))
    p.add_argument("--filter", default=None, help="run only this benchmark")
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
        print(f"       Start it with: ./build/mskql")
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


if __name__ == "__main__":
    main()
