#!/usr/bin/env bash
#
# bench_vs_pg.sh — run identical SQL workloads against mskql, PostgreSQL, and DuckDB
#
# Prerequisites:
#   - mskql server running on port 5433 (or set MSKQL_PORT)
#   - PostgreSQL running on port 5432 (or set PG_PORT)
#   - psql available in PATH
#   - duckdb available in PATH (or set DUCKDB_BIN)
#   - python3 available in PATH
#
# Usage:
#   ./bench/bench_vs_pg.sh                  # run all benchmarks
#   ./bench/bench_vs_pg.sh insert_bulk      # run a single benchmark
#

set -euo pipefail

MSKQL_PORT="${MSKQL_PORT:-5433}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-mskql_bench}"
PG_USER="${PG_USER:-$(whoami)}"
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"
FILTER="${1:-}"

exec python3 "$(dirname "$0")/bench_vs_pg.py" \
    --mskql-port "$MSKQL_PORT" \
    --pg-port "$PG_PORT" \
    --pg-db "$PG_DB" \
    --pg-user "$PG_USER" \
    --duckdb-bin "$DUCKDB_BIN" \
    ${FILTER:+--filter "$FILTER"}
