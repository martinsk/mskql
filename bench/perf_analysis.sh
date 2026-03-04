#!/bin/bash
# perf_analysis.sh — Automated CPU cache/memory performance analysis for mskql
# Uses xctrace (Apple Instruments CLI) to capture CPU Counters + Time Profiler
# traces, then parses them into a human-readable summary.
#
# Usage:
#   bash bench/perf_analysis.sh                    # profile large_sort (default)
#   bash bench/perf_analysis.sh --bench wide_agg   # profile specific benchmark
#   bash bench/perf_analysis.sh --all              # profile all benchmarks
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCH_BIN="$PROJECT_DIR/build/mskql_bench"
PARSE_SCRIPT="$SCRIPT_DIR/parse_perf.py"

BENCH_NAME="large_sort"
RUN_ALL=0
TIME_LIMIT="30s"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --bench)  BENCH_NAME="$2"; shift 2 ;;
        --all)    RUN_ALL=1; shift ;;
        --time)   TIME_LIMIT="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--bench NAME] [--all] [--time LIMIT]"
            echo "  --bench NAME   Profile specific benchmark (default: large_sort)"
            echo "  --all          Profile all benchmarks sequentially"
            echo "  --time LIMIT   xctrace time limit (default: 30s)"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Build ─────────────────────────────────────────────────────────
echo "Building release bench binary..."
make -C "$PROJECT_DIR/src" bench 2>&1 | tail -2

if [[ ! -x "$BENCH_BIN" ]]; then
    echo "ERROR: bench binary not found at $BENCH_BIN"
    exit 1
fi

# ── Output directory ──────────────────────────────────────────────
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="$PROJECT_DIR/reports/perf/$TIMESTAMP"
mkdir -p "$OUTDIR"

# ── Determine which benchmarks to run ─────────────────────────────
if [[ $RUN_ALL -eq 1 ]]; then
    BENCHMARKS=$("$BENCH_BIN" __list_benchmarks__ 2>/dev/null || \
        echo "large_sort wide_agg multi_join aggregate order_by select_full_scan join window_rank")
else
    BENCHMARKS="$BENCH_NAME"
fi

# ── Profile each benchmark ────────────────────────────────────────
for bench in $BENCHMARKS; do
    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Profiling: $bench"
    echo "═══════════════════════════════════════════════════════════"

    BENCH_DIR="$OUTDIR/$bench"
    mkdir -p "$BENCH_DIR"

    # 1) CPU Counters trace
    echo "  [1/4] Recording CPU Counters..."
    COUNTERS_TRACE="$BENCH_DIR/counters.trace"
    xctrace record \
        --template 'CPU Counters' \
        --output "$COUNTERS_TRACE" \
        --time-limit "$TIME_LIMIT" \
        --no-prompt \
        --launch -- "$BENCH_BIN" "$bench" \
        2>&1 | grep -v "^$" | sed 's/^/        /'

    # 2) Time Profiler trace
    echo "  [2/4] Recording Time Profiler..."
    TIME_TRACE="$BENCH_DIR/time_profiler.trace"
    xctrace record \
        --template 'Time Profiler' \
        --output "$TIME_TRACE" \
        --time-limit "$TIME_LIMIT" \
        --no-prompt \
        --launch -- "$BENCH_BIN" "$bench" \
        2>&1 | grep -v "^$" | sed 's/^/        /'

    # 3) Export traces to XML
    echo "  [3/4] Exporting trace data..."
    xctrace export --input "$COUNTERS_TRACE" \
        --xpath '/trace-toc/run[@number="1"]/data/table[@schema="MetricAggregationForThread"]' \
        --output "$BENCH_DIR/counters_agg.xml" 2>/dev/null || true

    xctrace export --input "$TIME_TRACE" \
        --xpath '/trace-toc/run[@number="1"]/data/table[@schema="time-profile"]' \
        --output "$BENCH_DIR/time_profile.xml" 2>/dev/null || true

    xctrace export --input "$TIME_TRACE" \
        --xpath '/trace-toc/run[@number="1"]/data/table[@schema="time-sample"]' \
        --output "$BENCH_DIR/time_samples.xml" 2>/dev/null || true

    # 4) Parse and summarize
    echo "  [4/4] Analyzing..."
    python3 "$PARSE_SCRIPT" \
        --bench "$bench" \
        --counters "$BENCH_DIR/counters_agg.xml" \
        --time-profile "$BENCH_DIR/time_profile.xml" \
        --time-samples "$BENCH_DIR/time_samples.xml" \
        --output "$BENCH_DIR/summary.txt" \
        2>&1

    echo ""
    cat "$BENCH_DIR/summary.txt"
done

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Results saved to: $OUTDIR"
echo "═══════════════════════════════════════════════════════════"
