#!/bin/bash
# simd_report.sh — Analyze SIMD auto-vectorization and ILP in mskql hot paths
#
# Phase 1: Recompile key .c files with -Rpass=loop-vectorize remarks
# Phase 2: Generate assembly for hot-path files
# Phase 3: Run parse_simd.py to produce a graded Markdown report
#
# Usage:
#   bash bench/simd_report.sh              # full analysis
#   bash bench/simd_report.sh --quick      # skip assembly, remarks only
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SRC_DIR="$PROJECT_DIR/src"
PARSE_SCRIPT="$SCRIPT_DIR/parse_simd.py"
OUTDIR="$PROJECT_DIR/reports/simd"
QUICK=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick) QUICK=1; shift ;;
        -h|--help)
            echo "Usage: $0 [--quick]"
            echo "  --quick   Skip assembly generation, produce remarks-only report"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

mkdir -p "$OUTDIR"

# ── Detect compiler ──────────────────────────────────────────────
UNAME_S="$(uname -s)"
if [[ "$UNAME_S" == "Darwin" ]]; then
    HOMEBREW_CLANG="/opt/homebrew/opt/llvm/bin/clang"
    if [[ -x "$HOMEBREW_CLANG" ]]; then
        CC="$HOMEBREW_CLANG"
    else
        CC="clang"
    fi
else
    CC="${CC:-clang}"
fi

echo "Compiler: $CC"
$CC --version 2>&1 | head -1

# ── Detect include flags (same as Makefile) ──────────────────────
PQ_INCFLAGS=""
if [[ "$UNAME_S" == "Darwin" ]]; then
    ZSTD_PREFIX="$(brew --prefix zstd 2>/dev/null || true)"
    ZLIB_PREFIX="$(brew --prefix zlib 2>/dev/null || true)"
    [[ -n "$ZSTD_PREFIX" ]] && PQ_INCFLAGS="$PQ_INCFLAGS -I$ZSTD_PREFIX/include"
    [[ -n "$ZLIB_PREFIX" ]] && PQ_INCFLAGS="$PQ_INCFLAGS -I$ZLIB_PREFIX/include"
fi

# ── Base flags matching RELEASE_CFLAGS from Makefile ─────────────
BASE_FLAGS="-Wall -Wextra -Wswitch-enum -Wpedantic -std=c11 \
  -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE \
  -O3 -flto -DNDEBUG -g -MMD -MP \
  -march=native -ffast-math -funroll-loops $PQ_INCFLAGS"

# Files containing the hot-path functions
HOT_FILES="plan.c pgwire.c"

# ── Phase 1: Vectorization remarks ──────────────────────────────
REMARK_FLAGS="-Rpass=loop-vectorize -Rpass-missed=loop-vectorize -Rpass-analysis=loop-vectorize"

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Phase 1: Compiler vectorization remarks"
echo "═══════════════════════════════════════════════════════════"

REMARKS_FILE="$OUTDIR/vectorization_remarks.txt"
> "$REMARKS_FILE"

for src in $HOT_FILES; do
    echo "  Compiling $src with vectorization remarks..."
    # Compile to .o, capturing stderr (where remarks go)
    $CC $BASE_FLAGS $REMARK_FLAGS -c -o /dev/null "$SRC_DIR/$src" 2>&1 \
        | grep -E "^$SRC_DIR/" \
        >> "$REMARKS_FILE" || true
done

TOTAL_REMARKS=$(wc -l < "$REMARKS_FILE" | tr -d ' ')
VECTORIZED=$(grep -c "vectorized" "$REMARKS_FILE" 2>/dev/null || echo 0)
MISSED=$(grep -c "not vectorized" "$REMARKS_FILE" 2>/dev/null || echo 0)
echo "  Remarks: $TOTAL_REMARKS total ($VECTORIZED vectorized, $MISSED missed)"
echo "  Saved to: $REMARKS_FILE"

if [[ $QUICK -eq 1 ]]; then
    echo ""
    echo "  --quick mode: skipping assembly generation"
    echo ""
    python3 "$PARSE_SCRIPT" \
        --remarks "$REMARKS_FILE" \
        --output "$OUTDIR/report.md"
    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Report saved to: $OUTDIR/report.md"
    echo "═══════════════════════════════════════════════════════════"
    exit 0
fi

# ── Phase 2: Assembly generation ────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Phase 2: Assembly generation"
echo "═══════════════════════════════════════════════════════════"

ASM_DIR="$OUTDIR/asm"
mkdir -p "$ASM_DIR"

# Strip -flto for assembly generation — LTO emits LLVM bitcode, not native asm
# Add -fno-inline-functions so static functions get their own labels (not inlined
# into plan_next_block). This gives us per-function assembly we can analyze.
# The inner loops are still fully optimized (-O3, vectorized, unrolled).
ASM_FLAGS=$(echo "$BASE_FLAGS" | sed 's/-flto//g')
ASM_FLAGS="$ASM_FLAGS -fno-inline-functions"

for src in $HOT_FILES; do
    base="${src%.c}"
    echo "  Generating assembly for $src..."
    $CC $ASM_FLAGS -S -o "$ASM_DIR/${base}.s" "$SRC_DIR/$src" 2>/dev/null
    echo "    -> $ASM_DIR/${base}.s ($(wc -l < "$ASM_DIR/${base}.s" | tr -d ' ') lines)"
done

# ── Phase 3: Analysis ───────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Phase 3: Analysis and report generation"
echo "═══════════════════════════════════════════════════════════"

python3 "$PARSE_SCRIPT" \
    --remarks "$REMARKS_FILE" \
    --asm-dir "$ASM_DIR" \
    --output "$OUTDIR/report.md"

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Report saved to: $OUTDIR/report.md"
echo "═══════════════════════════════════════════════════════════"
