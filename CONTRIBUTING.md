# Contributing to mskql

A from-scratch, single-threaded, in-memory SQL database in C11. No external dependencies beyond libc and POSIX sockets.

## Building

Default `make` builds with ASAN (`-fsanitize=address`). `make release` builds with `-O3 -flto` and no sanitizers (for benchmarks / production).

**macOS**: Apple Clang has limited sanitizer support — no LeakSanitizer on ARM64. Use Homebrew LLVM instead:

```bash
make clean; CC=/opt/homebrew/opt/llvm/bin/clang make
```

The test runner (`tests/test.sh`) auto-detects Homebrew Clang and sets `CC` if available.

## Code layout

All source lives in `src/`, one `.c`/`.h` pair per module:

| File | Role |
|------|------|
| `parser.c` | Lexer + recursive-descent SQL parser |
| `query.c` | Row-at-a-time expression evaluator, aggregates, window functions |
| `plan.c` | Block-oriented columnar executor (scan, filter, join, sort, …) |
| `pgwire.c` | PostgreSQL wire protocol server + direct columnar→wire send |
| `database.c` | DDL execution, query dispatch, transactions |
| `table.c` / `row.c` / `column.c` / `index.c` | Storage primitives |
| `arena.h` / `arena_helpers.h` | Bump allocator + AST arena storage |
| `block.h` | Columnar block types (`col_block`, `row_block`, hash table) |
| `catalog.c` | Queryable system catalog tables |

Other directories: `tests/cases/` (SQL test files), `bench/` (micro-benchmarks).

## C style

**Standard**: C11 with `-Wall -Wextra -Wswitch-enum -Wpedantic`.

**Naming**: `snake_case` everywhere — functions, variables, types, files.

**Types**: `struct` with tagged unions for polymorphism (`plan_node`, `expr`, `cell`). Enums for type tags (`enum plan_op`, `enum expr_type`, `enum column_type`).

**Strings**: `sv` (string view) for non-owning references; `char *` (strdup'd) for owned strings. Use `SV_FMT` / `SV_ARG()` for printf formatting.

**Collections**: `DYNAMIC_ARRAY(T)` macro for growable arrays; `da_push` / `da_free` / `da_reset`.

**Memory ownership** (JPL-inspired): Allocations and their corresponding frees should live close together — ideally in the same function, or at least in the same file. If you `malloc` it, the same module should `free` it. Avoid scattering ownership across distant call sites.

**Memory model**: Two tiers:
- *Bump allocator* (`struct bump_alloc`) for per-query scratch — bulk-freed on reset, never individually freed. The arena owns all AST nodes; destroy the arena as a unit.
- *Heap* (`malloc`/`free`) for persistent data: tables, indexes, column names.

**Indices**: `uint32_t` indices into arena arrays instead of pointers. `IDX_NONE` (`0xFFFFFFFF`) as null sentinel.

**Error handling**: `arena_set_error(arena, sqlstate, fmt, ...)` — first error wins. Functions return `-1` for failure, `0` for success.

**Comments**: Brief `/* ... */` for non-obvious logic. Section headers: `/* ---- Section name ---- */`. `// TODO:` for known improvements. No doc-comments or Doxygen.

**Includes**: Project headers first, then system headers. Include guards: `#ifndef FILE_H` / `#define FILE_H`.

**Visibility**: Default to `static` for file-local functions; only expose in `.h` when needed cross-module. Use `static inline` in headers for small helpers.

## Architecture patterns

**Two execution paths**: The legacy row-at-a-time evaluator (`query.c`) and the block-oriented columnar executor (`plan.c`). `plan_build_select` tries to build a columnar plan; if it returns `IDX_NONE`, the query falls back to the legacy path. Both paths must produce identical results.

**Plan nodes**: Arena-allocated, children referenced by `uint32_t` index. Each node type has a `_next()` function that pulls blocks from its children (volcano / pull model).

**Wire protocol**: `try_plan_send` in `pgwire.c` serializes directly from `col_block` arrays into pgwire DataRow messages — bypasses the row-store entirely. Writes are batched into a 64 KB buffer.

**Caching**: Columnar scan cache on `struct table`, invalidated by a generation counter (bumped on every INSERT/UPDATE/DELETE). Result cache keyed by SQL text + sum of table generations.

## Adding a new feature

**New SQL syntax**: Add token(s) to the lexer → parse function in `parser.c` → AST fields in `query.h` → evaluation in `query.c` or `database.c`.

**New plan node**: Add `PLAN_*` to `enum plan_op` → node fields in `plan_node` union → state struct → `_next()` executor → wire into `plan_next_block` dispatcher + `plan_node_ncols` → build logic in `plan_build_select`.

**New scalar function**: Add `FUNC_*` enum → keyword registration in `parser.c` → evaluation case in `eval_expr()`.

**Bail-out pattern**: When the plan executor can't handle a query shape, return `IDX_NONE` from `plan_build_select`. The legacy path handles it. Never crash on unsupported queries.

## Testing

Test files live in `tests/cases/` as `.sql` files with this format:

```sql
-- description of what this tests
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t VALUES (1, 'alice');
-- input:
SELECT * FROM t ORDER BY id;
-- expected output:
1|alice
```

**Naming**: `test_<feature>.sql` or `test_bug_<description>.sql`. Adversarial edge cases: `test_adversarial_<description>.sql`.

**Running**: `make test` runs the full suite in parallel with ASAN. Each test gets a fresh database (reset between tests via `SELECT __reset_db()`).

**Expected output**: Pipe-delimited columns, one row per line. NULL renders as an empty field. Compared against `psql -tA` output.

**Always run the full suite** before committing — regressions in unrelated areas are common due to shared parser/evaluator state.

## Performance

`make bench` runs internal C micro-benchmarks. `bench/bench_vs_pg.sh` compares against PostgreSQL.

Hot-path priority: wire batching > columnar execution > algorithmic complexity > micro-optimization.

Rules of thumb:
- Avoid `malloc`/`free` in per-row loops — use the bump allocator or pre-allocated arrays
- Prefer `memcpy` over per-element copy for typed arrays
- Generation counters gate cache validity — bump on every mutation
