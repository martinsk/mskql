window.BENCHMARK_DATA = {
  "lastUpdate": 1770925643459,
  "repoUrl": "https://github.com/martinsk/mskql",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "8158d9437b101bd1720d82482f1b8dc6e5bdce16",
          "message": "initialize transaction state and snapshot fields in db_init and add snapshot cleanup in db_free",
          "timestamp": "2026-02-09T12:18:43-08:00",
          "tree_id": "8e0cfbefa481708920654115c10298b8b031ec5b",
          "url": "https://github.com/martinsk/mskql/commit/8158d9437b101bd1720d82482f1b8dc6e5bdce16"
        },
        "date": 1770668429512,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 23.831,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 158.401,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 275.053,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 481.753,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 327.889,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 707.347,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 42.275,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 260.27,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 389.308,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 12.46,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 31.892,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "78bb8857b65fc527c6d2f215b703a5d543162c2f",
          "message": "add support for multi-column IN, ANY/ALL operators, arithmetic expressions in UPDATE SET, configurable port via MSKQL_PORT, and use table names as fallback JOIN aliases",
          "timestamp": "2026-02-09T20:31:30-08:00",
          "tree_id": "2f24a4f4394acc145f9555bf552a5e0a4e9728b5",
          "url": "https://github.com/martinsk/mskql/commit/78bb8857b65fc527c6d2f215b703a5d543162c2f"
        },
        "date": 1770697905973,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 24.625,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 163.403,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 273.365,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 473.77,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 329.538,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 695.834,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 42.286,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 269.252,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 391.473,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 10.635,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 32.472,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "647e8080309e5efad26d40dd845a2058a1e68924",
          "message": "move query_free and condition_free from query.c to parser.c to satisfy JPL ownership rule that allocating module must deallocate, add condition_release_subquery_sql helper, and document ownership transfer for enum_values and subquery SQL strings",
          "timestamp": "2026-02-09T20:51:55-08:00",
          "tree_id": "d2617385c58b4a7512278372472d2318973a7a1a",
          "url": "https://github.com/martinsk/mskql/commit/647e8080309e5efad26d40dd845a2058a1e68924"
        },
        "date": 1770699130866,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 24.479,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 158.848,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 277.74,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 487.39,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 329.556,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 717.492,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 44.24,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 269.867,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 389.717,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 10.695,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 32.698,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "7acf3e2f70609f069137b6bf62f0f9076269e660",
          "message": "add expression AST parser with operator precedence, enable GROUP BY/aggregates on JOINs, and support arithmetic expressions in UPDATE SET clauses\n\nImplement recursive descent expression parser supporting literals, column references, arithmetic operators (+, -, *, /, %, unary -), string concatenation (||), function calls (COALESCE, NULLIF, GREATEST, LEAST, UPPER, LOWER, LENGTH, TRIM, SUBSTRING), CASE WHEN expressions, and subqueries. Add expr_free to recursively deallocate expression trees. Extend",
          "timestamp": "2026-02-10T10:17:18-08:00",
          "tree_id": "32b2378e1128154cd43ab0bfbff9db85af5abb0b",
          "url": "https://github.com/martinsk/mskql/commit/7acf3e2f70609f069137b6bf62f0f9076269e660"
        },
        "date": 1770747456078,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 23.671,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 159.402,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 277.193,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 535.205,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 326.224,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 698.492,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 53.419,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 265.747,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 403.096,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 10.254,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 33.966,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "f782fc7e5eac3c234b5d80e11a3c2582390fe515",
          "message": "introduce arena-based memory management for query parsing to eliminate recursive deallocation and fix memory leaks\n\nReplace pointer-based query AST with pool-based arena allocator where each parsed type (expr, condition, case_when_branch, set_clause, select_column, select_expr, agg_expr, order_by_item, join_info, cte_def) gets its own flat dynamic array. Structures reference items by uint32_t index instead of pointers, with IDX_NONE (0xFFFFFFFF) marking unused references. Add query_arena struct containing",
          "timestamp": "2026-02-10T12:25:49-08:00",
          "tree_id": "56c69e895f77d7b17e70e643299710d48110be30",
          "url": "https://github.com/martinsk/mskql/commit/f782fc7e5eac3c234b5d80e11a3c2582390fe515"
        },
        "date": 1770755166537,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 21.753,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 153.302,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 272.642,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 426.748,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 321.988,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 750.835,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 49.891,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 253.75,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 366.65,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 9.347,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 29.931,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "47aab95bde02c0cb9c7edf89b5782ff4bc0ed3eb",
          "message": "add DISTINCT support for aggregate functions, fix correlated EXISTS/NOT EXISTS evaluation, and enable UPDATE...FROM with expression evaluation against merged row context\n\nImplement DISTINCT keyword parsing in aggregate functions (COUNT/SUM/AVG/MIN/MAX) by adding has_distinct flag to agg_expr. Move EXISTS/NOT EXISTS evaluation from resolve_subqueries to eval_condition to support correlated subqueries that reference outer table columns via per-row SQL template substitution with literal value injection",
          "timestamp": "2026-02-10T13:55:29-08:00",
          "tree_id": "8e1ec8a881a5eb2688afd89f226f68ec64867c0e",
          "url": "https://github.com/martinsk/mskql/commit/47aab95bde02c0cb9c7edf89b5782ff4bc0ed3eb"
        },
        "date": 1770760547324,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 26.412,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 161.639,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 286.312,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 463.384,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 331.867,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 701.112,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 60.1,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 285.398,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 447.502,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 11.149,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 32.976,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "0b3d1cde97e08453f61a7df6cd7ffc7cd236ab1a",
          "message": "add bump allocator for arena-based string management, implement ON CONFLICT DO UPDATE, and enable full condition evaluation in JOIN ON clauses\n\nReplace per-string malloc with bump_alloc slab allocator in query_arena to eliminate individual string allocations. Add query_arena_reset() to reuse arena memory across queries by resetting counts and bump slabs while keeping backing buffers. Implement INSERT...ON CONFLICT DO UPDATE by detecting conflicts on unique/primary key columns and applying SET clauses",
          "timestamp": "2026-02-10T15:01:42-08:00",
          "tree_id": "d27a4455f633350dd3226f3730c7b8d02dc679fd",
          "url": "https://github.com/martinsk/mskql/commit/0b3d1cde97e08453f61a7df6cd7ffc7cd236ab1a"
        },
        "date": 1770764543387,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.419,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 88.822,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 179.784,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 220.559,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 162.563,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 2519.37,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 24.5,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 100.126,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 171.962,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 4.182,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 25.06,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "658f772d120e07bdbc3f7c1a6f3cee88c0d9a1e3",
          "message": "add support for TIME, TIMESTAMPTZ, INTERVAL types, SERIAL/BIGSERIAL auto-increment, foreign key constraints, sequences, views, window function frames, ROLLUP/CUBE grouping, and sequence functions (NEXTVAL/CURRVAL/GEN_RANDOM_UUID)\n\nExtend column_type enum with COLUMN_TYPE_TIME, COLUMN_TYPE_TIMESTAMPTZ, and COLUMN_TYPE_INTERVAL. Add SERIAL/BIGSERIAL support via is_serial flag and serial_next counter in column struct. Implement foreign key constraints with fk_table, fk_column, fk_on_delete_cascade, and fk_on_update",
          "timestamp": "2026-02-10T16:14:10-08:00",
          "tree_id": "66ebe07a4a86ca6cc552d6a073cc3a7e7113a76b",
          "url": "https://github.com/martinsk/mskql/commit/658f772d120e07bdbc3f7c1a6f3cee88c0d9a1e3"
        },
        "date": 1770768874578,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.34,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 91.285,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 172.972,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 224.703,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 163.654,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 2557.962,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 24.441,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 112.77,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 218.791,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 4.92,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 25.851,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "2f885f4ddf538f12adaf89c6e766698ddf69d8e7",
          "message": "introduce vectorized query execution with columnar blocks, hash tables, and query planner\n\nReplace row-at-a-time tuple processing with columnar block execution using 1024-row blocks that fit in L1 cache. Add struct col_block for typed column arrays with null bitmaps, struct row_block for horizontal slices with optional selection vectors to avoid copying during filters, and struct block_hash_table for arena-allocated hash tables. Implement multi-slab bump allocator where new slabs are chained instead",
          "timestamp": "2026-02-10T17:14:32-08:00",
          "tree_id": "d9d45426687e0308cffc91897582cc835d6310dd",
          "url": "https://github.com/martinsk/mskql/commit/2f885f4ddf538f12adaf89c6e766698ddf69d8e7"
        },
        "date": 1770772499189,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.891,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 87.647,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 122.509,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 177.125,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 152.699,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 2595.205,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 23.367,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 113.561,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 229.216,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.176,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.412,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "85ad54ecdcf61163cb077adbe00ecb70e861d108",
          "message": "add benchmark script comparing mskql vs PostgreSQL and implement hash join optimization for equi-joins\n\nAdd bench/bench_vs_pg.py Python script and bench/bench_vs_pg.sh wrapper to run identical SQL workloads against both mskql and PostgreSQL via psql. Script generates 8 benchmarks (insert_bulk, select_full_scan, select_where, aggregate, order_by, join, update, index_lookup) with setup and execution phases, times both systems, and reports ratio.\n\nReplace nested-loop join with hash join for equality",
          "timestamp": "2026-02-10T21:10:55-08:00",
          "tree_id": "0d9b9cdce4ced6e33d892771e2f256d8c58653d8",
          "url": "https://github.com/martinsk/mskql/commit/85ad54ecdcf61163cb077adbe00ecb70e861d108"
        },
        "date": 1770786675906,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 12.67,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 81.488,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 114.318,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 168.666,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 119.53,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 31.476,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 22.63,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 103.334,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 206.854,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 4.969,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.028,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "55d9720a7431d70083baa5e185585f447328fafa",
          "message": "add bump allocator for result row text to eliminate per-cell malloc/free in query execution\n\nIntroduce result_text bump allocator in query_arena to bulk-allocate text for result rows instead of individual malloc per cell. Add arena_owns_text flag to struct rows to track ownership mode. When flag is set, text lives in result_text bump and is freed in one shot via bump_reset; otherwise fall back to per-cell free for heap-allocated text.\n\nThread optional struct bump_alloc *rb parameter through db_exec,",
          "timestamp": "2026-02-10T22:38:28-08:00",
          "tree_id": "c1664650f3baa0643eb80f82a0f081b9ab30f4dd",
          "url": "https://github.com/martinsk/mskql/commit/55d9720a7431d70083baa5e185585f447328fafa"
        },
        "date": 1770791928491,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.379,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 87.826,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 131.931,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 182.744,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 127.052,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 33.253,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 23.25,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 111.175,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 211.099,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 4.814,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 18.675,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "c804ff9a33851e883a416f8b8ba76dfd66f9a282",
          "message": "add direct columnar-to-wire serialization path for SELECT queries bypassing row materialization and optimize sort comparator with flattened key arrays\n\nImplement try_plan_send() in pgwire.c to execute SELECT queries via plan executor and stream columnar blocks directly to PostgreSQL wire protocol without converting to struct rows. Add msgbuf_push_col_cell() to serialize col_block cells as pgwire text fields and send_row_desc_plan() to build RowDescription from plan metadata. Batch DataRow messages",
          "timestamp": "2026-02-11T09:40:36-08:00",
          "tree_id": "1c3f315f5c39f57728d97c5098a8c7b099b5e520",
          "url": "https://github.com/martinsk/mskql/commit/c804ff9a33851e883a416f8b8ba76dfd66f9a282"
        },
        "date": 1770831661983,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 11.004,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 77.981,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 105.809,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 166.948,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 113.87,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 28.072,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 19.074,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 85.652,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 165.74,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 3.881,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 22.074,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "7a19c252df4b89d02d0470fbb93b935ce6054de9",
          "message": "extend pgwire RowDescription to handle JOIN queries by passing optional second table and resolving column names across merged column space, and implement hash join fast path in plan builder for simple equi-joins without aggregates/grouping/window functions\n\nAdd t2 parameter to send_row_desc_plan() to support JOIN queries where columns come from two tables. When SELECT * on a JOIN, resolve column names by checking t1 first, then t2 with offset. For explicit column lists, search both tables and map",
          "timestamp": "2026-02-11T10:00:40-08:00",
          "tree_id": "f04a7dc731e31645bfff977423e0d93b80b77627",
          "url": "https://github.com/martinsk/mskql/commit/7a19c252df4b89d02d0470fbb93b935ce6054de9"
        },
        "date": 1770832882512,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.368,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 88.193,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 124.347,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 177.185,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 128.563,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 33.731,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 22.868,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 87.906,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 164.192,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 3.895,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 18.326,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "2280a2dfac84dc6b699c7017e6874e36157e9a9c",
          "message": "add PLAN_INDEX_SCAN operator to query planner for equality WHERE predicates on indexed columns\n\nImplement index_scan_next() to execute index lookups via index_lookup() and materialize matching rows into columnar blocks. Add PLAN_INDEX_SCAN case to plan_node_ncols() and plan_next_block(). Extend plan_build_select() to detect equality conditions on indexed columns and emit INDEX_SCAN instead of SEQ_SCAN + FILTER when applicable, setting est_rows to 1.0 for point lookups.",
          "timestamp": "2026-02-11T10:21:01-08:00",
          "tree_id": "aad22179118523bdb9ee862b4feb3e253ae6aa45",
          "url": "https://github.com/martinsk/mskql/commit/2280a2dfac84dc6b699c7017e6874e36157e9a9c"
        },
        "date": 1770834088175,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.375,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 85.33,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 119.802,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 183.873,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 122.371,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 33.43,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 23.556,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 110.961,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 216.526,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 4.988,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 18.783,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "a9f499f85eac134ac0c96fcfcf26257ca67843bb",
          "message": "replace heap allocations with arena scratch allocations in JOIN, UPDATE...FROM, and UNION operations to eliminate manual free() calls and fix memory leak in recursive CTE when table is dropped mid-iteration\n\nConvert t1_matched/t2_matched arrays and hash table structures (ht_buckets/ht_nexts/ht_hashes) in do_single_join() from calloc to bump_calloc on arena scratch, removing corresponding free() calls. Replace calloc with bump_calloc for new_vals/col_idxs arrays in UPDATE...FROM SET clause evaluation",
          "timestamp": "2026-02-11T11:09:29-08:00",
          "tree_id": "dd7afa8efd03d47d42d0cb8f73f9210393dc0bf6",
          "url": "https://github.com/martinsk/mskql/commit/a9f499f85eac134ac0c96fcfcf26257ca67843bb"
        },
        "date": 1770836992683,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.846,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 85.315,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 116.616,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 176.911,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 122.411,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 33.851,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 23.575,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 115.582,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 228.091,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.086,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.12,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "3b3b99433c1852977407d2d25e456faa8f258dea",
          "message": "add CAST syntax, EXTRACT function, date/time arithmetic, and temporal functions (NOW/DATE_TRUNC/DATE_PART/AGE/TO_CHAR) with :: postfix cast operator\n\nImplement CAST(expr AS type) and expr::type syntax by adding TOK_DOUBLE_COLON token, EXPR_CAST node type, and parse_cast_type_name() helper. Add EXTRACT(field FROM expr) parsing with field name lowercasing. Support CURRENT_TIMESTAMP keyword as zero-arg function.\n\nExtend eval_expr() with date/time arithmetic: date/timestamp +/- interval, timestamp -",
          "timestamp": "2026-02-11T11:31:47-08:00",
          "tree_id": "bf85f91adcdfd5c303be9fa94c6200566982f072",
          "url": "https://github.com/martinsk/mskql/commit/3b3b99433c1852977407d2d25e456faa8f258dea"
        },
        "date": 1770838332333,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 14.063,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 92.272,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 143.184,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 187.113,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 126.673,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 34.185,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 22.971,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 145,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 234.159,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.237,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 21.287,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "9b40df34ad39f0642565fc83d1fce9bba2a4a97c",
          "message": "add scan cache for repeated sequential scans, optimize sort emit phase with flat column arrays, replace snprintf with fast integer-to-string conversion, and track table generation to invalidate caches on mutations\n\nIntroduce struct scan_cache to cache columnar representation of tables for repeated scans. Add generation counter to struct table, incremented on INSERT/UPDATE/DELETE. Implement scan_cache_build() to materialize row-store into flat typed arrays and scan_cache_read() to copy slices into",
          "timestamp": "2026-02-11T11:57:06-08:00",
          "tree_id": "1a63625138f59cf4c58803ce7f65580b0fa7a175",
          "url": "https://github.com/martinsk/mskql/commit/9b40df34ad39f0642565fc83d1fce9bba2a4a97c"
        },
        "date": 1770839847299,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.535,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 81.223,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 103.747,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 173.752,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 118.121,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 33.223,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 23.709,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 112.801,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 216.827,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.024,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.12,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "1410c8cb7de05f4142e50493ee4dfdf42dece6dc",
          "message": "add Extended Query Protocol with prepared statements, portals, parameter substitution, and SQL-standard doubled-quote escaping in string literals\n\nImplement Parse/Bind/Execute/Describe/Close messages for prepared statements and portals. Add struct prepared_stmt and struct portal with MAX_PREPARED=32 and MAX_PORTALS=16 slots per connection. Implement substitute_params() to replace $1, $2, ... placeholders with literal values: numeric/boolean parameters inserted unquoted, text parameters single-quoted with",
          "timestamp": "2026-02-11T12:42:32-08:00",
          "tree_id": "5c0ddfcd1780e6b972fa8e83944831686923dd64",
          "url": "https://github.com/martinsk/mskql/commit/1410c8cb7de05f4142e50493ee4dfdf42dece6dc"
        },
        "date": 1770842576760,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.206,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 85.482,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 106.349,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 163.775,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 112.216,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 33.616,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 22.487,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 107.555,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 231.12,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.182,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 18.546,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "1f95b416fa51b45fc0808fefb6f34bd653ddb6b7",
          "message": "add arena state synchronization after query execution to preserve bump allocator and dynamic array growth across plan and db_exec paths\n\nCopy q.arena state back to conn_arena after try_plan_send() and db_exec() to capture bump slab growth and dynamic array reallocations that occurred during query execution. After plan path, copy entire arena struct. After db_exec path, selectively copy scratch/bump/plan_nodes/cells/svs fields while leaving result/result_text untouched since db_exec wrote those directly through conn_arena pointers.",
          "timestamp": "2026-02-11T14:53:41-08:00",
          "tree_id": "7a55278bee491ded1a5db4dbb19d8464623ab3a2",
          "url": "https://github.com/martinsk/mskql/commit/1f95b416fa51b45fc0808fefb6f34bd653ddb6b7"
        },
        "date": 1770850448852,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.845,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 80.501,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 102.859,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 174.989,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 118.993,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 33.285,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 23.268,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 113.697,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 217.614,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.015,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.194,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "07423d9ed5c9178d2b0811d81d04256ea330ff57",
          "message": "add -lm linker flag to Makefile for math library functions and silence -Wreturn-type warning in cmp_from_token() with unreachable return statement",
          "timestamp": "2026-02-11T16:54:20-08:00",
          "tree_id": "bc296e235c599e67f4ce239cd7d29e95a3259336",
          "url": "https://github.com/martinsk/mskql/commit/07423d9ed5c9178d2b0811d81d04256ea330ff57"
        },
        "date": 1770857685778,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 14.704,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 80.859,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 103.103,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 211.032,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 110.034,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 32.897,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 24.325,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 120.261,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 257.113,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.814,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.296,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "1e0422f43e8e9d1bdb509b4e9ceb9a6796f1d194",
          "message": "add generate_series() table function for integer and timestamp sequences with optional step parameter and AS alias(col_alias) syntax\n\nImplement FROM generate_series(start, stop [, step]) [AS alias [(col_alias)]] by parsing function call in parse_select(), evaluating start/stop/step expressions in db_exec(), and materializing results as temporary table. Support integer series with BIGINT/INT type selection based on range, and timestamp/date series with interval step parsing via parse_interval_to_seconds(",
          "timestamp": "2026-02-11T17:29:40-08:00",
          "tree_id": "d666588634dce3a28a4258095b46230e7be53dd9",
          "url": "https://github.com/martinsk/mskql/commit/1e0422f43e8e9d1bdb509b4e9ceb9a6796f1d194"
        },
        "date": 1770859802696,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 14.35,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 80.317,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 102.675,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 216.816,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 116.711,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 33.084,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 24.172,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 121.327,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 258.067,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.687,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.489,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "08a554df6428f03550fc864b9dfc2e51b400a381",
          "message": "Add freestanding WASM build: wasm_libc.h/c (minimal libc with JS bridge), wasm_api.c (C shim), Makefile wasm target, and CI workflow to build and deploy mskql.wasm to gh-pages",
          "timestamp": "2026-02-11T20:23:38-08:00",
          "tree_id": "f66e9e5f5368835850232ed91578a6bdaa90fa3a",
          "url": "https://github.com/martinsk/mskql/commit/08a554df6428f03550fc864b9dfc2e51b400a381"
        },
        "date": 1770870249933,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 14.566,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 80.588,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 107.493,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 196.423,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 115.873,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 32.75,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 24.119,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 128.492,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 270.672,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.714,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.318,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "2feb2c8e9fbc3567ea18f778e6d266ca2886dd60",
          "message": "wasm_api: add column headers to query output (parse query for column names)",
          "timestamp": "2026-02-11T20:54:03-08:00",
          "tree_id": "eb01a42470ac0ea00941169d6e4f968a18ffae86",
          "url": "https://github.com/martinsk/mskql/commit/2feb2c8e9fbc3567ea18f778e6d266ca2886dd60"
        },
        "date": 1770872070809,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 14.397,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 81.188,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 103.803,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 197.507,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 116.182,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 32.798,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 24.283,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 120.811,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 258.79,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.733,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.558,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "2de894600aaa4a07d4717dbc8705dbc9ceffefa4",
          "message": "add arena-based error reporting with SQLSTATE codes to replace fprintf(stderr) error messages throughout database.c and parser.c\n\nIntroduce errmsg[256] and sqlstate[6] fields to struct query_arena with arena_set_error() helper implementing first-error-wins semantics. Initialize and reset error fields in query_arena_init() and query_arena_reset(). Replace all fprintf(stderr) error messages in database.c and parser.c with arena_set_error() calls using appropriate SQLSTATE codes: 42P01 (undefined_table",
          "timestamp": "2026-02-12T08:31:48-08:00",
          "tree_id": "2904f9d518992ae22d9af84e9d664e45d8c48219",
          "url": "https://github.com/martinsk/mskql/commit/2de894600aaa4a07d4717dbc8705dbc9ceffefa4"
        },
        "date": 1770913935364,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 14.384,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 81.45,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 103.867,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 204.567,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 135.108,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 33.089,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 23.906,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 117.052,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 256.752,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.642,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.266,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "247c334697cec1cbecdb3e2e81f38c83047b424d",
          "message": "fix wasm build: add stdarg.h to stub headers list",
          "timestamp": "2026-02-12T08:38:08-08:00",
          "tree_id": "0734f765998ccae1df5d02ad126c403832843164",
          "url": "https://github.com/martinsk/mskql/commit/247c334697cec1cbecdb3e2e81f38c83047b424d"
        },
        "date": 1770914323314,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 14.406,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 80.714,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 103.688,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 189.671,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 117.249,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 33.028,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 24.147,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 121.386,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 256.725,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.723,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.615,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "041ebf2f0844d585202ec43ddb182d3279f74034",
          "message": "add project website, simplify README to quick-start guide with link to full documentation\n\nReplace detailed feature list and build instructions with brief introduction and link to martinsk.github.io/mskql for documentation, benchmarks, supported SQL, and tutorials. Condense README to quick-start commands and test runner invocation.",
          "timestamp": "2026-02-12T08:48:23-08:00",
          "tree_id": "0eca24c5f509f6830bf88c05c18ceebdbf8a9306",
          "url": "https://github.com/martinsk/mskql/commit/041ebf2f0844d585202ec43ddb182d3279f74034"
        },
        "date": 1770914928613,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 14.266,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 80.562,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 102.843,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 188.678,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 116.048,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 32.743,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 23.891,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 118.746,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 256.207,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.707,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.314,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "fcb3c37adea47bfba3684dac92e3962d0cb4a027",
          "message": "Correlated subquery with table alias",
          "timestamp": "2026-02-12T09:46:21-08:00",
          "tree_id": "5e5b31f34b791047dfb07860cb6ce3501f55544d",
          "url": "https://github.com/martinsk/mskql/commit/fcb3c37adea47bfba3684dac92e3962d0cb4a027"
        },
        "date": 1770918460006,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 14.56,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 81.339,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 104.113,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 191.389,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 110.918,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 33.658,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 24.078,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 119.748,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 256.462,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.681,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.396,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "6ae30a038b4a49c7f66187a0072b0662c5e8817c",
          "message": "add 26 test cases for playground examples and tutorial SQL from gh-pages\n\nCovers: playground (basics, joins, aggregation, window, generate_series,\nupsert, datetime), index.html tutorial (filter/orderby, inner join, upsert,\nsequence), task-tracker tutorial (filter, labels, update, upsert),\nmulti-table-joins tutorial (multi-join, left join, cross join, exists),\nreporting-dashboard tutorial (revenue), time-series tutorial (to_char),\nschema-evolution tutorial (add col, rename col, cast expr, transaction,\ndrop col).\n\nAll 767 tests pass.",
          "timestamp": "2026-02-12T10:26:09-08:00",
          "tree_id": "3c90b0d6c741c4e755bf8a2fb4dfc42cb5892a99",
          "url": "https://github.com/martinsk/mskql/commit/6ae30a038b4a49c7f66187a0072b0662c5e8817c"
        },
        "date": 1770920800002,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 15.085,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 81.165,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 103.385,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 189.418,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 109.96,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 33.997,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 23.877,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 117.489,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 255.718,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.601,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 18.986,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "2919d4047caf299f6a1946cdacc011cd032d81a2",
          "message": "remove CodSpeed benchmark workflow (replaced by github-action-benchmark in existing CI)",
          "timestamp": "2026-02-12T10:43:05-08:00",
          "tree_id": "03f4fef617e84abeb301e0e890fc94f6fa97332c",
          "url": "https://github.com/martinsk/mskql/commit/2919d4047caf299f6a1946cdacc011cd032d81a2"
        },
        "date": 1770921808596,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 14.276,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 80.884,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 103.198,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 189.976,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 110.718,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 33.554,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 24.275,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 118.687,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 254.728,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 5.644,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 19.142,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "committer": {
            "email": "msk@ajour.io",
            "name": "Martin Kristiansen",
            "username": "martinsk"
          },
          "distinct": true,
          "id": "5b57baeb832bf3824bc383bfdb032504f8601bfd",
          "message": "add 60 test cases covering aggregate expressions, type casts, date/time functions, set operations, window functions, and CTEs\n\nNew tests: expression-based aggregates (COUNT/MIN/MAX DISTINCT expr, GROUP BY), type casts (::BIGINT/TEXT/DATE/TIMESTAMP/BOOLEAN/NUMERIC), date/time functions (AGE, DATE_PART, DATE_TRUNC, EXTRACT with DOW/DOY/WEEK/EPOCH/INTERVAL, NOW, CURRENT_TIMESTAMP, RANDOM), set operations (EXCEPT ALL, INTERSECT ALL, UNION ALL), window functions (RANK, DENSE_RANK, ROW_NUMBER, LAG, LEAD",
          "timestamp": "2026-02-12T11:46:10-08:00",
          "tree_id": "d04f61600d17be69f5232d55747ade8a0bff2e63",
          "url": "https://github.com/martinsk/mskql/commit/5b57baeb832bf3824bc383bfdb032504f8601bfd"
        },
        "date": 1770925642908,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 11.373,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 72.296,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 91.276,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 168.023,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 129.906,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 29.949,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 20.412,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 90.333,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 200.131,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 4.421,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 17.178,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}