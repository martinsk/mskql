window.BENCHMARK_DATA = {
  "lastUpdate": 1771228678334,
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
          "id": "cb5685906ce413058bdc2f7017f82a520cc98c08",
          "message": "refactor parse_expr_atom: extract helper functions for CAST, EXTRACT, EXISTS, CASE, function calls, and number literals\n\nSplit 200-line parse_expr_atom() into focused helpers: parse_cast_expr(), parse_extract_expr(), parse_exists_expr() (with negate param for NOT EXISTS), parse_case_expr(), parse_func_call_expr(), and parse_number_literal(). Main function now delegates to helpers and falls through to column refs/string literals. No functional changes.",
          "timestamp": "2026-02-12T12:23:05-08:00",
          "tree_id": "02190965f4f19f0731892541ca00d5a32202b97f",
          "url": "https://github.com/martinsk/mskql/commit/cb5685906ce413058bdc2f7017f82a520cc98c08"
        },
        "date": 1770927809250,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 12.258,
            "unit": "ms"
          },
          {
            "name": "select_full_scan",
            "value": 71.915,
            "unit": "ms"
          },
          {
            "name": "select_where",
            "value": 91.104,
            "unit": "ms"
          },
          {
            "name": "aggregate",
            "value": 169.711,
            "unit": "ms"
          },
          {
            "name": "order_by",
            "value": 113.559,
            "unit": "ms"
          },
          {
            "name": "join",
            "value": 29.268,
            "unit": "ms"
          },
          {
            "name": "update",
            "value": 19.767,
            "unit": "ms"
          },
          {
            "name": "delete",
            "value": 91.196,
            "unit": "ms"
          },
          {
            "name": "parser",
            "value": 201,
            "unit": "ms"
          },
          {
            "name": "index_lookup",
            "value": 4.418,
            "unit": "ms"
          },
          {
            "name": "transaction",
            "value": 17.123,
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
          "id": "e9c6b6e296bbce0b46d91602d66febe920786e83",
          "message": "add 8 new benchmarks (window functions, DISTINCT, subquery, CTE, generate_series, scalar functions, expression-based aggregates, multi-column ORDER BY), collect per-iteration timings with percentile calculation helpers, and tighten CI alert threshold to 120% with fail-on-alert for push events",
          "timestamp": "2026-02-12T12:36:30-08:00",
          "tree_id": "e9ae02d3fb2454ad3aeb54cb85cbe46edf42e1e2",
          "url": "https://github.com/martinsk/mskql/commit/e9c6b6e296bbce0b46d91602d66febe920786e83"
        },
        "date": 1770928642185,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 14.985,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.052"
          },
          {
            "name": "select_full_scan",
            "value": 82.273,
            "unit": "ms",
            "extra": "iters=200  min=0.394  p50=0.403  p95=0.428  p99=0.694  max=0.971"
          },
          {
            "name": "select_where",
            "value": 103.777,
            "unit": "ms",
            "extra": "iters=500  min=0.201  p50=0.203  p95=0.223  p99=0.273  max=0.371"
          },
          {
            "name": "aggregate",
            "value": 196.415,
            "unit": "ms",
            "extra": "iters=500  min=0.379  p50=0.380  p95=0.408  p99=0.715  max=0.760"
          },
          {
            "name": "order_by",
            "value": 110.839,
            "unit": "ms",
            "extra": "iters=200  min=0.538  p50=0.551  p95=0.576  p99=0.682  max=0.738"
          },
          {
            "name": "join",
            "value": 33.266,
            "unit": "ms",
            "extra": "iters=50  min=0.643  p50=0.657  p95=0.677  p99=0.849  max=1.007"
          },
          {
            "name": "update",
            "value": 23.575,
            "unit": "ms",
            "extra": "iters=200  min=0.115  p50=0.116  p95=0.127  p99=0.138  max=0.139"
          },
          {
            "name": "delete",
            "value": 119.832,
            "unit": "ms",
            "extra": "iters=50  min=2.359  p50=2.391  p95=2.452  p99=2.506  max=2.520"
          },
          {
            "name": "parser",
            "value": 270.605,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.004  p95=0.013  p99=0.013  max=0.041"
          },
          {
            "name": "index_lookup",
            "value": 5.738,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.003  max=0.017"
          },
          {
            "name": "transaction",
            "value": 19.275,
            "unit": "ms",
            "extra": "iters=100  min=0.096  p50=0.189  p95=0.280  p99=0.285  max=0.289"
          },
          {
            "name": "window_functions",
            "value": 4559.276,
            "unit": "ms",
            "extra": "iters=20  min=227.457  p50=227.762  p95=229.097  p99=229.337  max=229.397"
          },
          {
            "name": "distinct",
            "value": 1520.909,
            "unit": "ms",
            "extra": "iters=500  min=3.016  p50=3.042  p95=3.087  p99=3.129  max=3.243"
          },
          {
            "name": "subquery",
            "value": 1626.742,
            "unit": "ms",
            "extra": "iters=50  min=32.394  p50=32.481  p95=32.786  p99=33.742  max=34.611"
          },
          {
            "name": "cte",
            "value": 192.62,
            "unit": "ms",
            "extra": "iters=200  min=0.937  p50=0.963  p95=0.985  p99=1.013  max=1.039"
          },
          {
            "name": "generate_series",
            "value": 144.089,
            "unit": "ms",
            "extra": "iters=200  min=0.707  p50=0.718  p95=0.734  p99=0.779  max=0.810"
          },
          {
            "name": "scalar_functions",
            "value": 243.956,
            "unit": "ms",
            "extra": "iters=200  min=1.205  p50=1.216  p95=1.235  p99=1.294  max=1.389"
          },
          {
            "name": "expression_agg",
            "value": 379.577,
            "unit": "ms",
            "extra": "iters=500  min=0.745  p50=0.756  p95=0.772  p99=0.807  max=1.430"
          },
          {
            "name": "multi_sort",
            "value": 117.657,
            "unit": "ms",
            "extra": "iters=200  min=0.572  p50=0.583  p95=0.600  p99=0.620  max=1.239"
          },
          {
            "name": "set_ops",
            "value": 2005.728,
            "unit": "ms",
            "extra": "iters=50  min=39.913  p50=40.085  p95=40.350  p99=40.698  max=41.006"
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
          "id": "3d7dd75dcf3b2559eca297e11cbf47bb626e7c53",
          "message": "add window function executor with PLAN_WINDOW node support, 12 window functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, SUM/COUNT/AVG with frames), partition/order-by sorting, frame clause handling (UNBOUNDED/CURRENT ROW/N PRECEDING/FOLLOWING), and pgwire column name resolution from select_exprs for window queries\n\nImplement window_next() executor: collect input into flat columnar arrays, sort by partition+order columns using window",
          "timestamp": "2026-02-12T13:07:20-08:00",
          "tree_id": "bc7123f93abdb89110ece469c9fac87ded75230c",
          "url": "https://github.com/martinsk/mskql/commit/3d7dd75dcf3b2559eca297e11cbf47bb626e7c53"
        },
        "date": 1770930473907,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 15.451,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.065"
          },
          {
            "name": "select_full_scan",
            "value": 80.995,
            "unit": "ms",
            "extra": "iters=200  min=0.393  p50=0.399  p95=0.412  p99=0.424  max=0.981"
          },
          {
            "name": "select_where",
            "value": 104.476,
            "unit": "ms",
            "extra": "iters=500  min=0.203  p50=0.206  p95=0.218  p99=0.237  max=0.300"
          },
          {
            "name": "aggregate",
            "value": 194.894,
            "unit": "ms",
            "extra": "iters=500  min=0.380  p50=0.385  p95=0.408  p99=0.427  max=0.667"
          },
          {
            "name": "order_by",
            "value": 162.331,
            "unit": "ms",
            "extra": "iters=200  min=0.747  p50=0.806  p95=0.828  p99=0.882  max=1.335"
          },
          {
            "name": "join",
            "value": 34.206,
            "unit": "ms",
            "extra": "iters=50  min=0.670  p50=0.683  p95=0.706  p99=0.710  max=0.711"
          },
          {
            "name": "update",
            "value": 23.662,
            "unit": "ms",
            "extra": "iters=200  min=0.115  p50=0.115  p95=0.128  p99=0.159  max=0.197"
          },
          {
            "name": "delete",
            "value": 123.597,
            "unit": "ms",
            "extra": "iters=50  min=2.446  p50=2.465  p95=2.525  p99=2.559  max=2.567"
          },
          {
            "name": "parser",
            "value": 292.274,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.014  p99=0.014  max=0.043"
          },
          {
            "name": "index_lookup",
            "value": 6.055,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.003  max=0.013"
          },
          {
            "name": "transaction",
            "value": 19.579,
            "unit": "ms",
            "extra": "iters=100  min=0.098  p50=0.194  p95=0.286  p99=0.292  max=0.299"
          },
          {
            "name": "window_functions",
            "value": 19.064,
            "unit": "ms",
            "extra": "iters=20  min=0.940  p50=0.951  p95=0.965  p99=0.972  max=0.974"
          },
          {
            "name": "distinct",
            "value": 1407.79,
            "unit": "ms",
            "extra": "iters=500  min=2.742  p50=2.818  p95=2.864  p99=3.045  max=5.083"
          },
          {
            "name": "subquery",
            "value": 1624.345,
            "unit": "ms",
            "extra": "iters=50  min=32.406  p50=32.470  p95=32.608  p99=32.748  max=32.829"
          },
          {
            "name": "cte",
            "value": 194.371,
            "unit": "ms",
            "extra": "iters=200  min=0.951  p50=0.966  p95=1.000  p99=1.131  max=1.238"
          },
          {
            "name": "generate_series",
            "value": 145.148,
            "unit": "ms",
            "extra": "iters=200  min=0.700  p50=0.715  p95=0.743  p99=0.897  max=1.392"
          },
          {
            "name": "scalar_functions",
            "value": 248.815,
            "unit": "ms",
            "extra": "iters=200  min=1.223  p50=1.238  p95=1.262  p99=1.315  max=1.900"
          },
          {
            "name": "expression_agg",
            "value": 372.791,
            "unit": "ms",
            "extra": "iters=500  min=0.733  p50=0.743  p95=0.762  p99=0.776  max=0.997"
          },
          {
            "name": "multi_sort",
            "value": 116.584,
            "unit": "ms",
            "extra": "iters=200  min=0.569  p50=0.581  p95=0.601  p99=0.689  max=0.759"
          },
          {
            "name": "set_ops",
            "value": 2104.072,
            "unit": "ms",
            "extra": "iters=50  min=41.842  p50=41.943  p95=42.826  p99=44.427  max=44.769"
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
          "id": "1569ea037bf59385b2ad40c7b430d15fc0adff75",
          "message": "add hash semi-join executor for IN subqueries, set operation executor (UNION/INTERSECT/EXCEPT with ALL variants), DISTINCT executor, and generate_series table function support with column alias handling in pgwire RowDescription\n\nImplement hash_semi_join_next() with hash table build phase on inner side and probe phase filtering outer rows by key match. Add set_op_next() with hash-based deduplication for UNION/INTERSECT/EXCEPT, supporting ALL variants with match counting. Add distinct_next() using",
          "timestamp": "2026-02-12T15:17:58-08:00",
          "tree_id": "6414884b3934dfac01960de9e8b03b4d86d51c55",
          "url": "https://github.com/martinsk/mskql/commit/1569ea037bf59385b2ad40c7b430d15fc0adff75"
        },
        "date": 1770938310282,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 11.886,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.042"
          },
          {
            "name": "select_full_scan",
            "value": 71.78,
            "unit": "ms",
            "extra": "iters=200  min=0.346  p50=0.355  p95=0.366  p99=0.379  max=0.743"
          },
          {
            "name": "select_where",
            "value": 92.501,
            "unit": "ms",
            "extra": "iters=500  min=0.180  p50=0.183  p95=0.192  p99=0.200  max=0.261"
          },
          {
            "name": "aggregate",
            "value": 174.322,
            "unit": "ms",
            "extra": "iters=500  min=0.340  p50=0.344  p95=0.357  p99=0.399  max=0.618"
          },
          {
            "name": "order_by",
            "value": 141.852,
            "unit": "ms",
            "extra": "iters=200  min=0.685  p50=0.704  p95=0.735  p99=0.807  max=0.987"
          },
          {
            "name": "join",
            "value": 30.052,
            "unit": "ms",
            "extra": "iters=50  min=0.568  p50=0.590  p95=0.673  p99=0.805  max=0.871"
          },
          {
            "name": "update",
            "value": 19.935,
            "unit": "ms",
            "extra": "iters=200  min=0.097  p50=0.099  p95=0.104  p99=0.108  max=0.122"
          },
          {
            "name": "delete",
            "value": 90.2,
            "unit": "ms",
            "extra": "iters=50  min=1.790  p50=1.798  p95=1.837  p99=1.904  max=1.941"
          },
          {
            "name": "parser",
            "value": 203.902,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.003  p95=0.010  p99=0.010  max=0.034"
          },
          {
            "name": "index_lookup",
            "value": 4.745,
            "unit": "ms",
            "extra": "iters=2000  min=0.002  p50=0.002  p95=0.002  p99=0.004  max=0.011"
          },
          {
            "name": "transaction",
            "value": 17.258,
            "unit": "ms",
            "extra": "iters=100  min=0.083  p50=0.168  p95=0.261  p99=0.268  max=0.274"
          },
          {
            "name": "window_functions",
            "value": 17.298,
            "unit": "ms",
            "extra": "iters=20  min=0.846  p50=0.862  p95=0.882  p99=0.915  max=0.923"
          },
          {
            "name": "distinct",
            "value": 60.106,
            "unit": "ms",
            "extra": "iters=500  min=0.117  p50=0.119  p95=0.128  p99=0.141  max=0.171"
          },
          {
            "name": "subquery",
            "value": 1738.02,
            "unit": "ms",
            "extra": "iters=50  min=34.034  p50=34.782  p95=35.118  p99=35.210  max=35.233"
          },
          {
            "name": "cte",
            "value": 178.275,
            "unit": "ms",
            "extra": "iters=200  min=0.867  p50=0.889  p95=0.909  p99=0.955  max=1.050"
          },
          {
            "name": "generate_series",
            "value": 132.958,
            "unit": "ms",
            "extra": "iters=200  min=0.644  p50=0.663  p95=0.682  p99=0.717  max=0.763"
          },
          {
            "name": "scalar_functions",
            "value": 217.634,
            "unit": "ms",
            "extra": "iters=200  min=1.076  p50=1.085  p95=1.109  p99=1.156  max=1.204"
          },
          {
            "name": "expression_agg",
            "value": 329.069,
            "unit": "ms",
            "extra": "iters=500  min=0.645  p50=0.657  p95=0.667  p99=0.709  max=0.803"
          },
          {
            "name": "multi_sort",
            "value": 118.236,
            "unit": "ms",
            "extra": "iters=200  min=0.581  p50=0.590  p95=0.601  p99=0.626  max=0.687"
          },
          {
            "name": "set_ops",
            "value": 1853.765,
            "unit": "ms",
            "extra": "iters=50  min=36.534  p50=37.086  p95=37.707  p99=37.813  max=37.814"
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
          "id": "3258cb201c81e03e0e93c817cbdbffb3fee7f78c",
          "message": "add CTE materialization to plan executor, expose materialize_subquery/remove_temp_table in database.h, add EXPR_FUNC_CALL/EXPR_CAST column name resolution in pgwire RowDescription, implement PLAN_EXPR_PROJECT executor for arbitrary expression evaluation per row, add text comparison fast path in filter_next, and move row ownership in materialize_subquery instead of deep copy\n\nMaterialize CTEs (non-recursive) into temporary tables before plan_build_select in try_plan_send(), save/restore CTE fields",
          "timestamp": "2026-02-12T15:47:18-08:00",
          "tree_id": "b2981bc4a1a11c4bb935351135bf37d55367bf84",
          "url": "https://github.com/martinsk/mskql/commit/3258cb201c81e03e0e93c817cbdbffb3fee7f78c"
        },
        "date": 1770940067146,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 15.214,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.054"
          },
          {
            "name": "select_full_scan",
            "value": 80.589,
            "unit": "ms",
            "extra": "iters=200  min=0.386  p50=0.394  p95=0.421  p99=0.685  max=0.955"
          },
          {
            "name": "select_where",
            "value": 102.505,
            "unit": "ms",
            "extra": "iters=500  min=0.200  p50=0.202  p95=0.212  p99=0.232  max=0.363"
          },
          {
            "name": "aggregate",
            "value": 195.89,
            "unit": "ms",
            "extra": "iters=500  min=0.385  p50=0.387  p95=0.408  p99=0.421  max=0.611"
          },
          {
            "name": "order_by",
            "value": 159.765,
            "unit": "ms",
            "extra": "iters=200  min=0.741  p50=0.795  p95=0.818  p99=0.859  max=1.051"
          },
          {
            "name": "join",
            "value": 33.533,
            "unit": "ms",
            "extra": "iters=50  min=0.659  p50=0.670  p95=0.682  p99=0.693  max=0.701"
          },
          {
            "name": "update",
            "value": 24.806,
            "unit": "ms",
            "extra": "iters=200  min=0.122  p50=0.123  p95=0.132  p99=0.132  max=0.147"
          },
          {
            "name": "delete",
            "value": 120.83,
            "unit": "ms",
            "extra": "iters=50  min=2.396  p50=2.405  p95=2.500  p99=2.537  max=2.553"
          },
          {
            "name": "parser",
            "value": 271.069,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.004  p95=0.013  p99=0.013  max=0.041"
          },
          {
            "name": "index_lookup",
            "value": 5.768,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.003  max=0.013"
          },
          {
            "name": "transaction",
            "value": 19.452,
            "unit": "ms",
            "extra": "iters=100  min=0.097  p50=0.196  p95=0.283  p99=0.297  max=0.299"
          },
          {
            "name": "window_functions",
            "value": 18.996,
            "unit": "ms",
            "extra": "iters=20  min=0.933  p50=0.942  p95=0.974  p99=1.043  max=1.060"
          },
          {
            "name": "distinct",
            "value": 66.071,
            "unit": "ms",
            "extra": "iters=500  min=0.129  p50=0.130  p95=0.140  p99=0.152  max=0.195"
          },
          {
            "name": "subquery",
            "value": 1683.752,
            "unit": "ms",
            "extra": "iters=50  min=33.588  p50=33.657  p95=33.873  p99=33.961  max=33.986"
          },
          {
            "name": "cte",
            "value": 121.044,
            "unit": "ms",
            "extra": "iters=200  min=0.589  p50=0.603  p95=0.626  p99=0.677  max=0.810"
          },
          {
            "name": "generate_series",
            "value": 137.73,
            "unit": "ms",
            "extra": "iters=200  min=0.678  p50=0.688  p95=0.702  p99=0.720  max=0.868"
          },
          {
            "name": "scalar_functions",
            "value": 262.08,
            "unit": "ms",
            "extra": "iters=200  min=1.293  p50=1.301  p95=1.329  p99=1.400  max=2.214"
          },
          {
            "name": "expression_agg",
            "value": 392.374,
            "unit": "ms",
            "extra": "iters=500  min=0.774  p50=0.784  p95=0.795  p99=0.825  max=0.933"
          },
          {
            "name": "multi_sort",
            "value": 115.538,
            "unit": "ms",
            "extra": "iters=200  min=0.563  p50=0.574  p95=0.595  p99=0.651  max=0.951"
          },
          {
            "name": "set_ops",
            "value": 1922.843,
            "unit": "ms",
            "extra": "iters=50  min=38.225  p50=38.356  p95=38.685  p99=40.313  max=41.255"
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
          "id": "37ff1ed19a3e52df016226971c433330c0a0eafa",
          "message": "add release build target with -O3/-flto, switch default CFLAGS to -O1/-fsanitize=address, pass bump allocator through query execution paths, fix memory leaks in eval_condition by cleaning up lhs_tmp, and make ALTER column name/enum_type_name bump-allocated instead of heap-allocated\n\nAdd `make release` target that builds with RELEASE_CFLAGS (-O3 -flto -DNDEBUG) into separate rel_*.o objects. Change default CFLAGS to -O1 with AddressSanitizer for development. Make `bench` depend on `release` target.",
          "timestamp": "2026-02-12T16:52:27-08:00",
          "tree_id": "803f2be61c734935af74267e6d60083e921a5be0",
          "url": "https://github.com/martinsk/mskql/commit/37ff1ed19a3e52df016226971c433330c0a0eafa"
        },
        "date": 1770944002416,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 15.272,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.051"
          },
          {
            "name": "select_full_scan",
            "value": 80.222,
            "unit": "ms",
            "extra": "iters=200  min=0.389  p50=0.397  p95=0.409  p99=0.431  max=0.964"
          },
          {
            "name": "select_where",
            "value": 109.916,
            "unit": "ms",
            "extra": "iters=500  min=0.201  p50=0.204  p95=0.384  p99=0.411  max=0.419"
          },
          {
            "name": "aggregate",
            "value": 201.937,
            "unit": "ms",
            "extra": "iters=500  min=0.397  p50=0.400  p95=0.416  p99=0.432  max=0.489"
          },
          {
            "name": "order_by",
            "value": 160.992,
            "unit": "ms",
            "extra": "iters=200  min=0.733  p50=0.798  p95=0.823  p99=0.953  max=1.137"
          },
          {
            "name": "join",
            "value": 33.638,
            "unit": "ms",
            "extra": "iters=50  min=0.655  p50=0.673  p95=0.688  p99=0.702  max=0.711"
          },
          {
            "name": "update",
            "value": 25.328,
            "unit": "ms",
            "extra": "iters=200  min=0.125  p50=0.125  p95=0.137  p99=0.139  max=0.140"
          },
          {
            "name": "delete",
            "value": 123.177,
            "unit": "ms",
            "extra": "iters=50  min=2.446  p50=2.458  p95=2.476  p99=2.583  max=2.627"
          },
          {
            "name": "parser",
            "value": 290.672,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.014  p99=0.014  max=0.041"
          },
          {
            "name": "index_lookup",
            "value": 6.16,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.005  max=0.012"
          },
          {
            "name": "transaction",
            "value": 19.492,
            "unit": "ms",
            "extra": "iters=100  min=0.098  p50=0.192  p95=0.286  p99=0.291  max=0.292"
          },
          {
            "name": "window_functions",
            "value": 18.786,
            "unit": "ms",
            "extra": "iters=20  min=0.926  p50=0.938  p95=0.949  p99=0.950  max=0.950"
          },
          {
            "name": "distinct",
            "value": 67.568,
            "unit": "ms",
            "extra": "iters=500  min=0.130  p50=0.133  p95=0.143  p99=0.159  max=0.215"
          },
          {
            "name": "subquery",
            "value": 1769.89,
            "unit": "ms",
            "extra": "iters=50  min=35.288  p50=35.377  p95=35.580  p99=35.605  max=35.613"
          },
          {
            "name": "cte",
            "value": 120.741,
            "unit": "ms",
            "extra": "iters=200  min=0.591  p50=0.603  p95=0.616  p99=0.652  max=0.688"
          },
          {
            "name": "generate_series",
            "value": 138.63,
            "unit": "ms",
            "extra": "iters=200  min=0.679  p50=0.689  p95=0.708  p99=0.790  max=0.931"
          },
          {
            "name": "scalar_functions",
            "value": 254.803,
            "unit": "ms",
            "extra": "iters=200  min=1.258  p50=1.269  p95=1.297  p99=1.355  max=1.593"
          },
          {
            "name": "expression_agg",
            "value": 398.194,
            "unit": "ms",
            "extra": "iters=500  min=0.776  p50=0.796  p95=0.810  p99=0.853  max=0.963"
          },
          {
            "name": "multi_sort",
            "value": 115.55,
            "unit": "ms",
            "extra": "iters=200  min=0.566  p50=0.577  p95=0.591  p99=0.625  max=0.676"
          },
          {
            "name": "set_ops",
            "value": 1915.06,
            "unit": "ms",
            "extra": "iters=50  min=38.113  p50=38.261  p95=38.512  p99=38.836  max=38.990"
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
          "id": "41f247bf5ce53693b61f8a8076e238699b6b0f40",
          "message": "increment CTE generation counter on recursive iteration to invalidate cached plans, and zero entire cell value union instead of only as_int field in expr_project_next null handling",
          "timestamp": "2026-02-12T17:48:56-08:00",
          "tree_id": "06ab6b38743049046075d073e949e4bb5b0afbf9",
          "url": "https://github.com/martinsk/mskql/commit/41f247bf5ce53693b61f8a8076e238699b6b0f40"
        },
        "date": 1770947383560,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.031,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.057"
          },
          {
            "name": "select_full_scan",
            "value": 75.752,
            "unit": "ms",
            "extra": "iters=200  min=0.356  p50=0.364  p95=0.463  p99=0.590  max=0.918"
          },
          {
            "name": "select_where",
            "value": 92.072,
            "unit": "ms",
            "extra": "iters=500  min=0.177  p50=0.181  p95=0.194  p99=0.258  max=0.285"
          },
          {
            "name": "aggregate",
            "value": 185.964,
            "unit": "ms",
            "extra": "iters=500  min=0.365  p50=0.368  p95=0.386  p99=0.406  max=0.453"
          },
          {
            "name": "order_by",
            "value": 148.149,
            "unit": "ms",
            "extra": "iters=200  min=0.688  p50=0.708  p95=1.104  p99=1.261  max=1.275"
          },
          {
            "name": "join",
            "value": 28.913,
            "unit": "ms",
            "extra": "iters=50  min=0.559  p50=0.576  p95=0.599  p99=0.614  max=0.615"
          },
          {
            "name": "update",
            "value": 20.133,
            "unit": "ms",
            "extra": "iters=200  min=0.098  p50=0.099  p95=0.108  p99=0.112  max=0.120"
          },
          {
            "name": "delete",
            "value": 96.892,
            "unit": "ms",
            "extra": "iters=50  min=1.920  p50=1.927  p95=1.948  p99=2.164  max=2.338"
          },
          {
            "name": "parser",
            "value": 226.081,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.004  p95=0.011  p99=0.011  max=0.034"
          },
          {
            "name": "index_lookup",
            "value": 5.024,
            "unit": "ms",
            "extra": "iters=2000  min=0.002  p50=0.002  p95=0.003  p99=0.003  max=0.012"
          },
          {
            "name": "transaction",
            "value": 17.705,
            "unit": "ms",
            "extra": "iters=100  min=0.087  p50=0.174  p95=0.265  p99=0.272  max=0.277"
          },
          {
            "name": "window_functions",
            "value": 17.748,
            "unit": "ms",
            "extra": "iters=20  min=0.866  p50=0.878  p95=0.925  p99=0.996  max=1.014"
          },
          {
            "name": "distinct",
            "value": 63.393,
            "unit": "ms",
            "extra": "iters=500  min=0.118  p50=0.120  p95=0.199  p99=0.220  max=0.236"
          },
          {
            "name": "subquery",
            "value": 1583.844,
            "unit": "ms",
            "extra": "iters=50  min=31.372  p50=31.609  p95=32.076  p99=32.959  max=33.440"
          },
          {
            "name": "cte",
            "value": 111.834,
            "unit": "ms",
            "extra": "iters=200  min=0.535  p50=0.551  p95=0.595  p99=0.704  max=0.919"
          },
          {
            "name": "generate_series",
            "value": 133.77,
            "unit": "ms",
            "extra": "iters=200  min=0.642  p50=0.665  p95=0.691  p99=0.718  max=0.907"
          },
          {
            "name": "scalar_functions",
            "value": 221.619,
            "unit": "ms",
            "extra": "iters=200  min=1.093  p50=1.104  p95=1.122  p99=1.157  max=1.368"
          },
          {
            "name": "expression_agg",
            "value": 351.039,
            "unit": "ms",
            "extra": "iters=500  min=0.690  p50=0.701  p95=0.714  p99=0.736  max=0.770"
          },
          {
            "name": "multi_sort",
            "value": 119.132,
            "unit": "ms",
            "extra": "iters=200  min=0.581  p50=0.592  p95=0.608  p99=0.662  max=0.844"
          },
          {
            "name": "set_ops",
            "value": 1807.039,
            "unit": "ms",
            "extra": "iters=50  min=35.739  p50=36.114  p95=36.326  p99=37.701  max=38.906"
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
          "id": "8832ad59a27ccb781271c84ec1489895f9310a6f",
          "message": "add SMALLINT/INT2 type support, EXPLAIN command, and extended foreign key actions (RESTRICT/SET NULL/SET DEFAULT)\n\nAdd COLUMN_TYPE_SMALLINT with int16_t storage in col_block union, hash/equality helpers in block.h, cell conversion in database.c/plan.c, PostgreSQL wire protocol OID 21, and parser recognition of SMALLINT/INT2/SMALLSERIAL/SERIAL2 keywords with SERIAL variant detection in CREATE TABLE.\n\nImplement QUERY_TYPE_EXPLAIN that parses inner SQL into temporary query, builds SELECT plan if applicable",
          "timestamp": "2026-02-12T19:20:51-08:00",
          "tree_id": "ea710e81ffe4c1bd273facbb125f88a855f620e4",
          "url": "https://github.com/martinsk/mskql/commit/8832ad59a27ccb781271c84ec1489895f9310a6f"
        },
        "date": 1770952898624,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 15.249,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.063"
          },
          {
            "name": "select_full_scan",
            "value": 80.371,
            "unit": "ms",
            "extra": "iters=200  min=0.392  p50=0.397  p95=0.409  p99=0.436  max=0.965"
          },
          {
            "name": "select_where",
            "value": 102.801,
            "unit": "ms",
            "extra": "iters=500  min=0.200  p50=0.202  p95=0.215  p99=0.233  max=0.280"
          },
          {
            "name": "aggregate",
            "value": 190.73,
            "unit": "ms",
            "extra": "iters=500  min=0.374  p50=0.377  p95=0.397  p99=0.417  max=0.432"
          },
          {
            "name": "order_by",
            "value": 169.228,
            "unit": "ms",
            "extra": "iters=200  min=0.767  p50=0.842  p95=0.865  p99=0.913  max=0.968"
          },
          {
            "name": "join",
            "value": 32.972,
            "unit": "ms",
            "extra": "iters=50  min=0.647  p50=0.660  p95=0.667  p99=0.671  max=0.673"
          },
          {
            "name": "update",
            "value": 26.232,
            "unit": "ms",
            "extra": "iters=200  min=0.128  p50=0.129  p95=0.141  p99=0.163  max=0.185"
          },
          {
            "name": "delete",
            "value": 120.567,
            "unit": "ms",
            "extra": "iters=50  min=2.396  p50=2.408  p95=2.431  p99=2.489  max=2.514"
          },
          {
            "name": "parser",
            "value": 275.821,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.004  p95=0.013  p99=0.013  max=0.033"
          },
          {
            "name": "index_lookup",
            "value": 5.996,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.004  max=0.014"
          },
          {
            "name": "transaction",
            "value": 19.333,
            "unit": "ms",
            "extra": "iters=100  min=0.096  p50=0.191  p95=0.280  p99=0.291  max=0.297"
          },
          {
            "name": "window_functions",
            "value": 21.457,
            "unit": "ms",
            "extra": "iters=20  min=1.063  p50=1.069  p95=1.096  p99=1.100  max=1.102"
          },
          {
            "name": "distinct",
            "value": 65.949,
            "unit": "ms",
            "extra": "iters=500  min=0.127  p50=0.127  p95=0.147  p99=0.214  max=0.279"
          },
          {
            "name": "subquery",
            "value": 1927.302,
            "unit": "ms",
            "extra": "iters=50  min=38.220  p50=38.331  p95=39.179  p99=42.291  max=43.971"
          },
          {
            "name": "cte",
            "value": 119.715,
            "unit": "ms",
            "extra": "iters=200  min=0.586  p50=0.598  p95=0.617  p99=0.637  max=0.654"
          },
          {
            "name": "generate_series",
            "value": 139.503,
            "unit": "ms",
            "extra": "iters=200  min=0.683  p50=0.694  p95=0.711  p99=0.775  max=0.919"
          },
          {
            "name": "scalar_functions",
            "value": 260.516,
            "unit": "ms",
            "extra": "iters=200  min=1.276  p50=1.299  p95=1.333  p99=1.383  max=1.494"
          },
          {
            "name": "expression_agg",
            "value": 380.591,
            "unit": "ms",
            "extra": "iters=500  min=0.748  p50=0.759  p95=0.778  p99=0.801  max=0.963"
          },
          {
            "name": "multi_sort",
            "value": 123.536,
            "unit": "ms",
            "extra": "iters=200  min=0.607  p50=0.618  p95=0.632  p99=0.670  max=0.694"
          },
          {
            "name": "set_ops",
            "value": 2102.588,
            "unit": "ms",
            "extra": "iters=50  min=41.898  p50=42.038  p95=42.200  p99=42.266  max=42.286"
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
          "id": "117164b4d81f9beb8f8afbe8b8e8e3c4cdaad56d",
          "message": "add 100 adversarial test cases covering edge cases: empty tables, NULL handling, type casts, division by zero, string operations, aggregates, joins, subqueries, CTEs, window functions, generate_series, date/time, ALTER TABLE, transactions, indexes, foreign keys, LIMIT/OFFSET, LIKE patterns, BETWEEN, CASE expressions, and COALESCE\n\nNew tests verify: empty table operations (SELECT/aggregates/ORDER BY/GROUP BY/JOIN), NULL behavior (DISTINCT/GROUP BY/COUNT DISTINCT/comparisons/arithmetic/COALESCE/string",
          "timestamp": "2026-02-12T19:41:36-08:00",
          "tree_id": "b31863fa51dcd362c6cc35e5a82cd724dfc9726d",
          "url": "https://github.com/martinsk/mskql/commit/117164b4d81f9beb8f8afbe8b8e8e3c4cdaad56d"
        },
        "date": 1770954142359,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 15.253,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.053"
          },
          {
            "name": "select_full_scan",
            "value": 81.195,
            "unit": "ms",
            "extra": "iters=200  min=0.392  p50=0.398  p95=0.412  p99=0.535  max=0.964"
          },
          {
            "name": "select_where",
            "value": 104.115,
            "unit": "ms",
            "extra": "iters=500  min=0.201  p50=0.203  p95=0.221  p99=0.344  max=0.378"
          },
          {
            "name": "aggregate",
            "value": 193.307,
            "unit": "ms",
            "extra": "iters=500  min=0.377  p50=0.381  p95=0.401  p99=0.462  max=0.513"
          },
          {
            "name": "order_by",
            "value": 168.605,
            "unit": "ms",
            "extra": "iters=200  min=0.781  p50=0.839  p95=0.856  p99=1.024  max=1.127"
          },
          {
            "name": "join",
            "value": 33.304,
            "unit": "ms",
            "extra": "iters=50  min=0.649  p50=0.667  p95=0.677  p99=0.684  max=0.687"
          },
          {
            "name": "update",
            "value": 26.143,
            "unit": "ms",
            "extra": "iters=200  min=0.128  p50=0.129  p95=0.141  p99=0.143  max=0.144"
          },
          {
            "name": "delete",
            "value": 122.12,
            "unit": "ms",
            "extra": "iters=50  min=2.396  p50=2.409  p95=2.569  p99=2.983  max=3.339"
          },
          {
            "name": "parser",
            "value": 275.098,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.004  p95=0.013  p99=0.013  max=0.070"
          },
          {
            "name": "index_lookup",
            "value": 5.936,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.003  max=0.012"
          },
          {
            "name": "transaction",
            "value": 19.272,
            "unit": "ms",
            "extra": "iters=100  min=0.096  p50=0.193  p95=0.280  p99=0.287  max=0.299"
          },
          {
            "name": "window_functions",
            "value": 21.363,
            "unit": "ms",
            "extra": "iters=20  min=1.059  p50=1.064  p95=1.088  p99=1.093  max=1.094"
          },
          {
            "name": "distinct",
            "value": 65.032,
            "unit": "ms",
            "extra": "iters=500  min=0.126  p50=0.128  p95=0.138  p99=0.144  max=0.183"
          },
          {
            "name": "subquery",
            "value": 1930.156,
            "unit": "ms",
            "extra": "iters=50  min=38.226  p50=38.375  p95=38.714  p99=43.947  max=48.917"
          },
          {
            "name": "cte",
            "value": 122.232,
            "unit": "ms",
            "extra": "iters=200  min=0.596  p50=0.608  p95=0.633  p99=0.658  max=0.708"
          },
          {
            "name": "generate_series",
            "value": 140.629,
            "unit": "ms",
            "extra": "iters=200  min=0.688  p50=0.701  p95=0.719  p99=0.737  max=0.890"
          },
          {
            "name": "scalar_functions",
            "value": 266.092,
            "unit": "ms",
            "extra": "iters=200  min=1.296  p50=1.328  p95=1.357  p99=1.425  max=1.456"
          },
          {
            "name": "expression_agg",
            "value": 394.616,
            "unit": "ms",
            "extra": "iters=500  min=0.764  p50=0.789  p95=0.803  p99=0.860  max=0.883"
          },
          {
            "name": "multi_sort",
            "value": 123.892,
            "unit": "ms",
            "extra": "iters=200  min=0.608  p50=0.618  p95=0.641  p99=0.695  max=0.759"
          },
          {
            "name": "set_ops",
            "value": 2108.868,
            "unit": "ms",
            "extra": "iters=50  min=41.963  p50=42.128  p95=42.324  p99=43.366  max=44.303"
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
          "id": "961e3b355976f87360f59282e9ec0dbeeed1d32d",
          "message": "add COPY FROM/TO STDIN/STDOUT support, STRING_AGG/ARRAY_AGG aggregate functions, and CHECK constraint SQL text storage\n\nImplement COPY protocol: parse COPY table FROM/TO STDIN/STDOUT [WITH CSV [HEADER]] commands, send CopyOutResponse/CopyInResponse messages, handle CopyData/CopyDone/CopyFail messages in pgwire, buffer line-by-line input with delimiter detection (comma for CSV, tab for text), parse fields into cells with NULL handling (\\N for text format, empty for CSV), and send data rows with proper formatting.",
          "timestamp": "2026-02-12T20:53:49-08:00",
          "tree_id": "841d7866efa9026c4601dc21a538949537bb60e1",
          "url": "https://github.com/martinsk/mskql/commit/961e3b355976f87360f59282e9ec0dbeeed1d32d"
        },
        "date": 1770958476703,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 16.11,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.003  p99=0.003  max=0.053"
          },
          {
            "name": "select_full_scan",
            "value": 80.252,
            "unit": "ms",
            "extra": "iters=200  min=0.389  p50=0.396  p95=0.411  p99=0.433  max=0.983"
          },
          {
            "name": "select_where",
            "value": 144.353,
            "unit": "ms",
            "extra": "iters=500  min=0.201  p50=0.223  p95=0.410  p99=0.423  max=0.436"
          },
          {
            "name": "aggregate",
            "value": 209.909,
            "unit": "ms",
            "extra": "iters=500  min=0.411  p50=0.415  p95=0.432  p99=0.480  max=0.604"
          },
          {
            "name": "order_by",
            "value": 172.502,
            "unit": "ms",
            "extra": "iters=200  min=0.784  p50=0.846  p95=0.927  p99=1.358  max=1.483"
          },
          {
            "name": "join",
            "value": 33.976,
            "unit": "ms",
            "extra": "iters=50  min=0.663  p50=0.681  p95=0.694  p99=0.701  max=0.702"
          },
          {
            "name": "update",
            "value": 27.418,
            "unit": "ms",
            "extra": "iters=200  min=0.134  p50=0.135  p95=0.145  p99=0.154  max=0.175"
          },
          {
            "name": "delete",
            "value": 130.087,
            "unit": "ms",
            "extra": "iters=50  min=2.580  p50=2.598  p95=2.632  p99=2.642  max=2.649"
          },
          {
            "name": "parser",
            "value": 306.69,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.015  p99=0.015  max=0.040"
          },
          {
            "name": "index_lookup",
            "value": 6.428,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.003  max=0.013"
          },
          {
            "name": "transaction",
            "value": 20.001,
            "unit": "ms",
            "extra": "iters=100  min=0.101  p50=0.198  p95=0.289  p99=0.295  max=0.312"
          },
          {
            "name": "window_functions",
            "value": 22.893,
            "unit": "ms",
            "extra": "iters=20  min=1.072  p50=1.085  p95=1.605  p99=1.676  max=1.693"
          },
          {
            "name": "distinct",
            "value": 64.466,
            "unit": "ms",
            "extra": "iters=500  min=0.126  p50=0.126  p95=0.138  p99=0.152  max=0.197"
          },
          {
            "name": "subquery",
            "value": 1982.101,
            "unit": "ms",
            "extra": "iters=50  min=39.388  p50=39.477  p95=39.735  p99=43.393  max=46.851"
          },
          {
            "name": "cte",
            "value": 121.46,
            "unit": "ms",
            "extra": "iters=200  min=0.594  p50=0.605  p95=0.632  p99=0.654  max=0.674"
          },
          {
            "name": "generate_series",
            "value": 140.322,
            "unit": "ms",
            "extra": "iters=200  min=0.689  p50=0.699  p95=0.715  p99=0.760  max=0.870"
          },
          {
            "name": "scalar_functions",
            "value": 264.27,
            "unit": "ms",
            "extra": "iters=200  min=1.303  p50=1.315  p95=1.348  p99=1.422  max=1.647"
          },
          {
            "name": "expression_agg",
            "value": 409.122,
            "unit": "ms",
            "extra": "iters=500  min=0.800  p50=0.816  p95=0.836  p99=0.854  max=1.368"
          },
          {
            "name": "multi_sort",
            "value": 122.951,
            "unit": "ms",
            "extra": "iters=200  min=0.603  p50=0.614  p95=0.626  p99=0.695  max=0.776"
          },
          {
            "name": "set_ops",
            "value": 2019.781,
            "unit": "ms",
            "extra": "iters=50  min=40.117  p50=40.328  p95=40.615  p99=41.934  max=43.101"
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
          "id": "00991a8146cd5f62d7ce7cf7b1c146e08e0cf5d7",
          "message": "add IF NOT EXISTS support for CREATE TABLE/INDEX, implement SET/RESET/DISCARD/SHOW commands, and add system catalog query interception for information_schema and pg_catalog tables\n\nParse IF NOT EXISTS clause in CREATE TABLE/INDEX statements, check existence before creating (return early if exists). Add QUERY_TYPE_SET (no-op) and QUERY_TYPE_SHOW with hardcoded responses for common parameters (search_path, server_version, server_encoding, client_encoding, standard_conforming_strings, is_superuser, Time",
          "timestamp": "2026-02-12T22:18:19-08:00",
          "tree_id": "4b69cc3182b75e51fa61257ec6891c5ef15fecd9",
          "url": "https://github.com/martinsk/mskql/commit/00991a8146cd5f62d7ce7cf7b1c146e08e0cf5d7"
        },
        "date": 1770963547662,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 16.43,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.054"
          },
          {
            "name": "select_full_scan",
            "value": 80.801,
            "unit": "ms",
            "extra": "iters=200  min=0.395  p50=0.398  p95=0.409  p99=0.422  max=0.956"
          },
          {
            "name": "select_where",
            "value": 105.479,
            "unit": "ms",
            "extra": "iters=500  min=0.204  p50=0.206  p95=0.224  p99=0.301  max=0.356"
          },
          {
            "name": "aggregate",
            "value": 197.68,
            "unit": "ms",
            "extra": "iters=500  min=0.387  p50=0.392  p95=0.410  p99=0.422  max=0.502"
          },
          {
            "name": "order_by",
            "value": 166.874,
            "unit": "ms",
            "extra": "iters=200  min=0.786  p50=0.833  p95=0.856  p99=0.860  max=0.881"
          },
          {
            "name": "join",
            "value": 33.923,
            "unit": "ms",
            "extra": "iters=50  min=0.648  p50=0.664  p95=0.691  p99=1.000  max=1.129"
          },
          {
            "name": "update",
            "value": 27.187,
            "unit": "ms",
            "extra": "iters=200  min=0.132  p50=0.133  p95=0.147  p99=0.162  max=0.169"
          },
          {
            "name": "delete",
            "value": 134.32,
            "unit": "ms",
            "extra": "iters=50  min=2.666  p50=2.681  p95=2.731  p99=2.759  max=2.761"
          },
          {
            "name": "parser",
            "value": 314.577,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.015  p99=0.015  max=0.037"
          },
          {
            "name": "index_lookup",
            "value": 6.583,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.005  max=0.019"
          },
          {
            "name": "transaction",
            "value": 19.978,
            "unit": "ms",
            "extra": "iters=100  min=0.104  p50=0.199  p95=0.288  p99=0.294  max=0.300"
          },
          {
            "name": "window_functions",
            "value": 21.464,
            "unit": "ms",
            "extra": "iters=20  min=1.065  p50=1.072  p95=1.086  p99=1.088  max=1.088"
          },
          {
            "name": "distinct",
            "value": 64.706,
            "unit": "ms",
            "extra": "iters=500  min=0.126  p50=0.127  p95=0.138  p99=0.158  max=0.209"
          },
          {
            "name": "subquery",
            "value": 1974.381,
            "unit": "ms",
            "extra": "iters=50  min=39.395  p50=39.462  p95=39.693  p99=39.773  max=39.781"
          },
          {
            "name": "cte",
            "value": 120.167,
            "unit": "ms",
            "extra": "iters=200  min=0.590  p50=0.601  p95=0.611  p99=0.632  max=0.678"
          },
          {
            "name": "generate_series",
            "value": 141.432,
            "unit": "ms",
            "extra": "iters=200  min=0.694  p50=0.705  p95=0.717  p99=0.782  max=0.956"
          },
          {
            "name": "scalar_functions",
            "value": 252.543,
            "unit": "ms",
            "extra": "iters=200  min=1.251  p50=1.256  p95=1.279  p99=1.339  max=1.488"
          },
          {
            "name": "expression_agg",
            "value": 388.927,
            "unit": "ms",
            "extra": "iters=500  min=0.749  p50=0.778  p95=0.794  p99=0.843  max=1.007"
          },
          {
            "name": "multi_sort",
            "value": 124.631,
            "unit": "ms",
            "extra": "iters=200  min=0.607  p50=0.622  p95=0.640  p99=0.708  max=0.728"
          },
          {
            "name": "set_ops",
            "value": 2118.763,
            "unit": "ms",
            "extra": "iters=50  min=41.929  p50=42.139  p95=42.716  p99=47.723  max=52.349"
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
          "id": "68147d9b8bbf2be5262bb1e0cb6ae4938f052f53",
          "message": "add CROSS JOIN row limit (10M), subquery depth limit (32), query size limit (1MB), function argument limit (16), integer overflow detection in number parsing, dynamic allocation for large strings in lexer/COPY/parameter substitution, LIKE pattern matching optimization from exponential to O(n*m), negative LIMIT/OFFSET clamping, and memory leak fixes in LATERAL join rewriting and parameter substitution error paths",
          "timestamp": "2026-02-13T16:31:20-08:00",
          "tree_id": "959b59219625b7285bee6b70b692b59e5babd365",
          "url": "https://github.com/martinsk/mskql/commit/68147d9b8bbf2be5262bb1e0cb6ae4938f052f53"
        },
        "date": 1771029125508,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 16.25,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.004  max=0.055"
          },
          {
            "name": "select_full_scan",
            "value": 79.776,
            "unit": "ms",
            "extra": "iters=200  min=0.388  p50=0.395  p95=0.405  p99=0.416  max=0.970"
          },
          {
            "name": "select_where",
            "value": 103.345,
            "unit": "ms",
            "extra": "iters=500  min=0.201  p50=0.203  p95=0.217  p99=0.240  max=0.281"
          },
          {
            "name": "aggregate",
            "value": 202.219,
            "unit": "ms",
            "extra": "iters=500  min=0.395  p50=0.400  p95=0.419  p99=0.449  max=0.580"
          },
          {
            "name": "order_by",
            "value": 166.514,
            "unit": "ms",
            "extra": "iters=200  min=0.766  p50=0.825  p95=0.846  p99=1.044  max=1.327"
          },
          {
            "name": "join",
            "value": 33.956,
            "unit": "ms",
            "extra": "iters=50  min=0.669  p50=0.679  p95=0.692  p99=0.701  max=0.706"
          },
          {
            "name": "update",
            "value": 27.025,
            "unit": "ms",
            "extra": "iters=200  min=0.131  p50=0.131  p95=0.143  p99=0.249  max=0.270"
          },
          {
            "name": "delete",
            "value": 129.587,
            "unit": "ms",
            "extra": "iters=50  min=2.557  p50=2.578  p95=2.624  p99=2.918  max=3.143"
          },
          {
            "name": "parser",
            "value": 317.53,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.015  p99=0.015  max=0.047"
          },
          {
            "name": "index_lookup",
            "value": 6.466,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.003  max=0.013"
          },
          {
            "name": "transaction",
            "value": 19.804,
            "unit": "ms",
            "extra": "iters=100  min=0.101  p50=0.197  p95=0.288  p99=0.293  max=0.300"
          },
          {
            "name": "window_functions",
            "value": 21.347,
            "unit": "ms",
            "extra": "iters=20  min=1.060  p50=1.064  p95=1.082  p99=1.093  max=1.096"
          },
          {
            "name": "distinct",
            "value": 64.055,
            "unit": "ms",
            "extra": "iters=500  min=0.125  p50=0.126  p95=0.136  p99=0.147  max=0.198"
          },
          {
            "name": "subquery",
            "value": 1975.815,
            "unit": "ms",
            "extra": "iters=50  min=39.374  p50=39.472  p95=39.702  p99=40.608  max=41.418"
          },
          {
            "name": "cte",
            "value": 120.536,
            "unit": "ms",
            "extra": "iters=200  min=0.585  p50=0.595  p95=0.640  p99=0.767  max=0.849"
          },
          {
            "name": "generate_series",
            "value": 140.74,
            "unit": "ms",
            "extra": "iters=200  min=0.691  p50=0.701  p95=0.716  p99=0.782  max=0.942"
          },
          {
            "name": "scalar_functions",
            "value": 257.419,
            "unit": "ms",
            "extra": "iters=200  min=1.279  p50=1.283  p95=1.299  p99=1.320  max=1.385"
          },
          {
            "name": "expression_agg",
            "value": 399.344,
            "unit": "ms",
            "extra": "iters=500  min=0.761  p50=0.785  p95=0.812  p99=1.295  max=1.426"
          },
          {
            "name": "multi_sort",
            "value": 120.549,
            "unit": "ms",
            "extra": "iters=200  min=0.591  p50=0.602  p95=0.619  p99=0.655  max=0.685"
          },
          {
            "name": "set_ops",
            "value": 2015.133,
            "unit": "ms",
            "extra": "iters=50  min=40.152  p50=40.287  p95=40.498  p99=40.588  max=40.656"
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
          "id": "e39b41241bbfb59094768c6fd97bc0fd942e8c9c",
          "message": "add transaction state initialization in bench_transaction to fix missing active_txn pointer before CREATE TABLE execution",
          "timestamp": "2026-02-14T06:54:29-08:00",
          "tree_id": "63af0377a2f75342c4fd1272ac84ee77eb062863",
          "url": "https://github.com/martinsk/mskql/commit/e39b41241bbfb59094768c6fd97bc0fd942e8c9c"
        },
        "date": 1771080916554,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 15.66,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.056"
          },
          {
            "name": "select_full_scan",
            "value": 81.022,
            "unit": "ms",
            "extra": "iters=200  min=0.366  p50=0.403  p95=0.417  p99=0.444  max=0.959"
          },
          {
            "name": "select_where",
            "value": 104.846,
            "unit": "ms",
            "extra": "iters=500  min=0.204  p50=0.206  p95=0.226  p99=0.237  max=0.337"
          },
          {
            "name": "aggregate",
            "value": 197.76,
            "unit": "ms",
            "extra": "iters=500  min=0.357  p50=0.396  p95=0.411  p99=0.457  max=0.502"
          },
          {
            "name": "order_by",
            "value": 169.552,
            "unit": "ms",
            "extra": "iters=200  min=0.762  p50=0.838  p95=0.877  p99=1.153  max=1.451"
          },
          {
            "name": "join",
            "value": 33.594,
            "unit": "ms",
            "extra": "iters=50  min=0.654  p50=0.670  p95=0.690  p99=0.695  max=0.697"
          },
          {
            "name": "update",
            "value": 30.343,
            "unit": "ms",
            "extra": "iters=200  min=0.148  p50=0.149  p95=0.161  p99=0.172  max=0.174"
          },
          {
            "name": "delete",
            "value": 127.087,
            "unit": "ms",
            "extra": "iters=50  min=2.520  p50=2.534  p95=2.599  p99=2.643  max=2.647"
          },
          {
            "name": "parser",
            "value": 292.5,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.014  p99=0.014  max=0.108"
          },
          {
            "name": "index_lookup",
            "value": 6.225,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.003  max=0.017"
          },
          {
            "name": "transaction",
            "value": 19.875,
            "unit": "ms",
            "extra": "iters=100  min=0.101  p50=0.197  p95=0.288  p99=0.300  max=0.304"
          },
          {
            "name": "window_functions",
            "value": 22.044,
            "unit": "ms",
            "extra": "iters=20  min=1.089  p50=1.099  p95=1.123  p99=1.136  max=1.139"
          },
          {
            "name": "distinct",
            "value": 68.619,
            "unit": "ms",
            "extra": "iters=500  min=0.131  p50=0.134  p95=0.151  p99=0.192  max=0.238"
          },
          {
            "name": "subquery",
            "value": 2210.557,
            "unit": "ms",
            "extra": "iters=50  min=43.236  p50=44.116  p95=44.500  p99=47.035  max=47.850"
          },
          {
            "name": "cte",
            "value": 122.884,
            "unit": "ms",
            "extra": "iters=200  min=0.596  p50=0.613  p95=0.628  p99=0.657  max=0.870"
          },
          {
            "name": "generate_series",
            "value": 138.822,
            "unit": "ms",
            "extra": "iters=200  min=0.680  p50=0.691  p95=0.709  p99=0.778  max=0.879"
          },
          {
            "name": "scalar_functions",
            "value": 257.553,
            "unit": "ms",
            "extra": "iters=200  min=1.270  p50=1.283  p95=1.320  p99=1.359  max=1.578"
          },
          {
            "name": "expression_agg",
            "value": 413.25,
            "unit": "ms",
            "extra": "iters=500  min=0.756  p50=0.828  p95=0.854  p99=1.085  max=1.202"
          },
          {
            "name": "multi_sort",
            "value": 118.097,
            "unit": "ms",
            "extra": "iters=200  min=0.561  p50=0.579  p95=0.629  p99=0.647  max=0.841"
          },
          {
            "name": "set_ops",
            "value": 2007.785,
            "unit": "ms",
            "extra": "iters=50  min=38.638  p50=40.254  p95=40.539  p99=40.941  max=41.192"
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
          "id": "21d912efbcab1a10cc9f8a97f23a5167c65f77b5",
          "message": "implement lazy copy-on-write snapshots for transactions: record table names/generations at BEGIN, deep-copy tables only on first mutation (INSERT/UPDATE/DELETE/DROP/TRUNCATE/ALTER), restore only COW-saved tables on ROLLBACK, remove tables created during transaction, restore dropped tables from snapshot, and invalidate result cache on COMMIT instead of per-write inside transactions",
          "timestamp": "2026-02-14T08:07:06-08:00",
          "tree_id": "4824e29c5a66f6ae2ba6c02a5bb2a01ee4cec566",
          "url": "https://github.com/martinsk/mskql/commit/21d912efbcab1a10cc9f8a97f23a5167c65f77b5"
        },
        "date": 1771085278358,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 15.816,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.101"
          },
          {
            "name": "select_full_scan",
            "value": 82.846,
            "unit": "ms",
            "extra": "iters=200  min=0.403  p50=0.412  p95=0.424  p99=0.430  max=0.975"
          },
          {
            "name": "select_where",
            "value": 106.735,
            "unit": "ms",
            "extra": "iters=500  min=0.209  p50=0.210  p95=0.221  p99=0.234  max=0.313"
          },
          {
            "name": "aggregate",
            "value": 204.227,
            "unit": "ms",
            "extra": "iters=500  min=0.385  p50=0.394  p95=0.419  p99=0.816  max=0.833"
          },
          {
            "name": "order_by",
            "value": 165.475,
            "unit": "ms",
            "extra": "iters=200  min=0.798  p50=0.823  p95=0.847  p99=0.861  max=0.864"
          },
          {
            "name": "join",
            "value": 33.632,
            "unit": "ms",
            "extra": "iters=50  min=0.654  p50=0.671  p95=0.695  p99=0.712  max=0.725"
          },
          {
            "name": "update",
            "value": 30.159,
            "unit": "ms",
            "extra": "iters=200  min=0.133  p50=0.135  p95=0.272  p99=0.298  max=0.304"
          },
          {
            "name": "delete",
            "value": 125.747,
            "unit": "ms",
            "extra": "iters=50  min=2.489  p50=2.511  p95=2.563  p99=2.581  max=2.595"
          },
          {
            "name": "parser",
            "value": 299.702,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.014  p99=0.016  max=0.049"
          },
          {
            "name": "index_lookup",
            "value": 6.273,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.003  max=0.014"
          },
          {
            "name": "transaction",
            "value": 24.508,
            "unit": "ms",
            "extra": "iters=100  min=0.181  p50=0.244  p95=0.301  p99=0.337  max=0.346"
          },
          {
            "name": "window_functions",
            "value": 22.808,
            "unit": "ms",
            "extra": "iters=20  min=1.110  p50=1.123  p95=1.185  p99=1.344  max=1.384"
          },
          {
            "name": "distinct",
            "value": 65.06,
            "unit": "ms",
            "extra": "iters=500  min=0.126  p50=0.128  p95=0.139  p99=0.148  max=0.198"
          },
          {
            "name": "subquery",
            "value": 2123.403,
            "unit": "ms",
            "extra": "iters=50  min=42.352  p50=42.454  p95=42.602  p99=42.848  max=43.075"
          },
          {
            "name": "cte",
            "value": 124.118,
            "unit": "ms",
            "extra": "iters=200  min=0.604  p50=0.616  p95=0.641  p99=0.735  max=0.850"
          },
          {
            "name": "generate_series",
            "value": 141.003,
            "unit": "ms",
            "extra": "iters=200  min=0.689  p50=0.701  p95=0.726  p99=0.733  max=1.026"
          },
          {
            "name": "scalar_functions",
            "value": 265.164,
            "unit": "ms",
            "extra": "iters=200  min=1.296  p50=1.323  p95=1.352  p99=1.402  max=1.531"
          },
          {
            "name": "expression_agg",
            "value": 391.772,
            "unit": "ms",
            "extra": "iters=500  min=0.765  p50=0.781  p95=0.800  p99=0.827  max=1.029"
          },
          {
            "name": "multi_sort",
            "value": 122.455,
            "unit": "ms",
            "extra": "iters=200  min=0.590  p50=0.604  p95=0.629  p99=0.852  max=1.184"
          },
          {
            "name": "set_ops",
            "value": 2032.879,
            "unit": "ms",
            "extra": "iters=50  min=40.226  p50=40.515  p95=41.719  p99=42.905  max=42.999"
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
          "id": "3c1043ae4fc7c226530b12c99d60539e7bf3e126",
          "message": "add GROUP BY ROLLUP/CUBE support with per-grouping-set execution, nested transaction support with parent snapshot pointers, ON CONFLICT column mapping fix for explicit INSERT column lists, ALTER COLUMN TYPE with automatic cell value conversion for numeric/boolean/text types, window function PARTITION BY/ORDER BY expression parsing improvements, GROUP BY expression index buffering to avoid interleaving with function call arguments, SELECT column expression evaluation in joins via parsed_columns, and db",
          "timestamp": "2026-02-14T13:00:40-08:00",
          "tree_id": "04d839ccbf99f44cb625fe20686571961cd113e9",
          "url": "https://github.com/martinsk/mskql/commit/3c1043ae4fc7c226530b12c99d60539e7bf3e126"
        },
        "date": 1771102888007,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 15.78,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.062"
          },
          {
            "name": "select_full_scan",
            "value": 82.133,
            "unit": "ms",
            "extra": "iters=200  min=0.392  p50=0.401  p95=0.470  p99=0.700  max=0.964"
          },
          {
            "name": "select_where",
            "value": 103.684,
            "unit": "ms",
            "extra": "iters=500  min=0.202  p50=0.204  p95=0.214  p99=0.224  max=0.373"
          },
          {
            "name": "aggregate",
            "value": 208.069,
            "unit": "ms",
            "extra": "iters=500  min=0.403  p50=0.412  p95=0.433  p99=0.569  max=0.770"
          },
          {
            "name": "order_by",
            "value": 165.319,
            "unit": "ms",
            "extra": "iters=200  min=0.779  p50=0.827  p95=0.841  p99=0.861  max=0.902"
          },
          {
            "name": "join",
            "value": 34.417,
            "unit": "ms",
            "extra": "iters=50  min=0.669  p50=0.682  p95=0.718  p99=0.796  max=0.819"
          },
          {
            "name": "update",
            "value": 27.853,
            "unit": "ms",
            "extra": "iters=200  min=0.136  p50=0.137  p95=0.149  p99=0.166  max=0.246"
          },
          {
            "name": "delete",
            "value": 127.733,
            "unit": "ms",
            "extra": "iters=50  min=2.501  p50=2.514  p95=2.886  p99=3.032  max=3.043"
          },
          {
            "name": "parser",
            "value": 317.613,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.016  p99=0.016  max=0.039"
          },
          {
            "name": "index_lookup",
            "value": 6.16,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.003  max=0.013"
          },
          {
            "name": "transaction",
            "value": 19.759,
            "unit": "ms",
            "extra": "iters=100  min=0.099  p50=0.196  p95=0.286  p99=0.299  max=0.357"
          },
          {
            "name": "window_functions",
            "value": 21.679,
            "unit": "ms",
            "extra": "iters=20  min=1.066  p50=1.077  p95=1.110  p99=1.178  max=1.195"
          },
          {
            "name": "distinct",
            "value": 64.985,
            "unit": "ms",
            "extra": "iters=500  min=0.127  p50=0.128  p95=0.138  p99=0.150  max=0.195"
          },
          {
            "name": "subquery",
            "value": 1954.605,
            "unit": "ms",
            "extra": "iters=50  min=38.211  p50=38.290  p95=45.205  p99=51.075  max=51.251"
          },
          {
            "name": "cte",
            "value": 120.609,
            "unit": "ms",
            "extra": "iters=200  min=0.592  p50=0.603  p95=0.620  p99=0.633  max=0.687"
          },
          {
            "name": "generate_series",
            "value": 140.467,
            "unit": "ms",
            "extra": "iters=200  min=0.684  p50=0.698  p95=0.718  p99=0.816  max=0.949"
          },
          {
            "name": "scalar_functions",
            "value": 262.993,
            "unit": "ms",
            "extra": "iters=200  min=1.293  p50=1.312  p95=1.337  p99=1.376  max=1.422"
          },
          {
            "name": "expression_agg",
            "value": 402.007,
            "unit": "ms",
            "extra": "iters=500  min=0.774  p50=0.803  p95=0.818  p99=0.847  max=0.888"
          },
          {
            "name": "multi_sort",
            "value": 121.011,
            "unit": "ms",
            "extra": "iters=200  min=0.594  p50=0.605  p95=0.615  p99=0.661  max=0.691"
          },
          {
            "name": "set_ops",
            "value": 2014.83,
            "unit": "ms",
            "extra": "iters=50  min=40.067  p50=40.229  p95=40.536  p99=41.733  max=42.822"
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
          "id": "7c558fc1105a41693a9392b3a15814226692d2cc",
          "message": "add SQL injection protection by escaping single quotes in cell_to_sql_literal and LATERAL join rewriting: double embedded quotes when converting text cells to SQL literals, add two adversarial test cases for correlated/scalar subqueries with O'Brien values",
          "timestamp": "2026-02-14T13:58:20-08:00",
          "tree_id": "0718d61726885b7a7b8855b58f0c5130c1e5ee88",
          "url": "https://github.com/martinsk/mskql/commit/7c558fc1105a41693a9392b3a15814226692d2cc"
        },
        "date": 1771107000809,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 15.691,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.054"
          },
          {
            "name": "select_full_scan",
            "value": 81.189,
            "unit": "ms",
            "extra": "iters=200  min=0.393  p50=0.398  p95=0.426  p99=0.438  max=1.004"
          },
          {
            "name": "select_where",
            "value": 104.8,
            "unit": "ms",
            "extra": "iters=500  min=0.204  p50=0.206  p95=0.221  p99=0.251  max=0.303"
          },
          {
            "name": "aggregate",
            "value": 205.581,
            "unit": "ms",
            "extra": "iters=500  min=0.402  p50=0.406  p95=0.433  p99=0.446  max=0.528"
          },
          {
            "name": "order_by",
            "value": 165.75,
            "unit": "ms",
            "extra": "iters=200  min=0.767  p50=0.825  p95=0.849  p99=0.855  max=0.875"
          },
          {
            "name": "join",
            "value": 34.414,
            "unit": "ms",
            "extra": "iters=50  min=0.669  p50=0.683  p95=0.709  p99=0.783  max=0.829"
          },
          {
            "name": "update",
            "value": 28.331,
            "unit": "ms",
            "extra": "iters=200  min=0.137  p50=0.138  p95=0.149  p99=0.236  max=0.270"
          },
          {
            "name": "delete",
            "value": 127.802,
            "unit": "ms",
            "extra": "iters=50  min=2.498  p50=2.518  p95=2.605  p99=3.382  max=4.041"
          },
          {
            "name": "parser",
            "value": 318.711,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.016  p99=0.019  max=0.045"
          },
          {
            "name": "index_lookup",
            "value": 6.31,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.003  max=0.016"
          },
          {
            "name": "transaction",
            "value": 19.968,
            "unit": "ms",
            "extra": "iters=100  min=0.102  p50=0.200  p95=0.288  p99=0.296  max=0.300"
          },
          {
            "name": "window_functions",
            "value": 21.384,
            "unit": "ms",
            "extra": "iters=20  min=1.062  p50=1.066  p95=1.090  p99=1.092  max=1.092"
          },
          {
            "name": "distinct",
            "value": 65.102,
            "unit": "ms",
            "extra": "iters=500  min=0.127  p50=0.128  p95=0.139  p99=0.152  max=0.191"
          },
          {
            "name": "subquery",
            "value": 1949.266,
            "unit": "ms",
            "extra": "iters=50  min=38.217  p50=38.364  p95=40.054  p99=51.812  max=52.044"
          },
          {
            "name": "cte",
            "value": 122.44,
            "unit": "ms",
            "extra": "iters=200  min=0.600  p50=0.613  p95=0.624  p99=0.642  max=0.699"
          },
          {
            "name": "generate_series",
            "value": 151.506,
            "unit": "ms",
            "extra": "iters=200  min=0.682  p50=0.697  p95=1.228  p99=1.619  max=1.722"
          },
          {
            "name": "scalar_functions",
            "value": 265.816,
            "unit": "ms",
            "extra": "iters=200  min=1.315  p50=1.325  p95=1.344  p99=1.377  max=1.590"
          },
          {
            "name": "expression_agg",
            "value": 405.019,
            "unit": "ms",
            "extra": "iters=500  min=0.786  p50=0.809  p95=0.830  p99=0.877  max=0.910"
          },
          {
            "name": "multi_sort",
            "value": 121.202,
            "unit": "ms",
            "extra": "iters=200  min=0.593  p50=0.604  p95=0.623  p99=0.675  max=0.743"
          },
          {
            "name": "set_ops",
            "value": 2117.891,
            "unit": "ms",
            "extra": "iters=50  min=41.956  p50=42.172  p95=42.461  p99=46.522  max=48.924"
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
          "id": "ee6c895a18cf0c5bf55d2918136d743968e0d8ad",
          "message": "add multi-word type name parsing in CAST expressions: handle TIMESTAMP WITH/WITHOUT TIME ZONE, TIME WITH/WITHOUT TIME ZONE, CHARACTER VARYING, and DOUBLE PRECISION by consuming additional tokens after base type, map TIMESTAMP WITH TIME ZONE to COLUMN_TYPE_TIMESTAMPTZ, treat WITHOUT as identifier token since it's not in keyword list, and move (n)/(p,s) parameter skipping after multi-word type handling",
          "timestamp": "2026-02-14T14:33:26-08:00",
          "tree_id": "235965ca89dfaf49ceffce31da100bc7bf0998d8",
          "url": "https://github.com/martinsk/mskql/commit/ee6c895a18cf0c5bf55d2918136d743968e0d8ad"
        },
        "date": 1771108453281,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 16.353,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.052"
          },
          {
            "name": "select_full_scan",
            "value": 82.903,
            "unit": "ms",
            "extra": "iters=200  min=0.403  p50=0.409  p95=0.423  p99=0.441  max=0.984"
          },
          {
            "name": "select_where",
            "value": 107.935,
            "unit": "ms",
            "extra": "iters=500  min=0.210  p50=0.213  p95=0.223  p99=0.254  max=0.402"
          },
          {
            "name": "aggregate",
            "value": 204.773,
            "unit": "ms",
            "extra": "iters=500  min=0.389  p50=0.392  p95=0.418  p99=0.822  max=0.846"
          },
          {
            "name": "order_by",
            "value": 167.612,
            "unit": "ms",
            "extra": "iters=200  min=0.743  p50=0.828  p95=0.851  p99=1.012  max=1.537"
          },
          {
            "name": "join",
            "value": 35.31,
            "unit": "ms",
            "extra": "iters=50  min=0.692  p50=0.705  p95=0.722  p99=0.740  max=0.750"
          },
          {
            "name": "update",
            "value": 27.974,
            "unit": "ms",
            "extra": "iters=200  min=0.138  p50=0.138  p95=0.148  p99=0.155  max=0.171"
          },
          {
            "name": "delete",
            "value": 132.758,
            "unit": "ms",
            "extra": "iters=50  min=2.628  p50=2.638  p95=2.660  p99=3.030  max=3.339"
          },
          {
            "name": "parser",
            "value": 340.492,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.017  p99=0.018  max=0.057"
          },
          {
            "name": "index_lookup",
            "value": 6.523,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.003  max=0.012"
          },
          {
            "name": "transaction",
            "value": 26.268,
            "unit": "ms",
            "extra": "iters=100  min=0.102  p50=0.264  p95=0.467  p99=0.482  max=0.499"
          },
          {
            "name": "window_functions",
            "value": 22.923,
            "unit": "ms",
            "extra": "iters=20  min=1.125  p50=1.137  p95=1.183  p99=1.199  max=1.202"
          },
          {
            "name": "distinct",
            "value": 66.832,
            "unit": "ms",
            "extra": "iters=500  min=0.129  p50=0.131  p95=0.144  p99=0.153  max=0.219"
          },
          {
            "name": "subquery",
            "value": 2280.025,
            "unit": "ms",
            "extra": "iters=50  min=45.312  p50=45.436  p95=45.869  p99=48.963  max=51.814"
          },
          {
            "name": "cte",
            "value": 122.508,
            "unit": "ms",
            "extra": "iters=200  min=0.599  p50=0.612  p95=0.627  p99=0.642  max=0.703"
          },
          {
            "name": "generate_series",
            "value": 139.659,
            "unit": "ms",
            "extra": "iters=200  min=0.685  p50=0.697  p95=0.707  p99=0.761  max=0.888"
          },
          {
            "name": "scalar_functions",
            "value": 273.427,
            "unit": "ms",
            "extra": "iters=200  min=1.342  p50=1.359  p95=1.397  p99=1.472  max=1.997"
          },
          {
            "name": "expression_agg",
            "value": 404.938,
            "unit": "ms",
            "extra": "iters=500  min=0.791  p50=0.808  p95=0.825  p99=0.871  max=0.938"
          },
          {
            "name": "multi_sort",
            "value": 120.997,
            "unit": "ms",
            "extra": "iters=200  min=0.590  p50=0.603  p95=0.615  p99=0.707  max=0.840"
          },
          {
            "name": "set_ops",
            "value": 2015.684,
            "unit": "ms",
            "extra": "iters=50  min=40.054  p50=40.282  p95=40.560  p99=40.842  max=40.893"
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
          "id": "d7f419398a4bf84c3e7420c1692ed4661a47716c",
          "message": "fix extended protocol RowDescription handling for JDBC compatibility and add test infrastructure improvements: send RowDescription from Execute instead of only from Describe (skip_row_desc=0) so JDBC drivers always receive field metadata before DataRows, add wait_for_port_free and kill_stale_servers functions to prevent port conflicts from previous test runs, add port availability checks before starting C test suites (concurrent on 15400, extended on 15401), write test-failures.log with timestamp",
          "timestamp": "2026-02-14T15:04:33-08:00",
          "tree_id": "efedf7716bec4043e804f11e65bf06fdd1fc4dc5",
          "url": "https://github.com/martinsk/mskql/commit/d7f419398a4bf84c3e7420c1692ed4661a47716c"
        },
        "date": 1771110323167,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 16.336,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.054"
          },
          {
            "name": "select_full_scan",
            "value": 83.984,
            "unit": "ms",
            "extra": "iters=200  min=0.403  p50=0.413  p95=0.434  p99=0.587  max=0.978"
          },
          {
            "name": "select_where",
            "value": 108.065,
            "unit": "ms",
            "extra": "iters=500  min=0.209  p50=0.211  p95=0.231  p99=0.290  max=0.378"
          },
          {
            "name": "aggregate",
            "value": 202.366,
            "unit": "ms",
            "extra": "iters=500  min=0.393  p50=0.402  p95=0.424  p99=0.450  max=0.643"
          },
          {
            "name": "order_by",
            "value": 165.983,
            "unit": "ms",
            "extra": "iters=200  min=0.741  p50=0.824  p95=0.857  p99=0.875  max=1.265"
          },
          {
            "name": "join",
            "value": 35.41,
            "unit": "ms",
            "extra": "iters=50  min=0.694  p50=0.705  p95=0.727  p99=0.760  max=0.761"
          },
          {
            "name": "update",
            "value": 28.114,
            "unit": "ms",
            "extra": "iters=200  min=0.138  p50=0.138  p95=0.149  p99=0.164  max=0.180"
          },
          {
            "name": "delete",
            "value": 132.534,
            "unit": "ms",
            "extra": "iters=50  min=2.630  p50=2.645  p95=2.692  p99=2.720  max=2.726"
          },
          {
            "name": "parser",
            "value": 333.45,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.017  p99=0.017  max=0.054"
          },
          {
            "name": "index_lookup",
            "value": 6.648,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.005  max=0.023"
          },
          {
            "name": "transaction",
            "value": 20.188,
            "unit": "ms",
            "extra": "iters=100  min=0.102  p50=0.200  p95=0.292  p99=0.296  max=0.296"
          },
          {
            "name": "window_functions",
            "value": 22.94,
            "unit": "ms",
            "extra": "iters=20  min=1.115  p50=1.145  p95=1.187  p99=1.205  max=1.210"
          },
          {
            "name": "distinct",
            "value": 65.769,
            "unit": "ms",
            "extra": "iters=500  min=0.127  p50=0.128  p95=0.142  p99=0.163  max=0.199"
          },
          {
            "name": "subquery",
            "value": 2266.761,
            "unit": "ms",
            "extra": "iters=50  min=45.213  p50=45.328  p95=45.449  p99=45.489  max=45.517"
          },
          {
            "name": "cte",
            "value": 123.302,
            "unit": "ms",
            "extra": "iters=200  min=0.601  p50=0.615  p95=0.634  p99=0.660  max=0.726"
          },
          {
            "name": "generate_series",
            "value": 140.734,
            "unit": "ms",
            "extra": "iters=200  min=0.686  p50=0.699  p95=0.723  p99=0.790  max=0.960"
          },
          {
            "name": "scalar_functions",
            "value": 271.255,
            "unit": "ms",
            "extra": "iters=200  min=1.337  p50=1.351  p95=1.380  p99=1.453  max=1.613"
          },
          {
            "name": "expression_agg",
            "value": 408.802,
            "unit": "ms",
            "extra": "iters=500  min=0.801  p50=0.816  p95=0.831  p99=0.867  max=1.038"
          },
          {
            "name": "multi_sort",
            "value": 120.818,
            "unit": "ms",
            "extra": "iters=200  min=0.591  p50=0.602  p95=0.615  p99=0.680  max=0.852"
          },
          {
            "name": "set_ops",
            "value": 2021.511,
            "unit": "ms",
            "extra": "iters=50  min=40.129  p50=40.363  p95=40.665  p99=42.616  max=43.255"
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
          "id": "3011d8bffc5ffcc7bdcdf5ba5b2f7c051afdfa72",
          "message": "add system catalog implementation with pg_namespace, pg_type, pg_class, pg_attribute, pg_index, pg_attrdef, pg_constraint, pg_database, pg_tables, pg_views, information_schema.tables, information_schema.columns, information_schema.schemata support: create catalog.c/catalog.h with table builders that populate catalog tables from user table metadata, add helper functions for OID/type mapping (col_type_to_oid, col_type_to_len, col_type_pg_name, col_type_to_typname), implement catalog_rebuild to regener",
          "timestamp": "2026-02-14T16:19:59-08:00",
          "tree_id": "07599e4d5ea5567edaf0c3afbcc79a5c7d78c6f6",
          "url": "https://github.com/martinsk/mskql/commit/3011d8bffc5ffcc7bdcdf5ba5b2f7c051afdfa72"
        },
        "date": 1771114847748,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.207,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.045"
          },
          {
            "name": "select_full_scan",
            "value": 71.937,
            "unit": "ms",
            "extra": "iters=200  min=0.347  p50=0.353  p95=0.370  p99=0.472  max=0.721"
          },
          {
            "name": "select_where",
            "value": 91.107,
            "unit": "ms",
            "extra": "iters=500  min=0.176  p50=0.181  p95=0.188  p99=0.195  max=0.257"
          },
          {
            "name": "aggregate",
            "value": 182.103,
            "unit": "ms",
            "extra": "iters=500  min=0.349  p50=0.358  p95=0.391  p99=0.496  max=0.596"
          },
          {
            "name": "order_by",
            "value": 140.971,
            "unit": "ms",
            "extra": "iters=200  min=0.685  p50=0.701  p95=0.719  p99=0.776  max=1.118"
          },
          {
            "name": "join",
            "value": 31.022,
            "unit": "ms",
            "extra": "iters=50  min=0.599  p50=0.610  p95=0.682  p99=0.816  max=0.890"
          },
          {
            "name": "update",
            "value": 23.443,
            "unit": "ms",
            "extra": "iters=200  min=0.114  p50=0.116  p95=0.122  p99=0.137  max=0.167"
          },
          {
            "name": "delete",
            "value": 98.413,
            "unit": "ms",
            "extra": "iters=50  min=1.956  p50=1.963  p95=1.977  p99=2.065  max=2.149"
          },
          {
            "name": "parser",
            "value": 241.351,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.004  p95=0.013  p99=0.013  max=0.046"
          },
          {
            "name": "index_lookup",
            "value": 5.233,
            "unit": "ms",
            "extra": "iters=2000  min=0.002  p50=0.003  p95=0.003  p99=0.003  max=0.010"
          },
          {
            "name": "transaction",
            "value": 17.948,
            "unit": "ms",
            "extra": "iters=100  min=0.092  p50=0.175  p95=0.267  p99=0.276  max=0.281"
          },
          {
            "name": "window_functions",
            "value": 19.063,
            "unit": "ms",
            "extra": "iters=20  min=0.939  p50=0.949  p95=0.970  p99=1.008  max=1.018"
          },
          {
            "name": "distinct",
            "value": 61.286,
            "unit": "ms",
            "extra": "iters=500  min=0.119  p50=0.121  p95=0.129  p99=0.150  max=0.214"
          },
          {
            "name": "subquery",
            "value": 1760.824,
            "unit": "ms",
            "extra": "iters=50  min=34.694  p50=35.175  p95=35.643  p99=36.342  max=36.788"
          },
          {
            "name": "cte",
            "value": 108.718,
            "unit": "ms",
            "extra": "iters=200  min=0.530  p50=0.543  p95=0.551  p99=0.593  max=0.628"
          },
          {
            "name": "generate_series",
            "value": 135.005,
            "unit": "ms",
            "extra": "iters=200  min=0.658  p50=0.674  p95=0.685  p99=0.703  max=0.873"
          },
          {
            "name": "scalar_functions",
            "value": 227.356,
            "unit": "ms",
            "extra": "iters=200  min=1.121  p50=1.131  p95=1.153  p99=1.227  max=1.452"
          },
          {
            "name": "expression_agg",
            "value": 348.444,
            "unit": "ms",
            "extra": "iters=500  min=0.679  p50=0.694  p95=0.731  p99=0.756  max=0.799"
          },
          {
            "name": "multi_sort",
            "value": 117.983,
            "unit": "ms",
            "extra": "iters=200  min=0.577  p50=0.588  p95=0.607  p99=0.665  max=0.721"
          },
          {
            "name": "set_ops",
            "value": 2026.591,
            "unit": "ms",
            "extra": "iters=50  min=40.003  p50=40.417  p95=41.365  p99=42.416  max=42.621"
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
          "id": "7b3a5220624909123565ad9552e03fdaa9ed497d",
          "message": "add LATERAL column name inference from parsed_columns/aggregates, INSERT column list support with default/NULL padding and SERIAL auto-increment, ALTER ADD COLUMN default value handling, IS TRUE/FALSE conditions, NOT BETWEEN/LIKE/ILIKE operators, ANY/ALL subquery support, ORDER BY expression parsing and NULLS FIRST/LAST, comma-LATERAL syntax conversion to CROSS JOIN LATERAL",
          "timestamp": "2026-02-15T06:57:03-08:00",
          "tree_id": "0bb5c90939e47d71a051866aff270dbfb4205314",
          "url": "https://github.com/martinsk/mskql/commit/7b3a5220624909123565ad9552e03fdaa9ed497d"
        },
        "date": 1771167473850,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 16.644,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.003  p99=0.004  max=0.056"
          },
          {
            "name": "select_full_scan",
            "value": 80.976,
            "unit": "ms",
            "extra": "iters=200  min=0.392  p50=0.400  p95=0.413  p99=0.420  max=0.968"
          },
          {
            "name": "select_where",
            "value": 106.792,
            "unit": "ms",
            "extra": "iters=500  min=0.204  p50=0.211  p95=0.225  p99=0.237  max=0.383"
          },
          {
            "name": "aggregate",
            "value": 209.754,
            "unit": "ms",
            "extra": "iters=500  min=0.409  p50=0.413  p95=0.434  p99=0.484  max=0.810"
          },
          {
            "name": "order_by",
            "value": 165.702,
            "unit": "ms",
            "extra": "iters=200  min=0.757  p50=0.825  p95=0.849  p99=0.873  max=0.933"
          },
          {
            "name": "join",
            "value": 34.082,
            "unit": "ms",
            "extra": "iters=50  min=0.669  p50=0.680  p95=0.698  p99=0.714  max=0.726"
          },
          {
            "name": "update",
            "value": 27.61,
            "unit": "ms",
            "extra": "iters=200  min=0.129  p50=0.135  p95=0.144  p99=0.191  max=0.282"
          },
          {
            "name": "delete",
            "value": 131.492,
            "unit": "ms",
            "extra": "iters=50  min=2.609  p50=2.626  p95=2.652  p99=2.705  max=2.752"
          },
          {
            "name": "parser",
            "value": 330.16,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.017  p99=0.017  max=0.041"
          },
          {
            "name": "index_lookup",
            "value": 6.632,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.004  max=0.018"
          },
          {
            "name": "transaction",
            "value": 20.078,
            "unit": "ms",
            "extra": "iters=100  min=0.102  p50=0.197  p95=0.308  p99=0.328  max=0.332"
          },
          {
            "name": "window_functions",
            "value": 23.721,
            "unit": "ms",
            "extra": "iters=20  min=1.179  p50=1.182  p95=1.199  p99=1.207  max=1.209"
          },
          {
            "name": "distinct",
            "value": 65.017,
            "unit": "ms",
            "extra": "iters=500  min=0.127  p50=0.128  p95=0.139  p99=0.151  max=0.182"
          },
          {
            "name": "subquery",
            "value": 1775.936,
            "unit": "ms",
            "extra": "iters=50  min=35.321  p50=35.437  p95=35.686  p99=37.199  max=38.625"
          },
          {
            "name": "cte",
            "value": 123.053,
            "unit": "ms",
            "extra": "iters=200  min=0.598  p50=0.611  p95=0.636  p99=0.655  max=0.858"
          },
          {
            "name": "generate_series",
            "value": 139.003,
            "unit": "ms",
            "extra": "iters=200  min=0.683  p50=0.693  p95=0.707  p99=0.744  max=0.954"
          },
          {
            "name": "scalar_functions",
            "value": 261.688,
            "unit": "ms",
            "extra": "iters=200  min=1.290  p50=1.304  p95=1.338  p99=1.386  max=1.531"
          },
          {
            "name": "expression_agg",
            "value": 412.477,
            "unit": "ms",
            "extra": "iters=500  min=0.802  p50=0.824  p95=0.839  p99=0.900  max=1.061"
          },
          {
            "name": "multi_sort",
            "value": 121.684,
            "unit": "ms",
            "extra": "iters=200  min=0.593  p50=0.606  p95=0.621  p99=0.684  max=0.809"
          },
          {
            "name": "set_ops",
            "value": 2102.827,
            "unit": "ms",
            "extra": "iters=50  min=41.881  p50=42.018  p95=42.317  p99=42.453  max=42.483"
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
          "id": "7ed872c88b7880fb9a2110c9afd8d7787fb2834d",
          "message": "add CONTRIBUTING.md with code style guide and architecture documentation, CREATE TABLE AS SELECT support with column name inference from SELECT list and AS aliases, DELETE USING cross-table WHERE evaluation via merged row building, aggregate FILTER (WHERE ...) clause parsing and storage, non-LATERAL subquery join materialization as temp tables, ON CONFLICT DO UPDATE EXCLUDED.col reference resolution, cell_hash switch-case exhaustive enum handling, and inline subquery parsing for JOIN (SELECT ...) AS alias",
          "timestamp": "2026-02-15T08:36:00-08:00",
          "tree_id": "f61113d833c0d44e9802c64e599bf93eb42f3584",
          "url": "https://github.com/martinsk/mskql/commit/7ed872c88b7880fb9a2110c9afd8d7787fb2834d"
        },
        "date": 1771173415670,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.089,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.051"
          },
          {
            "name": "select_full_scan",
            "value": 72.898,
            "unit": "ms",
            "extra": "iters=200  min=0.353  p50=0.360  p95=0.379  p99=0.412  max=0.815"
          },
          {
            "name": "select_where",
            "value": 93.328,
            "unit": "ms",
            "extra": "iters=500  min=0.181  p50=0.184  p95=0.194  p99=0.229  max=0.301"
          },
          {
            "name": "aggregate",
            "value": 189.731,
            "unit": "ms",
            "extra": "iters=500  min=0.365  p50=0.373  p95=0.395  p99=0.556  max=0.682"
          },
          {
            "name": "order_by",
            "value": 144.222,
            "unit": "ms",
            "extra": "iters=200  min=0.700  p50=0.717  p95=0.741  p99=0.798  max=0.959"
          },
          {
            "name": "join",
            "value": 31.262,
            "unit": "ms",
            "extra": "iters=50  min=0.593  p50=0.607  p95=0.712  p99=0.957  max=1.025"
          },
          {
            "name": "update",
            "value": 23.942,
            "unit": "ms",
            "extra": "iters=200  min=0.109  p50=0.112  p95=0.172  p99=0.210  max=0.224"
          },
          {
            "name": "delete",
            "value": 100.402,
            "unit": "ms",
            "extra": "iters=50  min=1.974  p50=1.988  p95=2.126  p99=2.138  max=2.142"
          },
          {
            "name": "parser",
            "value": 247.005,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.004  p95=0.013  p99=0.013  max=0.076"
          },
          {
            "name": "index_lookup",
            "value": 6.856,
            "unit": "ms",
            "extra": "iters=2000  min=0.002  p50=0.003  p95=0.005  p99=0.006  max=0.020"
          },
          {
            "name": "transaction",
            "value": 19.903,
            "unit": "ms",
            "extra": "iters=100  min=0.091  p50=0.194  p95=0.298  p99=0.310  max=0.316"
          },
          {
            "name": "window_functions",
            "value": 23.027,
            "unit": "ms",
            "extra": "iters=20  min=1.096  p50=1.113  p95=1.265  p99=1.399  max=1.432"
          },
          {
            "name": "distinct",
            "value": 60.328,
            "unit": "ms",
            "extra": "iters=500  min=0.117  p50=0.119  p95=0.129  p99=0.145  max=0.198"
          },
          {
            "name": "subquery",
            "value": 1726.068,
            "unit": "ms",
            "extra": "iters=50  min=34.156  p50=34.493  p95=34.807  p99=35.078  max=35.141"
          },
          {
            "name": "cte",
            "value": 107.574,
            "unit": "ms",
            "extra": "iters=200  min=0.526  p50=0.537  p95=0.548  p99=0.571  max=0.669"
          },
          {
            "name": "generate_series",
            "value": 135.536,
            "unit": "ms",
            "extra": "iters=200  min=0.651  p50=0.672  p95=0.693  p99=0.799  max=1.086"
          },
          {
            "name": "scalar_functions",
            "value": 234.416,
            "unit": "ms",
            "extra": "iters=200  min=1.156  p50=1.165  p95=1.198  p99=1.296  max=1.573"
          },
          {
            "name": "expression_agg",
            "value": 370.137,
            "unit": "ms",
            "extra": "iters=500  min=0.723  p50=0.737  p95=0.757  p99=0.838  max=0.932"
          },
          {
            "name": "multi_sort",
            "value": 122.081,
            "unit": "ms",
            "extra": "iters=200  min=0.597  p50=0.608  p95=0.623  p99=0.739  max=0.825"
          },
          {
            "name": "set_ops",
            "value": 2063.602,
            "unit": "ms",
            "extra": "iters=50  min=40.709  p50=41.253  p95=41.671  p99=42.435  max=43.129"
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
          "id": "4c7f896e3f19e07f72094bcfc4e72d51b4bf18ef",
          "message": "add Makefile targets for release/bench builds, auto-detect Homebrew LLVM clang on macOS with error if missing, separate debug (mskql_debug) and release (mskql) binaries, generate_series rewriting for SELECT generate_series(...) AS alias to FROM syntax with column name inference, HAVING clause subquery resolution, INTERSECT/EXCEPT ALL support with per-row RHS matching via used-flags array, ALTER RENAME TABLE implementation with generation increment, multi-column PARTITION BY parsing, TRIM(LEADING",
          "timestamp": "2026-02-15T12:25:35-08:00",
          "tree_id": "0c80d39df93ff9a65bf130bf34bb890bb54a5954",
          "url": "https://github.com/martinsk/mskql/commit/4c7f896e3f19e07f72094bcfc4e72d51b4bf18ef"
        },
        "date": 1771187186031,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 17.264,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.002  p95=0.002  p99=0.004  max=0.058"
          },
          {
            "name": "select_full_scan",
            "value": 82.096,
            "unit": "ms",
            "extra": "iters=200  min=0.398  p50=0.408  p95=0.422  p99=0.447  max=0.985"
          },
          {
            "name": "select_where",
            "value": 105.038,
            "unit": "ms",
            "extra": "iters=500  min=0.205  p50=0.207  p95=0.219  p99=0.228  max=0.321"
          },
          {
            "name": "aggregate",
            "value": 209.144,
            "unit": "ms",
            "extra": "iters=500  min=0.410  p50=0.419  p95=0.431  p99=0.443  max=0.537"
          },
          {
            "name": "order_by",
            "value": 168.579,
            "unit": "ms",
            "extra": "iters=200  min=0.791  p50=0.841  p95=0.854  p99=0.947  max=1.072"
          },
          {
            "name": "join",
            "value": 35.364,
            "unit": "ms",
            "extra": "iters=50  min=0.697  p50=0.707  p95=0.717  p99=0.733  max=0.739"
          },
          {
            "name": "update",
            "value": 27.653,
            "unit": "ms",
            "extra": "iters=200  min=0.135  p50=0.136  p95=0.148  p99=0.155  max=0.157"
          },
          {
            "name": "delete",
            "value": 141.8,
            "unit": "ms",
            "extra": "iters=50  min=2.783  p50=2.798  p95=3.060  p99=3.435  max=3.484"
          },
          {
            "name": "parser",
            "value": 365.494,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.019  p99=0.019  max=0.044"
          },
          {
            "name": "index_lookup",
            "value": 7.168,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.004  p95=0.004  p99=0.004  max=0.013"
          },
          {
            "name": "transaction",
            "value": 20.697,
            "unit": "ms",
            "extra": "iters=100  min=0.108  p50=0.206  p95=0.294  p99=0.302  max=0.308"
          },
          {
            "name": "window_functions",
            "value": 23.852,
            "unit": "ms",
            "extra": "iters=20  min=1.176  p50=1.188  p95=1.217  p99=1.220  max=1.221"
          },
          {
            "name": "distinct",
            "value": 64.645,
            "unit": "ms",
            "extra": "iters=500  min=0.127  p50=0.127  p95=0.137  p99=0.147  max=0.203"
          },
          {
            "name": "subquery",
            "value": 1914.897,
            "unit": "ms",
            "extra": "iters=50  min=38.211  p50=38.289  p95=38.426  p99=38.517  max=38.523"
          },
          {
            "name": "cte",
            "value": 122.991,
            "unit": "ms",
            "extra": "iters=200  min=0.598  p50=0.612  p95=0.632  p99=0.681  max=0.740"
          },
          {
            "name": "generate_series",
            "value": 141.351,
            "unit": "ms",
            "extra": "iters=200  min=0.693  p50=0.704  p95=0.723  p99=0.765  max=0.899"
          },
          {
            "name": "scalar_functions",
            "value": 279.987,
            "unit": "ms",
            "extra": "iters=200  min=1.360  p50=1.377  p95=1.412  p99=2.340  max=2.378"
          },
          {
            "name": "expression_agg",
            "value": 410.97,
            "unit": "ms",
            "extra": "iters=500  min=0.798  p50=0.817  p95=0.846  p99=1.011  max=1.098"
          },
          {
            "name": "multi_sort",
            "value": 125.209,
            "unit": "ms",
            "extra": "iters=200  min=0.609  p50=0.624  p95=0.640  p99=0.706  max=0.749"
          },
          {
            "name": "set_ops",
            "value": 2000.752,
            "unit": "ms",
            "extra": "iters=50  min=39.766  p50=39.902  p95=40.151  p99=42.319  max=44.393"
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
          "id": "da3d661275c86568cda0a811e56ce01f5f500300",
          "message": "add total_generation counter to database struct for O(1) result cache validation: replace rcache_db_generation sum loop with db->total_generation field initialized to 0 in db_init, increment db->total_generation alongside every t->generation++ in TRUNCATE/INSERT/DELETE/ALTER RENAME TABLE/ALTER COLUMN TYPE/COPY IN/UPDATE/foreign key enforcement (CASCADE/SET NULL/SET DEFAULT), remove rcache_invalidate_all calls from COMMIT and non-SELECT queries since cache entries now auto-invalidate via generation",
          "timestamp": "2026-02-15T12:59:39-08:00",
          "tree_id": "f86fbab8eaa0ea44c01af029e83df0a189b630d1",
          "url": "https://github.com/martinsk/mskql/commit/da3d661275c86568cda0a811e56ce01f5f500300"
        },
        "date": 1771189230968,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 17.832,
            "unit": "ms",
            "extra": "iters=10000  min=0.002  p50=0.002  p95=0.002  p99=0.004  max=0.055"
          },
          {
            "name": "select_full_scan",
            "value": 80.98,
            "unit": "ms",
            "extra": "iters=200  min=0.395  p50=0.400  p95=0.410  p99=0.414  max=0.988"
          },
          {
            "name": "select_where",
            "value": 103.397,
            "unit": "ms",
            "extra": "iters=500  min=0.203  p50=0.204  p95=0.216  p99=0.227  max=0.289"
          },
          {
            "name": "aggregate",
            "value": 209.264,
            "unit": "ms",
            "extra": "iters=500  min=0.408  p50=0.417  p95=0.433  p99=0.446  max=0.535"
          },
          {
            "name": "order_by",
            "value": 171.414,
            "unit": "ms",
            "extra": "iters=200  min=0.740  p50=0.854  p95=0.878  p99=0.886  max=0.887"
          },
          {
            "name": "join",
            "value": 35.463,
            "unit": "ms",
            "extra": "iters=50  min=0.691  p50=0.705  p95=0.731  p99=0.771  max=0.805"
          },
          {
            "name": "update",
            "value": 27.522,
            "unit": "ms",
            "extra": "iters=200  min=0.131  p50=0.133  p95=0.159  p99=0.218  max=0.263"
          },
          {
            "name": "delete",
            "value": 146.042,
            "unit": "ms",
            "extra": "iters=50  min=2.893  p50=2.909  p95=2.950  p99=3.111  max=3.240"
          },
          {
            "name": "parser",
            "value": 391.722,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.006  p95=0.020  p99=0.020  max=0.048"
          },
          {
            "name": "index_lookup",
            "value": 7.598,
            "unit": "ms",
            "extra": "iters=2000  min=0.004  p50=0.004  p95=0.004  p99=0.004  max=0.014"
          },
          {
            "name": "transaction",
            "value": 21.025,
            "unit": "ms",
            "extra": "iters=100  min=0.111  p50=0.210  p95=0.301  p99=0.309  max=0.314"
          },
          {
            "name": "window_functions",
            "value": 23.943,
            "unit": "ms",
            "extra": "iters=20  min=1.189  p50=1.194  p95=1.211  p99=1.218  max=1.219"
          },
          {
            "name": "distinct",
            "value": 65.744,
            "unit": "ms",
            "extra": "iters=500  min=0.129  p50=0.129  p95=0.140  p99=0.150  max=0.191"
          },
          {
            "name": "subquery",
            "value": 1917.827,
            "unit": "ms",
            "extra": "iters=50  min=38.214  p50=38.256  p95=38.508  p99=40.340  max=41.317"
          },
          {
            "name": "cte",
            "value": 119.824,
            "unit": "ms",
            "extra": "iters=200  min=0.589  p50=0.600  p95=0.610  p99=0.619  max=0.699"
          },
          {
            "name": "generate_series",
            "value": 140.512,
            "unit": "ms",
            "extra": "iters=200  min=0.692  p50=0.702  p95=0.717  p99=0.726  max=0.820"
          },
          {
            "name": "scalar_functions",
            "value": 277.471,
            "unit": "ms",
            "extra": "iters=200  min=1.369  p50=1.382  p95=1.405  p99=1.480  max=1.912"
          },
          {
            "name": "expression_agg",
            "value": 412.402,
            "unit": "ms",
            "extra": "iters=500  min=0.806  p50=0.824  p95=0.837  p99=0.858  max=1.023"
          },
          {
            "name": "multi_sort",
            "value": 126.381,
            "unit": "ms",
            "extra": "iters=200  min=0.611  p50=0.621  p95=0.637  p99=1.125  max=1.136"
          },
          {
            "name": "set_ops",
            "value": 2000.92,
            "unit": "ms",
            "extra": "iters=50  min=39.791  p50=39.908  p95=40.289  p99=42.084  max=42.159"
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
          "id": "acf7e7b59de6064ffb7d7872b3526931fb32adce",
          "message": "add plan builder helper functions and refactor join/generate_series into separate build functions: extract build_limit, table_has_mixed_types, append_project_node, build_seq_scan, append_filter_node, try_append_simple_filter, append_sort_node helpers, move single equi-join logic from plan_build_select into build_join function, move generate_series logic into build_generate_series function, add build_simple_select for single-table queries with WHERE/ORDER BY/LIMIT support, update plan_build_select to dispatch",
          "timestamp": "2026-02-15T15:10:13-08:00",
          "tree_id": "4d1fb78605fa5b34285e62de70a3c136cb24a139",
          "url": "https://github.com/martinsk/mskql/commit/acf7e7b59de6064ffb7d7872b3526931fb32adce"
        },
        "date": 1771197066037,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 16.6,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.003  p99=0.003  max=0.053"
          },
          {
            "name": "select_full_scan",
            "value": 81.588,
            "unit": "ms",
            "extra": "iters=200  min=0.392  p50=0.402  p95=0.425  p99=0.496  max=0.969"
          },
          {
            "name": "select_where",
            "value": 127.072,
            "unit": "ms",
            "extra": "iters=500  min=0.206  p50=0.209  p95=0.398  p99=0.417  max=0.421"
          },
          {
            "name": "aggregate",
            "value": 215.502,
            "unit": "ms",
            "extra": "iters=500  min=0.408  p50=0.420  p95=0.448  p99=0.873  max=0.892"
          },
          {
            "name": "order_by",
            "value": 174.896,
            "unit": "ms",
            "extra": "iters=200  min=0.771  p50=0.837  p95=1.225  p99=1.364  max=1.459"
          },
          {
            "name": "join",
            "value": 35.257,
            "unit": "ms",
            "extra": "iters=50  min=0.693  p50=0.704  p95=0.725  p99=0.742  max=0.755"
          },
          {
            "name": "update",
            "value": 28.37,
            "unit": "ms",
            "extra": "iters=200  min=0.139  p50=0.140  p95=0.150  p99=0.156  max=0.207"
          },
          {
            "name": "delete",
            "value": 136.053,
            "unit": "ms",
            "extra": "iters=50  min=2.693  p50=2.711  p95=2.772  p99=2.866  max=2.918"
          },
          {
            "name": "parser",
            "value": 347.404,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.018  p99=0.019  max=0.054"
          },
          {
            "name": "index_lookup",
            "value": 7.083,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.004  p99=0.004  max=0.017"
          },
          {
            "name": "transaction",
            "value": 20.539,
            "unit": "ms",
            "extra": "iters=100  min=0.105  p50=0.199  p95=0.300  p99=0.311  max=0.354"
          },
          {
            "name": "window_functions",
            "value": 23.961,
            "unit": "ms",
            "extra": "iters=20  min=1.183  p50=1.193  p95=1.218  p99=1.219  max=1.219"
          },
          {
            "name": "distinct",
            "value": 66.106,
            "unit": "ms",
            "extra": "iters=500  min=0.125  p50=0.126  p95=0.146  p99=0.234  max=0.255"
          },
          {
            "name": "subquery",
            "value": 2124.271,
            "unit": "ms",
            "extra": "iters=50  min=42.273  p50=42.371  p95=42.522  p99=45.041  max=47.379"
          },
          {
            "name": "cte",
            "value": 127.274,
            "unit": "ms",
            "extra": "iters=200  min=0.602  p50=0.617  p95=0.638  p99=1.131  max=1.137"
          },
          {
            "name": "generate_series",
            "value": 139.302,
            "unit": "ms",
            "extra": "iters=200  min=0.683  p50=0.694  p95=0.712  p99=0.734  max=0.868"
          },
          {
            "name": "scalar_functions",
            "value": 280.7,
            "unit": "ms",
            "extra": "iters=200  min=1.383  p50=1.395  p95=1.416  p99=1.510  max=2.420"
          },
          {
            "name": "expression_agg",
            "value": 422.381,
            "unit": "ms",
            "extra": "iters=500  min=0.830  p50=0.842  p95=0.859  p99=0.922  max=0.938"
          },
          {
            "name": "multi_sort",
            "value": 122.674,
            "unit": "ms",
            "extra": "iters=200  min=0.599  p50=0.611  p95=0.627  p99=0.701  max=0.740"
          },
          {
            "name": "set_ops",
            "value": 2187.063,
            "unit": "ms",
            "extra": "iters=50  min=43.584  p50=43.717  p95=43.990  p99=44.078  max=44.083"
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
          "id": "4ad4230e968f6be54486484eb5d581d0a6af6493",
          "message": "increase result cache slots from 256 to 8192 and enable TCP_NOPUSH/TCP_CORK for batched wire protocol sends: expand RCACHE_SLOTS to 8192 for better cache hit rates on workloads with many unique queries, add TCP_NOPUSH (BSD/macOS) or TCP_CORK (Linux) socket option alongside TCP_NODELAY to buffer small writes into larger packets while maintaining low latency for final flush",
          "timestamp": "2026-02-15T15:38:15-08:00",
          "tree_id": "88d4fa4cc9c759b21bacda0fb97b50557a3e3dc7",
          "url": "https://github.com/martinsk/mskql/commit/4ad4230e968f6be54486484eb5d581d0a6af6493"
        },
        "date": 1771198747386,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.348,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.051"
          },
          {
            "name": "select_full_scan",
            "value": 72.66,
            "unit": "ms",
            "extra": "iters=200  min=0.352  p50=0.360  p95=0.372  p99=0.393  max=0.722"
          },
          {
            "name": "select_where",
            "value": 91.477,
            "unit": "ms",
            "extra": "iters=500  min=0.178  p50=0.181  p95=0.189  p99=0.209  max=0.249"
          },
          {
            "name": "aggregate",
            "value": 191.296,
            "unit": "ms",
            "extra": "iters=500  min=0.367  p50=0.372  p95=0.394  p99=0.677  max=0.745"
          },
          {
            "name": "order_by",
            "value": 139.775,
            "unit": "ms",
            "extra": "iters=200  min=0.681  p50=0.695  p95=0.720  p99=0.754  max=0.771"
          },
          {
            "name": "join",
            "value": 31.307,
            "unit": "ms",
            "extra": "iters=50  min=0.596  p50=0.610  p95=0.731  p99=0.777  max=0.794"
          },
          {
            "name": "update",
            "value": 22.215,
            "unit": "ms",
            "extra": "iters=200  min=0.107  p50=0.110  p95=0.117  p99=0.122  max=0.131"
          },
          {
            "name": "delete",
            "value": 105.043,
            "unit": "ms",
            "extra": "iters=50  min=2.057  p50=2.063  p95=2.182  p99=2.769  max=3.255"
          },
          {
            "name": "parser",
            "value": 252.816,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.004  p95=0.013  p99=0.013  max=0.038"
          },
          {
            "name": "index_lookup",
            "value": 5.35,
            "unit": "ms",
            "extra": "iters=2000  min=0.002  p50=0.003  p95=0.003  p99=0.003  max=0.012"
          },
          {
            "name": "transaction",
            "value": 18.042,
            "unit": "ms",
            "extra": "iters=100  min=0.090  p50=0.178  p95=0.268  p99=0.277  max=0.279"
          },
          {
            "name": "window_functions",
            "value": 23.794,
            "unit": "ms",
            "extra": "iters=20  min=1.107  p50=1.123  p95=1.396  p99=1.401  max=1.403"
          },
          {
            "name": "distinct",
            "value": 60.965,
            "unit": "ms",
            "extra": "iters=500  min=0.118  p50=0.120  p95=0.129  p99=0.142  max=0.192"
          },
          {
            "name": "subquery",
            "value": 1813.463,
            "unit": "ms",
            "extra": "iters=50  min=35.718  p50=36.072  p95=36.450  p99=40.382  max=43.562"
          },
          {
            "name": "cte",
            "value": 106.947,
            "unit": "ms",
            "extra": "iters=200  min=0.524  p50=0.535  p95=0.545  p99=0.567  max=0.613"
          },
          {
            "name": "generate_series",
            "value": 136.981,
            "unit": "ms",
            "extra": "iters=200  min=0.657  p50=0.672  p95=0.712  p99=1.153  max=1.238"
          },
          {
            "name": "scalar_functions",
            "value": 233.679,
            "unit": "ms",
            "extra": "iters=200  min=1.146  p50=1.157  p95=1.184  p99=1.258  max=2.112"
          },
          {
            "name": "expression_agg",
            "value": 373.417,
            "unit": "ms",
            "extra": "iters=500  min=0.724  p50=0.741  p95=0.753  p99=0.832  max=1.444"
          },
          {
            "name": "multi_sort",
            "value": 117.959,
            "unit": "ms",
            "extra": "iters=200  min=0.576  p50=0.587  p95=0.596  p99=0.647  max=0.968"
          },
          {
            "name": "set_ops",
            "value": 1893.191,
            "unit": "ms",
            "extra": "iters=50  min=37.324  p50=37.856  p95=38.325  p99=38.490  max=38.600"
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
          "id": "6cc02f948883c7ce5e4986c5140f7db8bcd8042b",
          "message": "add 7 complex analytical benchmarks with shared bootstrap dataset: add multi_join (3-table join+aggregate), analytical_cte (CTE pipeline with HAVING), wide_agg (many-column aggregation), large_sort (50K row sort), subquery_complex (IN subquery+filter+sort+limit), window_rank (RANK() OVER PARTITION BY), and mixed_analytical (CTE+join+aggregate+sort) benchmarks to bench.c and bench_throughput.c, create setup_bootstrap/setup_bootstrap_tp functions that populate customers (2K rows, TEXT columns), products",
          "timestamp": "2026-02-15T16:32:27-08:00",
          "tree_id": "c5c9719878c2542e18eb3d1dba3d8c36154d2ab3",
          "url": "https://github.com/martinsk/mskql/commit/6cc02f948883c7ce5e4986c5140f7db8bcd8042b"
        },
        "date": 1771202011316,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 16.722,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.004  max=0.054"
          },
          {
            "name": "select_full_scan",
            "value": 82.268,
            "unit": "ms",
            "extra": "iters=200  min=0.394  p50=0.404  p95=0.440  p99=0.625  max=0.967"
          },
          {
            "name": "select_where",
            "value": 105.497,
            "unit": "ms",
            "extra": "iters=500  min=0.204  p50=0.206  p95=0.224  p99=0.249  max=0.307"
          },
          {
            "name": "aggregate",
            "value": 205.541,
            "unit": "ms",
            "extra": "iters=500  min=0.400  p50=0.406  p95=0.433  p99=0.483  max=0.710"
          },
          {
            "name": "order_by",
            "value": 167.815,
            "unit": "ms",
            "extra": "iters=200  min=0.758  p50=0.838  p95=0.855  p99=0.876  max=0.915"
          },
          {
            "name": "join",
            "value": 34.76,
            "unit": "ms",
            "extra": "iters=50  min=0.685  p50=0.694  p95=0.710  p99=0.717  max=0.720"
          },
          {
            "name": "update",
            "value": 31.667,
            "unit": "ms",
            "extra": "iters=200  min=0.135  p50=0.136  p95=0.282  p99=0.308  max=0.315"
          },
          {
            "name": "delete",
            "value": 134.474,
            "unit": "ms",
            "extra": "iters=50  min=2.664  p50=2.683  p95=2.723  p99=2.784  max=2.829"
          },
          {
            "name": "parser",
            "value": 338.612,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.018  p99=0.018  max=0.039"
          },
          {
            "name": "index_lookup",
            "value": 6.852,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.003  p99=0.004  max=0.017"
          },
          {
            "name": "transaction",
            "value": 20.672,
            "unit": "ms",
            "extra": "iters=100  min=0.105  p50=0.203  p95=0.296  p99=0.315  max=0.318"
          },
          {
            "name": "window_functions",
            "value": 24.026,
            "unit": "ms",
            "extra": "iters=20  min=1.188  p50=1.197  p95=1.230  p99=1.233  max=1.233"
          },
          {
            "name": "distinct",
            "value": 65.139,
            "unit": "ms",
            "extra": "iters=500  min=0.127  p50=0.128  p95=0.139  p99=0.154  max=0.187"
          },
          {
            "name": "subquery",
            "value": 2122.645,
            "unit": "ms",
            "extra": "iters=50  min=42.287  p50=42.412  p95=42.814  p99=43.482  max=43.572"
          },
          {
            "name": "cte",
            "value": 120.287,
            "unit": "ms",
            "extra": "iters=200  min=0.587  p50=0.600  p95=0.617  p99=0.639  max=0.684"
          },
          {
            "name": "generate_series",
            "value": 145.092,
            "unit": "ms",
            "extra": "iters=200  min=0.685  p50=0.697  p95=0.752  p99=1.327  max=1.337"
          },
          {
            "name": "scalar_functions",
            "value": 274.676,
            "unit": "ms",
            "extra": "iters=200  min=1.357  p50=1.369  p95=1.402  p99=1.439  max=1.607"
          },
          {
            "name": "expression_agg",
            "value": 413.049,
            "unit": "ms",
            "extra": "iters=500  min=0.804  p50=0.823  p95=0.844  p99=0.898  max=1.020"
          },
          {
            "name": "multi_sort",
            "value": 122.867,
            "unit": "ms",
            "extra": "iters=200  min=0.600  p50=0.611  p95=0.633  p99=0.696  max=0.832"
          },
          {
            "name": "set_ops",
            "value": 2196.531,
            "unit": "ms",
            "extra": "iters=50  min=43.528  p50=43.690  p95=44.019  p99=49.341  max=54.386"
          },
          {
            "name": "multi_join",
            "value": 500.933,
            "unit": "ms",
            "extra": "iters=5  min=94.781  p50=101.521  p95=106.558  p99=107.432  max=107.651"
          },
          {
            "name": "analytical_cte",
            "value": 858.651,
            "unit": "ms",
            "extra": "iters=20  min=42.488  p50=42.927  p95=43.408  p99=43.576  max=43.619"
          },
          {
            "name": "wide_agg",
            "value": 1081.965,
            "unit": "ms",
            "extra": "iters=20  min=53.803  p50=54.078  p95=54.310  p99=54.493  max=54.539"
          },
          {
            "name": "large_sort",
            "value": 80.492,
            "unit": "ms",
            "extra": "iters=10  min=7.824  p50=7.890  p95=8.822  p99=9.386  max=9.527"
          },
          {
            "name": "subquery_complex",
            "value": 2620.776,
            "unit": "ms",
            "extra": "iters=20  min=130.445  p50=130.983  p95=131.400  p99=131.600  max=131.649"
          },
          {
            "name": "window_rank",
            "value": 95.746,
            "unit": "ms",
            "extra": "iters=5  min=18.849  p50=18.930  p95=19.818  p99=19.974  max=20.013"
          },
          {
            "name": "mixed_analytical",
            "value": 5100.875,
            "unit": "ms",
            "extra": "iters=5  min=1016.915  p50=1018.218  p95=1027.299  p99=1029.052  max=1029.490"
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
          "id": "1cdcea7e752ea379d55cde87c96ea6786bbf7d63",
          "message": "add compound WHERE clause support for IN-subquery AND simple filter combinations: decompose COND_AND into semi-join + extra filter when one child is CMP_IN with subquery and other is simple comparison, add extra_filter_cond tracking and try_append_simple_filter call after semi-join build, extend build_semi_join to accept TEXT column filters with bump_strdup for plan arena lifetime, add test_subquery_complex_compound_where.sql test case with customers/events tables and WHERE user_id IN (SELECT id FROM customers",
          "timestamp": "2026-02-15T16:54:01-08:00",
          "tree_id": "ec3671266600ac0c462343bbfe3140f074c3f4b1",
          "url": "https://github.com/martinsk/mskql/commit/1cdcea7e752ea379d55cde87c96ea6786bbf7d63"
        },
        "date": 1771203309842,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 17.323,
            "unit": "ms",
            "extra": "iters=10000  min=0.002  p50=0.002  p95=0.003  p99=0.004  max=0.058"
          },
          {
            "name": "select_full_scan",
            "value": 82.174,
            "unit": "ms",
            "extra": "iters=200  min=0.400  p50=0.403  p95=0.424  p99=0.444  max=0.967"
          },
          {
            "name": "select_where",
            "value": 104.918,
            "unit": "ms",
            "extra": "iters=500  min=0.203  p50=0.206  p95=0.221  p99=0.256  max=0.326"
          },
          {
            "name": "aggregate",
            "value": 211.591,
            "unit": "ms",
            "extra": "iters=500  min=0.401  p50=0.412  p95=0.445  p99=0.817  max=0.849"
          },
          {
            "name": "order_by",
            "value": 166.891,
            "unit": "ms",
            "extra": "iters=200  min=0.772  p50=0.830  p95=0.855  p99=0.871  max=0.966"
          },
          {
            "name": "join",
            "value": 56.248,
            "unit": "ms",
            "extra": "iters=50  min=0.686  p50=1.342  p95=1.413  p99=1.464  max=1.503"
          },
          {
            "name": "update",
            "value": 36.877,
            "unit": "ms",
            "extra": "iters=200  min=0.133  p50=0.136  p95=0.283  p99=0.298  max=0.300"
          },
          {
            "name": "delete",
            "value": 140.062,
            "unit": "ms",
            "extra": "iters=50  min=2.782  p50=2.795  p95=2.846  p99=2.877  max=2.880"
          },
          {
            "name": "parser",
            "value": 364.984,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.019  p99=0.019  max=0.042"
          },
          {
            "name": "index_lookup",
            "value": 7.22,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.004  p95=0.004  p99=0.004  max=0.013"
          },
          {
            "name": "transaction",
            "value": 20.861,
            "unit": "ms",
            "extra": "iters=100  min=0.110  p50=0.207  p95=0.297  p99=0.310  max=0.311"
          },
          {
            "name": "window_functions",
            "value": 24.206,
            "unit": "ms",
            "extra": "iters=20  min=1.197  p50=1.205  p95=1.227  p99=1.239  max=1.242"
          },
          {
            "name": "distinct",
            "value": 65.172,
            "unit": "ms",
            "extra": "iters=500  min=0.127  p50=0.128  p95=0.139  p99=0.149  max=0.185"
          },
          {
            "name": "subquery",
            "value": 2120.953,
            "unit": "ms",
            "extra": "iters=50  min=42.299  p50=42.387  p95=42.618  p99=42.950  max=43.151"
          },
          {
            "name": "cte",
            "value": 121.053,
            "unit": "ms",
            "extra": "iters=200  min=0.593  p50=0.605  p95=0.618  p99=0.627  max=0.697"
          },
          {
            "name": "generate_series",
            "value": 144.549,
            "unit": "ms",
            "extra": "iters=200  min=0.687  p50=0.698  p95=0.754  p99=1.327  max=1.367"
          },
          {
            "name": "scalar_functions",
            "value": 269.445,
            "unit": "ms",
            "extra": "iters=200  min=1.324  p50=1.344  p95=1.364  p99=1.438  max=1.533"
          },
          {
            "name": "expression_agg",
            "value": 419.843,
            "unit": "ms",
            "extra": "iters=500  min=0.821  p50=0.838  p95=0.859  p99=0.911  max=1.058"
          },
          {
            "name": "multi_sort",
            "value": 122.887,
            "unit": "ms",
            "extra": "iters=200  min=0.603  p50=0.614  p95=0.629  p99=0.694  max=0.745"
          },
          {
            "name": "set_ops",
            "value": 1998.14,
            "unit": "ms",
            "extra": "iters=50  min=39.818  p50=39.919  p95=40.317  p99=40.503  max=40.618"
          },
          {
            "name": "multi_join",
            "value": 491.823,
            "unit": "ms",
            "extra": "iters=5  min=93.508  p50=97.776  p95=102.604  p99=102.777  max=102.821"
          },
          {
            "name": "analytical_cte",
            "value": 879.118,
            "unit": "ms",
            "extra": "iters=20  min=43.330  p50=43.946  p95=44.613  p99=44.654  max=44.665"
          },
          {
            "name": "wide_agg",
            "value": 1112.493,
            "unit": "ms",
            "extra": "iters=20  min=55.007  p50=55.570  p95=56.039  p99=56.395  max=56.484"
          },
          {
            "name": "large_sort",
            "value": 81.498,
            "unit": "ms",
            "extra": "iters=10  min=7.863  p50=8.006  p95=8.953  p99=9.536  max=9.681"
          },
          {
            "name": "subquery_complex",
            "value": 2630.341,
            "unit": "ms",
            "extra": "iters=20  min=130.438  p50=131.515  p95=131.962  p99=132.163  max=132.214"
          },
          {
            "name": "window_rank",
            "value": 95.847,
            "unit": "ms",
            "extra": "iters=5  min=18.972  p50=19.040  p95=19.497  p99=19.542  max=19.553"
          },
          {
            "name": "mixed_analytical",
            "value": 5232.097,
            "unit": "ms",
            "extra": "iters=5  min=1042.008  p50=1047.080  p95=1048.834  p99=1049.064  max=1049.121"
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
          "id": "aca325eb9da172e6f1ccfb2a10af71011f7f7d9b",
          "message": "add flat_col columnar storage for hash join build side to eliminate col_block union overhead and enable dynamic resizing: replace col_block arrays with flat_col struct containing type/nulls/data pointers, add flat_col_init/grow/set_from_cb/hash/eq/copy_to_out helpers for type-safe operations, remove jc_copy_to_colblock/jc_copy_from_colblock in favor of direct memcpy in hash_join_restore_from_cache/hash_join_save_to_cache, rewrite hash_join_build to use flat_col_set_from_cb and flat_col_grow for",
          "timestamp": "2026-02-15T17:45:15-08:00",
          "tree_id": "20f51a0114b0bba8b6757b7151a8a79b50a167b6",
          "url": "https://github.com/martinsk/mskql/commit/aca325eb9da172e6f1ccfb2a10af71011f7f7d9b"
        },
        "date": 1771206374780,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 13.258,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.041"
          },
          {
            "name": "select_full_scan",
            "value": 73.107,
            "unit": "ms",
            "extra": "iters=200  min=0.354  p50=0.361  p95=0.374  p99=0.390  max=0.711"
          },
          {
            "name": "select_where",
            "value": 92.281,
            "unit": "ms",
            "extra": "iters=500  min=0.179  p50=0.183  p95=0.191  p99=0.203  max=0.260"
          },
          {
            "name": "aggregate",
            "value": 184.191,
            "unit": "ms",
            "extra": "iters=500  min=0.359  p50=0.366  p95=0.384  p99=0.406  max=0.495"
          },
          {
            "name": "order_by",
            "value": 141.086,
            "unit": "ms",
            "extra": "iters=200  min=0.685  p50=0.701  p95=0.727  p99=0.766  max=0.782"
          },
          {
            "name": "join",
            "value": 31.565,
            "unit": "ms",
            "extra": "iters=50  min=0.599  p50=0.612  p95=0.654  p99=1.020  max=1.036"
          },
          {
            "name": "update",
            "value": 22.411,
            "unit": "ms",
            "extra": "iters=200  min=0.108  p50=0.111  p95=0.118  p99=0.123  max=0.133"
          },
          {
            "name": "delete",
            "value": 105.028,
            "unit": "ms",
            "extra": "iters=50  min=2.061  p50=2.064  p95=2.359  p99=2.601  max=2.770"
          },
          {
            "name": "parser",
            "value": 254.551,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.004  p95=0.013  p99=0.013  max=0.044"
          },
          {
            "name": "index_lookup",
            "value": 5.325,
            "unit": "ms",
            "extra": "iters=2000  min=0.002  p50=0.003  p95=0.003  p99=0.003  max=0.011"
          },
          {
            "name": "transaction",
            "value": 18.06,
            "unit": "ms",
            "extra": "iters=100  min=0.091  p50=0.177  p95=0.268  p99=0.278  max=0.282"
          },
          {
            "name": "window_functions",
            "value": 22.418,
            "unit": "ms",
            "extra": "iters=20  min=1.111  p50=1.114  p95=1.139  p99=1.181  max=1.192"
          },
          {
            "name": "distinct",
            "value": 60.521,
            "unit": "ms",
            "extra": "iters=500  min=0.118  p50=0.120  p95=0.128  p99=0.131  max=0.205"
          },
          {
            "name": "subquery",
            "value": 1893.205,
            "unit": "ms",
            "extra": "iters=50  min=36.642  p50=37.179  p95=43.945  p99=45.569  max=46.157"
          },
          {
            "name": "cte",
            "value": 106.652,
            "unit": "ms",
            "extra": "iters=200  min=0.520  p50=0.533  p95=0.542  p99=0.556  max=0.624"
          },
          {
            "name": "generate_series",
            "value": 134.832,
            "unit": "ms",
            "extra": "iters=200  min=0.652  p50=0.671  p95=0.687  p99=0.763  max=0.921"
          },
          {
            "name": "scalar_functions",
            "value": 233.565,
            "unit": "ms",
            "extra": "iters=200  min=1.151  p50=1.165  p95=1.177  p99=1.231  max=1.408"
          },
          {
            "name": "expression_agg",
            "value": 369.218,
            "unit": "ms",
            "extra": "iters=500  min=0.721  p50=0.737  p95=0.748  p99=0.791  max=0.960"
          },
          {
            "name": "multi_sort",
            "value": 119.961,
            "unit": "ms",
            "extra": "iters=200  min=0.587  p50=0.598  p95=0.615  p99=0.649  max=0.805"
          },
          {
            "name": "set_ops",
            "value": 1910.6,
            "unit": "ms",
            "extra": "iters=50  min=37.327  p50=38.175  p95=38.750  p99=39.953  max=40.998"
          },
          {
            "name": "multi_join",
            "value": 650.871,
            "unit": "ms",
            "extra": "iters=5  min=122.943  p50=126.664  p95=141.636  p99=143.777  max=144.312"
          },
          {
            "name": "analytical_cte",
            "value": 1303.798,
            "unit": "ms",
            "extra": "iters=20  min=64.607  p50=65.233  p95=65.940  p99=66.116  max=66.160"
          },
          {
            "name": "wide_agg",
            "value": 1154.318,
            "unit": "ms",
            "extra": "iters=20  min=56.576  p50=57.215  p95=61.682  p99=64.473  max=65.171"
          },
          {
            "name": "large_sort",
            "value": 86.399,
            "unit": "ms",
            "extra": "iters=10  min=8.026  p50=8.287  p95=10.227  p99=10.995  max=11.187"
          },
          {
            "name": "subquery_complex",
            "value": 2344.834,
            "unit": "ms",
            "extra": "iters=20  min=115.654  p50=116.783  p95=118.249  p99=124.538  max=126.110"
          },
          {
            "name": "window_rank",
            "value": 113.67,
            "unit": "ms",
            "extra": "iters=5  min=21.917  p50=21.926  p95=24.916  p99=25.430  max=25.558"
          },
          {
            "name": "mixed_analytical",
            "value": 72.572,
            "unit": "ms",
            "extra": "iters=5  min=13.365  p50=13.888  p95=17.094  p99=17.726  max=17.884"
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
          "id": "ec1b2b43e48842ddebc164b2998cd97323224de2",
          "message": "refactor plan_build_select to return plan_result struct with status/node fields instead of IDX_NONE sentinel: add plan_result struct with PLAN_OK/PLAN_NOTIMPL status enum, update plan_build_select signature to return plan_result, change all callers in database.c/pgwire.c to check pr.status == PLAN_OK instead of root != IDX_NONE, add columnar filter evaluation with filter_eval_leaf/filter_eval_cond_columnar for COND_AND/COND_OR/BETWEEN/IN/LIKE/IS NULL compound predicates using selection vectors, extend",
          "timestamp": "2026-02-15T23:56:57-08:00",
          "tree_id": "119841870e380b9cbbc5fbbf1846034a540a6c4f",
          "url": "https://github.com/martinsk/mskql/commit/ec1b2b43e48842ddebc164b2998cd97323224de2"
        },
        "date": 1771228677800,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "insert_bulk",
            "value": 16.655,
            "unit": "ms",
            "extra": "iters=10000  min=0.001  p50=0.001  p95=0.002  p99=0.003  max=0.083"
          },
          {
            "name": "select_full_scan",
            "value": 84.108,
            "unit": "ms",
            "extra": "iters=200  min=0.406  p50=0.413  p95=0.440  p99=0.522  max=1.024"
          },
          {
            "name": "select_where",
            "value": 132.053,
            "unit": "ms",
            "extra": "iters=500  min=0.211  p50=0.218  p95=0.413  p99=0.423  max=0.431"
          },
          {
            "name": "aggregate",
            "value": 205.276,
            "unit": "ms",
            "extra": "iters=500  min=0.400  p50=0.405  p95=0.433  p99=0.455  max=0.565"
          },
          {
            "name": "order_by",
            "value": 166.406,
            "unit": "ms",
            "extra": "iters=200  min=0.786  p50=0.829  p95=0.855  p99=0.870  max=0.893"
          },
          {
            "name": "join",
            "value": 36.527,
            "unit": "ms",
            "extra": "iters=50  min=0.700  p50=0.714  p95=0.757  p99=1.050  max=1.168"
          },
          {
            "name": "update",
            "value": 28.99,
            "unit": "ms",
            "extra": "iters=200  min=0.142  p50=0.143  p95=0.153  p99=0.156  max=0.158"
          },
          {
            "name": "delete",
            "value": 132.194,
            "unit": "ms",
            "extra": "iters=50  min=2.622  p50=2.637  p95=2.700  p99=2.751  max=2.755"
          },
          {
            "name": "parser",
            "value": 341.801,
            "unit": "ms",
            "extra": "iters=50000  min=0.001  p50=0.005  p95=0.018  p99=0.018  max=0.058"
          },
          {
            "name": "index_lookup",
            "value": 6.977,
            "unit": "ms",
            "extra": "iters=2000  min=0.003  p50=0.003  p95=0.004  p99=0.004  max=0.013"
          },
          {
            "name": "transaction",
            "value": 20.253,
            "unit": "ms",
            "extra": "iters=100  min=0.105  p50=0.200  p95=0.293  p99=0.302  max=0.310"
          },
          {
            "name": "window_functions",
            "value": 24.717,
            "unit": "ms",
            "extra": "iters=20  min=1.199  p50=1.213  p95=1.319  p99=1.355  max=1.364"
          },
          {
            "name": "distinct",
            "value": 66.15,
            "unit": "ms",
            "extra": "iters=500  min=0.127  p50=0.130  p95=0.142  p99=0.158  max=0.238"
          },
          {
            "name": "subquery",
            "value": 306.009,
            "unit": "ms",
            "extra": "iters=50  min=6.090  p50=6.101  p95=6.218  p99=6.370  max=6.432"
          },
          {
            "name": "cte",
            "value": 126.917,
            "unit": "ms",
            "extra": "iters=200  min=0.621  p50=0.633  p95=0.653  p99=0.682  max=0.745"
          },
          {
            "name": "generate_series",
            "value": 148.264,
            "unit": "ms",
            "extra": "iters=200  min=0.700  p50=0.713  p95=0.792  p99=1.357  max=1.372"
          },
          {
            "name": "scalar_functions",
            "value": 285.423,
            "unit": "ms",
            "extra": "iters=200  min=1.405  p50=1.421  p95=1.465  p99=1.513  max=1.671"
          },
          {
            "name": "expression_agg",
            "value": 398.673,
            "unit": "ms",
            "extra": "iters=500  min=0.782  p50=0.794  p95=0.811  p99=0.851  max=1.012"
          },
          {
            "name": "multi_sort",
            "value": 121.957,
            "unit": "ms",
            "extra": "iters=200  min=0.592  p50=0.605  p95=0.638  p99=0.720  max=0.875"
          },
          {
            "name": "set_ops",
            "value": 2096.003,
            "unit": "ms",
            "extra": "iters=50  min=41.698  p50=41.855  p95=42.173  p99=42.628  max=43.040"
          },
          {
            "name": "multi_join",
            "value": 554.248,
            "unit": "ms",
            "extra": "iters=5  min=107.193  p50=111.627  p95=114.022  p99=114.317  max=114.391"
          },
          {
            "name": "analytical_cte",
            "value": 891.713,
            "unit": "ms",
            "extra": "iters=20  min=44.237  p50=44.575  p95=44.868  p99=44.988  max=45.018"
          },
          {
            "name": "wide_agg",
            "value": 1127.84,
            "unit": "ms",
            "extra": "iters=20  min=55.706  p50=56.029  p95=56.969  p99=61.639  max=62.806"
          },
          {
            "name": "large_sort",
            "value": 85.219,
            "unit": "ms",
            "extra": "iters=10  min=8.232  p50=8.304  p95=9.480  p99=10.068  max=10.216"
          },
          {
            "name": "subquery_complex",
            "value": 395.038,
            "unit": "ms",
            "extra": "iters=20  min=19.377  p50=19.459  p95=20.859  p99=23.322  max=23.937"
          },
          {
            "name": "window_rank",
            "value": 100.32,
            "unit": "ms",
            "extra": "iters=5  min=19.795  p50=19.968  p95=20.437  p99=20.478  max=20.489"
          },
          {
            "name": "mixed_analytical",
            "value": 78.143,
            "unit": "ms",
            "extra": "iters=5  min=15.301  p50=15.456  p95=16.275  p99=16.400  max=16.432"
          }
        ]
      }
    ]
  }
}