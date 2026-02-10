window.BENCHMARK_DATA = {
  "lastUpdate": 1770747456708,
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
      }
    ]
  }
}