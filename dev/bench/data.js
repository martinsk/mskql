window.BENCHMARK_DATA = {
  "lastUpdate": 1770697906513,
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
      }
    ]
  }
}