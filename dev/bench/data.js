window.BENCHMARK_DATA = {
  "lastUpdate": 1770668430413,
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
      }
    ]
  }
}