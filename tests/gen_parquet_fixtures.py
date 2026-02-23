#!/usr/bin/env python3
"""Generate small Parquet fixture files for mskql foreign table tests.
Run once: python3 tests/gen_parquet_fixtures.py
Requires: pip install pyarrow
"""
import pyarrow as pa
import pyarrow.parquet as pq
import os

OUT = os.path.join(os.path.dirname(__file__), "fixtures")
os.makedirs(OUT, exist_ok=True)

# 1. basic.parquet — simple int/text table
t = pa.table({
    "id":   pa.array([1, 2, 3, 4, 5], type=pa.int32()),
    "name": pa.array(["alice", "bob", "charlie", "diana", "eve"], type=pa.string()),
    "score": pa.array([95, 87, 72, 91, 88], type=pa.int32()),
})
pq.write_table(t, os.path.join(OUT, "basic.parquet"))

# 2. types.parquet — various types
t = pa.table({
    "col_int":    pa.array([10, 20, 30], type=pa.int32()),
    "col_bigint": pa.array([1000000000000, 2000000000000, 3000000000000], type=pa.int64()),
    "col_float":  pa.array([1.5, 2.5, 3.5], type=pa.float64()),
    "col_text":   pa.array(["hello", "world", "test"], type=pa.string()),
    "col_bool":   pa.array([True, False, True], type=pa.bool_()),
})
pq.write_table(t, os.path.join(OUT, "types.parquet"))

# 3. nulls.parquet — with NULL values
t = pa.table({
    "id":    pa.array([1, 2, 3, 4, 5], type=pa.int32()),
    "value": pa.array([10, None, 30, None, 50], type=pa.int32()),
    "label": pa.array(["a", None, "c", "d", None], type=pa.string()),
})
pq.write_table(t, os.path.join(OUT, "nulls.parquet"))

# 4. large.parquet — 1000 rows for aggregation/limit tests
import random
random.seed(42)
ids = list(range(1, 1001))
categories = [random.choice(["A", "B", "C"]) for _ in ids]
amounts = [round(random.uniform(1.0, 100.0), 2) for _ in ids]
t = pa.table({
    "id":       pa.array(ids, type=pa.int32()),
    "category": pa.array(categories, type=pa.string()),
    "amount":   pa.array(amounts, type=pa.float64()),
})
pq.write_table(t, os.path.join(OUT, "large.parquet"))

# 5. dates.parquet — date/timestamp columns
import datetime
dates = [datetime.date(2024, 1, 1) + datetime.timedelta(days=i) for i in range(5)]
timestamps = [datetime.datetime(2024, 1, 1, 10, 0, 0) + datetime.timedelta(hours=i) for i in range(5)]
t = pa.table({
    "id":   pa.array([1, 2, 3, 4, 5], type=pa.int32()),
    "dt":   pa.array(dates, type=pa.date32()),
    "ts":   pa.array(timestamps, type=pa.timestamp("us")),
})
pq.write_table(t, os.path.join(OUT, "dates.parquet"))

# 6. join_left.parquet + join_right.parquet — for join tests
t_left = pa.table({
    "id":   pa.array([1, 2, 3, 4], type=pa.int32()),
    "val":  pa.array(["a", "b", "c", "d"], type=pa.string()),
})
pq.write_table(t_left, os.path.join(OUT, "join_left.parquet"))

t_right = pa.table({
    "id":    pa.array([2, 3, 4, 5], type=pa.int32()),
    "extra": pa.array(["x", "y", "z", "w"], type=pa.string()),
})
pq.write_table(t_right, os.path.join(OUT, "join_right.parquet"))

# 7. Analytics stress test fixtures (small datasets matching benchmark schemas)

# mini_orders.parquet — 10 rows: id, customer_id, product_id, quantity, amount
pq.write_table(pa.table({
    "id":          pa.array(list(range(10)), type=pa.int32()),
    "customer_id": pa.array([0, 1, 2, 0, 1, 2, 0, 1, 2, 0], type=pa.int32()),
    "product_id":  pa.array([0, 1, 2, 0, 1, 2, 0, 1, 2, 0], type=pa.int32()),
    "quantity":    pa.array([3, 1, 5, 2, 4, 1, 6, 2, 3, 1], type=pa.int32()),
    "amount":      pa.array([100, 200, 50, 300, 150, 80, 600, 400, 90, 700], type=pa.int32()),
}), os.path.join(OUT, "mini_orders.parquet"))

# mini_events.parquet — 10 rows: id, user_id, event_type, amount, score
pq.write_table(pa.table({
    "id":         pa.array(list(range(10)), type=pa.int32()),
    "user_id":    pa.array([0, 0, 0, 1, 1, 1, 2, 2, 2, 0], type=pa.int32()),
    "event_type": pa.array([0, 0, 1, 0, 0, 1, 0, 0, 1, 0], type=pa.int32()),
    "amount":     pa.array([100, 200, 50, 300, 400, 60, 10, 20, 30, 150], type=pa.int32()),
    "score":      pa.array([9000, 8500, 7000, 9500, 8000, 6000, 7500, 8200, 5000, 9100], type=pa.int32()),
}), os.path.join(OUT, "mini_events.parquet"))

# mini_regions.parquet — 4 rows: id, name, tax_rate
pq.write_table(pa.table({
    "id":       pa.array([0, 1, 2, 3], type=pa.int32()),
    "name":     pa.array(["north", "south", "east", "west"], type=pa.string()),
    "tax_rate": pa.array([10, 8, 12, 9], type=pa.int32()),
}), os.path.join(OUT, "mini_regions.parquet"))

print(f"Generated fixtures in {OUT}/")
for f in sorted(os.listdir(OUT)):
    sz = os.path.getsize(os.path.join(OUT, f))
    print(f"  {f}: {sz} bytes")
