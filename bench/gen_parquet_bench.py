#!/usr/bin/env python3
"""Generate Parquet fixture files for benchmarking.
Run once: python3 bench/gen_parquet_bench.py
Requires: pip install pyarrow
"""
import os
import random
import pyarrow as pa
import pyarrow.parquet as pq

random.seed(42)
OUT = os.path.join(os.path.dirname(__file__), "parquet_fixtures")
os.makedirs(OUT, exist_ok=True)

N = 50000

# 1. orders.parquet — fact table (50K rows)
#    id INT, customer_id INT, product_id INT, quantity INT, amount INT
pq.write_table(pa.table({
    "id":          pa.array(list(range(N)), type=pa.int32()),
    "customer_id": pa.array([i % 2000 for i in range(N)], type=pa.int32()),
    "product_id":  pa.array([i % 500 for i in range(N)], type=pa.int32()),
    "quantity":    pa.array([1 + (i * 7) % 20 for i in range(N)], type=pa.int32()),
    "amount":      pa.array([10 + (i * 13) % 990 for i in range(N)], type=pa.int32()),
}), os.path.join(OUT, "orders.parquet"))

# 2. customers.parquet — dimension table (2K rows)
#    id INT, name TEXT, region TEXT, tier TEXT
regions = ["north", "south", "east", "west"]
tiers = ["basic", "premium", "enterprise"]
pq.write_table(pa.table({
    "id":     pa.array(list(range(2000)), type=pa.int32()),
    "name":   pa.array([f"cust_{i}" for i in range(2000)], type=pa.string()),
    "region": pa.array([regions[i % 4] for i in range(2000)], type=pa.string()),
    "tier":   pa.array([tiers[i % 3] for i in range(2000)], type=pa.string()),
}), os.path.join(OUT, "customers.parquet"))

# 3. products.parquet — dimension table (500 rows)
#    id INT, name TEXT, category TEXT, price INT
categories = ["electronics", "clothing", "food", "books", "toys"]
pq.write_table(pa.table({
    "id":       pa.array(list(range(500)), type=pa.int32()),
    "name":     pa.array([f"prod_{i}" for i in range(500)], type=pa.string()),
    "category": pa.array([categories[i % 5] for i in range(500)], type=pa.string()),
    "price":    pa.array([10 + (i * 17) % 490 for i in range(500)], type=pa.int32()),
}), os.path.join(OUT, "products.parquet"))

# 4. events.parquet — fact table (50K rows)
#    id INT, user_id INT, event_type INT, amount INT, score INT
pq.write_table(pa.table({
    "id":         pa.array(list(range(N)), type=pa.int32()),
    "user_id":    pa.array([i % 2000 for i in range(N)], type=pa.int32()),
    "event_type": pa.array([i % 5 for i in range(N)], type=pa.int32()),
    "amount":     pa.array([(i * 11) % 1000 for i in range(N)], type=pa.int32()),
    "score":      pa.array([(i * 31337) % 10000 for i in range(N)], type=pa.int32()),
}), os.path.join(OUT, "events.parquet"))

# 5. metrics.parquet — wide fact table (50K rows)
#    id INT, sensor_id INT, v1-v5 INT
pq.write_table(pa.table({
    "id":        pa.array(list(range(N)), type=pa.int32()),
    "sensor_id": pa.array([i % 100 for i in range(N)], type=pa.int32()),
    "v1":        pa.array([(i * 7) % 1000 for i in range(N)], type=pa.int32()),
    "v2":        pa.array([(i * 13) % 1000 for i in range(N)], type=pa.int32()),
    "v3":        pa.array([(i * 19) % 1000 for i in range(N)], type=pa.int32()),
    "v4":        pa.array([(i * 23) % 1000 for i in range(N)], type=pa.int32()),
    "v5":        pa.array([(i * 29) % 1000 for i in range(N)], type=pa.int32()),
}), os.path.join(OUT, "metrics.parquet"))

# 6. sales.parquet — fact table (50K rows)
#    id INT, rep_id INT, region_id INT, amount INT
pq.write_table(pa.table({
    "id":        pa.array(list(range(N)), type=pa.int32()),
    "rep_id":    pa.array([i % 200 for i in range(N)], type=pa.int32()),
    "region_id": pa.array([i % 4 for i in range(N)], type=pa.int32()),
    "amount":    pa.array([10 + (i * 17) % 990 for i in range(N)], type=pa.int32()),
}), os.path.join(OUT, "sales.parquet"))

# 7. lineitem.parquet — large fact table (200K rows) for stress tests
M = 200000
pq.write_table(pa.table({
    "id":          pa.array(list(range(M)), type=pa.int32()),
    "order_id":    pa.array([i % N for i in range(M)], type=pa.int32()),
    "product_id":  pa.array([i % 500 for i in range(M)], type=pa.int32()),
    "quantity":    pa.array([1 + (i * 3) % 50 for i in range(M)], type=pa.int32()),
    "unit_price":  pa.array([10 + (i * 17) % 490 for i in range(M)], type=pa.int32()),
    "discount":    pa.array([(i * 7) % 30 for i in range(M)], type=pa.int32()),
}), os.path.join(OUT, "lineitem.parquet"))

print(f"Generated benchmark fixtures in {OUT}/")
for f in sorted(os.listdir(OUT)):
    sz = os.path.getsize(os.path.join(OUT, f))
    print(f"  {f}: {sz:,} bytes")
