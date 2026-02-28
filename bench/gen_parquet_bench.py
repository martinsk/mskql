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

# 8. ref_regions.parquet — tiny dimension table (4 rows) for analytics stress benchmark
#    id INT, name TEXT, tax_rate INT
pq.write_table(pa.table({
    "id":       pa.array([0, 1, 2, 3], type=pa.int32()),
    "name":     pa.array(["north", "south", "east", "west"], type=pa.string()),
    "tax_rate": pa.array([10, 8, 12, 9], type=pa.int32()),
}), os.path.join(OUT, "ref_regions.parquet"))

# ── Large-scale stress test fixtures ──────────────────────────────────────────

L = 5_000_000   # 5M row fact table

CATS_20 = [
    "groceries", "dining", "travel", "fuel", "utilities",
    "entertainment", "healthcare", "insurance", "clothing", "electronics",
    "education", "subscriptions", "home", "auto", "gifts",
    "charity", "sports", "pets", "beauty", "office",
]
REGIONS_8 = ["us_east", "us_west", "us_central", "eu_west",
             "eu_east", "apac", "latam", "africa"]
TIERS_4 = ["free", "basic", "premium", "enterprise"]

# 9. large_transactions.parquet — 5M rows
#    id INT, account_id INT (100K), merchant_id INT (20K), amount INT,
#    fee INT, ts_day INT (0..729 ≈ 2 years), category TEXT (20 categories)
print("Generating large_transactions.parquet (5M rows)...")
pq.write_table(pa.table({
    "id":          pa.array(list(range(L)), type=pa.int32()),
    "account_id":  pa.array([i % 100_000 for i in range(L)], type=pa.int32()),
    "merchant_id": pa.array([i % 20_000 for i in range(L)], type=pa.int32()),
    "amount":      pa.array([1 + (i * 13) % 9999 for i in range(L)], type=pa.int32()),
    "fee":         pa.array([(i * 7) % 50 for i in range(L)], type=pa.int32()),
    "ts_day":      pa.array([i % 730 for i in range(L)], type=pa.int32()),
    "category":    pa.array([CATS_20[i % 20] for i in range(L)], type=pa.string()),
}), os.path.join(OUT, "large_transactions.parquet"))

# 10. large_accounts.parquet — 100K rows
#     id INT, name TEXT, region TEXT (8), tier TEXT (4), balance INT, created_day INT
print("Generating large_accounts.parquet (100K rows)...")
pq.write_table(pa.table({
    "id":          pa.array(list(range(100_000)), type=pa.int32()),
    "name":        pa.array([f"acct_{i}" for i in range(100_000)], type=pa.string()),
    "region":      pa.array([REGIONS_8[i % 8] for i in range(100_000)], type=pa.string()),
    "tier":        pa.array([TIERS_4[i % 4] for i in range(100_000)], type=pa.string()),
    "balance":     pa.array([100 + (i * 31) % 99_900 for i in range(100_000)], type=pa.int32()),
    "created_day": pa.array([i % 730 for i in range(100_000)], type=pa.int32()),
}), os.path.join(OUT, "large_accounts.parquet"))

# 11. large_merchants.parquet — 20K rows
#     id INT, name TEXT, category TEXT (20), city TEXT (200), rating INT
print("Generating large_merchants.parquet (20K rows)...")
pq.write_table(pa.table({
    "id":       pa.array(list(range(20_000)), type=pa.int32()),
    "name":     pa.array([f"merch_{i}" for i in range(20_000)], type=pa.string()),
    "category": pa.array([CATS_20[i % 20] for i in range(20_000)], type=pa.string()),
    "city":     pa.array([f"city_{i % 200}" for i in range(20_000)], type=pa.string()),
    "rating":   pa.array([1 + (i * 17) % 5 for i in range(20_000)], type=pa.int32()),
}), os.path.join(OUT, "large_merchants.parquet"))

print(f"Generated benchmark fixtures in {OUT}/")
for f in sorted(os.listdir(OUT)):
    sz = os.path.getsize(os.path.join(OUT, f))
    print(f"  {f}: {sz:,} bytes")
