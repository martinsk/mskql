-- tutorial: string & math - SUBSTRING and REPLACE (string-math-functions.html)
-- setup:
CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT NOT NULL, sku TEXT NOT NULL, price FLOAT NOT NULL, discount FLOAT, category TEXT, notes TEXT);
INSERT INTO products (name, sku, price, discount, category, notes) VALUES ('Wireless Mouse', 'WM-2024-PRO', 49.99, 0.10, 'Electronics', 'Best seller'), ('USB-C Hub', 'HUB-7PORT', 34.50, NULL, 'Electronics', 'New arrival'), ('Standing Desk', 'DESK-ADJ-60', 599.00, 0.15, 'Furniture', NULL), ('Ergonomic Chair', 'CHAIR-ERG-V2', 449.99, 0.20, 'Furniture', 'Updated model'), ('Mechanical Keyboard', 'KB-MX-BLUE', 89.95, 0.05, 'Electronics', 'Cherry MX Blue'), ('Monitor Arm', 'ARM-DUAL-27', 129.00, NULL, 'Accessories', NULL), ('Desk Lamp', 'LAMP-LED-DIM', 45.00, 0.10, 'Accessories', 'Dimmable LED');
-- input:
SELECT sku, SUBSTRING(sku FROM 1 FOR 3) AS prefix, REPLACE(sku, '-', '/') AS slash_sku FROM products ORDER BY sku;
-- expected output:
ARM-DUAL-27|ARM|ARM/DUAL/27
CHAIR-ERG-V2|CHA|CHAIR/ERG/V2
DESK-ADJ-60|DES|DESK/ADJ/60
HUB-7PORT|HUB|HUB/7PORT
KB-MX-BLUE|KB-|KB/MX/BLUE
LAMP-LED-DIM|LAM|LAMP/LED/DIM
WM-2024-PRO|WM-|WM/2024/PRO
-- expected status: 0
