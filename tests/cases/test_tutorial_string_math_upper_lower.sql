-- tutorial: string & math - UPPER, LOWER, LENGTH (string-math-functions.html)
-- setup:
CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT NOT NULL, sku TEXT NOT NULL, price FLOAT NOT NULL, discount FLOAT, category TEXT, notes TEXT);
INSERT INTO products (name, sku, price, discount, category, notes) VALUES ('Wireless Mouse', 'WM-2024-PRO', 49.99, 0.10, 'Electronics', 'Best seller'), ('USB-C Hub', 'HUB-7PORT', 34.50, NULL, 'Electronics', 'New arrival'), ('Standing Desk', 'DESK-ADJ-60', 599.00, 0.15, 'Furniture', NULL), ('Ergonomic Chair', 'CHAIR-ERG-V2', 449.99, 0.20, 'Furniture', 'Updated model'), ('Mechanical Keyboard', 'KB-MX-BLUE', 89.95, 0.05, 'Electronics', 'Cherry MX Blue'), ('Monitor Arm', 'ARM-DUAL-27', 129.00, NULL, 'Accessories', NULL), ('Desk Lamp', 'LAMP-LED-DIM', 45.00, 0.10, 'Accessories', 'Dimmable LED');
-- input:
SELECT name, UPPER(name) AS upper_name, LOWER(sku) AS lower_sku, LENGTH(name) AS name_len FROM products WHERE LENGTH(name) > 10 ORDER BY name_len DESC;
-- expected output:
Mechanical Keyboard|MECHANICAL KEYBOARD|kb-mx-blue|19
Ergonomic Chair|ERGONOMIC CHAIR|chair-erg-v2|15
Wireless Mouse|WIRELESS MOUSE|wm-2024-pro|14
Standing Desk|STANDING DESK|desk-adj-60|13
Monitor Arm|MONITOR ARM|arm-dual-27|11
-- expected status: 0
