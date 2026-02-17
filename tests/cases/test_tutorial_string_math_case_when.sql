-- tutorial: string & math - CASE WHEN (string-math-functions.html)
-- setup:
CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT NOT NULL, sku TEXT NOT NULL, price FLOAT NOT NULL, discount FLOAT, category TEXT, notes TEXT);
INSERT INTO products (name, sku, price, discount, category, notes) VALUES ('Wireless Mouse', 'WM-2024-PRO', 49.99, 0.10, 'Electronics', 'Best seller'), ('USB-C Hub', 'HUB-7PORT', 34.50, NULL, 'Electronics', 'New arrival'), ('Standing Desk', 'DESK-ADJ-60', 599.00, 0.15, 'Furniture', NULL), ('Ergonomic Chair', 'CHAIR-ERG-V2', 449.99, 0.20, 'Furniture', 'Updated model'), ('Mechanical Keyboard', 'KB-MX-BLUE', 89.95, 0.05, 'Electronics', 'Cherry MX Blue'), ('Monitor Arm', 'ARM-DUAL-27', 129.00, NULL, 'Accessories', NULL), ('Desk Lamp', 'LAMP-LED-DIM', 45.00, 0.10, 'Accessories', 'Dimmable LED');
-- input:
SELECT name, price, CASE WHEN price >= 400 THEN 'premium' WHEN price >= 80 THEN 'mid-range' ELSE 'budget' END AS tier FROM products ORDER BY price DESC;
-- expected output:
Standing Desk|599|premium
Ergonomic Chair|449.99|premium
Monitor Arm|129|mid-range
Mechanical Keyboard|89.95|mid-range
Wireless Mouse|49.99|budget
Desk Lamp|45|budget
USB-C Hub|34.5|budget
-- expected status: 0
