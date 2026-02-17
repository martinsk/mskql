-- tutorial: string & math - COALESCE and NULLIF (string-math-functions.html)
-- setup:
CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT NOT NULL, sku TEXT NOT NULL, price FLOAT NOT NULL, discount FLOAT, category TEXT, notes TEXT);
INSERT INTO products (name, sku, price, discount, category, notes) VALUES ('Wireless Mouse', 'WM-2024-PRO', 49.99, 0.10, 'Electronics', 'Best seller'), ('USB-C Hub', 'HUB-7PORT', 34.50, NULL, 'Electronics', 'New arrival'), ('Standing Desk', 'DESK-ADJ-60', 599.00, 0.15, 'Furniture', NULL), ('Ergonomic Chair', 'CHAIR-ERG-V2', 449.99, 0.20, 'Furniture', 'Updated model'), ('Mechanical Keyboard', 'KB-MX-BLUE', 89.95, 0.05, 'Electronics', 'Cherry MX Blue'), ('Monitor Arm', 'ARM-DUAL-27', 129.00, NULL, 'Accessories', NULL), ('Desk Lamp', 'LAMP-LED-DIM', 45.00, 0.10, 'Accessories', 'Dimmable LED');
-- input:
SELECT name, COALESCE(notes, 'No notes') AS display_notes FROM products ORDER BY name;
-- expected output:
Desk Lamp|Dimmable LED
Ergonomic Chair|Updated model
Mechanical Keyboard|Cherry MX Blue
Monitor Arm|No notes
Standing Desk|No notes
USB-C Hub|New arrival
Wireless Mouse|Best seller
-- expected status: 0
