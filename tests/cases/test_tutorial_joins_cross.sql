-- tutorial: CROSS JOIN with WHERE (multi-table-joins.html)
-- setup:
CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT NOT NULL, city TEXT);
CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT NOT NULL, price INT NOT NULL);
INSERT INTO customers (name, city) VALUES ('Alice', 'Portland'), ('Bob', 'Seattle'), ('Carol', 'Portland'), ('Dave', NULL), ('Eve', 'Denver');
INSERT INTO products (name, price) VALUES ('Widget', 25), ('Gadget', 50), ('Sprocket', 15), ('Gizmo', 75);
-- input:
SELECT c.name AS customer, p.name AS product FROM customers c CROSS JOIN products p WHERE c.city = 'Portland' AND p.price <= 25 ORDER BY c.name, p.name;
-- expected output:
Alice|Sprocket
Alice|Widget
Carol|Sprocket
Carol|Widget
-- expected status: 0
