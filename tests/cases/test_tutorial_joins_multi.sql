-- tutorial: multi-table joins (multi-table-joins.html)
-- setup:
CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT NOT NULL, city TEXT);
CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT NOT NULL, price INT NOT NULL);
CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INT REFERENCES customers(id), ordered_at DATE NOT NULL);
CREATE TABLE order_items (id SERIAL PRIMARY KEY, order_id INT NOT NULL REFERENCES orders(id), product_id INT NOT NULL REFERENCES products(id), qty INT NOT NULL DEFAULT 1);
INSERT INTO customers (name, city) VALUES ('Alice', 'Portland'), ('Bob', 'Seattle'), ('Carol', 'Portland'), ('Dave', NULL), ('Eve', 'Denver');
INSERT INTO products (name, price) VALUES ('Widget', 25), ('Gadget', 50), ('Sprocket', 15), ('Gizmo', 75);
INSERT INTO orders (customer_id, ordered_at) VALUES (1, '2025-01-05'), (1, '2025-01-18'), (2, '2025-01-10'), (3, '2025-01-12');
INSERT INTO order_items (order_id, product_id, qty) VALUES (1, 1, 3), (1, 2, 1), (2, 3, 5), (3, 1, 2), (3, 4, 1), (4, 2, 2);
-- input:
SELECT c.name AS customer, o.ordered_at, p.name AS product, oi.qty FROM customers c JOIN orders o ON c.id = o.customer_id JOIN order_items oi ON o.id = oi.order_id JOIN products p ON oi.product_id = p.id ORDER BY o.ordered_at, p.name;
-- expected output:
Alice|2025-01-05|Gadget|1
Alice|2025-01-05|Widget|3
Bob|2025-01-10|Gizmo|1
Bob|2025-01-10|Widget|2
Carol|2025-01-12|Gadget|2
Alice|2025-01-18|Sprocket|5
-- expected status: 0
