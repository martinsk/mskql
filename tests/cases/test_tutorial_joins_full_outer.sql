-- tutorial: FULL OUTER JOIN (multi-table-joins.html step 4)
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
SELECT COALESCE(c.name, '(none)') AS customer, COALESCE(p.name, '(none)') AS product FROM customers c FULL JOIN orders o ON c.id = o.customer_id FULL JOIN order_items oi ON o.id = oi.order_id FULL JOIN products p ON oi.product_id = p.id WHERE c.name IS NULL OR p.name IS NULL ORDER BY c.name, p.name;
-- expected output:
Dave|(none)
Eve|(none)
-- expected status: 0
