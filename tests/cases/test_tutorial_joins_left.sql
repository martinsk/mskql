-- tutorial: LEFT JOIN with GROUP BY (multi-table-joins.html)
-- setup:
CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT NOT NULL, city TEXT);
CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INT REFERENCES customers(id), ordered_at DATE NOT NULL);
INSERT INTO customers (name, city) VALUES ('Alice', 'Portland'), ('Bob', 'Seattle'), ('Carol', 'Portland'), ('Dave', NULL), ('Eve', 'Denver');
INSERT INTO orders (customer_id, ordered_at) VALUES (1, '2025-01-05'), (1, '2025-01-18'), (2, '2025-01-10'), (3, '2025-01-12');
-- input:
SELECT c.name, c.city, COUNT(o.id) AS orders FROM customers c LEFT JOIN orders o ON c.id = o.customer_id GROUP BY c.name, c.city ORDER BY orders, c.name;
-- expected output:
Alice|Portland|2
Bob|Seattle|1
Carol|Portland|1
Dave||0
Eve|Denver|0
-- expected status: 0
