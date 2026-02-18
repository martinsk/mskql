-- Test: predicate pushdown with 3-table join
-- Setup
CREATE TABLE pd_mj_customers (id INT PRIMARY KEY, name TEXT, tier TEXT);
INSERT INTO pd_mj_customers VALUES (1, 'Alice', 'premium');
INSERT INTO pd_mj_customers VALUES (2, 'Bob', 'basic');

CREATE TABLE pd_mj_orders (id INT PRIMARY KEY, customer_id INT, product_id INT, qty INT);
INSERT INTO pd_mj_orders VALUES (1, 1, 1, 5);
INSERT INTO pd_mj_orders VALUES (2, 1, 2, 3);
INSERT INTO pd_mj_orders VALUES (3, 2, 1, 10);
INSERT INTO pd_mj_orders VALUES (4, 2, 2, 1);

CREATE TABLE pd_mj_products (id INT PRIMARY KEY, name TEXT, price INT);
INSERT INTO pd_mj_products VALUES (1, 'Widget', 10);
INSERT INTO pd_mj_products VALUES (2, 'Gadget', 50);

-- Input: filter on customers and products, both pushable
SELECT c.name, p.name, o.qty
FROM pd_mj_orders o
JOIN pd_mj_customers c ON o.customer_id = c.id
JOIN pd_mj_products p ON o.product_id = p.id
WHERE c.tier = 'premium' AND p.price > 20
ORDER BY c.name, p.name;

-- Expected
-- Alice|Gadget|3
