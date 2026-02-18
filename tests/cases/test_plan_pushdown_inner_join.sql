-- Test: predicate pushdown for INNER JOIN — both sides filtered
-- Setup
CREATE TABLE pd_customers (id INT PRIMARY KEY, name TEXT, tier TEXT);
INSERT INTO pd_customers VALUES (1, 'Alice', 'premium');
INSERT INTO pd_customers VALUES (2, 'Bob', 'basic');
INSERT INTO pd_customers VALUES (3, 'Carol', 'premium');
INSERT INTO pd_customers VALUES (4, 'Dave', 'basic');

CREATE TABLE pd_orders (id INT PRIMARY KEY, customer_id INT, amount INT);
INSERT INTO pd_orders VALUES (1, 1, 150);
INSERT INTO pd_orders VALUES (2, 1, 50);
INSERT INTO pd_orders VALUES (3, 2, 200);
INSERT INTO pd_orders VALUES (4, 3, 300);
INSERT INTO pd_orders VALUES (5, 4, 75);

-- Input
SELECT o.id, o.amount, c.name, c.tier
FROM pd_orders o
JOIN pd_customers c ON o.customer_id = c.id
WHERE c.tier = 'premium' AND o.amount > 100
ORDER BY o.id;

-- Expected
-- 1|150|Alice|premium
-- 4|300|Carol|premium
