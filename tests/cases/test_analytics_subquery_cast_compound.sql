-- analytics stress: IN-subquery + compound WHERE + CAST
-- setup:
CREATE TABLE asc_customers (id INT, name TEXT, tier TEXT);
INSERT INTO asc_customers VALUES (0, 'alice', 'premium');
INSERT INTO asc_customers VALUES (1, 'bob', 'basic');
INSERT INTO asc_customers VALUES (2, 'carol', 'premium');
INSERT INTO asc_customers VALUES (3, 'dave', 'enterprise');
CREATE TABLE asc_orders (id INT, customer_id INT, amount INT, quantity INT);
INSERT INTO asc_orders VALUES (0, 0, 100, 2);
INSERT INTO asc_orders VALUES (1, 1, 600, 5);
INSERT INTO asc_orders VALUES (2, 2, 700, 3);
INSERT INTO asc_orders VALUES (3, 0, 800, 4);
INSERT INTO asc_orders VALUES (4, 3, 900, 1);
INSERT INTO asc_orders VALUES (5, 2, 50, 6);
-- input:
SELECT o.id, o.customer_id, o.amount, o.quantity FROM asc_orders o WHERE o.customer_id IN (SELECT id FROM asc_customers WHERE tier = 'premium') AND o.amount > 500 ORDER BY o.amount DESC LIMIT 100;
-- expected output:
3|0|800|4
2|2|700|3
-- expected status: 0
