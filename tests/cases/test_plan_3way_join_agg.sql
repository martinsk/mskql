-- Test 3-way join with GROUP BY + aggregates through plan executor
-- setup:
CREATE TABLE t3ja_orders (id INT, customer_id INT, product_id INT, qty INT);
CREATE TABLE t3ja_customers (id INT, name TEXT);
CREATE TABLE t3ja_products (id INT, pname TEXT, price INT);
INSERT INTO t3ja_customers VALUES (1, 'alice');
INSERT INTO t3ja_customers VALUES (2, 'bob');
INSERT INTO t3ja_products VALUES (10, 'widget', 5);
INSERT INTO t3ja_products VALUES (20, 'gadget', 10);
INSERT INTO t3ja_orders VALUES (100, 1, 10, 3);
INSERT INTO t3ja_orders VALUES (101, 2, 20, 1);
INSERT INTO t3ja_orders VALUES (102, 1, 20, 2);
-- input:
SELECT c.name, SUM(o.qty) FROM t3ja_orders o JOIN t3ja_customers c ON o.customer_id = c.id JOIN t3ja_products p ON o.product_id = p.id GROUP BY c.name ORDER BY c.name;
-- expected output:
alice|5
bob|1
