-- Test 3-way join through plan executor
-- setup:
CREATE TABLE t3j_orders (id INT, customer_id INT, product_id INT, qty INT);
CREATE TABLE t3j_customers (id INT, name TEXT);
CREATE TABLE t3j_products (id INT, pname TEXT, price INT);
INSERT INTO t3j_customers VALUES (1, 'alice');
INSERT INTO t3j_customers VALUES (2, 'bob');
INSERT INTO t3j_products VALUES (10, 'widget', 5);
INSERT INTO t3j_products VALUES (20, 'gadget', 10);
INSERT INTO t3j_orders VALUES (100, 1, 10, 3);
INSERT INTO t3j_orders VALUES (101, 2, 20, 1);
INSERT INTO t3j_orders VALUES (102, 1, 20, 2);
-- input:
SELECT o.id, c.name, p.pname, o.qty FROM t3j_orders o JOIN t3j_customers c ON o.customer_id = c.id JOIN t3j_products p ON o.product_id = p.id ORDER BY o.id;
-- expected output:
100|alice|widget|3
101|bob|gadget|1
102|alice|gadget|2
