-- analytics stress: 3-way join + expression aggregate + GROUP BY + ORDER BY
-- setup:
CREATE TABLE a3j_customers (id INT, name TEXT, region TEXT, tier TEXT);
INSERT INTO a3j_customers VALUES (0, 'alice', 'north', 'premium');
INSERT INTO a3j_customers VALUES (1, 'bob', 'south', 'basic');
INSERT INTO a3j_customers VALUES (2, 'carol', 'north', 'enterprise');
CREATE TABLE a3j_products (id INT, name TEXT, category TEXT, price INT);
INSERT INTO a3j_products VALUES (0, 'widget', 'electronics', 10);
INSERT INTO a3j_products VALUES (1, 'gadget', 'clothing', 20);
INSERT INTO a3j_products VALUES (2, 'thing', 'electronics', 5);
CREATE TABLE a3j_orders (id INT, customer_id INT, product_id INT, quantity INT, amount INT);
INSERT INTO a3j_orders VALUES (0, 0, 0, 3, 100);
INSERT INTO a3j_orders VALUES (1, 1, 1, 1, 200);
INSERT INTO a3j_orders VALUES (2, 2, 2, 5, 50);
INSERT INTO a3j_orders VALUES (3, 0, 0, 2, 300);
INSERT INTO a3j_orders VALUES (4, 0, 2, 4, 150);
-- input:
SELECT c.region, p.category, SUM(o.quantity * p.price) AS revenue, COUNT(*) FROM a3j_orders o JOIN a3j_customers c ON o.customer_id = c.id JOIN a3j_products p ON o.product_id = p.id GROUP BY c.region, p.category ORDER BY revenue DESC;
-- expected output:
north|electronics|95|4
south|clothing|20|1
-- expected status: 0
