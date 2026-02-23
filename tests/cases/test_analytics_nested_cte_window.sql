-- analytics stress: nested CTE with join + aggregate
-- setup:
CREATE TABLE anc_orders (id INT, customer_id INT, product_id INT, quantity INT);
INSERT INTO anc_orders VALUES (0, 0, 0, 3);
INSERT INTO anc_orders VALUES (1, 0, 1, 2);
INSERT INTO anc_orders VALUES (2, 1, 0, 5);
INSERT INTO anc_orders VALUES (3, 1, 1, 1);
INSERT INTO anc_orders VALUES (4, 2, 0, 10);
CREATE TABLE anc_products (id INT, price INT);
INSERT INTO anc_products VALUES (0, 100);
INSERT INTO anc_products VALUES (1, 200);
CREATE TABLE anc_customers (id INT, region TEXT);
INSERT INTO anc_customers VALUES (0, 'north');
INSERT INTO anc_customers VALUES (1, 'south');
INSERT INTO anc_customers VALUES (2, 'north');
-- input:
WITH order_totals AS (SELECT o.customer_id, SUM(o.quantity * p.price) AS total FROM anc_orders o JOIN anc_products p ON o.product_id = p.id GROUP BY o.customer_id), customer_ranked AS (SELECT ot.customer_id, ot.total, c.region FROM order_totals ot JOIN anc_customers c ON ot.customer_id = c.id WHERE ot.total > 500) SELECT region, COUNT(*) AS num_customers, SUM(total) AS revenue FROM customer_ranked GROUP BY region ORDER BY revenue DESC;
-- expected output:
north|2|1700
south|1|700
-- expected status: 0
