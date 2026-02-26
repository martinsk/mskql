-- Test GROUP BY alias resolution in join context
-- setup:
CREATE TABLE jga_orders (id INT, customer_id INT, amount INT);
CREATE TABLE jga_customers (id INT, name TEXT);
INSERT INTO jga_orders VALUES (1, 1, 100);
INSERT INTO jga_orders VALUES (2, 1, 200);
INSERT INTO jga_orders VALUES (3, 2, 150);
INSERT INTO jga_orders VALUES (4, 2, 50);
INSERT INTO jga_orders VALUES (5, 3, 300);
INSERT INTO jga_customers VALUES (1, 'Alice');
INSERT INTO jga_customers VALUES (2, 'Bob');
INSERT INTO jga_customers VALUES (3, 'Carol');
-- input:
SELECT c.name AS customer_name, SUM(o.amount) AS total FROM jga_orders o JOIN jga_customers c ON o.customer_id = c.id GROUP BY customer_name ORDER BY customer_name;
-- expected output:
Alice|300
Bob|200
Carol|300
