-- analytics stress: DISTINCT + LIKE + multi-column ORDER BY on join
-- setup:
CREATE TABLE adl_customers (id INT, name TEXT, region TEXT);
INSERT INTO adl_customers VALUES (0, 'cust_10', 'north');
INSERT INTO adl_customers VALUES (1, 'cust_12', 'south');
INSERT INTO adl_customers VALUES (2, 'cust_1', 'north');
INSERT INTO adl_customers VALUES (3, 'cust_20', 'east');
INSERT INTO adl_customers VALUES (4, 'cust_15', 'south');
CREATE TABLE adl_orders (id INT, customer_id INT, amount INT);
INSERT INTO adl_orders VALUES (0, 0, 600);
INSERT INTO adl_orders VALUES (1, 1, 700);
INSERT INTO adl_orders VALUES (2, 2, 800);
INSERT INTO adl_orders VALUES (3, 0, 300);
INSERT INTO adl_orders VALUES (4, 3, 100);
INSERT INTO adl_orders VALUES (5, 4, 900);
-- input:
SELECT DISTINCT c.name, c.region FROM adl_customers c JOIN adl_orders o ON c.id = o.customer_id WHERE c.name LIKE 'cust_1%' AND o.amount > 500 ORDER BY c.region, c.name;
-- expected output:
cust_1|north
cust_10|north
cust_12|south
cust_15|south
-- expected status: 0
