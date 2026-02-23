-- analytics stress: UNION ALL of in-memory aggregate + in-memory aggregate
-- setup:
CREATE TABLE aum_orders (id INT, customer_id INT, amount INT);
INSERT INTO aum_orders VALUES (0, 0, 100);
INSERT INTO aum_orders VALUES (1, 0, 200);
INSERT INTO aum_orders VALUES (2, 1, 300);
INSERT INTO aum_orders VALUES (3, 1, 400);
CREATE TABLE aum_customers (id INT, region TEXT);
INSERT INTO aum_customers VALUES (0, 'north');
INSERT INTO aum_customers VALUES (1, 'south');
CREATE TABLE aum_returns (id INT, region_id INT, refund_amount INT);
INSERT INTO aum_returns VALUES (0, 0, 50);
INSERT INTO aum_returns VALUES (1, 0, 60);
INSERT INTO aum_returns VALUES (2, 1, 70);
CREATE TABLE aum_regions (id INT, name TEXT);
INSERT INTO aum_regions VALUES (0, 'north');
INSERT INTO aum_regions VALUES (1, 'south');
-- input:
SELECT region, SUM(total) AS grand_total FROM (SELECT c.region AS region, SUM(o.amount) AS total FROM aum_orders o JOIN aum_customers c ON o.customer_id = c.id GROUP BY c.region UNION ALL SELECT rg.name AS region, SUM(r.refund_amount) AS total FROM aum_returns r JOIN aum_regions rg ON r.region_id = rg.id GROUP BY rg.name) sub GROUP BY region ORDER BY region;
-- expected output:
north|410
south|770
-- expected status: 0
