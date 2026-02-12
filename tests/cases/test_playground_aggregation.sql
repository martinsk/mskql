-- playground example: aggregation (GROUP BY with multiple aggregates)
-- setup:
CREATE TABLE sales (id INT PRIMARY KEY, product TEXT, region TEXT, amount NUMERIC);
INSERT INTO sales VALUES (1, 'Widget', 'North', 150.00), (2, 'Widget', 'South', 200.00), (3, 'Gadget', 'North', 300.00), (4, 'Gadget', 'South', 250.00), (5, 'Widget', 'North', 175.00), (6, 'Gadget', 'North', 325.00);
-- input:
SELECT product, region, COUNT(*) AS num_sales, SUM(amount) AS total, AVG(amount) AS avg_sale FROM sales GROUP BY product, region ORDER BY product, region;
-- expected output:
Gadget|North|2|625|312.5
Gadget|South|1|250|250
Widget|North|2|325|162.5
Widget|South|1|200|200
-- expected status: 0
