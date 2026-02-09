-- CTE with aggregate in inner query
-- setup:
CREATE TABLE sales (dept TEXT, amount INT);
INSERT INTO sales (dept, amount) VALUES ('eng', 100), ('eng', 200), ('sales', 50), ('sales', 150);
-- input:
WITH dept_totals AS (SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept) SELECT dept, total FROM dept_totals ORDER BY dept;
-- expected output:
eng|300
sales|200
-- expected status: 0
