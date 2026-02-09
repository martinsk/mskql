-- GROUP BY with SUM should skip NULL values within groups
-- setup:
CREATE TABLE sales (dept TEXT, amount INT);
INSERT INTO sales (dept, amount) VALUES ('eng', 100), ('eng', NULL), ('eng', 200), ('sales', NULL);
-- input:
SELECT dept, SUM(amount) FROM sales GROUP BY dept ORDER BY dept;
-- expected output:
eng|300
sales|0
-- expected status: 0
