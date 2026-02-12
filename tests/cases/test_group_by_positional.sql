-- GROUP BY positional reference (GROUP BY 1)
-- setup:
CREATE TABLE sales (category TEXT, amount INT);
INSERT INTO sales (category, amount) VALUES ('A', 10), ('A', 20), ('B', 30), ('B', 40);
-- input:
SELECT category, SUM(amount) FROM sales GROUP BY 1 ORDER BY category;
-- expected output:
A|30
B|70
-- expected status: 0
