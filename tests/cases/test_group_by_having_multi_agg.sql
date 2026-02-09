-- HAVING with aggregate alias referencing COUNT and SUM together
-- setup:
CREATE TABLE sales (dept TEXT, amount INT);
INSERT INTO sales (dept, amount) VALUES ('eng', 100), ('eng', 200), ('eng', 50), ('sales', 300), ('sales', 400);
-- input:
SELECT dept, COUNT(*), SUM(amount) FROM sales GROUP BY dept HAVING count > 2 ORDER BY dept;
-- expected output:
eng|3|350
-- expected status: 0
