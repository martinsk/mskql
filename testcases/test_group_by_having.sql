-- group by with having filter
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT, val INT);
INSERT INTO "t1" (id, dept, val) VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 40), (5, 'b', 50);
-- input:
SELECT dept, SUM(val) FROM "t1" GROUP BY dept HAVING sum > 50;
-- expected output:
b|120
-- expected status: 0
