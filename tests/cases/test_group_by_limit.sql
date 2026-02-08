-- group by with limit
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT, val INT);
INSERT INTO "t1" (id, dept, val) VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'c', 40);
-- input:
SELECT dept, SUM(val) FROM "t1" GROUP BY dept LIMIT 2;
-- expected output:
a|30
b|30
-- expected status: 0
