-- group by with order by on aggregate result
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT, val INT);
INSERT INTO "t1" (id, dept, val) VALUES (1, 'b', 30), (2, 'a', 10), (3, 'b', 40), (4, 'a', 20);
-- input:
SELECT dept, SUM(val) FROM "t1" GROUP BY dept ORDER BY dept;
-- expected output:
a|30
b|70
-- expected status: 0
