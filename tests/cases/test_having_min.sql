-- having with min aggregate
-- setup:
CREATE TABLE "t1" (dept TEXT, val INT);
INSERT INTO "t1" (dept, val) VALUES ('a', 5), ('a', 15), ('b', 20), ('b', 25), ('c', 1);
-- input:
SELECT dept, MIN(val) FROM "t1" GROUP BY dept HAVING min > 3 ORDER BY dept;
-- expected output:
a|5
b|20
-- expected status: 0
