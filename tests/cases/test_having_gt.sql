-- having with greater-than comparison
-- setup:
CREATE TABLE "t1" (dept TEXT, val INT);
INSERT INTO "t1" (dept, val) VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 3), ('c', 50);
-- input:
SELECT dept, SUM(val) FROM "t1" GROUP BY dept HAVING sum > 10 ORDER BY dept;
-- expected output:
a|30
c|50
-- expected status: 0
