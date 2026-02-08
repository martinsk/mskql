-- group by with multiple aggregates (count and sum)
-- setup:
CREATE TABLE "t1" (dept TEXT, val INT);
INSERT INTO "t1" (dept, val) VALUES ('a', 10), ('a', 20), ('b', 30), ('b', 40), ('b', 50);
-- input:
SELECT dept, COUNT(*), SUM(val) FROM "t1" GROUP BY dept ORDER BY dept;
-- expected output:
a|2|30
b|3|120
-- expected status: 0
