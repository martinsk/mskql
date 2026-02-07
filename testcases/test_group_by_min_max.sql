-- group by with min and max
-- setup:
CREATE TABLE "t1" (dept TEXT, val INT);
INSERT INTO "t1" (dept, val) VALUES ('a', 10), ('a', 30), ('b', 20), ('b', 40);
-- input:
SELECT dept, MIN(val), MAX(val) FROM "t1" GROUP BY dept;
-- expected output:
a|10|30
b|20|40
-- expected status: 0
