-- group by with max aggregate
-- setup:
CREATE TABLE "t1" (dept TEXT, val INT);
INSERT INTO "t1" (dept, val) VALUES ('a', 5), ('a', 15), ('b', 20), ('b', 25);
-- input:
SELECT dept, MAX(val) FROM "t1" GROUP BY dept ORDER BY max DESC;
-- expected output:
b|25
a|15
-- expected status: 0
