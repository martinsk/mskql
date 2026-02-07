-- group by with table-qualified order by column
-- setup:
CREATE TABLE "t1" (dept TEXT, val INT);
INSERT INTO "t1" (dept, val) VALUES ('b', 10), ('a', 20), ('b', 30), ('a', 40);
-- input:
SELECT dept, SUM(val) FROM "t1" GROUP BY dept ORDER BY dept DESC;
-- expected output:
b|40
a|60
-- expected status: 0
