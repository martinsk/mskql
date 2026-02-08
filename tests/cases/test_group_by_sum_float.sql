-- group by with sum on float column
-- setup:
CREATE TABLE "t1" (dept TEXT, price FLOAT);
INSERT INTO "t1" (dept, price) VALUES ('a', 1.25), ('a', 2.50), ('b', 3.75);
-- input:
SELECT dept, SUM(price) FROM "t1" GROUP BY dept ORDER BY dept;
-- expected output:
a|3.75
b|3.75
-- expected status: 0
