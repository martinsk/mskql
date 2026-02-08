-- window SUM on float column
-- setup:
CREATE TABLE "t1" (dept TEXT, val FLOAT);
INSERT INTO "t1" (dept, val) VALUES ('a', 1.25), ('a', 2.50), ('b', 3.75);
-- input:
SELECT dept, SUM(val) OVER (PARTITION BY dept) FROM "t1";
-- expected output:
a|3.75
a|3.75
b|3.75
-- expected status: 0
