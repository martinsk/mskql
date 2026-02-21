-- NTH_VALUE window function
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" VALUES (1, 10);
INSERT INTO "t1" VALUES (2, 20);
INSERT INTO "t1" VALUES (3, 30);
-- input:
SELECT val, NTH_VALUE(val, 2) OVER (ORDER BY id) FROM "t1";
-- expected output:
10|
20|20
30|20
-- expected status: 0
