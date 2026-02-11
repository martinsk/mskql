-- NTILE window function
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" VALUES (1, 10);
INSERT INTO "t1" VALUES (2, 20);
INSERT INTO "t1" VALUES (3, 30);
INSERT INTO "t1" VALUES (4, 40);
-- input:
SELECT val, NTILE(2) OVER (ORDER BY id) FROM "t1";
-- expected output:
10|1
20|1
30|2
40|2
-- expected status: 0
