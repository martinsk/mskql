-- Window frame ROWS BETWEEN for running sum
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" VALUES (1, 10);
INSERT INTO "t1" VALUES (2, 20);
INSERT INTO "t1" VALUES (3, 30);
INSERT INTO "t1" VALUES (4, 40);
-- input:
SELECT val, SUM(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM "t1";
-- expected output:
10|10
20|30
30|60
40|100
-- expected status: 0
