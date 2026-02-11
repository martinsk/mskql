-- Window frame sliding window (1 preceding to 1 following)
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" VALUES (1, 10);
INSERT INTO "t1" VALUES (2, 20);
INSERT INTO "t1" VALUES (3, 30);
INSERT INTO "t1" VALUES (4, 40);
-- input:
SELECT val, SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM "t1";
-- expected output:
10|30
20|60
30|90
40|70
-- expected status: 0
