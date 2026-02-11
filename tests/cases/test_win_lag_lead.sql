-- LAG and LEAD window functions
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" VALUES (1, 10);
INSERT INTO "t1" VALUES (2, 20);
INSERT INTO "t1" VALUES (3, 30);
INSERT INTO "t1" VALUES (4, 40);
-- input:
SELECT val, LAG(val) OVER (ORDER BY id), LEAD(val) OVER (ORDER BY id) FROM "t1";
-- expected output:
10||20
20|10|30
30|20|40
40|30|
-- expected status: 0
