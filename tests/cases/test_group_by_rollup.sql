-- GROUP BY ROLLUP generates subtotals
-- setup:
CREATE TABLE "t1" (dept TEXT, role TEXT, salary INT);
INSERT INTO "t1" VALUES ('eng', 'dev', 100);
INSERT INTO "t1" VALUES ('eng', 'dev', 90);
INSERT INTO "t1" VALUES ('eng', 'mgr', 120);
INSERT INTO "t1" VALUES ('sales', 'rep', 80);
INSERT INTO "t1" VALUES ('sales', 'rep', 70);
-- input:
SELECT dept, SUM(salary) FROM "t1" GROUP BY ROLLUP(dept);
-- expected output:
eng|310
sales|150
|460
-- expected status: 0
