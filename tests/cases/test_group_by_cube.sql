-- GROUP BY CUBE generates all combinations
-- setup:
CREATE TABLE "t1" (dept TEXT, role TEXT, salary INT);
INSERT INTO "t1" VALUES ('eng', 'dev', 100);
INSERT INTO "t1" VALUES ('eng', 'mgr', 120);
INSERT INTO "t1" VALUES ('sales', 'rep', 80);
-- input:
SELECT dept, role, SUM(salary) FROM "t1" GROUP BY CUBE(dept, role);
-- expected output:
eng|dev|100
eng|mgr|120
sales|rep|80
|dev|100
|mgr|120
|rep|80
eng||220
sales||80
||300
-- expected status: 0
