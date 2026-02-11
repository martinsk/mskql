-- FIRST_VALUE and LAST_VALUE window functions
-- setup:
CREATE TABLE "t1" (dept TEXT, name TEXT, salary INT);
INSERT INTO "t1" VALUES ('eng', 'alice', 100);
INSERT INTO "t1" VALUES ('eng', 'bob', 90);
INSERT INTO "t1" VALUES ('sales', 'charlie', 80);
INSERT INTO "t1" VALUES ('sales', 'dave', 70);
-- input:
SELECT name, FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary DESC), LAST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary DESC) FROM "t1";
-- expected output:
alice|alice|bob
bob|alice|bob
charlie|charlie|dave
dave|charlie|dave
-- expected status: 0
