-- DISTINCT ON keeps first row per distinct value
-- setup:
CREATE TABLE "t1" (dept TEXT, name TEXT, salary INT);
INSERT INTO "t1" VALUES ('eng', 'alice', 100);
INSERT INTO "t1" VALUES ('eng', 'bob', 90);
INSERT INTO "t1" VALUES ('sales', 'charlie', 80);
INSERT INTO "t1" VALUES ('sales', 'dave', 70);
INSERT INTO "t1" VALUES ('hr', 'eve', 60);
-- input:
SELECT DISTINCT ON (dept) dept, name, salary FROM "t1" ORDER BY dept, salary DESC;
-- expected output:
eng|alice|100
hr|eve|60
sales|charlie|80
-- expected status: 0
