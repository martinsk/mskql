-- DENSE_RANK window function
-- setup:
CREATE TABLE "t1" (name TEXT, score INT);
INSERT INTO "t1" VALUES ('alice', 100);
INSERT INTO "t1" VALUES ('bob', 90);
INSERT INTO "t1" VALUES ('charlie', 90);
INSERT INTO "t1" VALUES ('dave', 80);
-- input:
SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) FROM "t1";
-- expected output:
alice|1
bob|2
charlie|2
dave|3
-- expected status: 0
