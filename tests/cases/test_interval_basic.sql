-- INTERVAL type basic insert and select
-- setup:
CREATE TABLE "t1" (id INT, dur INTERVAL);
INSERT INTO "t1" VALUES (1, '1 day');
INSERT INTO "t1" VALUES (2, '2 hours');
INSERT INTO "t1" VALUES (3, '30 minutes');
-- input:
SELECT * FROM "t1";
-- expected output:
1|1 day
2|02:00:00
3|00:30:00
-- expected status: 0
