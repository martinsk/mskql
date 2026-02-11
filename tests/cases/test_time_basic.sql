-- TIME type basic insert and select
-- setup:
CREATE TABLE "t1" (id INT, t TIME);
INSERT INTO "t1" VALUES (1, '08:30:00');
INSERT INTO "t1" VALUES (2, '14:00:00');
INSERT INTO "t1" VALUES (3, '23:59:59');
-- input:
SELECT * FROM "t1" ORDER BY t;
-- expected output:
1|08:30:00
2|14:00:00
3|23:59:59
-- expected status: 0
