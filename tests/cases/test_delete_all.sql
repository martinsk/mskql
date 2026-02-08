-- delete all rows (no where)
-- setup:
CREATE TABLE "t1" (id INT);
INSERT INTO "t1" (id) VALUES (1), (2), (3);
DELETE FROM "t1";
-- input:
SELECT COUNT(*) FROM "t1";
-- expected output:
0
-- expected status: 0
