-- boolean insert and select all
-- setup:
CREATE TABLE "t1" (id INT, flag BOOLEAN);
INSERT INTO "t1" (id, flag) VALUES (1, TRUE), (2, FALSE);
-- input:
SELECT id, flag FROM "t1" ORDER BY id;
-- expected output:
1|t
2|f
-- expected status: 0
