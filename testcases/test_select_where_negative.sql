-- select with negative number in where clause
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, -10), (2, 0), (3, 10);
-- input:
SELECT id FROM "t1" WHERE val < 0;
-- expected output:
1
-- expected status: 0
