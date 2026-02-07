-- aggregate with where clause
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
-- input:
SELECT SUM(val), COUNT(*) FROM "t1" WHERE id = 3;
-- expected output:
30|1
-- expected status: 0
