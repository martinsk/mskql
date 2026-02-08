-- aggregate with greater-than where clause
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
-- input:
SELECT SUM(val) FROM "t1" WHERE id > 2;
-- expected output:
120
-- expected status: 0
