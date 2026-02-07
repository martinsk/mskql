-- where with BETWEEN
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 5), (2, 15), (3, 25), (4, 35);
-- input:
SELECT id FROM "t1" WHERE val BETWEEN 10 AND 30;
-- expected output:
2
3
-- expected status: 0
