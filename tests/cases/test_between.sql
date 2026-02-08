-- between range filter
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 5), (2, 10), (3, 15), (4, 20), (5, 25);
-- input:
SELECT id, val FROM "t1" WHERE val BETWEEN 10 AND 20 ORDER BY id;
-- expected output:
2|10
3|15
4|20
-- expected status: 0
