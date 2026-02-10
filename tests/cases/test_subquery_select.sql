-- subquery in SELECT list
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30);
CREATE TABLE "t2" (id INT, score INT);
INSERT INTO "t2" (id, score) VALUES (1, 100), (2, 200);
-- input:
SELECT (SELECT MAX(score) FROM "t2"), id FROM "t1" ORDER BY id;
-- expected output:
200|1
200|2
200|3
-- expected status: 0
