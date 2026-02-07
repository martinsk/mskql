-- join with table-qualified column in where
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30);
CREATE TABLE "t2" (id INT, name TEXT);
INSERT INTO "t2" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- input:
SELECT name FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.val > 15;
-- expected output:
bob
charlie
-- expected status: 0
