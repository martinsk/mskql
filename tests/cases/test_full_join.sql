-- full outer join
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20);
CREATE TABLE "t2" (id INT, name TEXT);
INSERT INTO "t2" (id, name) VALUES (2, 'bob'), (3, 'charlie');
-- input:
SELECT val, name FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id ORDER BY val;
-- expected output:
10|
20|bob
|charlie
-- expected status: 0
