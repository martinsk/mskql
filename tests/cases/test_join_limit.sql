-- join with limit
-- setup:
CREATE TABLE "t1" (id INT);
INSERT INTO "t1" (id) VALUES (1), (2), (3);
CREATE TABLE "t2" (id INT, name TEXT);
INSERT INTO "t2" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- input:
SELECT name FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 2;
-- expected output:
alice
bob
-- expected status: 0
