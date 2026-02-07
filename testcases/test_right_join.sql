-- right join with unmatched rows
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (3, 30);
CREATE TABLE "t2" (id INT, name TEXT);
INSERT INTO "t2" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- input:
SELECT val, name FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY name;
-- expected output:
10|alice
|bob
30|charlie
-- expected status: 0
