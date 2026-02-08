-- join on columns with different types (int vs float)
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20);
CREATE TABLE "t2" (id FLOAT, name TEXT);
INSERT INTO "t2" (id, name) VALUES (1.0, 'alice'), (2.0, 'bob');
-- input:
SELECT val, name FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY val;
-- expected output:
10|alice
20|bob
-- expected status: 0
