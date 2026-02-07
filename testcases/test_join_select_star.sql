-- join with select star shows all columns from both tables
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20);
CREATE TABLE "t2" (id INT, name TEXT);
INSERT INTO "t2" (id, name) VALUES (1, 'alice'), (2, 'bob');
-- input:
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.id;
-- expected output:
1|10|1|alice
2|20|2|bob
-- expected status: 0
