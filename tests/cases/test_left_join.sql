-- left join with unmatched rows
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30);
CREATE TABLE "t2" (id INT, name TEXT);
INSERT INTO "t2" (id, name) VALUES (1, 'alice'), (3, 'charlie');
-- input:
SELECT val, name FROM t1 LEFT JOIN t2 ON t1.id = t2.id ORDER BY val;
-- expected output:
10|alice
20|
30|charlie
-- expected status: 0
