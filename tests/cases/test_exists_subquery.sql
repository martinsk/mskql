-- EXISTS subquery in WHERE
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
CREATE TABLE "t2" (id INT, t1_id INT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
INSERT INTO "t2" (id, t1_id) VALUES (10, 1), (11, 1), (12, 3);
-- input:
SELECT id, name FROM "t1" WHERE EXISTS (SELECT id FROM "t2" WHERE t1_id = 1) ORDER BY id;
-- expected output:
1|alice
2|bob
3|charlie
-- expected status: 0
