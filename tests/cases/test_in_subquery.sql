-- in subquery filter
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
CREATE TABLE "t2" (uid INT, active INT);
INSERT INTO "t2" (uid, active) VALUES (1, 1), (3, 1);
-- input:
SELECT id, name FROM "t1" WHERE id IN (SELECT uid FROM "t2") ORDER BY id;
-- expected output:
1|alice
3|charlie
-- expected status: 0
