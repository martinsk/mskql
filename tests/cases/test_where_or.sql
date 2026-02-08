-- where with OR
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- input:
SELECT * FROM "t1" WHERE id = 1 OR id = 3 ORDER BY id;
-- expected output:
1|alice
3|charlie
-- expected status: 0
