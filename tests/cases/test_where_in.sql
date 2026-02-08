-- where with IN list
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie'), (4, 'dave');
-- input:
SELECT name FROM "t1" WHERE id IN (1, 3);
-- expected output:
alice
charlie
-- expected status: 0
