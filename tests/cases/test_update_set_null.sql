-- update set column to null
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob');
UPDATE "t1" SET name = NULL WHERE id = 1;
-- input:
SELECT * FROM "t1" WHERE name IS NULL;
-- expected output:
1|
-- expected status: 0
