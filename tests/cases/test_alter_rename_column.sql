-- alter table rename column
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob');
ALTER TABLE "t1" RENAME COLUMN name TO username;
-- input:
SELECT id, username FROM "t1" ORDER BY id;
-- expected output:
1|alice
2|bob
-- expected status: 0
