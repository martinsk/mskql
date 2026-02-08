-- transaction rollback restores previous state
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice');
BEGIN;
INSERT INTO "t1" (id, name) VALUES (2, 'bob');
ROLLBACK;
-- input:
SELECT id, name FROM "t1" ORDER BY id;
-- expected output:
1|alice
-- expected status: 0
