-- select with is null
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, NULL), (3, 'charlie');
-- input:
SELECT id FROM "t1" WHERE name IS NULL;
-- expected output:
2
-- expected status: 0
