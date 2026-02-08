-- select with where clause (no index, full scan)
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- input:
SELECT * FROM "t1" WHERE id = 2;
-- expected output:
2|bob
-- expected status: 0
