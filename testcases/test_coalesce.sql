-- COALESCE returns first non-NULL value
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice');
INSERT INTO "t1" VALUES (2, 'bob');
-- input:
SELECT id, COALESCE(name, 'unknown') FROM "t1" ORDER BY id;
-- expected output:
1|alice
2|bob
-- expected status: 0
