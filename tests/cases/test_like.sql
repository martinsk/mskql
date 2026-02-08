-- like pattern matching
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'alicia'), (4, 'charlie');
-- input:
SELECT id, name FROM "t1" WHERE name LIKE 'ali%' ORDER BY id;
-- expected output:
1|alice
3|alicia
-- expected status: 0
