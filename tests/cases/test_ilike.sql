-- ilike case insensitive pattern matching
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'Alice'), (2, 'BOB'), (3, 'alice'), (4, 'Charlie');
-- input:
SELECT id, name FROM "t1" WHERE name ILIKE 'alice' ORDER BY id;
-- expected output:
1|Alice
3|alice
-- expected status: 0
