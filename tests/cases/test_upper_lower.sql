-- UPPER and LOWER string functions
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'Alice'), (2, 'Bob');
-- input:
SELECT id, UPPER(name), LOWER(name) FROM "t1" ORDER BY id;
-- expected output:
1|ALICE|alice
2|BOB|bob
-- expected status: 0
