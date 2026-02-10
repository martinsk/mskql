-- LENGTH string function
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'Alice'), (2, 'Bob');
-- input:
SELECT id, LENGTH(name) FROM "t1" ORDER BY id;
-- expected output:
1|5
2|3
-- expected status: 0
