-- where with LIKE pattern
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'anna'), (4, 'dave');
-- input:
SELECT name FROM "t1" WHERE name LIKE 'a%';
-- expected output:
alice
anna
-- expected status: 0
