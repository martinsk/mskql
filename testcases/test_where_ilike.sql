-- where with ILIKE case-insensitive pattern
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'Alice'), (2, 'BOB'), (3, 'anna'), (4, 'Dave');
-- input:
SELECT name FROM "t1" WHERE name ILIKE 'a%';
-- expected output:
Alice
anna
-- expected status: 0
