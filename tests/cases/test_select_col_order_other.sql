-- select single column ordered by a different column
-- setup:
CREATE TABLE "t1" (id INT, name TEXT, val INT);
INSERT INTO "t1" (id, name, val) VALUES (1, 'charlie', 30), (2, 'alice', 10), (3, 'bob', 20);
-- input:
SELECT name FROM "t1" ORDER BY val;
-- expected output:
alice
bob
charlie
-- expected status: 0
