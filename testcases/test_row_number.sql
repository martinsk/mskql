-- row_number window function
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (3, 'charlie'), (1, 'alice'), (2, 'bob');
-- input:
SELECT name, ROW_NUMBER() OVER (ORDER BY id) FROM "t1";
-- expected output:
alice|1
bob|2
charlie|3
-- expected status: 0
