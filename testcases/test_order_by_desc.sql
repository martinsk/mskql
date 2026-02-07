-- order by descending
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (3, 'charlie'), (2, 'bob');
-- input:
SELECT * FROM "t1" ORDER BY id DESC;
-- expected output:
3|charlie
2|bob
1|alice
-- expected status: 0
