-- order by ascending
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (3, 'charlie'), (1, 'alice'), (2, 'bob');
-- input:
SELECT * FROM "t1" ORDER BY id;
-- expected output:
1|alice
2|bob
3|charlie
-- expected status: 0
