-- select with text columns
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob');
-- input:
SELECT * FROM "t1";
-- expected output:
1|alice
2|bob
-- expected status: 0
