-- insert without column list
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" VALUES (1, 'alice'), (2, 'bob');
-- input:
SELECT * FROM "t1" ORDER BY id;
-- expected output:
1|alice
2|bob
-- expected status: 0
