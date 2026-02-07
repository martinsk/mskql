-- select multiple named columns
-- setup:
CREATE TABLE "t1" (id INT, name TEXT, val INT);
INSERT INTO "t1" (id, name, val) VALUES (1, 'alice', 10), (2, 'bob', 20);
-- input:
SELECT id, name FROM "t1" ORDER BY id;
-- expected output:
1|alice
2|bob
-- expected status: 0
