-- update multiple columns
-- setup:
CREATE TABLE "t1" (id INT, name TEXT, val INT);
INSERT INTO "t1" (id, name, val) VALUES (1, 'alice', 10), (2, 'bob', 20);
UPDATE "t1" SET name = 'robert', val = 25 WHERE id = 2;
-- input:
SELECT * FROM "t1" WHERE id = 2;
-- expected output:
2|robert|25
-- expected status: 0
