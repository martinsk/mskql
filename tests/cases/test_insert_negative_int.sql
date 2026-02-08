-- insert and select negative integer values
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, -5), (2, -10), (3, 15);
-- input:
SELECT id, val FROM "t1" ORDER BY id;
-- expected output:
1|-5
2|-10
3|15
-- expected status: 0
