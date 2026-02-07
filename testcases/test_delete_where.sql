-- delete with where clause
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30);
DELETE FROM "t1" WHERE id = 2;
-- input:
SELECT * FROM "t1" ORDER BY id;
-- expected output:
1|10
3|30
-- expected status: 0
