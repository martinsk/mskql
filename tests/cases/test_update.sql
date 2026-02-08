-- update rows
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30);
UPDATE "t1" SET val = 99 WHERE id = 2;
-- input:
SELECT * FROM "t1" ORDER BY id;
-- expected output:
1|10
2|99
3|30
-- expected status: 0
