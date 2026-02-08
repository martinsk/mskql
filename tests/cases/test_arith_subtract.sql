-- arithmetic subtraction in select
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id, val - 5 FROM "t1" ORDER BY id;
-- expected output:
1|5
2|15
3|25
-- expected status: 0
