-- column and table aliases
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20);
-- input:
SELECT id AS user_id, val AS amount FROM "t1" AS t ORDER BY id;
-- expected output:
1|10
2|20
-- expected status: 0
