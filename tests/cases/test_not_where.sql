-- NOT condition in WHERE
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id, val FROM "t1" WHERE NOT val = 20 ORDER BY id;
-- expected output:
1|10
3|30
-- expected status: 0
