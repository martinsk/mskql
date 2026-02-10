-- NULLIF returns NULL when both args are equal
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 5), (2, 0), (3, 5);
-- input:
SELECT id, NULLIF(val, 5) FROM "t1" ORDER BY id;
-- expected output:
1|
2|0
3|
-- expected status: 0
