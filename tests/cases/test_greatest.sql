-- GREATEST returns the largest value
-- setup:
CREATE TABLE "t1" (id INT, a INT, b INT, c INT);
INSERT INTO "t1" (id, a, b, c) VALUES (1, 10, 20, 15), (2, 50, 30, 40);
-- input:
SELECT id, GREATEST(a, b, c) FROM "t1" ORDER BY id;
-- expected output:
1|20
2|50
-- expected status: 0
