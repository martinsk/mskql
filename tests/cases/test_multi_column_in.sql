-- multi-column IN
-- setup:
CREATE TABLE "t1" (id INT, a INT, b INT);
INSERT INTO "t1" (id, a, b) VALUES (1, 10, 20), (2, 30, 40), (3, 10, 40), (4, 50, 60);
-- input:
SELECT id FROM "t1" WHERE (a, b) IN ((10, 20), (30, 40)) ORDER BY id;
-- expected output:
1
2
-- expected status: 0
