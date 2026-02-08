-- rank window function
-- setup:
CREATE TABLE "t1" (id INT, score INT);
INSERT INTO "t1" (id, score) VALUES (1, 100), (2, 90), (3, 100), (4, 80);
-- input:
SELECT id, RANK() OVER (ORDER BY score) FROM "t1";
-- expected output:
4|1
2|2
1|3
3|3
-- expected status: 0
