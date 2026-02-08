-- count over partition by
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT, val INT);
INSERT INTO "t1" (id, dept, val) VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 40);
-- input:
SELECT id, COUNT(*) OVER (PARTITION BY dept) FROM "t1";
-- expected output:
1|2
2|2
3|2
4|2
-- expected status: 0
