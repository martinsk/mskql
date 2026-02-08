-- sum over partition by
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT, val INT);
INSERT INTO "t1" (id, dept, val) VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30);
-- input:
SELECT id, SUM(val) OVER (PARTITION BY dept) FROM "t1";
-- expected output:
1|30
2|30
3|30
-- expected status: 0
