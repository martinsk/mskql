-- group by with aggregates
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT, val INT);
INSERT INTO "t1" (id, dept, val) VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 40), (5, 'b', 50);
-- input:
SELECT dept, SUM(val), COUNT(*) FROM "t1" GROUP BY dept;
-- expected output:
a|30|2
b|120|3
-- expected status: 0
