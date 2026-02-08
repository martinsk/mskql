-- window function with where clause
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT, val INT);
INSERT INTO "t1" (id, dept, val) VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 40);
-- input:
SELECT dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY val) FROM "t1" WHERE val > 15;
-- expected output:
a|1
b|1
b|2
-- expected status: 0
