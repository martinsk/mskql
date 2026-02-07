-- distinct with order by
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT);
INSERT INTO "t1" (id, dept) VALUES (1, 'b'), (2, 'a'), (3, 'b'), (4, 'a'), (5, 'c');
-- input:
SELECT DISTINCT dept FROM "t1" ORDER BY dept;
-- expected output:
a
b
c
-- expected status: 0
