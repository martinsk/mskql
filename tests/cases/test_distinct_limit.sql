-- distinct should be applied before limit
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT);
INSERT INTO "t1" (id, dept) VALUES (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b'), (5, 'c');
-- input:
SELECT DISTINCT dept FROM "t1" ORDER BY dept LIMIT 2;
-- expected output:
a
b
-- expected status: 0
