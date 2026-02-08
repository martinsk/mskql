-- select distinct values
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT);
INSERT INTO "t1" (id, dept) VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'b'), (5, 'c');
-- input:
SELECT DISTINCT dept FROM "t1";
-- expected output:
a
b
c
-- expected status: 0
