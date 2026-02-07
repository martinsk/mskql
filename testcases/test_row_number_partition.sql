-- row_number with partition by and order by
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT);
INSERT INTO "t1" (id, dept) VALUES (3, 'b'), (1, 'a'), (4, 'b'), (2, 'a');
-- input:
SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY id) FROM "t1";
-- expected output:
1|1
2|2
3|1
4|2
-- expected status: 0
