-- case when with text comparison
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT);
INSERT INTO "t1" (id, dept) VALUES (1, 'sales'), (2, 'engineering'), (3, 'sales');
-- input:
SELECT id, CASE WHEN dept = 'sales' THEN 'S' ELSE 'O' END FROM "t1" ORDER BY id;
-- expected output:
1|S
2|O
3|S
-- expected status: 0
