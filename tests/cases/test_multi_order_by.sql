-- multi-column order by
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT, val INT);
INSERT INTO "t1" (id, dept, val) VALUES (1, 'b', 10), (2, 'a', 20), (3, 'a', 10), (4, 'b', 20);
-- input:
SELECT id, dept, val FROM "t1" ORDER BY dept ASC, val DESC;
-- expected output:
2|a|20
3|a|10
4|b|20
1|b|10
-- expected status: 0
