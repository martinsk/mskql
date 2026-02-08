-- delete with compound where (AND)
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT, val INT);
INSERT INTO "t1" (id, dept, val) VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30);
DELETE FROM "t1" WHERE dept = 'a' AND val > 15;
-- input:
SELECT * FROM "t1" ORDER BY id;
-- expected output:
1|a|10
3|b|30
-- expected status: 0
