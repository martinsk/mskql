-- parenthesized OR with AND in WHERE
-- setup:
CREATE TABLE "t1" (id INT, dept TEXT, val INT);
INSERT INTO "t1" (id, dept, val) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'c', 40);
-- input:
SELECT id, dept, val FROM "t1" WHERE (dept = 'a' OR dept = 'c') AND val > 15 ORDER BY id;
-- expected output:
3|a|30
4|c|40
-- expected status: 0
