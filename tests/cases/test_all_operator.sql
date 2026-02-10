-- ALL operator with ARRAY syntax
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30), (4, 40);
-- input:
SELECT id, val FROM "t1" WHERE val > ALL(ARRAY[5, 15, 25]) ORDER BY id;
-- expected output:
3|30
4|40
-- expected status: 0
