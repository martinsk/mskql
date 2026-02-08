-- where with AND
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
-- input:
SELECT * FROM "t1" WHERE val >= 20 AND val <= 40 ORDER BY id;
-- expected output:
2|20
3|30
4|40
-- expected status: 0
