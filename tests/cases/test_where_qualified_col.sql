-- where clause with table-qualified column name
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT * FROM "t1" WHERE t1.val > 15;
-- expected output:
2|20
3|30
-- expected status: 0
