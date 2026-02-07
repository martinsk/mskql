-- select all rows
-- setup:
CREATE TABLE "t1" (id INT);
INSERT INTO "t1" (id) VALUES (1), (2), (3), (4), (5);
-- input:
SELECT * FROM "t1";
-- expected output:
1
2
3
4
5
-- expected status: 0
