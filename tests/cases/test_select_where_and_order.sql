-- select with where and order by combined
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 30), (2, 10), (3, 20), (4, 50), (5, 40);
-- input:
SELECT id, val FROM "t1" WHERE val > 15 ORDER BY val DESC;
-- expected output:
4|50
5|40
1|30
3|20
-- expected status: 0
