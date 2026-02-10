-- modulo operator %
-- setup:
CREATE TABLE "t1" (id INT, val INT);
INSERT INTO "t1" (id, val) VALUES (1, 10), (2, 7), (3, 15);
-- input:
SELECT id, val % 3 FROM "t1" ORDER BY id;
-- expected output:
1|1
2|1
3|0
-- expected status: 0
