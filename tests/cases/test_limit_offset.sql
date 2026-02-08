-- limit with offset
-- setup:
CREATE TABLE "t1" (id INT);
INSERT INTO "t1" (id) VALUES (1), (2), (3), (4), (5);
-- input:
SELECT * FROM "t1" ORDER BY id LIMIT 2 OFFSET 2;
-- expected output:
3
4
-- expected status: 0
