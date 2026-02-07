-- limit rows
-- setup:
CREATE TABLE "t1" (id INT);
INSERT INTO "t1" (id) VALUES (1), (2), (3), (4), (5);
-- input:
SELECT * FROM "t1" ORDER BY id LIMIT 3;
-- expected output:
1
2
3
-- expected status: 0
