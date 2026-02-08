-- count on empty table
-- setup:
CREATE TABLE "t1" (id INT);
-- input:
SELECT COUNT(*) FROM "t1";
-- expected output:
0
-- expected status: 0
