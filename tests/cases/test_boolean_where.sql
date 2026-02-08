-- boolean column with WHERE filter
-- setup:
CREATE TABLE "t1" (id INT, active BOOLEAN);
INSERT INTO "t1" (id, active) VALUES (1, TRUE), (2, FALSE), (3, TRUE);
-- input:
SELECT id, active FROM "t1" WHERE active = TRUE ORDER BY id;
-- expected output:
1|t
3|t
-- expected status: 0
