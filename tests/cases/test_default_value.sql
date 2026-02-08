-- default value fills in missing column
-- setup:
CREATE TABLE "t1" (id INT, status TEXT DEFAULT 'active');
INSERT INTO "t1" (id) VALUES (1);
INSERT INTO "t1" (id, status) VALUES (2, 'inactive');
-- input:
SELECT id, status FROM "t1" ORDER BY id;
-- expected output:
1|active
2|inactive
-- expected status: 0
