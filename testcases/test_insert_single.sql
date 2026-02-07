-- insert single row
-- setup:
CREATE TABLE "t1" (id INT);
-- input:
INSERT INTO "t1" (id) VALUES (1);
-- expected output:
INSERT 0 1
-- expected status: 0
