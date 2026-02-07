-- insert multi-row
-- setup:
CREATE TABLE "t1" (id INT);
-- input:
INSERT INTO "t1" (id) VALUES (1), (2), (3);
-- expected output:
INSERT 0 3
-- expected status: 0
