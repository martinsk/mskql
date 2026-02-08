-- insert with returning
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
-- input:
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob') RETURNING id;
-- expected output:
1
2
INSERT 0 2
-- expected status: 0
