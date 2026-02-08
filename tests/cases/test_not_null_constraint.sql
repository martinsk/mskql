-- not null constraint rejects null insert
-- setup:
CREATE TABLE "t1" (id INT, name TEXT NOT NULL);
INSERT INTO "t1" (id, name) VALUES (1, 'alice');
-- input:
INSERT INTO "t1" (id) VALUES (2);
-- expected output:
-- expected status: 2
