-- index backfills existing rows
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (10, 'x'), (20, 'y'), (30, 'z');
CREATE INDEX idx_id ON "t1" (id);
-- input:
SELECT * FROM "t1" WHERE id = 20;
-- expected output:
20|y
-- expected status: 0
