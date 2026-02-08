-- create index
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- input:
CREATE INDEX idx_t1_id ON "t1" (id);
-- expected output:
CREATE INDEX
-- expected status: 0
