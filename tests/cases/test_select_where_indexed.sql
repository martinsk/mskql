-- select with where using index
-- setup:
CREATE TABLE "t1" (id INT, name TEXT);
INSERT INTO "t1" (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
CREATE INDEX idx_t1_id ON "t1" (id);
-- input:
SELECT * FROM "t1" WHERE id = 3;
-- expected output:
3|charlie
-- expected status: 0
