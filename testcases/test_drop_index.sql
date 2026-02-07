-- drop index
-- setup:
CREATE TABLE "t1" (id INT);
CREATE INDEX idx_id ON "t1" (id);
-- input:
DROP INDEX idx_id;
-- expected output:
DROP INDEX
-- expected status: 0
