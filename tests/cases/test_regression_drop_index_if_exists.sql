-- BUG: DROP INDEX IF EXISTS not supported (parses IF as index name)
-- setup:
CREATE TABLE t (id INT, val INT);
CREATE INDEX idx_val ON t (val);
-- input:
DROP INDEX IF EXISTS idx_val;
-- expected output:
DROP INDEX
-- expected status: 0
