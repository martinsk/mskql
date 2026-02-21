-- BUG: CREATE UNIQUE INDEX not supported
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20);
-- input:
CREATE UNIQUE INDEX idx_val ON t (val);
-- expected output:
CREATE INDEX
-- expected status: 0
