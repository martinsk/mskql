-- CREATE INDEX IF NOT EXISTS
-- setup:
CREATE TABLE t1 (id INT, name TEXT)
INSERT INTO t1 VALUES (1, 'alice')
INSERT INTO t1 VALUES (2, 'bob')
CREATE INDEX idx_id ON t1 (id)
CREATE INDEX IF NOT EXISTS idx_id ON t1 (id)
-- input:
SELECT * FROM t1 WHERE id = 2
-- expected output:
2|bob
