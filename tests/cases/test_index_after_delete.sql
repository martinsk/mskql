-- index should be rebuilt after DELETE so lookups don't return stale rows
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 10);
CREATE INDEX idx_val ON t1 (val);
-- input:
DELETE FROM t1 WHERE id = 1;
SELECT id FROM t1 WHERE val = 10;
-- expected output:
DELETE 1
3
-- expected status: 0
