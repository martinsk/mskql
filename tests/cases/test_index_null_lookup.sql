-- index lookup should handle NULL values correctly
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL), (3, 20);
CREATE INDEX idx_val ON t1 (val);
-- input:
SELECT id FROM t1 WHERE val = 10;
-- expected output:
1
-- expected status: 0
