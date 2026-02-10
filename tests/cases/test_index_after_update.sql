-- index should be rebuilt after UPDATE so lookups use new values
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20);
CREATE INDEX idx_val ON t1 (val);
-- input:
UPDATE t1 SET val = 30 WHERE id = 1;
SELECT id FROM t1 WHERE val = 30;
-- expected output:
UPDATE 1
1
-- expected status: 0
