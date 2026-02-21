-- BUG: ALTER TABLE ADD CONSTRAINT corrupts table schema (subsequent INSERTs fail with column count mismatch)
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10);
ALTER TABLE t ADD CONSTRAINT chk_val CHECK (val > 0);
-- input:
INSERT INTO t VALUES (2, 20);
-- expected output:
INSERT 0 1
-- expected status: 0
