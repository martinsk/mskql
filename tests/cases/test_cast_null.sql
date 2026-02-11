-- CAST NULL preserves NULL
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, NULL);
-- input:
SELECT CAST(val AS TEXT) FROM t1;
-- expected output:

-- expected status: 0
