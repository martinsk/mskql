-- CAST boolean to text
-- setup:
CREATE TABLE t1 (id INT, val BOOLEAN);
INSERT INTO t1 (id, val) VALUES (1, TRUE);
-- input:
SELECT CAST(val AS TEXT) FROM t1;
-- expected output:
true
-- expected status: 0
