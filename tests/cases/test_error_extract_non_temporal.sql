-- EXTRACT from non-temporal type should error
-- setup:
CREATE TABLE t1 (val INT);
INSERT INTO t1 (val) VALUES (42);
-- input:
SELECT EXTRACT(year FROM val) FROM t1;
-- expected output:
ERROR:  function extract(year, ...) does not exist for this type
-- expected status: 1
