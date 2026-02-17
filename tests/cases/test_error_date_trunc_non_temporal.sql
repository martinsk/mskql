-- DATE_TRUNC on non-temporal type should error
-- setup:
CREATE TABLE t1 (val INT);
INSERT INTO t1 (val) VALUES (42);
-- input:
SELECT DATE_TRUNC('month', val) FROM t1;
-- expected output:
ERROR:  function date_trunc(text, ...) does not exist for this type
-- expected status: 1
