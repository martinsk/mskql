-- SUBSTRING of NULL returns NULL
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 VALUES (1, NULL);
-- input:
SELECT SUBSTRING(val FROM 1 FOR 3) FROM t1;
-- expected output:

-- expected status: 0
