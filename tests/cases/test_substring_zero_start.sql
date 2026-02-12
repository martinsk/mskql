-- SUBSTRING with start position 0 (should behave like 1 per SQL standard)
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 VALUES (1, 'hello');
-- input:
SELECT SUBSTRING(val FROM 0 FOR 3) FROM t1;
-- expected output:
hel
-- expected status: 0
