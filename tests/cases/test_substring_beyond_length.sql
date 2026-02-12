-- SUBSTRING with start beyond string length returns empty
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 VALUES (1, 'hi');
-- input:
SELECT SUBSTRING(val FROM 10 FOR 5) FROM t1;
-- expected output:

-- expected status: 0
