-- CASE WHEN with no matching condition and no ELSE returns NULL
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 100);
-- input:
SELECT CASE WHEN val = 0 THEN 'zero' END FROM t1;
-- expected output:

-- expected status: 0
