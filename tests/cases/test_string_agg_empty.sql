-- STRING_AGG on empty table returns NULL
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
-- input:
SELECT STRING_AGG(name, ',') FROM t1;
-- expected output:

-- expected status: 0
