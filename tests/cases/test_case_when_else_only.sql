-- CASE with only ELSE (no WHEN branches) should return the ELSE value
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 (id) VALUES (1);
-- input:
SELECT CASE ELSE 'default' END FROM t1;
-- expected output:
default
-- expected status: 0
