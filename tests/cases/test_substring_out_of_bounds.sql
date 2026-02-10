-- SUBSTRING with start beyond string length should return empty string
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 (id, val) VALUES (1, 'abc');
-- input:
SELECT id, SUBSTRING(val, 10, 5) FROM t1;
-- expected output:
1|
-- expected status: 0
