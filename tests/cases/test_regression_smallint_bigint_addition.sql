-- SMALLINT + BIGINT should return exact BIGINT, not scientific notation
-- setup:
CREATE TABLE t (id INT, s SMALLINT, b BIGINT);
INSERT INTO t VALUES (1, 100, 9999999999);
-- input:
SELECT id, s + b FROM t;
-- expected output:
1|10000000099
-- expected status: 0
