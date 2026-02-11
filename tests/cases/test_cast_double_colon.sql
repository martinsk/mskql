-- :: shorthand cast syntax
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 99);
-- input:
SELECT val::TEXT FROM t1;
-- expected output:
99
-- expected status: 0
