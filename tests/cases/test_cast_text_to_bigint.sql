-- CAST text to bigint
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 (id, val) VALUES (1, '9999999999');
-- input:
SELECT val::BIGINT FROM t1;
-- expected output:
9999999999
-- expected status: 0
