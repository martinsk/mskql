-- CAST text to integer
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 (id, val) VALUES (1, '123');
-- input:
SELECT CAST(val AS INT) FROM t1;
-- expected output:
123
-- expected status: 0
