-- CAST text to date using :: syntax
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 (id, val) VALUES (1, '2024-07-04');
-- input:
SELECT val::DATE FROM t1;
-- expected output:
2024-07-04
-- expected status: 0
