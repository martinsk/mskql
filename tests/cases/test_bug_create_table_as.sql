-- bug: CREATE TABLE ... AS SELECT is not supported
-- setup:
CREATE TABLE t_src (id INT, val INT);
INSERT INTO t_src VALUES (1, 10), (2, 20), (3, 30);
-- input:
CREATE TABLE t_copy AS SELECT id, val * 2 as doubled FROM t_src WHERE val > 10;
SELECT * FROM t_copy ORDER BY id;
-- expected output:
SELECT 2
2|40
3|60
