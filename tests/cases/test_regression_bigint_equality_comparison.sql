-- BUG: BIGINT column equality comparison with INT literal returns wrong results
-- WHERE val = 100 on BIGINT column matches rows with val=200 too
-- setup:
CREATE TABLE t (id INT, val BIGINT);
INSERT INTO t VALUES (1, 100), (2, 200), (3, 300);
-- input:
SELECT * FROM t WHERE val = 100;
-- expected output:
1|100
-- expected status: 0
