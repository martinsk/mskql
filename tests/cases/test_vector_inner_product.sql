-- vector: inner_product function computes negative dot product
-- setup:
CREATE TABLE t_vip (id INT, v VECTOR(3));
INSERT INTO t_vip VALUES (1, '[1,2,3]');
INSERT INTO t_vip VALUES (2, '[4,5,6]');
-- input:
SELECT id, inner_product(v, '[1,1,1]') FROM t_vip ORDER BY id;
-- expected output:
1|-6
2|-15
