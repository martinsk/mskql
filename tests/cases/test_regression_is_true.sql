-- regression: IS TRUE / IS NOT TRUE
-- setup:
CREATE TABLE t (id INT, active BOOLEAN);
INSERT INTO t VALUES (1,TRUE),(2,FALSE),(3,TRUE),(4,NULL);
-- input:
SELECT id FROM t WHERE active IS TRUE ORDER BY id;
-- expected output:
1
3
