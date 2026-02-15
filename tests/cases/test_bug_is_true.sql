-- bug: IS TRUE / IS NOT TRUE fails with parse error
-- setup:
CREATE TABLE t_ist (id INT, active BOOLEAN);
INSERT INTO t_ist VALUES (1, TRUE), (2, FALSE), (3, TRUE), (4, NULL);
-- input:
SELECT id FROM t_ist WHERE active IS TRUE ORDER BY id;
-- expected output:
1
3
