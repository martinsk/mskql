-- adversarial: ORDER BY on empty table
-- setup:
CREATE TABLE t_empty3 (id INT, name TEXT);
-- input:
SELECT * FROM t_empty3 ORDER BY id;
-- expected output:
