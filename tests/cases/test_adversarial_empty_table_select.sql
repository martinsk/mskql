-- adversarial: SELECT * from empty table should return no rows, not crash
-- setup:
CREATE TABLE t_empty2 (id INT, name TEXT);
-- input:
SELECT * FROM t_empty2;
-- expected output:
