-- adversarial: window function on empty table
-- setup:
CREATE TABLE t_we (id INT, val INT);
-- input:
SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM t_we;
-- expected output:
