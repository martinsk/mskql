-- adversarial: ALTER TABLE ADD COLUMN then SELECT — existing rows should have NULL for new column
-- setup:
CREATE TABLE t_aac (id INT, name TEXT);
INSERT INTO t_aac VALUES (1, 'alice');
INSERT INTO t_aac VALUES (2, 'bob');
ALTER TABLE t_aac ADD COLUMN age INT;
-- input:
SELECT id, name, age FROM t_aac ORDER BY id;
-- expected output:
1|alice|
2|bob|
