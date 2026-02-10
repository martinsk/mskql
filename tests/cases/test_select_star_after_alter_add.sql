-- SELECT * after ALTER TABLE ADD COLUMN should include new column
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
ALTER TABLE t1 ADD COLUMN age INT;
INSERT INTO t1 (id, name, age) VALUES (2, 'bob', 30);
-- input:
SELECT * FROM t1 ORDER BY id;
-- expected output:
1|alice|
2|bob|30
-- expected status: 0
