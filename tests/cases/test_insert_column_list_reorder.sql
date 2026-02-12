-- INSERT with column list in different order than table definition
-- setup:
CREATE TABLE t1 (id INT, name TEXT, age INT);
-- input:
INSERT INTO t1 (age, name, id) VALUES (30, 'alice', 1);
SELECT id, name, age FROM t1;
-- expected output:
INSERT 0 1
1|alice|30
-- expected status: 0
