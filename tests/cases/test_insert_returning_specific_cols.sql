-- INSERT RETURNING specific columns (not *)
-- setup:
CREATE TABLE t1 (id INT, name TEXT, score INT);
-- input:
INSERT INTO t1 (id, name, score) VALUES (1, 'alice', 100) RETURNING id, name;
-- expected output:
1|alice
INSERT 0 1
-- expected status: 0
