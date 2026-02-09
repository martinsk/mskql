-- INSERT multiple rows with RETURNING should return all inserted rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
-- input:
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob') RETURNING *;
-- expected output:
1|alice
2|bob
INSERT 0 2
-- expected status: 0
