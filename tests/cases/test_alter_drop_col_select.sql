-- ALTER TABLE DROP COLUMN then SELECT should not include dropped column
-- setup:
CREATE TABLE t1 (id INT, name TEXT, score INT);
INSERT INTO t1 (id, name, score) VALUES (1, 'alice', 100), (2, 'bob', 200);
ALTER TABLE t1 DROP COLUMN score;
-- input:
SELECT * FROM t1 ORDER BY id;
-- expected output:
1|alice
2|bob
-- expected status: 0
