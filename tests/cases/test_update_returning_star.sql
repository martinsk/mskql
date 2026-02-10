-- UPDATE with RETURNING * should return all columns of updated rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT, score INT);
INSERT INTO t1 (id, name, score) VALUES (1, 'alice', 80), (2, 'bob', 90);
-- input:
UPDATE t1 SET score = 100 WHERE id = 1 RETURNING *;
-- expected output:
1|alice|100
UPDATE 1
-- expected status: 0
