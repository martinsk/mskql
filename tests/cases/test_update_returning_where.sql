-- UPDATE with WHERE and RETURNING should only return updated rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT, score INT);
INSERT INTO t1 (id, name, score) VALUES (1, 'alice', 80), (2, 'bob', 90), (3, 'carol', 70);
-- input:
UPDATE t1 SET score = 100 WHERE score < 85 RETURNING id, name, score;
-- expected output:
1|alice|100
3|carol|100
UPDATE 2
-- expected status: 0
