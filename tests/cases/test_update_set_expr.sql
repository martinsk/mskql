-- UPDATE SET with arithmetic expression (col = col + value) should compute new value
-- setup:
CREATE TABLE t1 (id INT, score INT);
INSERT INTO t1 (id, score) VALUES (1, 80), (2, 90);
-- input:
UPDATE t1 SET score = score + 10 WHERE id = 1;
SELECT id, score FROM t1 ORDER BY id;
-- expected output:
UPDATE 1
1|90
2|90
-- expected status: 0
