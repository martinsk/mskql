-- UPDATE ... FROM with join to another table
-- setup:
CREATE TABLE t1 (id INT, score INT);
INSERT INTO t1 (id, score) VALUES (1, 80), (2, 90);
CREATE TABLE adjustments (id INT, bonus INT);
INSERT INTO adjustments (id, bonus) VALUES (1, 10), (2, 5);
-- input:
UPDATE t1 SET score = score + adjustments.bonus FROM adjustments WHERE t1.id = adjustments.id;
SELECT id, score FROM t1 ORDER BY id;
-- expected output:
UPDATE 2
1|90
2|95
-- expected status: 0
