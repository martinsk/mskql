-- CHECK constraint with DEFAULT value
-- setup:
CREATE TABLE t1 (id INT, score INT DEFAULT 50 CHECK(score >= 0));
-- input:
INSERT INTO t1 (id) VALUES (1);
SELECT id, score FROM t1;
-- expected output:
INSERT 0 1
1|50
-- expected status: 0
