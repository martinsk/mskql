-- NOT NULL column with DEFAULT: insert without specifying column should use default
-- setup:
CREATE TABLE t1 (id INT, status TEXT NOT NULL DEFAULT 'active');
-- input:
INSERT INTO t1 (id) VALUES (1);
SELECT id, status FROM t1;
-- expected output:
INSERT 0 1
1|active
-- expected status: 0
