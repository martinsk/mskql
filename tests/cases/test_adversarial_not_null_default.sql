-- adversarial: NOT NULL column with DEFAULT — insert without specifying should use default
-- setup:
CREATE TABLE t_nnd (id SERIAL, name TEXT NOT NULL DEFAULT 'unnamed');
INSERT INTO t_nnd (id) VALUES (1);
-- input:
SELECT id, name FROM t_nnd;
-- expected output:
1|unnamed
