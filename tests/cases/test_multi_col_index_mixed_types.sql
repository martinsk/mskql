-- multi-column index: composite index on (INT, TEXT) columns
-- setup:
CREATE TABLE t_mcimt (id INT, name TEXT, val INT);
CREATE INDEX idx_mcimt ON t_mcimt (id, name);
INSERT INTO t_mcimt VALUES (1, 'alice', 100);
INSERT INTO t_mcimt VALUES (1, 'bob', 200);
INSERT INTO t_mcimt VALUES (2, 'alice', 300);
-- input:
SELECT val FROM t_mcimt WHERE id = 1 AND name = 'bob';
-- expected output:
200
