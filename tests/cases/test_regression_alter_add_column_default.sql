-- regression: ALTER TABLE ADD COLUMN DEFAULT applies to existing rows
-- setup:
CREATE TABLE t (id INT);
INSERT INTO t VALUES (1),(2);
ALTER TABLE t ADD COLUMN score INT DEFAULT 0;
-- input:
SELECT id, score FROM t ORDER BY id;
-- expected output:
1|0
2|0
