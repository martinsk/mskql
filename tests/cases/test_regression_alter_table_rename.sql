-- bug: ALTER TABLE ... RENAME TO fails with "expected TO in RENAME COLUMN"
-- setup:
CREATE TABLE t_art (id INT, val INT);
INSERT INTO t_art VALUES (1, 10);
-- input:
ALTER TABLE t_art RENAME TO t_art_new;
SELECT * FROM t_art_new;
-- expected output:
ALTER TABLE
1|10
