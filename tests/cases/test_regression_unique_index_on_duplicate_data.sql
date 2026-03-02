-- Bug: CREATE UNIQUE INDEX does not enforce uniqueness on existing duplicate data
-- In PostgreSQL, creating a unique index on a column that already contains
-- duplicate values raises an error: "could not create unique index"
-- mskql silently creates the index without checking existing data
-- setup:
CREATE TABLE t_uidup (id INT);
INSERT INTO t_uidup VALUES (1),(2),(1),(3);
-- input:
CREATE UNIQUE INDEX t_uidup_id_idx ON t_uidup(id);
-- expected output:
ERROR:  could not create unique index "t_uidup_id_idx"
-- expected status: 1
