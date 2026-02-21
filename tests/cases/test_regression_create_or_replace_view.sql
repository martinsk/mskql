-- BUG: CREATE OR REPLACE VIEW not supported (errors with 'relation already exists')
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20);
CREATE VIEW v AS SELECT * FROM t;
-- input:
CREATE OR REPLACE VIEW v AS SELECT id, val * 2 AS doubled FROM t;
-- expected output:
CREATE VIEW
-- expected status: 0
