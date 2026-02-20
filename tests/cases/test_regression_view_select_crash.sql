-- BUG: SELECT from a VIEW crashes the server
-- setup:
CREATE TABLE t (id INT, name TEXT, active BOOLEAN);
INSERT INTO t VALUES (1, 'Alice', TRUE), (2, 'Bob', FALSE), (3, 'Charlie', TRUE);
CREATE VIEW v_active AS SELECT id, name FROM t WHERE active;
-- input:
SELECT * FROM v_active ORDER BY id;
-- expected output:
1|Alice
3|Charlie
-- expected status: 0
