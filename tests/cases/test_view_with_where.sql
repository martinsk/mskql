-- View with WHERE clause in the view definition
-- setup:
CREATE TABLE t1 (id INT, name TEXT, active INT);
INSERT INTO t1 (id, name, active) VALUES (1, 'alice', 1), (2, 'bob', 0), (3, 'carol', 1);
CREATE VIEW active_users AS SELECT id, name FROM t1 WHERE active = 1;
-- input:
SELECT * FROM active_users ORDER BY id;
-- expected output:
1|alice
3|carol
-- expected status: 0
