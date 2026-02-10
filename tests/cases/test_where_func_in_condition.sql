-- WHERE with function call on LHS: WHERE UPPER(name) = 'ALICE'
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'Alice'), (2, 'bob'), (3, 'CAROL');
-- input:
SELECT id FROM t1 WHERE UPPER(name) = 'ALICE';
-- expected output:
1
-- expected status: 0
