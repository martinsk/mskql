-- CASE WHEN with OR condition
-- setup:
CREATE TABLE t1 (id INT, status TEXT);
INSERT INTO t1 (id, status) VALUES (1, 'active'), (2, 'pending'), (3, 'closed');
-- input:
SELECT id, CASE WHEN status = 'active' OR status = 'pending' THEN 'open' ELSE 'done' END FROM t1 ORDER BY id;
-- expected output:
1|open
2|open
3|done
-- expected status: 0
