-- bug: COALESCE(SUM(...), 0) in LEFT JOIN + GROUP BY returns all zeros instead of correct sums
-- setup:
CREATE TABLE t_clj_parent (id INT, name TEXT);
CREATE TABLE t_clj_child (id INT, parent_id INT, amount INT);
INSERT INTO t_clj_parent VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
INSERT INTO t_clj_child VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150);
-- input:
SELECT p.name, COALESCE(SUM(c.amount), 0) AS total FROM t_clj_parent p LEFT JOIN t_clj_child c ON p.id = c.parent_id GROUP BY p.name ORDER BY p.name;
-- expected output:
alice|300
bob|150
charlie|0
-- expected status: 0
