-- bug: Non-correlated scalar subquery in SELECT list should return same constant for all rows
-- setup:
CREATE TABLE t_outer (id INT, name TEXT);
CREATE TABLE t_inner (id INT, amount INT);
INSERT INTO t_outer VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
INSERT INTO t_inner VALUES (1, 100), (2, 200), (3, 300);
-- input:
SELECT name, (SELECT MAX(amount) FROM t_inner) as max_amt FROM t_outer ORDER BY name;
-- expected output:
alice|300
bob|300
carol|300
