-- Test FROM subquery with outer WHERE through plan executor
-- setup:
CREATE TABLE t_fsw (id INT, name TEXT, score INT);
INSERT INTO t_fsw VALUES (1, 'alice', 80);
INSERT INTO t_fsw VALUES (2, 'bob', 90);
INSERT INTO t_fsw VALUES (3, 'charlie', 70);
INSERT INTO t_fsw VALUES (4, 'dave', 95);
-- input:
SELECT name, score FROM (SELECT * FROM t_fsw WHERE score >= 80) AS sub WHERE score < 95 ORDER BY name;
-- expected output:
alice|80
bob|90
