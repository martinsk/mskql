-- ORDER BY two columns with mixed ASC/DESC
-- setup:
CREATE TABLE t1 (dept TEXT, name TEXT, score INT);
INSERT INTO t1 (dept, name, score) VALUES ('eng', 'alice', 90), ('eng', 'bob', 80), ('sales', 'carol', 90), ('sales', 'dave', 70);
-- input:
SELECT dept, name, score FROM t1 ORDER BY dept ASC, score DESC;
-- expected output:
eng|alice|90
eng|bob|80
sales|carol|90
sales|dave|70
-- expected status: 0
