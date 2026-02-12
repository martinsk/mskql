-- INITCAP function
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'hello world'), (2, 'ALICE SMITH'), (3, 'foo-bar');
-- input:
SELECT id, INITCAP(name) FROM t1 ORDER BY id;
-- expected output:
1|Hello World
2|Alice Smith
3|Foo-Bar
-- expected status: 0
