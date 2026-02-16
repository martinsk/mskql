-- bug: GENERATED ALWAYS AS IDENTITY column gets empty value instead of auto-increment
-- setup:
CREATE TABLE t_gai (id INT GENERATED ALWAYS AS IDENTITY, name TEXT);
-- input:
INSERT INTO t_gai (name) VALUES ('test');
SELECT id, name FROM t_gai;
-- expected output:
INSERT 0 1
1|test
