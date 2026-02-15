-- bug: COUNT(*) FILTER (WHERE ...) fails with "expected FROM after aggregates"
-- setup:
CREATE TABLE t_af (grp TEXT, val INT);
INSERT INTO t_af VALUES ('a', 10), ('b', 20), ('a', 30), ('b', 40);
-- input:
SELECT COUNT(*) FILTER (WHERE grp = 'a') as a_count, COUNT(*) FILTER (WHERE grp = 'b') as b_count FROM t_af;
-- expected output:
2|2
