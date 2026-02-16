-- Test MIN/MAX on TEXT columns with GROUP BY through plan executor
-- setup:
CREATE TABLE t_mmtg (category TEXT, name TEXT);
INSERT INTO t_mmtg VALUES ('x', 'charlie');
INSERT INTO t_mmtg VALUES ('x', 'alice');
INSERT INTO t_mmtg VALUES ('x', 'bob');
INSERT INTO t_mmtg VALUES ('y', 'dave');
INSERT INTO t_mmtg VALUES ('y', NULL);
INSERT INTO t_mmtg VALUES ('y', 'eve');
-- input:
SELECT category, MIN(name), MAX(name) FROM t_mmtg GROUP BY category ORDER BY category;
-- expected output:
x|alice|charlie
y|dave|eve
