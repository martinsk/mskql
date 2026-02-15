-- bug: LATERAL JOIN returns NULLs for aggregated column
-- setup:
CREATE TABLE t_lat1 (id INT, name TEXT);
CREATE TABLE t_lat2 (ref_id INT, val INT);
INSERT INTO t_lat1 VALUES (1, 'alice'), (2, 'bob');
INSERT INTO t_lat2 VALUES (1, 100), (1, 200), (2, 300);
-- input:
SELECT t_lat1.name, lat.total FROM t_lat1, LATERAL (SELECT SUM(val) as total FROM t_lat2 WHERE ref_id = t_lat1.id) lat ORDER BY t_lat1.name;
-- expected output:
alice|300
bob|300
