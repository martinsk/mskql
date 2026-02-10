-- COUNT with DISTINCT should count unique non-NULL values
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 10), (3, 20), (4, NULL);
-- input:
SELECT COUNT(DISTINCT val) FROM t1;
-- expected output:
2
-- expected status: 0
