-- aggregates on empty table: COUNT should return 0, SUM/AVG/MIN/MAX should return NULL
-- setup:
CREATE TABLE empty_t (id INT, val INT);
-- input:
SELECT COUNT(*) FROM empty_t;
-- expected output:
0
-- expected status: 0
