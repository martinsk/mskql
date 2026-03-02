-- Bug: value IN (SELECT col FROM t) causes parse error "expected FROM, got 'col'"
-- In PostgreSQL, IN with a subquery is standard SQL and must work
-- mskql fails to parse this common pattern
-- setup:
CREATE TABLE t_insq (v INT);
INSERT INTO t_insq VALUES (1),(2),(3);
-- input:
SELECT 2 IN (SELECT v FROM t_insq);
-- expected output:
t
-- expected status: 0
