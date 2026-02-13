-- adversarial: LIKE with many % wildcards — potential exponential backtracking
-- setup:
CREATE TABLE t_like (s TEXT);
INSERT INTO t_like VALUES ('aaaaaaaaaaaaaaaaaaaab');
-- input:
SELECT s FROM t_like WHERE s LIKE '%a%a%a%a%a%a%a%a%a%b';
-- expected output:
aaaaaaaaaaaaaaaaaaaab
