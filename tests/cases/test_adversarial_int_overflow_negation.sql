-- adversarial: negating INT_MIN is undefined behavior in C (signed overflow)
-- -(-2147483648) overflows 32-bit int
-- setup:
CREATE TABLE t_neg (v INT);
INSERT INTO t_neg VALUES (-2147483648);
-- input:
SELECT ABS(v) FROM t_neg;
-- expected output:
-2147483648
