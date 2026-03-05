-- Test vectorized widening/narrowing integer casts
CREATE TABLE t_cast (si SMALLINT, i INT, bi BIGINT);
INSERT INTO t_cast VALUES (10, 1000, 100000);
INSERT INTO t_cast VALUES (20, 2000, 200000);
INSERT INTO t_cast VALUES (30, 3000, 300000);

-- SMALLINT → INT
SELECT si::int FROM t_cast ORDER BY si;
-- expected: 10
-- expected: 20
-- expected: 30

-- INT → BIGINT
SELECT i::bigint FROM t_cast ORDER BY i;
-- expected: 1000
-- expected: 2000
-- expected: 3000

-- SMALLINT → BIGINT
SELECT si::bigint FROM t_cast ORDER BY si;
-- expected: 10
-- expected: 20
-- expected: 30

-- BIGINT → INT (narrowing)
SELECT bi::int FROM t_cast ORDER BY bi;
-- expected: 100000
-- expected: 200000
-- expected: 300000

-- BOOLEAN → INT (passthrough, same storage)
CREATE TABLE t_bool (flag BOOLEAN);
INSERT INTO t_bool VALUES (true);
INSERT INTO t_bool VALUES (false);

SELECT flag::int FROM t_bool ORDER BY flag;
-- expected: 0
-- expected: 1
