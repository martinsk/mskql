-- adversarial: INSERT ON CONFLICT DO NOTHING — should silently skip
-- setup:
CREATE TABLE t_ocdn (id INT PRIMARY KEY, val INT);
INSERT INTO t_ocdn VALUES (1, 10);
INSERT INTO t_ocdn VALUES (1, 99) ON CONFLICT (id) DO NOTHING;
-- input:
SELECT id, val FROM t_ocdn;
-- expected output:
1|10
