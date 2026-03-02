-- PLAN_LEGACY_EXEC: verify outer query can apply WHERE/ORDER BY/LIMIT
-- over a PLAN_LEGACY_EXEC block source without losing correctness
CREATE TABLE lex_outer_t (id INT, score INT, label TEXT);
INSERT INTO lex_outer_t VALUES (1, 90, 'high'), (2, 40, 'low'), (3, 70, 'mid'), (4, 55, 'low'), (5, 80, 'high');

SELECT id, score, label FROM lex_outer_t ORDER BY score DESC;
-- Expected:
-- 1|90|high
-- 5|80|high
-- 3|70|mid
-- 4|55|low
-- 2|40|low

SELECT id, score FROM lex_outer_t WHERE score > 60 ORDER BY score;
-- Expected:
-- 3|70
-- 5|80
-- 1|90

SELECT COUNT(*), label FROM lex_outer_t GROUP BY label ORDER BY label;
-- Expected:
-- 2|high
-- 2|low
-- 1|mid

DROP TABLE lex_outer_t;
