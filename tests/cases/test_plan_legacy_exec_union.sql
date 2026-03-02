-- PLAN_LEGACY_EXEC: verify the IR architecture for UNION/SET_OP path
-- These go through L_SET_OP -> build_set_op in logical_to_physical
CREATE TABLE lex_union_a (id INT, val TEXT);
CREATE TABLE lex_union_b (id INT, val TEXT);
INSERT INTO lex_union_a VALUES (1, 'alpha'), (2, 'beta');
INSERT INTO lex_union_b VALUES (2, 'beta'), (3, 'gamma');

SELECT id, val FROM lex_union_a UNION SELECT id, val FROM lex_union_b ORDER BY id;
-- Expected:
-- 1|alpha
-- 2|beta
-- 3|gamma

SELECT id, val FROM lex_union_a UNION ALL SELECT id, val FROM lex_union_b ORDER BY id;
-- Expected:
-- 1|alpha
-- 2|beta
-- 2|beta
-- 3|gamma

DROP TABLE lex_union_a;
DROP TABLE lex_union_b;
