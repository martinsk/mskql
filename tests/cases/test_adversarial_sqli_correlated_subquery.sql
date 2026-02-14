-- adversarial: SQL injection via unescaped single quotes in correlated subquery
-- cell_to_sql_literal (query.c) formats text as '%s' without escaping quotes.
-- When subst_correlated_refs substitutes outer.name into the subquery SQL,
-- a value like O'Brien becomes 'O'Brien' which is malformed SQL.
-- This should return the correlated result correctly, not error.
-- setup:
CREATE TABLE t_sqli_outer (id INT, name TEXT);
CREATE TABLE t_sqli_inner (id INT, label TEXT);
INSERT INTO t_sqli_outer VALUES (1, 'O''Brien');
INSERT INTO t_sqli_outer VALUES (2, 'normal');
INSERT INTO t_sqli_inner VALUES (1, 'O''Brien');
INSERT INTO t_sqli_inner VALUES (2, 'normal');
-- input:
SELECT o.id, o.name FROM t_sqli_outer o WHERE EXISTS (SELECT 1 FROM t_sqli_inner i WHERE i.label = o.name) ORDER BY o.id;
-- expected output:
1|O'Brien
2|normal
