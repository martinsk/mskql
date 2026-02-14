-- adversarial: SQL injection via unescaped quotes in correlated scalar subquery
-- cell_to_sql_literal formats text as '%s' without escaping embedded quotes.
-- subst_correlated_refs substitutes outer column refs into the scalar subquery,
-- so a text value with a single quote breaks the generated SQL.
-- setup:
CREATE TABLE t_ssq_outer (id INT, name TEXT);
CREATE TABLE t_ssq_inner (name TEXT, score INT);
INSERT INTO t_ssq_outer VALUES (1, 'O''Brien');
INSERT INTO t_ssq_outer VALUES (2, 'Smith');
INSERT INTO t_ssq_inner VALUES ('O''Brien', 100);
INSERT INTO t_ssq_inner VALUES ('Smith', 200);
-- input:
SELECT o.id, (SELECT score FROM t_ssq_inner i WHERE i.name = o.name) AS score FROM t_ssq_outer o ORDER BY o.id;
-- expected output:
1|100
2|200
