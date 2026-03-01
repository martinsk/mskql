-- Regression test: ORDER BY an alias that refers to an expression column
-- (e.g. amount - fee AS net). Previously returned PLAN_RES_ERR and fell
-- back to the slow legacy executor because alias resolution only handled
-- need_project, not need_expr_project.

SELECT __reset_db();

CREATE TABLE txns (id INT, amount INT, fee INT, category TEXT);
INSERT INTO txns VALUES (1, 10000, 200, 'dining');
INSERT INTO txns VALUES (2, 8000, 150, 'dining');
INSERT INTO txns VALUES (3, 3000, 50, 'dining');
INSERT INTO txns VALUES (4, 6000, 100, 'travel');
INSERT INTO txns VALUES (5, 9000, 180, 'dining');

-- ORDER BY expression alias with WHERE filter (mirrors stress_filtered_expr)
SELECT amount - fee AS net FROM txns WHERE category = 'dining' AND amount > 5000 ORDER BY net DESC;
9800
8820
7850

-- ORDER BY expression alias without WHERE
SELECT amount - fee AS net FROM txns ORDER BY net DESC;
9800
8820
7850
5900
2950

-- ORDER BY expression alias with LIMIT
SELECT amount - fee AS net FROM txns WHERE category = 'dining' AND amount > 5000 ORDER BY net DESC LIMIT 2;
9800
8820
