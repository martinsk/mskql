-- Regression: eval_condition_flat via filter_next fallback (flat_row_ref path)
-- Exercises complex filter conditions that fall through to the slow path.
-- setup:
CREATE TABLE frr_filt (a INT, b INT, label TEXT);
INSERT INTO frr_filt (a, b, label) VALUES (1, 10, 'apple'), (2, 20, 'banana'), (3, 30, 'cherry'), (4, 40, 'date'), (5, 50, 'elderberry');
-- input:
SELECT a, label FROM frr_filt WHERE (a > 2 AND b < 45) OR label = 'apple' ORDER BY a;
-- expected output:
1|apple
3|cherry
4|date
-- expected status: 0
