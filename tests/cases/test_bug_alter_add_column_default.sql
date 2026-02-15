-- bug: ALTER TABLE ADD COLUMN with DEFAULT does not apply default to existing rows
-- setup:
CREATE TABLE t_adc (id INT, name TEXT);
INSERT INTO t_adc VALUES (1, 'alice'), (2, 'bob');
ALTER TABLE t_adc ADD COLUMN score INT DEFAULT 0;
-- input:
SELECT id, name, score FROM t_adc ORDER BY id;
-- expected output:
1|alice|0
2|bob|0
