-- adversarial: CREATE TABLE that already exists — should error
-- setup:
CREATE TABLE t_dc (id INT);
-- input:
CREATE TABLE t_dc (id INT);
-- expected status: error
