-- BUG: INSERT with invalid enum value should error, not silently accept
-- setup:
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE t (id INT, feeling mood);
-- input:
INSERT INTO t VALUES (1, 'angry');
-- expected output:
ERROR:  invalid input value for enum mood: "angry"
-- expected status: 0
