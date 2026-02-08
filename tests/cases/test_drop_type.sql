-- drop enum type
-- setup:
CREATE TYPE mood AS ENUM ('happy', 'sad');
-- input:
DROP TYPE mood;
-- expected output:
DROP TYPE
-- expected status: 0
