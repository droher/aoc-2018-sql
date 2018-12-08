DROP TABLE IF EXISTS polymer;
CREATE TABLE polymer (sequence TEXT);

COPY polymer(sequence)
FROM '5.txt'
DELIMITER ',';

DROP MATERIALIZED VIEW IF EXISTS exploded;
CREATE MATERIALIZED VIEW exploded AS (
  SELECT
    GENERATE_SUBSCRIPTS(REGEXP_SPLIT_TO_ARRAY(sequence, ''), 1) pos,
    UNNEST(REGEXP_SPLIT_TO_ARRAY(sequence, '')) letter
  FROM polymer
);

-- Part 1
WITH RECURSIVE ugh(s, pos) AS (
  SELECT letter AS s,
         pos + 1 pos
  FROM exploded
  WHERE pos = 1
  UNION ALL
  SELECT
    CASE WHEN ABS(ASCII(RIGHT(u.s, 1)) - ASCII(e.letter)) = 32
        THEN LEFT(s, -1)
        ELSE u.s || e.letter
      END AS s,
    u.pos + 1 pos
  FROM ugh u
  JOIN exploded e USING (pos)
)
SELECT LENGTH(s) FROM ugh WHERE pos IN (SELECT MAX(pos) + 1 FROM exploded) LIMIT 1;

-- Part 2
WITH RECURSIVE ugh(s, pos, removal) AS (
  SELECT
         CASE WHEN lower(letter) = removal THEN '' ELSE letter END AS s,
         pos + 1 pos,
         removal
  FROM exploded
  CROSS JOIN (SELECT CHR(GENERATE_SERIES(ASCII('a'), ASCII('z'))) removal) alphabet
  WHERE pos = 1
  UNION ALL
  SELECT
    CASE
      WHEN lower(e.letter) = u.removal
        THEN s
      WHEN ABS(ASCII(RIGHT(u.s, 1)) - ASCII(e.letter)) = 32
        THEN LEFT(s, -1)
      ELSE u.s || e.letter
      END AS s,
    u.pos + 1 pos,
    u.removal
  FROM ugh u
  JOIN exploded e USING (pos)
)
SELECT LENGTH(s), removal FROM ugh WHERE pos IN (SELECT MAX(pos)+1 FROM exploded) ORDER BY 1;