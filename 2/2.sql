DROP TABLE IF EXISTS sequences;
CREATE TABLE sequences (id SERIAL, sequence TEXT);

COPY sequences(sequence)
FROM '2.txt';

-- Part 1
WITH unnested AS (
  SELECT
    id,
    UNNEST(REGEXP_SPLIT_TO_ARRAY(sequence, '')) letter
  FROM sequences
), counter AS (
  SELECT
    u.*,
    COUNT(*) OVER (PARTITION BY id, letter) appearances
  FROM unnested u
)
SELECT COUNT(DISTINCT CASE WHEN appearances = 2 THEN id ELSE NULL END)
         * COUNT(DISTINCT CASE WHEN appearances = 3 THEN id ELSE NULL END)
FROM counter;

-- Part 2
WITH unnested AS (
  SELECT
    id,
    GENERATE_SUBSCRIPTS(REGEXP_SPLIT_TO_ARRAY(sequence, ''), 1) pos,
    UNNEST(REGEXP_SPLIT_TO_ARRAY(sequence, '')) letter
  FROM sequences
)
SELECT
  u1.id, u2.id,
  STRING_AGG(u1.letter, '' ORDER BY u1.pos)
FROM unnested u1
JOIN unnested u2 ON u1.id <> u2.id
  AND u1.pos = u2.pos
  AND u1.letter = u2.letter
GROUP BY 1, 2
HAVING COUNT(*) = 25
