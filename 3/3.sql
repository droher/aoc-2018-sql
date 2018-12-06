DROP TABLE IF EXISTS claims;
CREATE TABLE claims(id TEXT, not_used TEXT, pos text, size text);

COPY claims
FROM '3.txt'
DELIMITER ' ';

-- Part 1
WITH claims_clean AS (
  SELECT
    id,
    SPLIT_PART(pos, ',', 1)::int left_inches,
    LEFT(SPLIT_PART(pos, ',', 2), -1)::int top_inches,
    SPLIT_PART(size, 'x', 1)::int width,
    SPLIT_PART(size, 'x', 2)::int height
  FROM claims
), arr AS (
  SELECT id,
         ARRAY(SELECT GENERATE_SERIES(left_inches, left_inches + width - 1)) x,
         ARRAY(SELECT GENERATE_SERIES(top_inches, top_inches + height - 1))  y
  FROM claims_clean
), coordinates AS (
SELECT id,
     t1.x,
     t2.y
FROM (SELECT id, UNNEST(x) x FROM arr) t1
JOIN (SELECT id, UNNEST(y) y FROM arr) t2 USING (id)
), counter AS (
  SELECT x, y
  FROM coordinates
  GROUP BY x,y
  HAVING COUNT(DISTINCT id) > 1
)
SELECT COUNT(*) from counter

-- Part 2
WITH claims_clean AS (
  SELECT
    id,
    SPLIT_PART(pos, ',', 1)::int left_inches,
    LEFT(SPLIT_PART(pos, ',', 2), -1)::int top_inches,
    SPLIT_PART(size, 'x', 1)::int width,
    SPLIT_PART(size, 'x', 2)::int height
  FROM claims
), arr AS (
  SELECT id,
         ARRAY(SELECT GENERATE_SERIES(left_inches, left_inches + width - 1)) x,
         ARRAY(SELECT GENERATE_SERIES(top_inches, top_inches + height - 1))  y
  FROM claims_clean
), coordinates AS (
SELECT id,
     t1.x,
     t2.y
FROM (SELECT id, UNNEST(x) x FROM arr) t1
JOIN (SELECT id, UNNEST(y) y FROM arr) t2 USING (id)
), counter AS (
  SELECT x, y, COUNT(*) cnt
  FROM coordinates
  GROUP BY x, y
)
SELECT id
FROM coordinates
JOIN counter USING (x, y)
GROUP BY 1
HAVING MAX(cnt) = 1;