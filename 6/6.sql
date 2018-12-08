DROP TABLE IF EXISTS points;
CREATE TABLE points (id serial, x int, y int);

COPY points(x, y)
FROM '6.txt'
DELIMITER ',';

-- Part 1
WITH extrema AS (
  SELECT MIN(x) min_x,
         MAX(x) max_x,
         MIN(y) min_y,
         MAX(y) max_y
  FROM points
), grid AS (
  SELECT
    x.x,
    y.y,
    -- Because we're using Manhattan distance, if point is closest to a boundary point,
    -- then it is also closest to the infinite points extending orthogonally from the boundary
    (x.x IN (e.min_x, e.max_x) OR y.y IN (e.min_y, e.max_y)) is_boundary
  FROM extrema e
  CROSS JOIN (SELECT GENERATE_SERIES(min_x, max_x) x FROM extrema) x
  CROSS JOIN (SELECT GENERATE_SERIES(min_y, max_y) y FROM extrema) y
)
, distances AS (
  SELECT p.id,
         g.*,
         RANK() OVER (PARTITION BY g.x, g.y ORDER BY ABS(p.x - g.x) + ABS(p.y - g.y)) distance_rank,
         COUNT(*) OVER (PARTITION BY g.x, g.y ORDER BY ABS(p.x - g.x) + ABS(p.y - g.y)) rank_population
  FROM points p
  CROSS JOIN grid g
)
SELECT id,
       BOOL_OR(is_boundary),
       COUNT(*) FILTER (WHERE rank_population = 1)
FROM distances
WHERE distance_rank = 1
GROUP BY 1
ORDER BY 3


-- Part 1
WITH extrema AS (
  SELECT MIN(x) min_x,
         MAX(x) max_x,
         MIN(y) min_y,
         MAX(y) max_y
  FROM points
), grid AS (
  SELECT
    x.x,
    y.y,
    (x.x IN (e.min_x, e.max_x) OR y.y IN (e.min_y, e.max_y)) is_boundary
  FROM extrema e
  CROSS JOIN (SELECT GENERATE_SERIES(min_x, max_x) x FROM extrema) x
  CROSS JOIN (SELECT GENERATE_SERIES(min_y, max_y) y FROM extrema) y
)
, distances AS (
  SELECT p.id,
         g.*,
         RANK() OVER (PARTITION BY g.x, g.y ORDER BY ABS(p.x - g.x) + ABS(p.y - g.y)) distance_rank,
         COUNT(*) OVER (PARTITION BY g.x, g.y ORDER BY ABS(p.x - g.x) + ABS(p.y - g.y)) rank_population
  FROM points p
  CROSS JOIN grid g
)
SELECT id,
       COUNT(*) FILTER (WHERE rank_population = 1)
FROM distances
WHERE distance_rank = 1
GROUP BY 1
-- Because we're using Manhattan distance, if point is closest to a boundary point,
-- then it is also closest to the infinite points extending orthogonally from the boundary
HAVING NOT BOOL_OR(is_boundary)
ORDER BY 3 DESC

-- Part 2
WITH extrema AS (
  SELECT MIN(x) min_x,
         MAX(x) max_x,
         MIN(y) min_y,
         MAX(y) max_y
  FROM points
), grid AS (
  SELECT
    x.x,
    y.y,
    (x.x IN (e.min_x, e.max_x) OR y.y IN (e.min_y, e.max_y)) is_boundary
  FROM extrema e
  CROSS JOIN (SELECT GENERATE_SERIES(min_x, max_x) x FROM extrema) x
  CROSS JOIN (SELECT GENERATE_SERIES(min_y, max_y) y FROM extrema) y
)
, distances AS (
  SELECT p.id,
         g.*,
         ABS(p.x - g.x) + ABS(p.y - g.y) distance
  FROM points p
  CROSS JOIN grid g
), region AS (
  SELECT x, y
  FROM distances
  GROUP BY 1, 2
  HAVING SUM(distance) < 10000
)
SELECT COUNT(*) FROM region