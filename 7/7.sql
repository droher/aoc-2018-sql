DROP TABLE IF EXISTS steps;
CREATE TABLE steps(step TEXT);

COPY steps
FROM '/Users/davidroher/Repos/aoc-2018-sqlite/7/7.txt'
DELIMITER ',';

-- Part 1
WITH RECURSIVE steps_clean AS (
  SELECT SPLIT_PART(step, ' ', 2) prior_step,
         SPLIT_PART(step, ' ', 8) next_step
  FROM steps
), dependencies AS (
  SELECT
    next_step,
    ARRAY_AGG(DISTINCT prior_step) prior_steps
  FROM steps_clean
  GROUP BY 1
  UNION ALL
  SELECT DISTINCT
    prior_step AS next_step,
    ARRAY[]::text[] AS prior_steps
  FROM steps_clean
  WHERE prior_step NOT IN (SELECT next_step FROM steps_clean)
), dag(steps) AS (
  (SELECT ARRAY[next_step] steps
  FROM dependencies
  WHERE prior_steps = ARRAY[]::text[]
  ORDER BY next_step
  LIMIT 1)
  UNION ALL
  (SELECT d.steps || ARRAY[dep.next_step] steps
  FROM dependencies dep
  CROSS JOIN dag d
  WHERE dep.prior_steps <@ d.steps
    AND NOT(dep.next_step = ANY(d.steps))
  ORDER BY dep.next_step
  LIMIT 1)
)
SELECT ARRAY_TO_STRING(steps, '') FROM dag WHERE ARRAY_LENGTH(steps, 1) = 26;

-- Part 2
WITH RECURSIVE steps_clean AS (
  SELECT SPLIT_PART(step, ' ', 2) prior_step,
         SPLIT_PART(step, ' ', 8) next_step
  FROM steps
), dependencies AS (
  SELECT
    next_step as step,
    ARRAY_AGG(DISTINCT prior_step) prior_steps,
    -1 AS initial_worker
  FROM steps_clean
  GROUP BY 1, 3
  UNION ALL
  SELECT DISTINCT
    prior_step AS step,
    ARRAY[]::text[] AS prior_steps,
    DENSE_RANK() OVER (ORDER BY prior_step) initial_worker
  FROM steps_clean
  WHERE prior_step NOT IN (SELECT next_step FROM steps_clean)
), dag(second, step, seconds_remaining, is_runnable, has_worker, completed_steps) AS (
  SELECT
    0 AS second,
    step,
    ASCII(step) - 4 - (CASE WHEN initial_worker BETWEEN 1 AND 5 THEN 1 ELSE 0 END) seconds_remaining,
    (initial_worker > 0) is_runnable,
    (initial_worker BETWEEN 1 AND 5) has_worker,
    ARRAY[]::text[] completed_steps
  FROM dependencies dep
  UNION ALL
  SELECT
    d.second + 1 AS second,
    d.step,
    CASE WHEN has_worker
          THEN seconds_remaining - 1
        WHEN is_runnable
            AND SUM(has_worker::int) OVER ()
                  + COALESCE(SUM((is_runnable AND NOT has_worker)::int)
                              OVER (ORDER BY step ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                    ,0) < 5
          THEN seconds_remaining - 1
        ELSE d.seconds_remaining
      END seconds_remaining,
    dep.prior_steps <@ (d.completed_steps || ARRAY_AGG(d.step) FILTER (WHERE d.seconds_remaining = 1) OVER()) is_runnable,
    CASE WHEN is_runnable
            AND SUM(has_worker::int) OVER ()
                  + SUM(is_runnable::int) OVER (ORDER BY step ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) < 5
          THEN TRUE
        ELSE has_worker
      END has_worker,
    d.completed_steps || ARRAY_AGG(d.step) FILTER (WHERE d.seconds_remaining = 1) OVER () completed_steps
  FROM dag d
  JOIN dependencies dep USING (step)
  WHERE seconds_remaining > 0
)
SELECT MAX(second+1)
FROM dag