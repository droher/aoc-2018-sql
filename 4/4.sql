DROP TABLE IF EXISTS guards;
CREATE TABLE guards (note TEXT);

COPY guards
FROM '4.txt'
DELIMITER ',';

-- Part 1
WITH guards_clean AS (
  SELECT note,
         SUBSTRING(note FROM 2 FOR 16)::timestamp log_time,
         EXTRACT(minute FROM SUBSTRING(note FROM 2 FOR 16)::timestamp)::int min,
         LEAD(EXTRACT(minute FROM SUBSTRING(note FROM 2 FOR 16)::timestamp)::int)
            OVER (ORDER BY  SUBSTRING(note FROM 2 FOR 16)::timestamp) next_min,
         note ilike '%asleep%' as fell_asleep,
         note ilike '%wakes up%' as woke_up,
         note ilike '%begins_shift%' as began_shift,
         ROW_NUMBER() OVER (ORDER BY SUBSTRING(note FROM 2 FOR 16)::timestamp) id
  FROM guards
)
, sleep_info AS (
  SELECT note, id,
        fell_asleep,
         SUBSTRING(MAX(CASE WHEN began_shift THEN note ELSE NULL END)
           OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
          '#[0-9]+') guard,
         ARRAY(SELECT GENERATE_SERIES(min, next_min-1)) minutes_asleep_array,
         (next_min - min) minutes_asleep
  FROM guards_clean gc
)
--guard ID
/*
SELECT guard,
       SUM(minutes_asleep) minutes_asleep
FROM sleep_info
WHERE fell_asleep
GROUP BY 1
-- ORDER BY 2 DESC
*/
-- Minute
SELECT UNNEST(minutes_asleep_array),
       COUNT(*)
FROM sleep_info
WHERE guard = '#3041' AND fell_asleep
GROUP BY 1
ORDER BY 2 DESC

-- Part 2
WITH guards_clean AS (
  SELECT note,
         SUBSTRING(note FROM 2 FOR 16)::timestamp log_time,
         EXTRACT(minute FROM SUBSTRING(note FROM 2 FOR 16)::timestamp)::int min,
         LEAD(EXTRACT(minute FROM SUBSTRING(note FROM 2 FOR 16)::timestamp)::int)
            OVER (ORDER BY  SUBSTRING(note FROM 2 FOR 16)::timestamp) next_min,
         note ilike '%asleep%' as fell_asleep,
         note ilike '%wakes up%' as woke_up,
         note ilike '%begins_shift%' as began_shift,
         ROW_NUMBER() OVER (ORDER BY SUBSTRING(note FROM 2 FOR 16)::timestamp) id
  FROM guards
)
, sleep_info AS (
  SELECT note, id,
        fell_asleep,
         SUBSTRING(MAX(CASE WHEN began_shift THEN note ELSE NULL END)
           OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
          '#[0-9]+') guard,
         ARRAY(SELECT GENERATE_SERIES(min, next_min-1)) minutes_asleep_array,
         (next_min - min) minutes_asleep
  FROM guards_clean gc
)
SELECT guard, UNNEST(minutes_asleep_array), COUNT(*)
FROM sleep_info
WHERE fell_asleep
GROUP BY 1, 2
ORDER BY 3 DESC