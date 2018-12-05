DROP TABLE IF EXISTS frequency;
CREATE TABLE frequency(freq TEXT);
.separator ","
.import 1.txt frequency

-- Starting with a frequency of zero, what is the resulting frequency
-- after all of the changes in frequency have been applied?
SELECT SUM(CASE WHEN SUBSTR(freq, 1, 1) = '+'
                    THEN CAST(SUBSTR(freq, 2) AS INT)
                ELSE -1 * CAST(SUBSTR(freq, 2) AS INT)
                END)
FROM frequency;

-- What is the first frequency your device reaches twice?
WITH formatted AS (
  SELECT CASE
           WHEN SUBSTR(freq, 1, 1) = '+'
             THEN CAST(SUBSTR(freq, 2) AS INT)
           ELSE -1 * CAST(SUBSTR(freq, 2) AS INT)
           END as val,
         rowid as rownum
  FROM frequency
), kludge AS (
  SELECT 0 AS val, 0 as rownum
  UNION ALL
  SELECT t1.val,
         t1.rownum + 2000 * ROW_NUMBER() OVER (PARTITION BY t1.rownum ORDER BY TRUE) AS rownum
  FROM formatted t1
  CROSS JOIN (SELECT * FROM formatted LIMIT 145) t2
), cume_sum AS (
    SELECT
        val,
        rownum,
        SUM(val) OVER (ORDER BY rownum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cume_sum
    FROM kludge
), double_finder AS (
    SELECT val,
        cume_sum,
        rownum,
        COUNT(*) OVER (PARTITION BY cume_sum ORDER BY rownum) nth_instance
    FROM cume_sum
)
SELECT cume_sum
FROM double_finder
WHERE nth_instance = 2
ORDER BY rownum
LIMIT 1;