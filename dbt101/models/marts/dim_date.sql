WITH date_spine AS (
  SELECT DATEADD('day', SEQ4(), '2024-01-01'::DATE) AS date  -- noqa: RF04
  FROM TABLE(GENERATOR(ROWCOUNT => 1461))
),

final AS (
  SELECT
    date,
    FALSE AS is_holiday,
    YEAR(date) AS year,
    QUARTER(date) AS quarter,
    MONTH(date) AS month,
    DAY(date) AS day,
    WEEKOFYEAR(date) AS week,
    DAYOFWEEK(date) AS day_of_week,
    DAYNAME(date) AS day_name,
    MONTHNAME(date) AS month_name,
    DAYOFWEEK(date) IN (0, 6) AS is_weekend
  FROM date_spine
)

SELECT * FROM final
