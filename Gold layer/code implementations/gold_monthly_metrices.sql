{{ config(
    materialized = "incremental",
    unique_key = "metric_month",
    incremental_strategy = "merge"
) }}

WITH base AS (
  SELECT
    YEAR,
    MONTH,
    TO_DATE(TO_VARCHAR(YEAR) || '-' || MONTH || '-01') AS month_date,
    ARRIVAL_DELAY,
    CANCELLED
  FROM {{ ref('silver_flights_sample') }}
),

monthly AS (
  SELECT
    month_date,
    MONTH AS month,
    YEAR AS year,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN ARRIVAL_DELAY > 0 THEN 1 ELSE 0 END) AS total_delayed_flights,
    SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS total_cancelled_flights,
    AVG(ARRIVAL_DELAY) AS avg_delay
  FROM base
  GROUP BY month_date, MONTH, YEAR
)

SELECT *
FROM monthly

{% if is_incremental() %}
WHERE month_date > (SELECT MAX(month_date) FROM {{ this }})
{% endif %}
