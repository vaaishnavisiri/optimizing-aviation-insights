{{ config(
    materialized = "incremental",
    unique_key = "metric_quarter",
    incremental_strategy = "merge"
) }}

WITH base AS (
  SELECT
    YEAR,
    MONTH,
    QUARTER(TO_DATE(TO_VARCHAR(YEAR) || '-' || MONTH || '-01')) as quarter_no,
    TO_DATE(TO_VARCHAR(YEAR) || '-01-01') + (quarter_no - 1) * 90 AS quarter_date, -- rough placeholder
    ARRIVAL_DELAY,
    CANCELLED
  FROM {{ ref('silver_flights_sample') }}
),

quarterly AS (
  SELECT
    YEAR,
    QUARTER(TO_DATE(TO_VARCHAR(YEAR) || '-' || MONTH || '-01')) AS quarter,
    CONCAT(YEAR, '-', 'Q', QUARTER(TO_DATE(TO_VARCHAR(YEAR) || '-' || MONTH || '-01'))) AS metric_quarter,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN ARRIVAL_DELAY > 0 THEN 1 ELSE 0 END) AS total_delayed_flights,
    SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS total_cancelled_flights,
    AVG(ARRIVAL_DELAY) AS avg_delay
  FROM {{ ref('silver_flights_sample') }}
  GROUP BY YEAR, QUARTER(TO_DATE(TO_VARCHAR(YEAR) || '-' || MONTH || '-01'))
)

SELECT *
FROM quarterly

{% if is_incremental() %}
-- When incremental, only process new quarters
WHERE (YEAR, quarter) > (
   SELECT COALESCE(MAX(YEAR),0), COALESCE(MAX(quarter),0) FROM {{ this }}
)
{% endif %}
