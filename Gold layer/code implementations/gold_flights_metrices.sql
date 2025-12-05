{{ config(
    materialized = "incremental",
    unique_key = "metric_date",
    incremental_strategy = "merge"
) }}

WITH base AS (
    SELECT 
        YEAR,
        MONTH,
        DAY,
        TO_DATE(TO_VARCHAR(YEAR) || '-' || MONTH || '-' || DAY) AS FLIGHT_DATE,
        FLIGHT_STATUS,
        TOTAL_DELAY,
        CANCELLED,
        ARRIVAL_DELAY
    FROM {{ ref('silver_flights_sample') }}
),

daily_metrics AS (
    SELECT 
        FLIGHT_DATE AS metric_date,

        SUM(CASE WHEN ARRIVAL_DELAY > 0 THEN 1 ELSE 0 END) AS total_delayed_flights,
        SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS total_cancelled_flights,
        AVG(ARRIVAL_DELAY) AS avg_delay,

        MONTH,
        YEAR,
        CONCAT('Q', QUARTER(FLIGHT_DATE)) AS quarter
    FROM base
    GROUP BY 
        FLIGHT_DATE,
        MONTH,
        YEAR,
        CONCAT('Q', QUARTER(FLIGHT_DATE))
)

SELECT *
FROM daily_metrics

{% if is_incremental() %}
WHERE metric_date > (SELECT MAX(metric_date) FROM {{ this }})
{% endif %}
