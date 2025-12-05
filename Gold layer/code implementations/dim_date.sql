{{ config(materialized='table') }}

WITH src AS (
    SELECT DISTINCT
        YEAR,
        MONTH,
        DAY
    FROM {{ ref('silver_flights_sample') }}
)

SELECT
    {{ generate_sk(["YEAR","MONTH","DAY"]) }} AS DATE_SK,
    YEAR,
    MONTH,
    DAY
FROM src
