{{ config(materialized='table') }}

WITH src AS (
    SELECT *
    FROM {{ ref('silver_airlines') }}
)

SELECT
    {{ generate_sk(["IATA_CODE"]) }} AS AIRLINE_SK,
    IATA_CODE,
    AIRLINE_NAME,
    AIRLINE,
    AIRLINE_CATEGORY,
    CODE_VALIDATION,
    RECORD_DATE,
    LOAD_TIMESTAMP
FROM src
