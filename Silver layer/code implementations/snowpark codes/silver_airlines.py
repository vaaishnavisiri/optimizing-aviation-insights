from snowflake.snowpark.functions import col, upper, trim, current_timestamp

def main(session):

    DATASET_NAME = "AIRLINES"
    STEP_NAME = "CLEANING & STANDARDIZATION"
    message = "Uppercasing codes, trimming names, adding load timestamp"

    # Count BEFORE
    before_count = session.table("aviation_project.airlines.BRONZE_AIRLINES_RAW").count()

    df = session.table("aviation_project.airlines.BRONZE_AIRLINES_RAW")

    df_clean = (
        df
        .with_column("IATA_CODE", upper(trim(col("IATA_CODE"))))
        .with_column("AIRLINE", trim(col("AIRLINE")))
        .with_column("LOAD_TIMESTAMP", current_timestamp())
    )

    # Save to Silver
    df_clean.write.mode("overwrite").save_as_table("aviation_project.airlines.SILVER_AIRLINES")

    # Count AFTER
    after_count = df_clean.count()

    # Insert audit log
    session.sql(f"""
        INSERT INTO PROJECT_AUDIT_LOGS (
            LOG_TIME, PROCESS_NAME, STEP_NAME, DATASET_NAME,
            ROW_COUNT_BEFORE, ROW_COUNT_AFTER, MESSAGE, STATUS
        )
        VALUES (
            CURRENT_TIMESTAMP(),
            'SNOWPARK',
            '{STEP_NAME}',
            '{DATASET_NAME}',
            {before_count},
            {after_count},
            '{message}',
            'SUCCESS'
        )
    """).collect()

    return "done"
