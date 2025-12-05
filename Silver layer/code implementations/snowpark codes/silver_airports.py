from snowflake.snowpark.functions import col, initcap, upper, current_timestamp

def main(session):
    # Count rows before cleaning
    before_count = session.table("aviation_project.airlines.BRONZE_AIRPORTS_RAW").count()

    # Load Bronze airports raw table
    df = session.table("aviation_project.airlines.BRONZE_AIRPORTS_RAW")

    # Transformations: basic cleaning only
    df_clean = (
        df
        .with_column("CITY", initcap(col("CITY")))
        .with_column("STATE", upper(col("STATE")))
        .with_column("COUNTRY", upper(col("COUNTRY")))
        .with_column("LATITUDE", col("LATITUDE").cast("FLOAT"))
        .with_column("LONGITUDE", col("LONGITUDE").cast("FLOAT"))
        .with_column("LOAD_TIMESTAMP", current_timestamp())
        .filter(col("IATA_CODE").is_not_null())
        .filter(col("CITY").is_not_null())
        .filter(col("STATE").is_not_null())
    )

    # Write to Silver table
    df_clean.write.mode("overwrite").save_as_table("aviation_project.airlines.SILVER_AIRPORTS")

    # Count rows after cleaning
    after_count = df_clean.count()

    # Insert audit log
    session.sql(f"""
    INSERT INTO PROJECT_AUDIT_LOGS (
        LOG_TIME, PROCESS_NAME, STEP_NAME, DATASET_NAME,
        ROW_COUNT_BEFORE, ROW_COUNT_AFTER,
        MESSAGE, STATUS
    )
    VALUES (
        CURRENT_TIMESTAMP(),
        'SNOWPARK',
        'CLEANING',
        'AIRPORTS',
        {before_count},
        {after_count},
        'Basic cleaning: CITY initcap, STATE/COUNTRY uppercased, lat/lon cast, null rows removed',
        'SUCCESS'
    )
    """).collect()

    return df_clean