from snowflake.snowpark.functions import col, lit, upper, coalesce, current_timestamp

def main(session):

    before_count = session.table("aviation_project.airlines.BRONZE_FLIGHTS_SAMPLE_RAW").count()
    df = session.table("aviation_project.airlines.BRONZE_FLIGHTS_SAMPLE_RAW")

    df_clean = (
        df
        # --- TYPE CASTING ---
        .with_column("YEAR", col("YEAR").cast("INT"))
        .with_column("MONTH", col("MONTH").cast("INT"))
        .with_column("DAY", col("DAY").cast("INT"))
        .with_column("DEPARTURE_DELAY", col("DEPARTURE_DELAY").cast("INT"))
        .with_column("ARRIVAL_DELAY", col("ARRIVAL_DELAY").cast("INT"))
        .with_column("CANCELLED", col("CANCELLED").cast("INT"))
        .with_column("DIVERTED", col("DIVERTED").cast("INT"))

        # --- UPPER CASE CLEANING ---
        .with_column("AIRLINE", upper(col("AIRLINE")))
        .with_column("ORIGIN_AIRPORT", upper(col("ORIGIN_AIRPORT")))
        .with_column("DESTINATION_AIRPORT", upper(col("DESTINATION_AIRPORT")))

        # --- NULL HANDLING ---
        .with_column("CANCELLATION_REASON", coalesce(col("CANCELLATION_REASON"), lit("NONE")))
        .with_column("AIR_SYSTEM_DELAY", coalesce(col("AIR_SYSTEM_DELAY"), lit(0)))
        .with_column("SECURITY_DELAY", coalesce(col("SECURITY_DELAY"), lit(0)))
        .with_column("AIRLINE_DELAY", coalesce(col("AIRLINE_DELAY"), lit(0)))
        .with_column("LATE_AIRCRAFT_DELAY", coalesce(col("LATE_AIRCRAFT_DELAY"), lit(0)))
        .with_column("WEATHER_DELAY", coalesce(col("WEATHER_DELAY"), lit(0)))

        # --- DERIVED FIELD ---
        .with_column("TOTAL_DELAY", col("DEPARTURE_DELAY") + col("ARRIVAL_DELAY"))

        # --- LOAD TIMESTAMP ---
        .with_column("LOAD_TIMESTAMP", current_timestamp())

        # --- DROP CRITICAL NULLS ---
        .dropna(subset=["AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT"])
    )

    df_clean.write.mode("overwrite").save_as_table("aviation_project.airlines.SILVER_FLIGHTS_SAMPLE")

    after_count = df_clean.count()

    session.sql(f"""
        INSERT INTO PROJECT_AUDIT_LOGS (
            LOG_TIME, PROCESS_NAME, STEP_NAME, DATASET_NAME,
            ROW_COUNT_BEFORE, ROW_COUNT_AFTER, MESSAGE, STATUS
        ) VALUES (
            CURRENT_TIMESTAMP(),
            'SNOWPARK',
            'CLEANING',
            'FLIGHTS_SAMPLE',
            {before_count},
            {after_count},
            'Cleaned raw data, computed TOTAL_DELAY',
            'SUCCESS'
        )
    """).collect()

    return df_clean