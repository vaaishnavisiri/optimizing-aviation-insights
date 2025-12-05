DROP TASK IF EXISTS aviation_project.airlines.task_scd2_flights;
DROP PROCEDURE IF EXISTS aviation_project.airlines.scd2_flights_proc();
DROP STREAM IF EXISTS aviation_project.airlines.flights_stream;
DROP TABLE IF EXISTS aviation_project.airlines.dim_flights;
DROP SEQUENCE IF EXISTS aviation_project.airlines.ts_seq;

CREATE SEQUENCE aviation_project.airlines.ts_seq
START = 1 INCREMENT = 1;

CREATE OR REPLACE TABLE aviation_project.airlines.dim_flights (
    dim_flight_id       NUMBER AUTOINCREMENT,
    flight_number       STRING,
    tail_number         STRING,
    origin_airport      STRING,
    destination_airport STRING,
    departure_time      NUMBER,
    arrival_time        NUMBER,
    total_delay         NUMBER,
    flight_status       STRING,
    effective_start     TIMESTAMP_NTZ,
    effective_end       TIMESTAMP_NTZ,
    is_current          STRING
);




INSERT INTO aviation_project.airlines.dim_flights (
    flight_number, tail_number, origin_airport, destination_airport,
    departure_time, arrival_time, total_delay, flight_status,
    effective_start, effective_end, is_current
)
SELECT
    flight_number, tail_number, origin_airport, destination_airport,
    departure_time, arrival_time, total_delay, flight_status,
    CURRENT_TIMESTAMP(), NULL, 'Y'
FROM aviation_project.airlines.silver_flights_sample;



CREATE OR REPLACE STREAM aviation_project.airlines.flights_stream
ON TABLE aviation_project.airlines.silver_flights_sample
APPEND_ONLY = FALSE;


CREATE OR REPLACE PROCEDURE aviation_project.airlines.scd2_flights_proc()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

  MERGE INTO aviation_project.airlines.dim_flights tgt
  USING (
      SELECT *
      FROM aviation_project.airlines.flights_stream
      WHERE METADATA$ACTION = 'INSERT'
  ) src
  ON tgt.flight_number = src.flight_number
     AND tgt.is_current = 'Y'

  WHEN MATCHED AND (
        tgt.departure_time IS DISTINCT FROM src.departure_time
    OR  tgt.arrival_time   IS DISTINCT FROM src.arrival_time
    OR  tgt.total_delay    IS DISTINCT FROM src.total_delay
    OR  tgt.flight_status  IS DISTINCT FROM src.flight_status
  )
  THEN UPDATE SET
      effective_end = CURRENT_TIMESTAMP(),
      is_current = 'N'

  WHEN NOT MATCHED THEN
    INSERT (
      flight_number, tail_number, origin_airport, destination_airport,
      departure_time, arrival_time, total_delay, flight_status,
      effective_start, effective_end, is_current
    )
    VALUES (
      src.flight_number, src.tail_number, src.origin_airport, src.destination_airport,
      src.departure_time, src.arrival_time, src.total_delay, src.flight_status,
      CURRENT_TIMESTAMP(), NULL, 'Y'
    );

  RETURN 'SCD2 executed';
END;
$$;


CREATE OR REPLACE TASK aviation_project.airlines.task_scd2_flights
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('aviation_project.airlines.flights_stream')
AS
  CALL aviation_project.airlines.scd2_flights_proc();



ALTER TASK aviation_project.airlines.task_scd2_flights RESUME;



INSERT INTO aviation_project.airlines.silver_flights_sample (
  YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER,
  ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE, DEPARTURE_TIME,
  DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME,
  AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN, SCHEDULED_ARRIVAL, ARRIVAL_TIME,
  ARRIVAL_DELAY, DIVERTED, CANCELLED, CANCELLATION_REASON,
  AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, LATE_AIRCRAFT_DELAY,
  WEATHER_DELAY, LOAD_TIMESTAMP, TOTAL_DELAY,
  DELAY_CATEGORY, DISTANCE_CATEGORY, DAY_TYPE,
  FLIGHT_STATUS, WEATHER_IMPACT, ON_TIME_FLAG, RECORD_DATE
)
SELECT
  2015, 2, 10, 3, 'UA', '60001', 'TAIL40001',
  'SFO', 'ORD', 1400, 1410,
  10, 12, 1422, 240, 230,
  205, 1846, 1837, 5, 1845, 1842,
  -3, 0, 0, 'NONE', 0, 0, 0, 0,
  0,
  DATEADD(MICROSECOND, aviation_project.airlines.ts_seq.nextval, CURRENT_TIMESTAMP()),
  -3,
  'NO_DELAY', 'MEDIUM_HAUL', 'WEEKDAY',
  'COMPLETED', 'NO_WEATHER_IMPACT', 1, CURRENT_DATE();




INSERT INTO aviation_project.airlines.silver_flights_sample (
  YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER,
  ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE, DEPARTURE_TIME,
  DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME,
  AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN, SCHEDULED_ARRIVAL, ARRIVAL_TIME,
  ARRIVAL_DELAY, DIVERTED, CANCELLED, CANCELLATION_REASON,
  AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, LATE_AIRCRAFT_DELAY,
  WEATHER_DELAY, LOAD_TIMESTAMP, TOTAL_DELAY,
  DELAY_CATEGORY, DISTANCE_CATEGORY, DAY_TYPE,
  FLIGHT_STATUS, WEATHER_IMPACT, ON_TIME_FLAG, RECORD_DATE
)
SELECT
  2015, 2, 10, 3, 'UA', '60001', 'TAIL40001',
  'SFO', 'ORD', 1400, 1500,
  60, 15, 1515, 240, 230,
  200, 1846, 1835, 6, 1900, 1855,
  45, 0, 0, 'NONE', 0, 0, 30, 15,
  0,
  DATEADD(MICROSECOND, aviation_project.airlines.ts_seq.nextval, CURRENT_TIMESTAMP()),
  45,
  'SEVERE', 'MEDIUM_HAUL', 'WEEKDAY',
  'DELAYED', 'NO_WEATHER_IMPACT', 0, CURRENT_DATE();




SELECT flight_number, departure_time, total_delay, flight_status,
       effective_start, effective_end, is_current
FROM aviation_project.airlines.dim_flights
WHERE flight_number = '60001'
ORDER BY effective_start DESC;


delete from dim_flights where flight_number=60001;
