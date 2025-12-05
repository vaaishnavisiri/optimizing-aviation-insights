CREATE OR REPLACE TABLE dim_flights_demo
CLONE aviation_project.airlines.dim_flights;


SELECT *
FROM dim_flights_demo
ORDER BY dim_flight_id
LIMIT 20;


SELECT CURRENT_TIMESTAMP() AS before_update_ts;  ---before timestamp

UPDATE dim_flights_demo                    ---updating the column
SET flight_status = 'DELAYED_DEMO_EDIT'
WHERE dim_flight_id = 10;

SELECT dim_flight_id, flight_status            ---after update 
FROM dim_flights_demo
WHERE dim_flight_id = 10;


SELECT dim_flight_id, flight_status           --- after update using time travel to retrive past data before update
FROM dim_flights_demo
AT (OFFSET => -600)
WHERE dim_flight_id = 10;


-- shows the table properties including data_retention_time_in_days
SHOW TABLES LIKE 'DIM_FLIGHTS' IN SCHEMA aviation_project.airlines;
