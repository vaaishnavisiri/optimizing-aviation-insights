## **Project Title: Optimizing Aviation Insights: A Data Engineering Approach for Analyzing Flight Delays and Cancellations**

## **📘 Project Overview**

This project focuses on analyzing the on-time performance of domestic U.S. flights using a fully automated, scalable, and modern data engineering architecture built with Snowflake, AWS S3, Snowpipe, Streams & Tasks, Snowpark, and dbt Cloud/Core.

The U.S. Department of Transportation (DOT) publishes detailed datasets containing flight delays, cancellations, diversions, and operational metrics. Using both historical (batch) and latest (streaming) flight data, this project builds a complete Bronze → Silver → Gold medallion architecture, enabling real-time analytics, SCD2 tracking, anomaly detection, and business insights.

This case study was executed by a team of 3 members, working collaboratively through a structured data pipeline approach:
Ingestion → Cleaning → Transformation → Dimensional Modeling → Analytics → Dashboards

## **Bronze Layer – Raw Data Ingestion (Snowflake + AWS S3 + Snowpipe)**

This layer stores the raw, unmodified data exactly as received from the source systems.
Bronze is the source-of-truth in the Medallion Architecture and enables reproducibility, auditing, and reprocessing.

## **Objective**

To ingest the aviation datasets (Airlines, Airports, Historical Flights, Latest Flights) into Snowflake in raw format using:

AWS S3 (Landing Zone)

Snowflake External Stage

Snowpipe (Auto-ingestion)

Bronze Raw Tables in Snowflake

No cleaning or transformation is applied at this stage.

## **Datasets Used**

| Dataset            | Source                               | Bronze Table                |
| ------------------ | ------------------------------------ | --------------------------- |
| airlines.csv       | Static batch file                    | `bronze_airlines`           |
| airports.csv       | Static batch file                    | `bronze_airports`           |
| flights_sample.csv | Historical flight data               | `bronze_flights_historical` |
| flights_latest.csv | Latest streaming/near-real-time data | `bronze_flights_latest`     |


## **1. AWS S3 Bucket Setup**
A single S3 bucket is used as the raw landing zone.

aviation-data-bucket-2025/
    └── raw/
         ├── airlines/
         ├── airports/
         ├── flights_historical/
         └── flights_latest/
The four CSV datasets are uploaded into their respective folders.
then AWS glue is used to convert csv files to parquet files using pyspart etl scripts.

## **2. Snowflake Storage Integration**
The integration allows Snowflake to read files from the S3 raw folder.

## **3. External Stage Creation**
A single stage pointing to the raw S3 directory:
CREATE STAGE aviation_raw_stage
  URL = 's3://aviation-data-bucket-2025/raw/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = (TYPE = CSV ...);

## **4. Bronze Raw Tables**
Four raw tables created with no transformations and schema matching exactly the CSV structure.

bronze_airlines

bronze_airports

bronze_flights_historical

bronze_flights_latest

These tables store the raw CSV rows as-is.

## **5. Snowpipe (Continuous Auto-Ingestion)**
A dedicated Snowpipe is created for each dataset to load data automatically into the Bronze tables:
| Pipe Name                 | Source Folder              | Target Table                |
| ------------------------- | -------------------------- | --------------------------- |
| `pipe_airlines`           | `/raw/airlines/`           | `bronze_airlines`           |
| `pipe_airports`           | `/raw/airports/`           | `bronze_airports`           |
| `pipe_flights_historical` | `/raw/flights_historical/` | `bronze_flights_historical` |
| `pipe_flights_latest`     | `/raw/flights_latest/`     | `bronze_flights_latest`     |
Pipes watch their S3 folders and load new files automatically.

## **6. Verification**

LIST @aviation_raw_stage; to confirm S3 connectivity

SELECT COUNT(*) FROM bronze_*; to confirm the data loaded

SHOW PIPES; to check pipe status

Once counts were validated, the Bronze layer was successfully completed.


---------------------------------------------
## **⭐ Silver Layer – Data Cleaning & Standardization (Snowpark + dbt)**
---------------------------------------------

The Silver Layer applies data cleansing, validation, standardization, and feature engineering on top of the raw Bronze ingestion.
This layer converts raw aviation data into trusted, analytics-ready structured tables using Snowpark Python and dbt transformations.

## **Silver Tables Created**

| Silver Table Name         | Source Bronze Table       | Purpose                     |
| ------------------------- | ------------------------- | --------------------------- |
| **silver_airlines**       | bronze_airlines           | Clean airline metadata      |
| **silver_airports**       | bronze_airports           | Standardized airport info   |
| **silver_flights_sample** | bronze_flights_historical | Cleaned historical flights  |
| **silver_flights_latest** | bronze_flights_latest     | Standardized latest updates |

Note: Both historical & latest flights are integrated through Silver to enable downstream SCD2 & analytics.

## **Transformations Applied (Snowpark + dbt)**
## *Using Snowpark (Python Stored Procedures & DataFrames)*

We performed:

> Schema enforcement (correcting data types: INT, STRING, TIMESTAMP)

> Timestamp parsing (SCHEDULED_DEPARTURE, LOAD_TIMESTAMP, etc.)

Derived fields:

> TOTAL_DELAY = ARRIVAL_DELAY + DEPARTURE_DELAY

> DELAY_CATEGORY (NO_DELAY, SHORT, MEDIUM, LONG)

> DISTANCE_CATEGORY (Short-Haul / Medium-Haul / Long-Haul)

> WEATHER_IMPACT flag

> ON_TIME_FLAG

> Removal of corrupted/incomplete records

> Normalization of airline and airport codes

> CAST operations (ensuring Snowflake-compatible types)

## *Using dbt Models (SQL transformations)*
dbt performs:

> Renaming & formatting fields

> Null handling & default values

> Date parsing (RECORD_DATE)

> Unique keys generation

> Applying business rules

Creating Silver SQL models:
models/silver/silver_airlines.sql  
models/silver/silver_airports.sql  
models/silver/silver_flights_sample.sql  
models/silver/silver_flights_latest.sql

## *Data Quality Testing (dbt Tests)*

We implemented:

> Not Null Tests (AIRLINE, ORIGIN_AIRPORT, DESTINATION_AIRPORT)

> Unique Key Tests (FLIGHT_NUMBER + DATE)

> Valid Range Tests (DELAY values, DISTANCE)

> Referential Integrity Tests (Airline & Airport → DIM tables)

> Timestamp consistency tests

## *Audit Logging*
This helps maintain data governance and transparency.
Rows rejected during Snowpark transformations


---------------------------------------------
## **⭐ Gold Layer – Business Models, Dimensions & Fact Tables**
---------------------------------------------

The Gold layer contains the analytics-ready entities, SCD2-tracked dimensions, KPIs, and aggregated flight metrics used by dashboards.

## *Gold Tables Created*
Dimension Tables
| Table Name       | Type                 | Description                             |
| ---------------- | -------------------- | --------------------------------------- |
| **dim_airlines** | Static Dimension     | Airline key mapped from Silver          |
| **dim_airports** | Static Dimension     | Airport details + standardized codes    |
| **dim_date**     | Calendar Dimension   | Generated 365 days for 2015             |
| **dim_flights**  | SCD Type-2 Dimension | Stores historical states of each flight |

Fact Table
| Fact Table Name  | Description                                                   |
| ---------------- | ------------------------------------------------------------- |
| **fact_flights** | Central fact table linking all dimensions with flight metrics |

Fact table measures include:

> Departure Delay

> Arrival Delay

> Total Delay

> Airline Delay components

> Weather Delay metrics

> Distance

> Cancellation flags

> Diverted indicators

> Load Timestamp

> SCD2 Flight SK mapping


## **SCD Type-2 Implementation (Snowflake Streams + Tasks)**

To track real-time schedule updates:

A Stream was created on silver_flights_sample

A Stored Procedure applied MERGE logic:

Closes old record (EFFECTIVE_END)

Inserts new state with IS_CURRENT = 'Y'

A Snowflake Task executes procedure every minute

This ensures full historical traceability of flight updates (live SCD2).

## **Analytical Gold Views (+ dbt Models)**
We created three KPI-driven models:
| Model Name                 | Purpose                                     |
| -------------------------- | ------------------------------------------- |
| **gold_flight_metrics**    | Daily delayed, cancelled counts & avg delay |
| **gold_monthly_metrics**   | Monthly aggregated KPIs                     |
| **gold_quarterly_metrics** | Quarterly airline/route performance         |

These support:

Trend analysis

KPI dashboards

Operational insights


---------------------------------------------
## **⭐ Dashboard – Aviation Analytics (Streamlit on Snowflake)**
---------------------------------------------
A fully interactive dashboard is integrated into Snowflake’s Streamlit workspace.
*Main Dashboard Visuals*
> Key Metrics Cards (Total Flights, Delays, Cancellations, Avg Delay)

> Delay Trend Over Time (Line Chart)

> Airline Delay Comparison (Bar Chart)

> Route Delay Heatmap (Origin → Destination delays)

*Advanced Analytics Dashboard*
> Weather Impact Analysis

> Delay Reason Contribution (Airline/System/Security/Weather)

> Top 10 Worst Routes

> Cancellation Insights

> Busiest Routes

*Filters include:*
> Date Range

> Airline Selector

> Airport Filters

These dashboards read directly from the Gold Layer, ensuring live, clean, analytics-ready data.

## **DASHBOARDS**

<img width="1229" height="412" alt="image" src="https://github.com/user-attachments/assets/b13e3406-da88-4aa4-be27-2b3dc45d7aaf" />

<img width="1191" height="560" alt="image" src="https://github.com/user-attachments/assets/fd84c5d5-e1ae-4478-8510-69165b8d7aba" />

<img width="1224" height="614" alt="image" src="https://github.com/user-attachments/assets/a8fa84fc-0101-488a-880c-80a7385cf6df" />

<img width="1207" height="562" alt="image" src="https://github.com/user-attachments/assets/6b06d20c-ceed-4900-b1be-69e50e06380b" />

<img width="1193" height="523" alt="image" src="https://github.com/user-attachments/assets/9801e3b7-c354-4e32-81f2-f2e21c18e370" />


## **🎯 Outcome**

> Fully automated ingestion (S3 → Snowflake Bronze)

> Clean, validated, enriched Silver datasets

> Dimensional Gold models with SCD2 tracking

> Real-time operational insights with Streamlit dashboards

> dbt tests guaranteeing data quality

> A production-ready Data Engineering architecture



