# Aviation Data Engineering & Analytics Project

## Overview

This project analyzes the US Flights 2015 dataset to generate insights on delays, cancellations, airline performance, and airport traffic.

It implements an end-to-end data engineering pipeline using PySpark, Snowflake, and Power BI, following a Medallion Architecture (Bronze → Silver → Gold).

---

## Architecture

The project follows a layered data architecture:

**Bronze Layer**
Raw ingested data from Kaggle (CSV files)

**Silver Layer**
Cleaned and transformed data using PySpark:

* Null handling
* Data type corrections
* Feature preparation

**Gold Layer**
Business-level aggregations:

* Fact and dimension tables
* KPI-ready datasets

**Serving Layer**
Data is loaded into Snowflake and visualized using Power BI

---

## Tech Stack

* PySpark – Distributed data processing
* Pandas – Initial preprocessing
* Snowflake – Cloud data warehouse
* Power BI – Data visualization
* Python – Pipeline development

---

## Data Pipeline

**1. Data Ingestion**
Load raw CSV files into the Bronze layer

**2. Data Cleaning (Silver Layer)**

* Remove null values
* Standardize schema
* Filter invalid records

**3. Batch and Streaming Simulation**

* Split dataset into batch and streaming data
* Generate streaming batches

**4. Data Merge**
Combine batch and streaming datasets

**5. Gold Layer Transformation**

* Create fact and dimension tables
* Compute KPIs (delay rate, on-time percentage)

**6. Data Warehousing**

* Load processed data into Snowflake
* Create analytical views

**7. Visualization**
Build interactive dashboards in Power BI

---

## Dashboard Insights

The dashboard provides:

* Total flights, delayed flights, cancelled flights, and diverted flights
* On-time performance percentage
* Delay analysis by reason (Airline, Weather, NAS, Security)
* Monthly trends
* Top and bottom performing airlines
* Airport traffic analysis using map visualization

---

## Project Structure

```
aviation_project/

│── data/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── raw/
│       └── streaming_batches/

│── pipelines/
│   ├── ingestion/
│   ├── transformation/
│   ├── streaming/
│   └── serving/

│── warehouse/
│   └── snowflake/
│       ├── tables.sql
│       └── views.sql

│── dashboard/
│   ├── powerbi/
│   │   └── aviation_insights.pbix
│   └── app.py

│── docs/
│   ├── architecture.md
│   ├── pipeline.md
│   ├── dataset.md
│   └── dashboard.md

│── requirements.txt
│── README.md
```

---

## Dataset

Source: Kaggle – US Flights 2015 Dataset
Records: Approximately 5.8 million flights

Key columns include:

* ARRIVAL_DELAY
* DEPARTURE_DELAY
* CANCELLED
* DIVERTED
* AIRLINE
* ORIGIN_AIRPORT
* DESTINATION_AIRPORT

---

## Key Metrics

* Delay rate (%)
* On-time performance (%)
* Total cancelled flights
* Total diverted flights
* Average delay (minutes)

---

## Key Features

* End-to-end ETL pipeline using PySpark
* Batch and streaming data simulation
* Medallion architecture implementation
* Snowflake-based data warehousing
* Interactive Power BI dashboard

---

## How to Run

Install dependencies:

```
pip install -r requirements.txt
```

Run ingestion:

```
python pipelines/ingestion/load_flights_data.py
```

Run transformation:

```
python pipelines/transformation/clean_flights.py
```

Run streaming pipeline:

```
python pipelines/streaming/spark_stream_pipeline.py
```

Load data into Snowflake and execute SQL scripts.

Open dashboard:

```
dashboard/powerbi/aviation_insights.pbix
```

---

## Future Improvements

* Real-time streaming using Kafka
* Workflow orchestration using Airflow
* Incremental data loading
* Machine learning for delay prediction

