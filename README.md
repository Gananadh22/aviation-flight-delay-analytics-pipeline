Aviation Data Engineering & Analytics Project

Overview
This project analyzes the US Flights 2015 dataset to generate insights on delays, cancellations, airline performance, and airport traffic.
It implements an end-to-end data engineering pipeline using PySpark, Snowflake, and Power BI, following a Medallion Architecture (Bronze → Silver → Gold).

🏗️ Architecture
The project is designed using a layered data architecture:
Bronze Layer
Raw ingested data from Kaggle (CSV files)
Silver Layer
Cleaned and transformed data using PySpark
Null handling
Data type corrections
Feature preparation
Gold Layer
Business-level aggregations
Fact & Dimension tables
KPI-ready datasets
Serving Layer
Data loaded into Snowflake and visualized in Power BI

⚙️ Tech Stack
PySpark → Distributed data processing
Pandas → Initial preprocessing
Snowflake → Cloud Data Warehouse
Power BI → Dashboard & visualization
Python → Pipeline development

🔄 Data Pipeline
Data Ingestion
Load raw CSV files into Bronze layer
Data Cleaning (Silver Layer)
Remove null values
Standardize schema
Filter invalid records
Batch & Streaming Simulation
Split dataset into batch + streaming data
Generate streaming batches
Data Merge
Combine batch and streaming datasets
Gold Layer Transformation
Create fact and dimension tables
Compute KPIs (delay rate, on-time %, etc.)
Data Warehousing
Load processed data into Snowflake
Create analytical views
Visualization
Build interactive dashboard in Power BI

📊 Dashboard Insights
The dashboard provides:
✈️ Total Flights, Delayed Flights, Cancelled Flights, Diverted Flights
⏱️ On-Time Performance %
⚠️ Delay Analysis by Reason (Airline, Weather, NAS, Security)
📅 Monthly Trends
🏆 Top & Bottom Airlines (performance comparison)
🗺️ Airport Traffic Analysis (geospatial map)

📁 Project Structure
aviation_project/

│── data/
│   ├── bronze/                  # Raw data
│   ├── silver/                  # Cleaned data
│   ├── gold/                    # Aggregated data
│   └── raw/
│       └── streaming_batches/   # Simulated streaming data

│── pipelines/
│   ├── ingestion/               # Data loading scripts
│   ├── transformation/          # Cleaning & transformation
│   ├── streaming/               # Streaming pipeline
│   └── serving/                 # Gold layer processing

│── warehouse/
│   └── snowflake/
│       ├── tables.sql           # Table creation scripts
│       └── views.sql            # Analytical views

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

📂 Dataset
Source: Kaggle – US Flights 2015 Dataset
Records: ~5.8 Million flights
Key Columns:
ARRIVAL_DELAY
DEPARTURE_DELAY
CANCELLED
DIVERTED
AIRLINE
ORIGIN_AIRPORT
DESTINATION_AIRPORT

📈 Key Metrics
Delay Rate (%)
On-Time Performance (%)
Total Cancelled Flights
Total Diverted Flights
Average Delay (minutes)

🚀 Key Features
End-to-end ETL pipeline using PySpark
Batch + Streaming data simulation
Medallion Architecture implementation
Snowflake-based data warehousing
Interactive Power BI dashboard

⚡ How to Run
Install dependencies:
pip install -r requirements.txt
Run ingestion pipeline:
python pipelines/ingestion/load_flights_data.py
Run transformation pipeline:
python pipelines/transformation/clean_flights.py
Run streaming pipeline:
python pipelines/streaming/spark_stream_pipeline.py
Load data into Snowflake and execute SQL scripts
Open Power BI dashboard:
dashboard/powerbi/aviation_insights.pbix

🔮 Future Improvements
Real-time streaming using Kafka
Workflow orchestration using Airflow
Incremental data loading
Machine Learning for delay prediction