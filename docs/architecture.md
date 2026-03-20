# Architecture

This project follows a medallion architecture:

- Raw Layer: Original Kaggle flight dataset
- Bronze Layer: Ingested raw CSV data
- Silver Layer: Cleaned and transformed data using PySpark
- Gold Layer: Aggregated business-level data

Data is loaded into Snowflake and visualized using Power BI.