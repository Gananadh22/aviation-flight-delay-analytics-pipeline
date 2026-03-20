import time
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Flight Streaming Simulation") \
    .master("local[2]") \
    .config("spark.sql.shuffle.partitions","4") \
    .getOrCreate()

print("Streaming Simulation Started")

stream_folder = "data/streaming"
output_file = "data/gold/streaming_updates/streaming_updates.csv"

os.makedirs("data/gold/streaming_updates", exist_ok=True)

files = sorted(os.listdir(stream_folder))

for file in files:

    if file.endswith(".csv"):

        file_path = os.path.join(stream_folder, file)

        print("\nNew Streaming Batch Arrived:", file)

        df = spark.read.csv(
            file_path,
            header=True,
            inferSchema=True
        )

        print("Original rows:", df.count())

        # Correct cleaning rule
        df_clean = df.filter(
            (col("CANCELLED") == 1) |
            (col("ARRIVAL_DELAY").isNotNull()) |
            (col("DEPARTURE_DELAY").isNotNull())
        )

        print("Rows after cleaning:", df_clean.count())

        # Convert to pandas
        pdf = df_clean.toPandas()

        # Ensure column order exactly matches fact table
        column_order = [
            "YEAR","MONTH","DAY","DAY_OF_WEEK","AIRLINE","FLIGHT_NUMBER",
            "TAIL_NUMBER","ORIGIN_AIRPORT","DESTINATION_AIRPORT",
            "SCHEDULED_DEPARTURE","DEPARTURE_TIME","DEPARTURE_DELAY",
            "TAXI_OUT","WHEELS_OFF","SCHEDULED_TIME","ELAPSED_TIME",
            "AIR_TIME","DISTANCE","WHEELS_ON","TAXI_IN",
            "SCHEDULED_ARRIVAL","ARRIVAL_TIME","ARRIVAL_DELAY",
            "DIVERTED","CANCELLED","CANCELLATION_REASON",
            "AIR_SYSTEM_DELAY","SECURITY_DELAY","AIRLINE_DELAY",
            "LATE_AIRCRAFT_DELAY","WEATHER_DELAY"
        ]

        pdf = pdf[column_order]

        # Append streaming data
        if os.path.exists(output_file):
            pdf.to_csv(output_file, mode="a", header=False, index=False)
        else:
            pdf.to_csv(output_file, index=False)

        print("Batch written to streaming_updates.csv")

        print("Processing next batch in 5 seconds...\n")

        time.sleep(5)

print("Streaming Simulation Completed")

spark.stop()