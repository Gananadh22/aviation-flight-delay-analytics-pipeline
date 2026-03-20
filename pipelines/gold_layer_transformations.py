from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import pandas as pd

spark = SparkSession.builder \
    .appName("Gold Layer Transformations") \
    .master("local[2]") \
    .getOrCreate()

print("Spark session started")

# -------------------------
# LOAD DIM DATA
# -------------------------

airlines_df = spark.read.csv(
    "data/raw/airlines.csv",
    header=True,
    inferSchema=True
)

airports_df = spark.read.csv(
    "data/raw/airports.csv",
    header=True,
    inferSchema=True
)

# -------------------------
# DIM_AIRLINE
# -------------------------

dim_airline = airlines_df.select(
    "IATA_CODE",
    "AIRLINE"
).dropDuplicates()

dim_airline.toPandas().to_csv(
    "data/gold/dim_airline.csv",
    index=False
)

print("DIM_AIRLINE created")

# -------------------------
# DIM_AIRPORT
# -------------------------

dim_airport = airports_df.select(
    "IATA_CODE",
    "AIRPORT",
    "CITY",
    "STATE",
    "COUNTRY",
    "LATITUDE",
    "LONGITUDE"
).dropDuplicates()

dim_airport.toPandas().to_csv(
    "data/gold/dim_airport.csv",
    index=False
)

print("DIM_AIRPORT created")

# -------------------------
# DIM_DATE
# -------------------------

start = datetime(2015,1,1)
end = datetime(2015,12,31)

dates = []
current = start

while current <= end:

    dates.append([
        current.year,
        current.month,
        current.day,
        current.isoweekday()
    ])

    current += timedelta(days=1)

dim_date = pd.DataFrame(
    dates,
    columns=["YEAR","MONTH","DAY","DAY_OF_WEEK"]
)

dim_date.to_csv(
    "data/gold/dim_date.csv",
    index=False
)

print("DIM_DATE created")

# -------------------------
# FACT_FLIGHTS
# -------------------------

print("Creating FACT_FLIGHTS...")

chunks = pd.read_csv(
    "data/processed/flights_clean.csv",
    chunksize=200000
)

first = True

for chunk in chunks:

    fact = chunk[[
        "YEAR","MONTH","DAY","DAY_OF_WEEK","AIRLINE","FLIGHT_NUMBER",
        "TAIL_NUMBER","ORIGIN_AIRPORT","DESTINATION_AIRPORT",
        "SCHEDULED_DEPARTURE","DEPARTURE_TIME","DEPARTURE_DELAY",
        "TAXI_OUT","WHEELS_OFF","SCHEDULED_TIME","ELAPSED_TIME",
        "AIR_TIME","DISTANCE","WHEELS_ON","TAXI_IN",
        "SCHEDULED_ARRIVAL","ARRIVAL_TIME","ARRIVAL_DELAY",
        "DIVERTED","CANCELLED","CANCELLATION_REASON",
        "AIR_SYSTEM_DELAY","SECURITY_DELAY","AIRLINE_DELAY",
        "LATE_AIRCRAFT_DELAY","WEATHER_DELAY"
    ]]

    if first:
        fact.to_csv("data/gold/fact_flights.csv", index=False)
        first = False
    else:
        fact.to_csv("data/gold/fact_flights.csv", mode="a", header=False, index=False)

print("FACT_FLIGHTS created")

spark.stop()