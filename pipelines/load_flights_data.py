from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Aviation Insights Project") \
    .getOrCreate()

# Load flights dataset
flights_df = spark.read.csv(
    "data/raw/flights.csv",
    header=True,
    inferSchema=True
)

# Show schema
flights_df.printSchema()

# Show sample records
flights_df.show(5)

# Count rows
print("Total Records:", flights_df.count())
