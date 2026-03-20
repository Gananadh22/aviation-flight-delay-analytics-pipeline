from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark Test") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv("data/raw/flights.csv", header=True)

print("Total Records:", df.count())

spark.stop()