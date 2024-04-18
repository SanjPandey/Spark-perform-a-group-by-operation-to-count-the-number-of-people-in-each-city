from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("SparkExample") \
    .getOrCreate()

# Define a schema for the DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# Sample data
data = [("John", 30, "New York"), ("Alice", 25, "Los Angeles"), ("Bob", 35, "Chicago")]

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

# Perform some transformations and actions
# Example: Group by city and count the number of people in each city
city_count = df.groupBy("city").count()
city_count.show()

# Stop the SparkSession
spark.stop()
