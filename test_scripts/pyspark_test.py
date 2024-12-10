from pyspark.sql import SparkSession
import shutil
import os

# Set a custom temp directory with a less restrictive path
temp_dir = "D:/PROGRAMS/SPARK/tmp"
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

# Spark Session
spark = SparkSession.builder \
    .appName("Minimal Test") \
    .config("spark.jars", "file:///D:/PROGRAMS/SPARK/jars/postgresql-42.7.4.jar") \
    .config("spark.executor.extraClassPath", "file:///D:/PROGRAMS/SPARK/jars/postgresql-42.7.4.jar") \
    .config("spark.driver.extraClassPath", "file:///D:/PROGRAMS/SPARK/jars/postgresql-42.7.4.jar") \
    .config("spark.local.dir", temp_dir) \
    .getOrCreate()

# Minimal DataFrame Test
data = [("A", 1), ("B", 2)]
df = spark.createDataFrame(data, ["Letter", "Number"])
df.show()

# Stop Spark
spark.stop()

# Clean up custom temp directory
shutil.rmtree(temp_dir)
