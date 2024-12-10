from pyspark.sql import SparkSession
import shutil
import os
from datetime import datetime

# Set a custom temp directory with a less restrictive path
temp_dir = "D:/PROGRAMS/SPARK/tmp"
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

# Spark Session
spark = SparkSession.builder \
    .appName("test_db") \
    .config("spark.jars", "file:///D:/PROGRAMS/SPARK/jars/postgresql-42.7.4.jar") \
    .config("spark.executor.extraClassPath", "file:///D:/PROGRAMS/SPARK/jars/postgresql-42.7.4.jar") \
    .config("spark.driver.extraClassPath", "file:///D:/PROGRAMS/SPARK/jars/postgresql-42.7.4.jar") \
    .config("spark.local.dir", temp_dir) \
    .getOrCreate()
# Fetch the password from the environment variable 'env_v'
password = os.environ.get("PG_PASSWORD")
if not password:
    raise ValueError("Environment variable 'env_v' is not set")
# Database Configuration
db_config = {
    "url": "jdbc:postgresql://localhost:5432/nba_dev",
    "user": "postgres",
    "password": password,  # Replace with your PostgreSQL password
    "driver": "org.postgresql.Driver"
}
# Record the start time
start_time = datetime.now()
print(f"Process started at: {start_time}")

# wirte data from the PostgreSQL database
schema = "nba"
table = "testSparkSpeedTable"
# File path to the CSV
csv_file_path = "D:/COMPUTER_SCIENCE/DBT/nba_raw_data/csv/play_by_play.csv"
# Read the CSV file into a DataFrame
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_file_path)

# Write the DataFrame to the PostgreSQL table
df.write \
    .format("jdbc") \
    .option("url", db_config["url"]) \
    .option("dbtable", schema+"."+table) \
    .option("user", db_config["user"]) \
    .option("password", db_config["password"]) \
    .option("driver", db_config["driver"]) \
    .mode("overwrite") \
    .save()

# Record the end time
end_time = datetime.now()
print(f"Process ended at: {end_time}")

# Calculate and print the total duration
duration = end_time - start_time
print(f"Total process duration: {duration}")
# Stop Spark session
spark.stop()

