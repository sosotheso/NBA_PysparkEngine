from pyspark.sql import SparkSession
import shutil
import os

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
schema = "raw_data"
table = "game"
# Read data from the PostgreSQL database
df = spark.read \
    .format("jdbc") \
    .option("url", db_config["url"]) \
    .option("dbtable", schema+"."+table) \
    .option("user", db_config["user"]) \
    .option("password", db_config["password"]) \
    .option("driver", db_config["driver"]) \
    .load()

# Show the data
df.show()

# Stop Spark session
spark.stop()

