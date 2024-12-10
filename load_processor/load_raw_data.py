import yaml
import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def load_config(config_path):
    """Load configuration from the YAML file."""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def connect_to_db(config):
    """Establish connection to PostgreSQL database using JDBC."""
    db_config = config['database']
    
    # JDBC connection URL format
    jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    
    # JDBC connection properties
    connection_properties = {
        "user": db_config['user'],
        "password": db_config['password'],
        "driver": "org.postgresql.Driver"
    }

    return jdbc_url, connection_properties

def load_csv_to_postgresql(csv_path, table_name, schema, jdbc_url, connection_properties):
    """
    Load a CSV file into a PostgreSQL table with a specified schema using PySpark.
    Overwrite existing data if the table exists.
    
    Args:
        csv_path (str): Path to the CSV file.
        table_name (str): Name of the table to load data into.
        schema (str): Schema under which the table resides.
        jdbc_url (str): JDBC URL for PostgreSQL connection.
        connection_properties (dict): JDBC connection properties.
    """
    # Initialize Spark session
    spark = SparkSession.builder.appName("CSV_to_Postgres").getOrCreate()

    # Load CSV into a Spark DataFrame
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # Set the full table name including schema
    full_table_name = f"{schema}.{table_name}"

    # Write the DataFrame to PostgreSQL, overwriting any existing data in the table
    try:
        df.write.jdbc(url=jdbc_url, table=full_table_name, mode='overwrite', properties=connection_properties)
        print(f"Data successfully loaded into {full_table_name}, overwriting any existing data.")
    except Exception as e:
        print(f"Error loading {csv_path}: {e}")

def main():
    config_path = "D:/COMPUTER_SCIENCE/NBA_DATA_PROJECT/PysparkEngine/config/config_source.yml"  # Path to the configuration file
    config = load_config(config_path)
    
    data_sources = config['data_sources']
    
    # Get JDBC URL and connection properties
    jdbc_url, connection_properties = connect_to_db(config)
    
    schema = "raw_data"
    
    try:
        for source in data_sources:
            folder_path = source['path']
            table_prefix = source['name']
            
            for file in os.listdir(folder_path):
                if file.endswith(".csv"):
                    csv_path = os.path.join(folder_path, file)
                    table_name = f"{table_prefix}_{os.path.splitext(file)[0]}"
                    print(f"Loading {csv_path} into {table_name}...")
                    load_csv_to_postgresql(csv_path, table_name, schema, jdbc_url, connection_properties)
    finally:
        print("Process completed.")

if __name__ == "__main__":
    main()
