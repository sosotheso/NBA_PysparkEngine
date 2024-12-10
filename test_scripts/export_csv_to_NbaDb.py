import os
import csv
import psycopg2
from pathlib import Path

# Configuration
chunk_size = 10000  # Number of rows per chunk
max_file_size_mb = 500  # Max file size before chunking (in MB)
db_config = {
    'dbname': 'nba_dev',
    'user': 'postgres',
    'password': '0000',
    'host': 'localhost'
}

# Function to get file size in MB
def get_file_size(file_path):
    return os.path.getsize(file_path) / (1024 * 1024)  # Size in MB

# Function to create table dynamically based on the CSV file header
def create_table(cursor, table_name, header_row):
    # Convert header row to SQL column names
    columns = [f'"{col.strip()}" TEXT' for col in header_row]
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"
    cursor.execute(create_table_query)

# Function to load CSV data in chunks
def load_csv_in_chunks(file_path, cursor, table_name):
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # Read the header row
        create_table(cursor, table_name, header)  # Create table dynamically based on header

        batch = []
        for i, row in enumerate(reader, start=1):
            batch.append(row)
            if len(batch) >= chunk_size:
                insert_data(batch, cursor, table_name)
                batch = []  # Reset batch

        # Insert remaining rows
        if batch:
            insert_data(batch, cursor, table_name)

# Function to insert data into PostgreSQL in a single batch
def insert_data(batch, cursor, table_name):
    # Dynamically create placeholders for SQL
    placeholders = ', '.join(['%s'] * len(batch[0]))
    query = f"INSERT INTO {table_name} VALUES ({placeholders});"
    cursor.executemany(query, batch)

# Function to process each CSV file
def process_csv_file(file_path):
    file_size = get_file_size(file_path)
    table_name = Path(file_path).stem  # Use the file name (without extension) as table name
    print(f"Processing file: {file_path} (Size: {file_size:.2f} MB, Table: {table_name})")
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    try:
        if file_size > max_file_size_mb:
            print(f"File size exceeds {max_file_size_mb} MB. Loading in chunks.")
            load_csv_in_chunks(file_path, cursor, table_name)
        else:
            print(f"File size is under {max_file_size_mb} MB. Loading directly.")
            load_csv_directly(file_path, cursor, table_name)

        # Commit the transaction
        conn.commit()
    except Exception as e:
        print(f"Error loading file: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to load a small file directly
def load_csv_directly(file_path, cursor, table_name):
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # Read the header row
        create_table(cursor, table_name, header)  # Create table dynamically based on header
        
        # Insert data directly
        placeholders = ', '.join(['%s'] * len(header))
        query = f"INSERT INTO {table_name} VALUES ({placeholders});"
        cursor.executemany(query, reader)

# Function to loop through multiple files in a directory
def process_files_in_directory(directory_path):
    # Loop through all CSV files in the directory
    for file in Path(directory_path).glob('*.csv'):
        process_csv_file(file)

# Example usage
directory_path = 'D:/COMPUTER_SCIENCE/DBT/nba_raw_data/csv/'  # Replace with your directory path
process_files_in_directory(directory_path)
