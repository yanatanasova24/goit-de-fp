import sys
import os
import requests
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def download_data(table_name):
    base_url = "https://ftp.goit.study/neoversity/"
    downloading_url = f"{base_url}{table_name}.csv"
    os.makedirs("landing", exist_ok=True)
    local_path = os.path.join("landing", f"{table_name}.csv")
    
    print(f"Downloading from {downloading_url}...")
    response = requests.get(downloading_url)
    if response.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(response.content)
        print(f"File successfully saved to {local_path}")
        return local_path
    else:
        sys.exit(f"Failed to download {downloading_url}. Status code: {response.status_code}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python landing_to_bronze.py <table_name>")
        sys.exit(1)
        
    table_name = sys.argv[1]
    csv_file_path = download_data(table_name)
    
    spark = SparkSession.builder \
        .appName(f"LandingToBronze_{table_name}") \
        .getOrCreate()
        
    print(f"Reading {csv_file_path} into Spark...")
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(csv_file_path)

    print(f"\n--- Bronze DataFrame Preview for {table_name} ---")
    df.show(5, truncate=False)
        
    output_path = os.path.join("bronze", table_name)
    print(f"Writing Bronze Parquet to {output_path}...")
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"Finished Bronze layer for {table_name}.")
    spark.stop()

if __name__ == "__main__":
    main()