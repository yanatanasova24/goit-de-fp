import sys
import os
import re

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, regexp_replace
from pyspark.sql.types import StringType

def clean_text(text):
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,.\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())

def main():
    if len(sys.argv) < 2:
        print("Usage: python bronze_to_silver.py <table_name>")
        sys.exit(1)
        
    table_name = sys.argv[1]
    input_path = os.path.join("bronze", table_name)
    output_path = os.path.join("silver", table_name)
    
    spark = SparkSession.builder \
        .appName(f"BronzeToSilver_{table_name}") \
        .getOrCreate()
        
    print(f"Reading Bronze data from {input_path}...")
    df = spark.read.parquet(input_path)
    
    string_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]
    for col_name in string_cols:
        df = df.withColumn(col_name, clean_text_udf(col(col_name)))
        
    print("Deduplicating rows...")
    df = df.dropDuplicates()

    print(f"\n--- Silver DataFrame Preview for {table_name} ---")
    df.show(5, truncate=False)
    
    print(f"Writing Silver Parquet to {output_path}...")
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"Finished Silver layer for {table_name}.")
    spark.stop()

if __name__ == "__main__":
    main()