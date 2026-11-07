import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, coalesce, lit
from pyspark.sql.types import DoubleType, StringType

def main():
    spark = SparkSession.builder \
        .appName("SilverToGold_AvgStats") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    bio_path = os.path.join("silver", "athlete_bio")
    results_path = os.path.join("silver", "athlete_event_results")
    gold_path = os.path.join("gold", "avg_stats")
    
    print("Reading Silver tables...")
    bio_df = spark.read.parquet(bio_path)
    results_df = spark.read.parquet(results_path)
    
    bio_prepared = bio_df \
        .withColumn("height", col("height").cast(DoubleType())) \
        .withColumn("weight", col("weight").cast(DoubleType())) \
        .filter(
            col("height").isNotNull() & (col("height") > 0) &
            col("weight").isNotNull() & (col("weight") > 0)
        ) \
        .select(
            col("athlete_id").cast(StringType()),
            col("sex"),
            col("height"),
            col("weight"),
            col("country_noc")
        )
        
    results_prepared = results_df \
        .withColumn("athlete_id", col("athlete_id").cast(StringType())) \
        .withColumn("medal", coalesce(col("medal"), lit("nan"))) \
        .select("athlete_id", "sport", "medal")
        
    print("Joining Silver datasets on athlete_id...")
    joined_df = results_prepared.join(bio_prepared, on="athlete_id", how="inner")
    
    print("Aggregating average height and weight...")
    gold_df = joined_df \
        .groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight")
        ) \
        .withColumn("timestamp", current_timestamp())
    
    print("\nSample of Gold table:")
    gold_df.show(15, truncate=False)
    
    print(f"Writing Gold layer to {gold_path}...")
    gold_df.write.mode("overwrite").parquet(gold_path)
    
    print("Gold layer successfully written.")
    spark.stop()

if __name__ == "__main__":
    main()