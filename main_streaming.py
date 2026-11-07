from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import configs

# Етап 1: Ініціалізація SparkSession з пакетами для підключення до Kafka та MySQL
spark = SparkSession.builder \
    .appName(f"OlympicStreaming_{configs.STUDENT_PREFIX}") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.3.0") \
    .config("spark.sql.streaming.checkpointLocation", f"/tmp/checkpoint_{configs.STUDENT_PREFIX}") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Етап 2: Зчитування статичної таблиці athlete_bio з MySQL та фільтрація невалідних значень
print("Loading athlete_bio from remote MySQL...")
bio_df = spark.read.format("jdbc") \
    .option("url", configs.JDBC_URL) \
    .option("dbtable", configs.TABLE_ATHLETE_BIO) \
    .option("user", configs.JDBC_USER) \
    .option("password", configs.JDBC_PASSWORD) \
    .option("driver", configs.JDBC_DRIVER) \
    .load()

bio_clean_df = bio_df.filter(
    col("height").isNotNull() & (col("height") > 0) &
    col("weight").isNotNull() & (col("weight") > 0)
).select(
    col("athlete_id"),
    col("sex"),
    col("height").cast(DoubleType()),
    col("weight").cast(DoubleType())
)

# Етап 3: Визначення схеми для вхідного стріму та підписка на топік Kafka
event_schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", StringType(), True)
])

print(f"Subscribing to Kafka topic: {configs.KAFKA_INPUT_TOPIC}...")
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", configs.KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", configs.KAFKA_INPUT_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

events_df = kafka_stream_df.select(
    from_json(col("value").cast("string"), event_schema).alias("data")
).select("data.*")

# Етап 4: Об'єднання потокових подій з біографічними даними атлета (Stream-to-Batch Join)
enriched_df = events_df.join(
    bio_clean_df,
    on="athlete_id",
    how="inner"
)

# Етап 5: Потокова агрегація середнього зросту і ваги
agg_df = enriched_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ) \
    .withColumn("timestamp", current_timestamp())

# Етап 6: Обробка та збереження мікробатчів
def process_micro_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"\n--- Batch {batch_id} Processing ---")
    batch_df.show(5, truncate=False)

    # Етап 6.а): Запис агрегованих результатів у вихідний топік Kafka
    kafka_output_df = batch_df.select(
        to_json(struct(
            col("sport"),
            col("medal"),
            col("sex"),
            col("country_noc"),
            col("avg_height"),
            col("avg_weight"),
            col("timestamp")
        )).alias("value")
    )

    kafka_output_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", configs.KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", configs.KAFKA_OUTPUT_TOPIC) \
        .save()

    # Етап 6.б): Запис агрегованих результатів у таблицю MySQL
    batch_df.write.format("jdbc") \
        .option("url", configs.JDBC_URL) \
        .option("dbtable", configs.TARGET_MYSQL_TABLE) \
        .option("user", configs.JDBC_USER) \
        .option("password", configs.JDBC_PASSWORD) \
        .option("driver", configs.JDBC_DRIVER) \
        .mode("append") \
        .save()

# Етап 7: Запуск запиту Structured Streaming у режимі complete
query = agg_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_micro_batch) \
    .start()

query.awaitTermination()