from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col
import configs

def main():
    print("Ініціалізація Spark для вичитування MySQL та запису в Kafka...")
    spark = SparkSession.builder \
        .appName(f"ProducerEvents_{configs.STUDENT_PREFIX}") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.3.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"Зчитування таблиці {configs.TABLE_ATHLETE_EVENT_RESULTS} з MySQL...")
    events_df = spark.read.format("jdbc") \
        .option("url", configs.JDBC_URL) \
        .option("dbtable", configs.TABLE_ATHLETE_EVENT_RESULTS) \
        .option("user", configs.JDBC_USER) \
        .option("password", configs.JDBC_PASSWORD) \
        .option("driver", configs.JDBC_DRIVER) \
        .load()

    total_count = events_df.count()
    print(f"Успішно зчитано {total_count} записів. Відправка в Kafka...")

    kafka_payload_df = events_df.select(
        to_json(struct([col(c) for c in events_df.columns])).alias("value")
    )
    
    kafka_payload_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", configs.KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", configs.KAFKA_INPUT_TOPIC) \
        .save()

    print(f"Всі {total_count} записів успішно надіслані в Kafka-топік {configs.KAFKA_INPUT_TOPIC}!")
    spark.stop()

if __name__ == "__main__":
    main()
