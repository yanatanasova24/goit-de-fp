import json
import time
import mysql.connector
from kafka import KafkaProducer
import configs

def get_db_connection():
    return mysql.connector.connect(
        host=configs.MYSQL_HOST,
        port=int(configs.MYSQL_PORT),
        user=configs.JDBC_USER,
        password=configs.JDBC_PASSWORD,
        database=configs.MYSQL_DB
    )

def main():
    print(f"Connecting to Kafka broker at {configs.KAFKA_BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers=configs.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
    )

    print("Connecting to MySQL and reading athlete_event_results...")
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {configs.TABLE_ATHLETE_EVENT_RESULTS} LIMIT 2000")

    print(f"Publishing events to Kafka topic: {configs.KAFKA_INPUT_TOPIC}...")
    count = 0
    for row in cursor:
        producer.send(configs.KAFKA_INPUT_TOPIC, value=row)
        count += 1
        if count % 100 == 0:
            print(f"Sent {count} events...")
            producer.flush()
        time.sleep(0.05)

    producer.flush()
    cursor.close()
    conn.close()
    print(f"Done! Successfully sent {count} events to topic {configs.KAFKA_INPUT_TOPIC}.")

if __name__ == "__main__":
    main()