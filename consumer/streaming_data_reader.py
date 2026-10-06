from kafka import KafkaConsumer
import json
import mysql.connector

topic = "toll_traffic"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="toll_consumer_group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# Connect to MySQL database
try:
    db_conn = mysql.connector.connect(
        host="localhost",
        port=3307,
        user="toll_user",
        password="toll_password",
        database="toll_db"
    )
    cursor = db_conn.cursor()
    print("Connected to MySQL database 'toll_db' on localhost:3307.")
except Exception as e:
    print(f"Warning: Could not connect to MySQL: {e}")
    db_conn = None
    cursor = None

print(f"Listening for messages on topic '{topic}'...")

try:
    for message in consumer:
        record = message.value
        print("Received:", record)

        if db_conn and cursor:
            try:
                insert_query = """
                INSERT INTO toll_data (vehicle_id, vehicle_type, toll_plaza_id, timestamp)
                VALUES (%s, %s, %s, %s)
                """
                cursor.execute(
                    insert_query,
                    (
                        record.get("vehicle_id"),
                        record.get("vehicle_type"),
                        record.get("toll_plaza_id"),
                        record.get("timestamp")
                    )
                )
                db_conn.commit()
                print("Inserted record into MySQL table 'toll_data'.")
            except Exception as err:
                print(f"Failed to insert record into MySQL: {err}")
except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    if cursor:
        cursor.close()
    if db_conn:
        db_conn.close()
    consumer.close()
