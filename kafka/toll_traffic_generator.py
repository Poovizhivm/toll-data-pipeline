from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "toll_traffic"

vehicles = ["CAR", "BIKE", "TRUCK", "BUS"]
toll_plazas = ["PLAZA_01", "PLAZA_02", "PLAZA_03"]

while True:
    data = {
        "vehicle_id": "V" + str(random.randint(1000, 9999)),
        "vehicle_type": random.choice(vehicles),
        "toll_plaza_id": random.choice(toll_plazas),
        "timestamp": datetime.now().isoformat()
    }

    producer.send(topic, value=data)
    producer.flush()

    print("Sent:", data)

    time.sleep(2)