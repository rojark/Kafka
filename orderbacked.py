import json
import time
from kafka import KafkaProducer

#writes order details 


order_kafka_topic="order_details"
order_limit=15

producer=KafkaProducer(bootstrap_servers="localhost:9092")

print("generate order")

for i in range(1,order_limit):

    data={
        "order_id":i,
        "user_id":f"tom_{i}",
        "total_cost":i*2,
        "items":"burger,pizza"
    }
    producer.send(
        order_kafka_topic,
        json.dumps(data).encode("utf-8")
    )
    print(f"done..{i}")
    time.sleep(3)



