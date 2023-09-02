import json
from kafka import KafkaConsumer
from kafka import KafkaProducer

#read from order confirmed topics

order_confirmed_kafka_topic="order_confirmed"

consumer=KafkaConsumer(
    order_confirmed_kafka_topic,bootstrap_servers='localhost:9092'
)
producer=KafkaProducer(bootstrap_servers='localhost:9092')

total_orders_count=0
total_revenue=0
print("analy list")
while True:
    for msg in consumer:
        print("updating analytics")
        consumed_message=json.loads(msg.value.decode())
        total_cost=float (consumed_message["total_cost"])
        total_orders_count+=1
        total_revenue+=total_cost
        print(f"orders so far{total_orders_count}")
        print(f"revenu so far{total_revenue}")

        

