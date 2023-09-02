import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
#read it real time

order_kafka_topics="order_details"
order_confirmed_kafka_topic="order_confirmed"

consumer=KafkaConsumer(order_kafka_topics,bootstrap_servers='localhost:9092')
producer=KafkaProducer(bootstrap_servers='localhost:9092')
print("staart listenting")

while True:
    for msg in consumer:
        print("ongoing")
        consumed_message=json.loads(msg.value.decode())
        print("consumed")

        user_id=consumed_message["user_id"]
        total_cost=consumed_message["total_cost"]

        data={
            "customer_id":user_id,
            "customer_email":f"{user_id}@gmail.com",
            "total_cost":total_cost
        }
        

        print("success")

        producer.send(
            order_confirmed_kafka_topic,
            json.dumps(data).encode("utf-8")
        )



