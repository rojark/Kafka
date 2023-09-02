import json
from kafka import KafkaConsumer
from kafka import KafkaProducer

#read from order confirmed topics

order_confirmed_kafka_topic="order_confirmed"

consumer=KafkaConsumer(
    order_confirmed_kafka_topic,bootstrap_servers='localhost:9092'
)
producer=KafkaProducer(bootstrap_servers='localhost:9092')


emails_sent_so_far=set() #unique mails so set
print("start listening")
while True:
    for msg in consumer:
        consumed_message=json.loads(msg.value.decode())# to get dictionary
        customer_email=consumed_message["customer_email"]
        print(f"sending mail to{customer_email}")
        emails_sent_so_far.add(customer_email)
        print(f"so far emails send to{len(emails_sent_so_far)}unique emails")