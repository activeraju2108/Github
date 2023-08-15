from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])

consumer.subscribe('rajubai')

for x in consumer:
    print(x)
