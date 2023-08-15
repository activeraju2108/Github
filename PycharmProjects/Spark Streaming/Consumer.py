from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"])

consumer.subscribe('real_time')

for x in consumer:
    print(x)