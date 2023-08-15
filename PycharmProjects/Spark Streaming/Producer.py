from kafka import KafkaProducer
import json
import time
from faker import Faker

kafka_producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])

faker_ele = Faker()

def generate_random_values():
    return {
        "name":faker_ele.name(),
        "address":faker_ele.address(),
        "year":faker_ele.year(),
        "email":faker_ele.email()
    }

while 1:
    sample_data = generate_random_values()
    print(sample_data)
    kafka_producer.send('real_time',value = json.dumps(sample_data).encode('utf-8'))
    time.sleep(2)

