from kafka import KafkaProducer
import json
import time
from faker import Faker

def data_serialize(x):
    return json.dumps(x).encode('utf-8')

producer = KafkaProducer(bootstrap_servers=["localhost:9092"])

fake_data = Faker()

def data_generate():
    return {
        "name":fake_data.name(),
        "Address": fake_data.address(),
        "Year": fake_data.year(),
        "email": fake_data.email()
    }

while 1:
    data = data_generate()
    producer.send('rajubai',value=data_serialize(data))
    time.sleep(2)
    print(data)
