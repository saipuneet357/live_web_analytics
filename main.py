import json
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    s = input('Send Message: ')
    data = {'string': s}
    producer.send('test-puneet', value=data)
    print('sent')
    if s == 'q':
        break
