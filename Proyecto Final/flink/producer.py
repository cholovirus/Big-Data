from kafka import KafkaProducer
import json

# Configurar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'quickstart-events'

while True:
    name = input("Enter name (or type 'exit' to quit): ")
    if name.lower() == 'exit':
        break

    age = input("Enter age: ")

    person = {'name': name, 'age': age}
    producer.send(topic, value=person)
    print(person)

producer.close()
