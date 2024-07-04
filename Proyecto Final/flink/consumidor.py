from kafka import KafkaConsumer
import firebase_admin
from firebase_admin import credentials, db

consumer = KafkaConsumer(
    'events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='mygroup'
)

cred = credentials.Certificate("google-service.json")
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://dronihc-parte2-default-rtdb.firebaseio.com/'
})
ref = db.reference('/')

ref.child('flink').set({'num': ""})
batch= 0

print('Waiting for messages...')


for message in consumer:
    msg = message.value.decode("utf-8")
    ref.child('flink').update({'num': msg})
    print(msg)








