from kafka import KafkaProducer
import requests
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092')

url = 'https://stream.wikimedia.org/v2/stream/recentchange'

def get_recent_changes_for_all_pages(topic,limit=5):
    url = "https://en.wikipedia.org/w/api.php"
    
    params = {
        "action": "query",
        "format": "json",
        "list": "recentchanges",
        "rcprop": "user|comment|timestamp|title|type",
        "rclimit": limit
    }

    response = requests.get(url, params=params)
    data = response.json()
    
    recent_changes = data['query']['recentchanges']
    for change in recent_changes:
        #print("Usuario:", change['user'])
        #print("Comentario:", change['comment'])
        #print("Fecha:", change['timestamp'])
        #print("Título de la página:", change['title'])
        #print("Título de la página:", change['type'])
        #print("\n")
        datos = {
            "user": change['user'],
            "comentario": change['comment']
        }
        json_str = json.dumps(datos, indent=4)
        #msg= change['comment'] +":"+change['user']
        msg=json_str
        msg= msg.encode('utf-8')
        producer.send(topic, value=msg)
        print(f'Sent: {msg}')
    producer.flush()
    producer.close()

get_recent_changes_for_all_pages('quickstart-events')
