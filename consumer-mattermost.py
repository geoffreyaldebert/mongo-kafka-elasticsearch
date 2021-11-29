from kafka import KafkaConsumer
import json
import requests

consumer = KafkaConsumer("dataset", bootstrap_servers='localhost:9092', group_id='toto')

for message in consumer:
  try:
    val  = message.value
    val_utf8 = val.decode("utf-8").replace("NaN","null")
    data = json.loads(val_utf8)
    post_data = {}
    post_data['text'] = 'Nouveau dataset : '+data['title']+'\n\n'+data['url']
    r = requests.post('https://mattermost.incubateur.net/hooks/geww4je6minn9p9m6qq6xiwu3a', json = post_data)
  except:
    pass
