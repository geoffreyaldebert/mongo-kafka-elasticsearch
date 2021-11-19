from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import requests

es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])
consumer = KafkaConsumer("dataset", bootstrap_servers='localhost:29092', group_id='python')

for message in consumer:
  val  = message.value
  val_utf8 = val.decode("utf-8").replace("NaN","null")
  data = json.loads(val_utf8)
  headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
  req = requests.post('http://localhost:9200/dataset/_doc',data=val_utf8, headers=headers)
  print(req.json())
  assert req.status_code == 201
  