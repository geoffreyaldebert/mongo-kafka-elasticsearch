from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch, helpers

consumer = KafkaConsumer("dataset", bootstrap_servers='localhost:9092', group_id='elastic')
es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])
index = 'dataset'

for message in consumer:
  value = message.value
  val_utf8 = value.decode("utf-8").replace("NaN","null")
  data = json.loads(val_utf8)
  key = message.key
  actions = []
  if(val_utf8 != 'null'):
    es.update(index=index, id=key.decode("utf-8"), body={ "doc": data, "doc_as_upsert": True })
  else:
    es.delete(index=index, id=key.decode("utf-8"))