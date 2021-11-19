from kafka import KafkaProducer
import pandas as pd
import json
import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["datagouv"]
col = db["dataset"]


producer = KafkaProducer(bootstrap_servers='localhost:29092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

cpt = 0
for insert_change in client.datagouv.dataset.watch():
  print(insert_change['fullDocument']['title'])
  insert_change['fullDocument']['id'] = str(insert_change['fullDocument']['_id'])

  insert_change['fullDocument']['dataset_featured'] = float(insert_change['fullDocument']['dataset_featured'])
  insert_change['fullDocument']['dataset_followers'] = float(insert_change['fullDocument']['dataset_followers'])
  insert_change['fullDocument']['dataset_views'] = float(insert_change['fullDocument']['dataset_views'])
  insert_change['fullDocument']['orga_followers'] = float(insert_change['fullDocument']['orga_followers'])
  insert_change['fullDocument']['orga_sp'] = float(insert_change['fullDocument']['orga_sp'])

  del insert_change['fullDocument']['_id']
  print(cpt)
  cpt = cpt + 1
  producer.send('dataset', insert_change['fullDocument'])
  producer.flush()
