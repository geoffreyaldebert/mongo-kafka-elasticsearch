from kafka import KafkaProducer
import json
import pandas as pd
import sys

producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

topic = 'dataset'

method = sys.argv[1]

df = pd.read_csv('dataset-4-records.csv',dtype=str)

if(method == 'update'):
    df = df.head(1)
    df['title'] = 'Holà tapas !'

if((method == 'update') | (method == 'create')):
    for index,row in df.iterrows():
        producer.send(topic, value=row.to_dict(), key=bytes(row['id'],encoding='utf8'))
        producer.flush()

if(method == 'delete'):
    df = df.head(1)
    producer.send(topic, value=None, key=bytes(df.iloc[0]['id'],encoding='utf8'))
    producer.flush()