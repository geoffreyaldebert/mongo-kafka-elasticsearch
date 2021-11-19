import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["datagouv"]
col = db["dataset"]

for insert_change in client.datagouv.dataset.watch():
    print(insert_change['fullDocument']['_id'])