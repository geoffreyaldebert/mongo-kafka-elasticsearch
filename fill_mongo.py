import pymongo
import pandas as pd
import time

df = pd.read_csv('export-dataset.csv',dtype=str)
df = df[['title','description','organization','orga_sp','dataset_views','dataset_followers','orga_followers','dataset_featured','concat_title_org', 'url', 'organization_id']]


df['dataset_reuses'] = 0
df['resources_count'] = 0
df['temporal_coverage_start'] = 0
df['spatial_granularity'] = 0
df['dataset_reuses'] = 0
df['temporal_coverage_end'] = 0
df['spatial_zones'] = 0

client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["datagouv"]
col = db["dataset"]


for index,row in df.iterrows():
    x = col.insert_one(row.to_dict())
    time.sleep(2)
print("--- END ---")
