
import pandas as pd
import requests
import json
import unidecode
import unicodedata
pd.options.display.max_columns = 100
import numpy as np

dfd = pd.read_csv('https://static.data.gouv.fr/resources/catalogue-des-donnees-de-data-gouv-fr/20211007-074534/export-dataset-20211007-074534.csv', dtype=str, sep=";")
dfo = pd.read_csv('https://static.data.gouv.fr/resources/catalogue-des-donnees-de-data-gouv-fr/20211007-075016/export-organization-20211007-075016.csv', dtype="str", sep=";")

dfo['orga_sp'] = dfo['badges'].apply(lambda x: 4 if 'public-service' in x else 1)
dfo = dfo[['logo','id', 'orga_sp', 'metric.followers']]
dfo = dfo.rename(columns={'id':'organization_id', 'metric.followers':'orga_followers'})
df = pd.merge(dfd,dfo,on='organization_id',how='left')

df['metric.views'] = df['metric.views'].astype(float)
df['metric.followers'] = df['metric.followers'].astype(float)
df['orga_followers'] = df['orga_followers'].astype(float)

mvbins = [-1,0,50,500,5000,df['metric.views'].max()]
df['dataset_views'] = pd.cut(df['metric.views'], mvbins, labels=list(range(1,6)))
mfbins = [-1,0,2,10,40,df['metric.followers'].max()]
df['dataset_followers'] = pd.cut(df['metric.followers'], mfbins, labels=list(range(1,6)))
fobins = [-1,0,10,50,100,df['orga_followers'].max()]
df['orga_followers'] = pd.cut(df['orga_followers'], fobins, labels=list(range(1,6)))

df['concat_title_org'] = df['title'] + ' ' + df['organization']

df['dataset_featured'] = df['featured'].apply(lambda x: 5 if x=='True' else 1)

df = df[['id', 'title','description','organization','orga_sp','dataset_views','dataset_followers','orga_followers','dataset_featured','concat_title_org', 'url', 'organization_id']]

df['dataset_reuses'] = 0
df['resources_count'] = 0
df['temporal_coverage_start'] = 0
df['spatial_granularity'] = 0
df['dataset_reuses'] = 0
df['temporal_coverage_end'] = 0
df['spatial_zones'] = 0

df = df.head(3)

df.to_csv('dataset-4-records.csv',index=False)