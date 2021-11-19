
import pandas as pd
import requests
import json
import unidecode
import unicodedata
pd.options.display.max_columns = 100
import numpy as np

ELK_URL = "http://localhost:9200"

r = requests.delete(ELK_URL+'/dataset')

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

df.to_csv('export-dataset.csv',index=False)

headers = {
    "Content-Type": "application/json"
}
mapping_with_analyzer = {
  "settings": {
    "analysis": {
      "filter": {
        "french_elision": {
          "type": "elision",
          "articles_case": True,
          "articles": ["l", "m", "t", "qu", "n", "s", "j", "d", "c", "jusqu", "quoiqu", "lorsqu", "puisqu"]
        },
        "french_stop": {
          "type":       "stop",
          "stopwords":  "_french_" 
        },
        "french_synonym": {
          "type": "synonym",
          "ignore_case": True,
          "expand": True,
          "synonyms": [
            "AMD, administrateur ministériel des données, AMDAC",
            "lolf, loi de finance",
            "waldec, RNA, répertoire national des associations",
            "ovq, baromètre des résultats",
            "contour, découpage"
          ]
        },
        "french_stemmer": {
          "type": "stemmer",
          "language": "light_french"
        }
      },
      "analyzer": {
        "french_dgv": {
          "tokenizer": "icu_tokenizer",
          "filter": [
            "french_elision",
            "icu_folding",
            "french_synonym",
            "french_stemmer",
            "french_stop"
          ]
        }
      }
    }
  }, 
  "mappings": {
    "properties": {
      "id": {
        "type": "text"
      },
      "title": {
        "type": "text",
        "analyzer": "french_dgv"
      },
      "description": {
        "type": "text",
        "analyzer": "french_dgv"
      },
      "organization": {
        "type": "text",
        "analyzer": "french_dgv"
      },
      "orga_sp": {
        "type": "integer"
      },
      "orga_followers": {
        "type": "integer"
      },
      "dataset_views": {
        "type": "integer"
      },
      "dataset_followers": {
        "type": "integer"
      },
      "concat_title_org": {
        "type": "text",
        "analyzer": "french_dgv"
      },
      "dataset_featured": {
        "type": "integer"
      },
    
    }
  }
}

r = requests.put(ELK_URL+'/dataset',
             headers=headers, 
             data=json.dumps(mapping_with_analyzer))

print(r.json())
# Vérification que tout s'est bien passé :
assert r.json()['acknowledged'] == True
