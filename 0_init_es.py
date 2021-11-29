
import pandas as pd
import requests
import json
import unidecode
import unicodedata
pd.options.display.max_columns = 100
import numpy as np

ELK_URL = "http://localhost:9200"

topic = 'dataset'

r = requests.delete(ELK_URL+'/'+topic)

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

r = requests.put(ELK_URL+'/'+topic,
             headers=headers, 
             data=json.dumps(mapping_with_analyzer))

print(r.json())
# Vérification que tout s'est bien passé :
assert r.json()['acknowledged'] == True
