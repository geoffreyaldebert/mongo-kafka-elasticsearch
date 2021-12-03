## Mongo - Kafka - Elasticsearch

Ce repo permet de générer un poc permettant :
- d'insérer des données dans mongo
- de monitorer des événements sur une collection mongo et d'envoyer un message à kafka à chaque événement
- de consommer des messages dans kafka et de les envoyer vers un index elasticsearch


### Installation

```
docker-compose up -d
```

Initialisation de la base mongo :

Pour que le monitoring mongo marche, il faut que la base mongo soit en mode replication set. Il y a une commande à passer dans le cli mongo du container :

```
docker exec -it mongodb bash
mongo

rs.initiate(
   {
     _id : 'rs0',
     members: [
       { _id : 0, host : "mongodb:27017" }
     ]
   }
)
```

### Utilisation

1) Créer répertoire locaux et Initialiser la base elasticsearch :

```
./0_init_folders.sh
python 0_init_es.py
python 0_get_data.py
```

Faire tourner le consumer et le producer en continu :

```
# Consumer pour poster un message sur mattermost
python consumer-mattermost.py 
# Consumer pour indexer des documents elastic
python consumer-elastic.py

# créer des datasets
python producer-datasets.py create
# modifier un dataset
python producer-datasets.py update
# Supprimer un dataset
python producer-datasets.py delete


# La partie mongo est laissée de côté pour le moment
```

Remplir la base mongo (un record est envoyé toutes les 1s) :

```
python fill_mongo.py
```
