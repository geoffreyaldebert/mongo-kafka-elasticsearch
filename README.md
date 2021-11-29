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
```

Faire tourner le consumer et le producer en continu :

```
python consumer.py
python producer.py
```

Remplir la base mongo (un record est envoyé toutes les 1s) :

```
python fill_mongo.py
```



## KSQL

CREATE STREAM dataset (id VARCHAR, title VARCHAR, description VARCHAR, organization VARCHAR, orga_sp INT, dataset_views INT, dataset_followers INT, orga_followers INT, dataset_featured INT, concat_title_org VARCHAR, url VARCHAR, organization_id VARCHAR, dataset_reuses INT, resources_count INT, temporal_coverage_start INT, spatial_granularity INT, temporal_coverage_end INT, spatial_zones INT) WITH (KAFKA_TOPIC='dataset', PARTITIONS=1, VALUE_FORMAT='JSON');

CREATE SINK CONNECTOR SINK_ELASTIC_dataset WITH (
    'connector.class' = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
    'connection.url' = 'http://elasticsearch:9200',
    'key.converter' = 'org.apache.kafka.connect.storage.StringConverter',
    'value.converter' = 'org.apache.kafka.connect.json.JsonConverter',
    'value.converter.schemas.enable' = 'false',
    'type.name' = '_doc',
    'topics' = 'dataset',
    'key.ignore' = 'false',
    'schema.ignore' = 'true',
    'behavior.on.null.values' = 'delete'
);

## Via command line docker

docker exec -it ksqldb-server /bin/ksql --execute "CREATE STREAM dataset (id VARCHAR, title VARCHAR, description VARCHAR, organization VARCHAR, orga_sp INT, dataset_views INT, dataset_followers INT, orga_followers INT, dataset_featured INT, concat_title_org VARCHAR, url VARCHAR, organization_id VARCHAR, dataset_reuses INT, resources_count INT, temporal_coverage_start INT, spatial_granularity INT, temporal_coverage_end INT, spatial_zones INT) WITH (KAFKA_TOPIC='dataset', PARTITIONS=1, VALUE_FORMAT='JSON');" -- http://localhost:8088

docker exec -it ksqldb-server /bin/ksql --execute "CREATE SINK CONNECTOR SINK_ELASTIC_dataset WITH (
    'connector.class' = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
    'connection.url' = 'http://elasticsearch:9200',
    'key.converter' = 'org.apache.kafka.connect.storage.StringConverter',
    'value.converter' = 'org.apache.kafka.connect.json.JsonConverter',
    'value.converter.schemas.enable' = 'false',
    'type.name' = '_doc',
    'topics' = 'dataset',
    'key.ignore' = 'false',
    'schema.ignore' = 'true',
    'behavior.on.null.values' = 'delete'
);" -- http://localhost:8088

