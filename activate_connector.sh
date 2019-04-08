#!/usr/bin/env bash

while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' localhost:8083)" != "200" ]]; do sleep 5; done

curl -X POST -H "Content-Type: application/json" localhost:8083/connectors -d @- << EOF
{
  "name": "debezium-connector-mysql",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.id": "184054",
    "database.server.name": "mysqlcdc",
    "database.whitelist": "test",
    "table.whitelist": "test.RawTransaction",
    "database.history.kafka.topic": "raw-transaction-topic",
    "database.history.kafka.bootstrap.servers": "kafka:29092",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter.schemas.enable": false,
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schemas.enable": false,
    "include.schema.changes": "true"
  }
}
EOF