#!/bin/bash
docker exec -it schema-registry \
  kafka-avro-console-producer \
    --broker-list http://broker:29092 \
    --property value.schema='{ "namespace": "DataContract.Item", "type": "record", "name": "Item", "fields": [ { "name": "ID", "type": "int" }, { "name": "Name", "type": "string" }, { "name": "TypeID", "type": "int" }, { "name": "DangerID", "type": "int" } ] }' \
    --property key.schema='{ "namespace": "DataContract.Item", "type": "record", "name": "ItemKey", "fields": [ { "name": "ID", "type": "int" } ] }' \
    --property parse.key=true \
    --topic Item
