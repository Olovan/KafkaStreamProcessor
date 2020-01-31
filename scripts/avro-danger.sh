#!/bin/bash
docker exec -it schema-registry \
  kafka-avro-console-producer \
    --broker-list http://broker:29092 \
    --property value.schema='{ "namespace": "DataContract.Item", "type": "record", "name": "ItemDanger", "fields": [ { "name": "ID", "type": "int" }, { "name": "Description", "type": "string" } ] }' \
    --property key.schema='{ "namespace": "DataContract.Item", "type": "record", "name": "ItemDangerKey", "fields": [ { "name": "ID", "type": "int" } ] },' \
    --property parse.key=true \
    --topic ItemDanger
