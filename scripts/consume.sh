docker exec broker kafka-console-consumer --topic Words --bootstrap-server broker:9092 --from-beginning --property print.key=true --value-deserializer org.apache.kafka.common.serialization.LongDeserializer