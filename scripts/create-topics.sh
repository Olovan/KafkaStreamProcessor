docker exec broker kafka-topics --create --topic Item --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker exec broker kafka-topics --create --topic ItemType --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker exec broker kafka-topics --create --topic ItemDanger --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker exec broker kafka-topics --create --topic FullItem --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker exec broker kafka-topics --create --topic INTERNAL_1 --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker exec broker kafka-topics --create --topic INTERNAL_2 --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
