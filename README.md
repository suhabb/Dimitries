# Dimitrios
Apache Kafka Project

Kafka Commands
---------------------

Kafka

kafka-topics --zookeeper 127.0.0.1 --topic second_topic --create --partitions 6 --replication-factor 1


kafka-console-producer --broker-list 127.0.0.1:9092 --topic st_topic --producer-property acks=all
\


kafka-console-consumer --bootstrap-server 127.0.0.1:9092 -topic first_topic


kafka-console-consumer --bootstrap-server 127.0.0.1:9092 -topic first_topic --from-beginning

kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my-first-group


kafka-consumer-groups --bootstrap-server localhost:9092 --group my-first-group --reset-offsets --to-earliest --execute --topic first_topic
———————————————————————————————————

kafka-topics.sh --create --topic inputItemTopic -zookeeper localhost:2181 --replication-factor 1 --partitions 3

kafka-topics --zookeeper 127.0.0.1 --topic inputItemTopic --create --partitions 3 --replication-factor 1

kafka-console-consumer --bootstrap-server 127.0.0.1:9092 -topic inputItemTopic --from-beginning

./kafka-topics.sh --create --topic errorTopic -zookeeper localhost:2181 --replication-factor 1 --partitions 3

kafka-topics --zookeeper 127.0.0.1 --topic errorTopic --create --partitions 3 --replication-factor 1