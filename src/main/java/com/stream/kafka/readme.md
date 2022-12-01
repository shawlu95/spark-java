## Kafka

Start kafka server using bootstrap server (no zookeeper)

```bash
# first window
bin/kafka-server-start.sh config/server.properties

# second window
bin/kafka-topics.sh --create --topic viewrecords --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

# third window (optional)
bin/kafka-topics.sh --describe --topic viewrecords --bootstrap-server localhost:9092

# third window
bin/kafka-console-consumer.sh --topic viewrecords --bootstrap-server localhost:9092
```